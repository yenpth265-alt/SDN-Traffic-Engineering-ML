import inspect
import os
import json
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from optimization.config_loader import compute_config_hash
from optimization.input_validator import (
    _parse_iso_utc,
    normalize_routing_entries,
    resolve_flow_path_id,
    validate_context_binding,
    validate_forecast,
    validate_routing_state,
)
from optimization.load_estimator import (
    calculate_mean_demand,
    evaluate_routing_utilization,
    load_topology_from_configs,
)
from optimization.milp.model import build_milp
from optimization.result_serializer import (
    _to_rfc3339_utc,
    resolve_apply_deadline_utc,
    resolve_decision_batch_id,
    serialize_apply_request,
    serialize_decision_batch,
)
from optimization.routing_policies.solution_validator import check_hysteresis, get_locked_groups
from optimization.routing_policies.trigger import check_trigger
from optimization.solver.solver import solve_model


# Mã reason cố định cho DecisionBatch/control tick (interfaces_v2 §12, Guide v1.4 §13.4).
# Data lưu reason dạng ID (<=256 ký tự) nên không ghép thông điệp tự do vào reason.
REASON_NO_SOLUTION = "NO_SOLUTION"
REASON_INVALID_RESULT = "INVALID_RESULT"

# Các status làm mất tính liên tục của chuỗi U_current: reset trigger counter.
_HISTORY_RESET_STATUSES = frozenset(
    {
        "STATE_UNKNOWN",
        "STATE_INCOMPLETE",
        "INVALID_STATE_VERSION",
        "PATH_MAPPING_ERROR",
        "DATA_STALE",
        "FORECAST_INVALID",
        "FORECAST_NOT_READY",
        "INVALID_FORECAST",
        "INVALID_FORECAST_OFFSET",
        "INVALID_FORECAST_WINDOW",
        "CONTEXT_MISMATCH",
        "INVALID_CONTEXT",
        "MISSING_CONTEXT",
        "INVALID_TIMESTAMP",
        "INVALID_DEADLINE",
    }
)


def _trigger_evidence(history: Sequence[float], th: float, n: int) -> dict[str, Any]:
    window = [float(u) for u in history][-max(n, 0):] if n > 0 else []
    consecutive = 0
    for u in reversed([float(v) for v in history]):
        if u > th:
            consecutive += 1
        else:
            break
    return {
        "U_current": round(float(history[-1]), 6) if history else None,
        "consecutive_above_TH": consecutive,
        "TH": th,
        "N": n,
        "window": [round(u, 6) for u in window],
    }


def _split_error_code(exc: ValueError, default_code: str = "INVALID_CONTEXT") -> tuple[str, str]:
    msg = str(exc).strip()
    if ":" in msg:
        prefix, rest = msg.split(":", 1)
        code = prefix.strip()
        if code.isupper() and "_" in code:
            return code, rest.strip()
    return default_code, msg


def _resolve_locked_groups(
    routes: Mapping[str, Mapping[str, Any]],
    t_hold: float,
    now_utc: datetime,
    hold_down_overrides: Mapping[str, Any] | None = None,
) -> set[str]:
    effective_routes = {fg: dict(info) for fg, info in routes.items()}
    if hold_down_overrides:
        for fg, ts_val in hold_down_overrides.items():
            if fg in effective_routes and ts_val:
                effective_routes[fg]["last_successful_change_at"] = ts_val

    sig = inspect.signature(get_locked_groups)
    if "now_utc" in sig.parameters:
        locked = get_locked_groups(effective_routes, t_hold, now_utc=now_utc)
    else:
        locked = get_locked_groups(effective_routes, t_hold)
    return set(locked)


def validate_post_solve_solution(
    *,
    topology: Mapping[str, Any],
    b_f: Mapping[str, float],
    current_selected: Mapping[str, str],
    x_vals: Mapping[str, Mapping[str, Any]],
    y_vals: Mapping[str, Any],
    locked_groups: set[str],
    config: Mapping[str, Any],
    U_reported: float,
    J_reported: float,
    apply_deadline_utc: str,
    now_utc: datetime,
    solve_ms: float = 0.0,
    prob: Any = None,
    x_vars: Mapping[str, Mapping[str, Any]] | None = None,
    y_vars: Mapping[str, Any] | None = None,
    u_var: Any = None,
) -> tuple[bool, str, dict[str, str]]:
    """Bộ kiểm tra khả thi độc lập hậu solver trước khi serialize (CF-15)."""
    F = topology["F"]
    P = topology["P"]
    E = topology["E"]
    C_map = topology.get("C", {})
    default_ce = float(config.get("C_e", 100.0))
    a_fpe = topology["a_fpe"]
    k_e = topology.get("k_e", {})
    budget_b = int(config.get("B", 4))

    selected_paths: dict[str, str] = {}
    actual_changes = 0

    for f in F:
        row_x = x_vals.get(f, {})
        ones: list[str] = []
        for p in P[f]:
            raw_x = row_x.get(p, 0)
            if isinstance(raw_x, bool) or not isinstance(raw_x, (int, float)):
                return False, f"Biến x[{f}][{p}] sai kiểu: {raw_x!r}", {}
            rounded_x = int(round(float(raw_x)))
            if abs(float(raw_x) - rounded_x) > 1e-5 or rounded_x not in (0, 1):
                return False, f"Biến x[{f}][{p}] không nhị phân: {raw_x!r}", {}
            if rounded_x == 1:
                ones.append(p)

        if len(ones) != 1:
            return False, f"Vi phạm ràng buộc chọn đúng 1 tuyến cho flow {f}: {ones}", {}

        chosen_p = ones[0]
        selected_paths[f] = chosen_p
        changed = 1 if chosen_p != current_selected[f] else 0

        raw_y = y_vals.get(f, 0)
        if isinstance(raw_y, bool) or not isinstance(raw_y, (int, float)):
            return False, f"Biến y[{f}] sai kiểu: {raw_y!r}", {}
        rounded_y = int(round(float(raw_y)))
        if abs(float(raw_y) - rounded_y) > 1e-5 or rounded_y not in (0, 1):
            return False, f"Biến y[{f}] không nhị phân: {raw_y!r}", {}
        if rounded_y != changed:
            return False, f"Biến y[{f}]={rounded_y} lệch với chuyển tuyến thực tế ({changed})", {}

        if f in locked_groups and changed == 1:
            return False, f"Vi phạm hold-down lock trên flow {f}", {}

        actual_changes += changed

    if actual_changes > budget_b:
        return False, f"Số luồng đổi tuyến ({actual_changes}) vượt ngân sách B={budget_b}", {}

    max_u = 0.0
    for e in E:
        cap = float(C_map.get(e, default_ce))
        if cap <= 0:
            return False, f"Dung lượng link {e} không hợp lệ: {cap}", {}
        load_e = sum(float(b_f[f]) for f in F if a_fpe[f][selected_paths[f]][e] == 1)
        u_e = (float(k_e.get(e, 1.0)) * load_e) / cap
        if u_e > max_u:
            max_u = u_e

    if abs(max_u - float(U_reported)) > 1e-4:
        return False, f"Sai lệch mức tải tối đa U: tính lại={max_u:.6f} != solver={U_reported}", {}

    if prob is not None and getattr(prob, "objective", None) is not None and x_vars and y_vars and u_var is not None:
        var_values: dict[str, float] = {u_var.name: max_u}
        for f in F:
            var_values[y_vars[f].name] = float(1 if selected_paths[f] != current_selected[f] else 0)
            for p in P[f]:
                var_values[x_vars[f][p].name] = float(1 if selected_paths[f] == p else 0)
        j_calc = float(getattr(prob.objective, "constant", 0.0) or 0.0)
        for var, coeff in prob.objective.items():
            if var.name in var_values:
                j_calc += float(coeff) * var_values[var.name]
        if abs(j_calc - float(J_reported)) > 1e-4:
            return False, f"Sai lệch hàm mục tiêu J: tính lại={j_calc:.6f} != solver={J_reported}", {}
    elif float(J_reported) < max_u - 1e-6:
        return False, f"Hàm mục tiêu J ({J_reported}) nhỏ hơn mức tải max_u ({max_u:.6f})", {}

    dt_deadline, err_dl = _parse_iso_utc(apply_deadline_utc, "apply_deadline_utc")
    if dt_deadline is None:
        return False, err_dl, {}
    dt_after_solve = now_utc + timedelta(milliseconds=max(0.0, float(solve_ms)))
    rem_ms = (dt_deadline - dt_after_solve).total_seconds() * 1000.0
    if rem_ms < float(config.get("apply_guard_ms", 200)):
        return False, f"Hết quỹ thời gian apply sau khi giải ({rem_ms:.1f}ms)", {}

    return True, "OK", selected_paths


def run_planner(
    forecast_data: Mapping[str, Any],
    routing_state: Mapping[str, Any],
    U_current_history: Sequence[float],
    config: Mapping[str, Any],
    topology_override: Mapping[str, Any] | None = None,
    now_utc: datetime | None = None,
    decision_batch_id: str | None = None,
    apply_deadline_utc: str | None = None,
    hold_down_overrides: Mapping[str, Any] | None = None,
    tick_id: int | str | None = None,
    tick_at_utc: str | None = None,
) -> dict[str, Any]:
    topology = (
        dict(topology_override)
        if topology_override is not None
        else load_topology_from_configs(config.get("network_config_dir", "configs/network"))
    )
    if not topology:
        return {"status": "ERROR", "reason": "Topology data missing"}

    merged_config = dict(config)
    merged_config.setdefault("topology_version", topology.get("topology_version", "1.0"))
    if topology.get("topology_hash") and not merged_config.get("topology_hash"):
        merged_config["topology_hash"] = topology["topology_hash"]
    merged_config.setdefault("optimizer_config_hash", compute_config_hash(merged_config))

    effective_tick_id = tick_id if tick_id is not None else forecast_data.get("tick_id")
    effective_tick_at = tick_at_utc if tick_at_utc is not None else forecast_data.get("tick_at_utc")

    # CF-13 & CF-14: Kiểm soát timestamp và không tự bịa đồng hồ hệ thống nếu thiếu ngữ cảnh online
    if now_utc is not None:
        ref_now, err_now = _parse_iso_utc(now_utc, "now_utc")
        if ref_now is None:
            return {"status": "INVALID_TIMESTAMP", "reason": err_now}
    elif effective_tick_at is not None:
        ref_now, err_tick = _parse_iso_utc(effective_tick_at, "tick_at_utc")
        if ref_now is None:
            return {"status": "INVALID_TIMESTAMP", "reason": err_tick}
    else:
        return {"status": "MISSING_CONTEXT", "reason": "Thiếu now_utc hoặc tick_at_utc cho chu kỳ tối ưu online"}

    if effective_tick_at is not None:
        _, err_tick_fmt = _parse_iso_utc(effective_tick_at, "tick_at_utc")
        if err_tick_fmt:
            return {"status": "INVALID_TIMESTAMP", "reason": err_tick_fmt}

    F = topology["F"]
    if merged_config.get("ml_deployment_config"):
        from ml.deployment import read_deployment
        from optimization.ml_contract import normalize_forecast
        try:
            deployment = read_deployment(merged_config["ml_deployment_config"])
            merged_config["H"] = deployment["horizon_H"]
            forecast_data = normalize_forecast(forecast_data, F, merged_config["ml_deployment_config"])
        except (ValueError, KeyError, TypeError, OSError) as exc:
            return {"status": "INVALID_FORECAST", "reason": str(exc)}
    routes = normalize_routing_entries(routing_state)

    # CF-05 & CF-11: Đối chiếu ràng buộc run_id, snapshot_id, topology/config version & hash
    ctx_ok, ctx_status, ctx_msg = validate_context_binding(forecast_data, routing_state, topology, merged_config)
    if not ctx_ok:
        return {"status": ctx_status, "reason": ctx_msg}

    state_ok, state_status, state_msg = validate_routing_state(routing_state, F, topology["P"])
    if not state_ok:
        return {"status": state_status, "reason": state_msg}

    # CF-13 & CF-14: Không tự bịa deadline khi thiếu apply_deadline_utc
    try:
        effective_deadline = resolve_apply_deadline_utc(
            forecast_data, merged_config, ref_now, override_deadline=apply_deadline_utc
        )
    except ValueError as exc:
        err_code, err_msg = _split_error_code(exc, "MISSING_CONTEXT")
        return {"status": err_code, "reason": err_msg}

    # CF-12: Ép kiểm tra Horizon H khớp cấu hình model/run
    expected_h = int(merged_config.get("H", 1))
    fc_ok, fc_status, fc_msg = validate_forecast(
        forecast_data,
        F,
        deadline_ms=int(merged_config.get("deadline_ms", 200)),
        apply_guard_ms=int(merged_config.get("apply_guard_ms", 200)),
        now_utc=ref_now,
        apply_deadline_utc=effective_deadline,
        expected_horizon=expected_h,
    )
    if not fc_ok:
        return {"status": fc_status, "reason": fc_msg}

    trigger_th = float(merged_config["TH"])
    trigger_n = int(merged_config["N"])
    if not check_trigger(U_current_history, trigger_th, trigger_n):
        # Guide v1.4 §13.4: NO_TRIGGER phải lưu U_current và trigger counter, không gọi SDN.
        return {
            "status": "NO_TRIGGER",
            "reason": "NO_TRIGGER",
            "trigger": _trigger_evidence(U_current_history, trigger_th, trigger_n),
        }

    b_f = calculate_mean_demand(forecast_data, F)
    default_ce = float(merged_config.get("C_e", 100.0))
    U_old = evaluate_routing_utilization(topology, b_f, routes, default_ce=default_ce)
    J_old = U_old

    raw_state_ver = (
        routing_state.get("state_version")
        if routing_state.get("state_version") is not None
        else routing_state["routing_version"]
    )
    state_ver = int(raw_state_ver)
    run_id = forecast_data["run_id"]
    batch_id = resolve_decision_batch_id(decision_batch_id, run_id, ref_now, tick_id=effective_tick_id)
    locked_groups = _resolve_locked_groups(
        routes=routes,
        t_hold=float(merged_config["T_hold"]),
        now_utc=ref_now,
        hold_down_overrides=hold_down_overrides,
    )

    current_selected = {f: str(resolve_flow_path_id(routes[f]) or "") for f in F}

    def _build_kept_result(
        reason_code: str,
        solver_status: str = "NotRun",
        u_after: float | None = None,
        j_after: float | None = None,
        solve_ms: float = 0.0,
    ) -> dict[str, Any]:
        u_fin = U_old if u_after is None else u_after
        j_fin = J_old if j_after is None else j_after
        try:
            decision_batch = serialize_decision_batch(
                decision_batch_id=batch_id,
                topology=topology,
                routing_state=routes,
                selected_paths=current_selected,
                locked_groups=locked_groups,
                b_f=b_f,
                forecast_data=forecast_data,
                state_version=state_ver,
                config=merged_config,
                apply_deadline_utc=effective_deadline,
                solver_metrics={
                    "status": solver_status,
                    "U_before": round(U_old, 6),
                    "U_after": round(u_fin, 6),
                    "J_before": round(J_old, 6),
                    "J_after": round(j_fin, 6),
                    "solve_ms": round(solve_ms, 3),
                },
                reason=reason_code,
                tick_id=effective_tick_id,
                tick_at_utc=effective_tick_at,
                now_utc=ref_now,
            )
        except ValueError as exc:
            err_code, err_msg = _split_error_code(exc, "INVALID_CONTEXT")
            return {"status": err_code, "reason": err_msg}

        out: dict[str, Any] = {
            "status": "KEPT",
            "reason": reason_code,
            "U_old": round(U_old, 4),
            "decision_batch": decision_batch,
            "apply_request": None,
        }
        if u_after is not None:
            out["U_new"] = round(u_after, 4)
            out["J_old"] = round(J_old, 4)
            out["J_new"] = round(j_fin, 4)
        return out

    if U_old <= float(merged_config["TH"]):
        return _build_kept_result("FORECAST_BELOW_THRESHOLD")

    if int(merged_config.get("B", 4)) <= 0:
        return _build_kept_result("BUDGET_ZERO")

    prob, x, y, U = build_milp(topology, b_f, locked_groups, routes, merged_config)
    status, U_new, J_new, y_vals, x_vals, solve_ms = solve_model(
        prob,
        x,
        y,
        U,
        F,
        topology["P"],
        int(merged_config["deadline_ms"]),
        topology=topology,
        b_f=b_f,
        routing_state=routes,
        locked_groups=locked_groups,
        config=merged_config,
    )

    if status not in ["Optimal", "Feasible"]:
        # Reason là mã cố định; trạng thái gốc của solver nằm trong solver_status.
        return _build_kept_result(REASON_NO_SOLUTION, solver_status=status, solve_ms=solve_ms)

    # CF-15: Hậu kiểm nghiệm MILP độc lập trước khi kiểm tra hysteresis và serialize
    post_ok, post_msg, selected_paths = validate_post_solve_solution(
        topology=topology,
        b_f=b_f,
        current_selected=current_selected,
        x_vals=x_vals,
        y_vals=y_vals,
        locked_groups=locked_groups,
        config=merged_config,
        U_reported=U_new,
        J_reported=J_new,
        apply_deadline_utc=effective_deadline,
        now_utc=ref_now,
        solve_ms=solve_ms,
        prob=prob,
        x_vars=x,
        y_vars=y,
        u_var=U,
    )
    if not post_ok:
        kept = _build_kept_result(REASON_INVALID_RESULT, solver_status=status, solve_ms=solve_ms)
        kept["reason_detail"] = post_msg
        return kept

    n_changes = sum(1 for f in F if selected_paths[f] != current_selected[f])
    if not check_hysteresis(J_old, J_new, n_changes, float(merged_config["h"])):
        return _build_kept_result(
            "NO_IMPROVEMENT",
            solver_status=status,
            u_after=U_new,
            j_after=J_new,
            solve_ms=solve_ms,
        )

    try:
        decision_batch = serialize_decision_batch(
            decision_batch_id=batch_id,
            topology=topology,
            routing_state=routes,
            selected_paths=selected_paths,
            locked_groups=locked_groups,
            b_f=b_f,
            forecast_data=forecast_data,
            state_version=state_ver,
            config=merged_config,
            apply_deadline_utc=effective_deadline,
            solver_metrics={
                "status": status,
                "U_before": round(U_old, 6),
                "U_after": round(U_new, 6),
                "J_before": round(J_old, 6),
                "J_after": round(J_new, 6),
                "solve_ms": round(solve_ms, 3),
            },
            reason="REROUTED",
            tick_id=effective_tick_id,
            tick_at_utc=effective_tick_at,
            now_utc=ref_now,
        )

        apply_req = serialize_apply_request(
            decision_batch_id=batch_id,
            decision_rows=decision_batch["rows"],
            forecast_data=forecast_data,
            state_version=state_ver,
            config=merged_config,
            apply_deadline_utc=effective_deadline,
            topology=topology,
            now_utc=ref_now,
        )
    except ValueError as exc:
        err_code, err_msg = _split_error_code(exc, "INVALID_CONTEXT")
        return {"status": err_code, "reason": err_msg}

    if apply_req is None:
        return _build_kept_result(
            "NO_EFFECTIVE_CHANGE",
            solver_status=status,
            u_after=U_new,
            j_after=J_new,
            solve_ms=solve_ms,
        )

    return {
        "status": "APPLY_REQUESTED",
        "decision_batch": decision_batch,
        "apply_request": apply_req,
    }


class OptimizationController:
    def __init__(self, config: Mapping[str, Any], topology_override: Mapping[str, Any] | None = None) -> None:
        self.config = dict(config)
        self.topology = (
            dict(topology_override)
            if topology_override is not None
            else load_topology_from_configs(self.config.get("network_config_dir", "configs/network"))
        )
        self.config.setdefault("topology_version", self.topology.get("topology_version", "1.0"))
        if self.topology.get("topology_hash") and not self.config.get("topology_hash"):
            self.config["topology_hash"] = self.topology["topology_hash"]
        self.config.setdefault("optimizer_config_hash", compute_config_hash(self.config))

        n_win = max(1, int(self.config.get("N", 3)))
        self._u_history: deque[float] = deque(maxlen=n_win)
        self._hold_down_overrides: dict[str, str] = {}
        self._audit_records: list[dict[str, Any]] = []

    def get_audit_records(self) -> list[dict[str, Any]]:
        return list(self._audit_records)

    def _emit_audit(
        self,
        res: Mapping[str, Any],
        forecast_data: Mapping[str, Any],
        routing_state: Mapping[str, Any],
        now_utc: datetime | None,
        tick_id: int | str | None,
    ) -> None:
        """Ghi nhận audit disposition/reason/context cho mọi tick kể cả NO_TRIGGER/SKIPPED (CF-16)."""
        db = res.get("decision_batch") if isinstance(res.get("decision_batch"), Mapping) else {}
        ts_dt: datetime | None = None
        if now_utc is not None:
            ts_dt, _ = _parse_iso_utc(now_utc, "now_utc")
        if ts_dt is None:
            ts_dt = datetime.now(timezone.utc)

        raw_tick = tick_id if tick_id is not None else forecast_data.get("tick_id")
        record = {
            "recorded_at_utc": _to_rfc3339_utc(ts_dt),
            "run_id": str(db.get("run_id") or forecast_data.get("run_id") or routing_state.get("run_id") or ""),
            "tick_id": str(raw_tick) if raw_tick is not None else "",
            "decision_batch_id": db.get("decision_batch_id"),
            "forecast_batch_id": forecast_data.get("forecast_batch_id") or forecast_data.get("batch_id"),
            "snapshot_id": forecast_data.get("snapshot_id") or routing_state.get("snapshot_id"),
            "state_version": routing_state.get("state_version", routing_state.get("routing_version")),
            "status": res.get("status"),
            "reason": res.get("reason", ""),
            "topology_version": self.topology.get("topology_version"),
            "topology_hash": self.topology.get("topology_hash"),
            "config_version": self.config.get("config_version"),
            # run config hash (Network/Runner) và optimizer parameter hash là hai namespace riêng.
            "config_hash": self.config.get("config_hash"),
            "optimizer_config_hash": self.config.get("optimizer_config_hash"),
        }
        if res.get("reason_detail"):
            record["reason_detail"] = res["reason_detail"]
        if isinstance(res.get("trigger"), Mapping):
            record["trigger"] = dict(res["trigger"])
        if db:
            record["solver_status"] = db.get("solver_status")
            record["changed_flows_count"] = db.get("changed_flows_count")
        self._audit_records.append(record)

        audit_path = self.config.get("audit_log_path")
        if audit_path:
            out_path = Path(str(audit_path))
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open(mode="a", encoding="utf-8") as fa:
                fa.write(json.dumps(record, ensure_ascii=False) + "\n")
                fa.flush()
                os.fsync(fa.fileno())

    def step(
        self,
        u_current: float,
        forecast_data: Mapping[str, Any],
        routing_state: Mapping[str, Any],
        now_utc: datetime | None = None,
        decision_batch_id: str | None = None,
        apply_deadline_utc: str | None = None,
        tick_id: int | str | None = None,
        tick_at_utc: str | None = None,
    ) -> dict[str, Any]:
        self._u_history.append(float(u_current))
        res = run_planner(
            forecast_data=forecast_data,
            routing_state=routing_state,
            U_current_history=list(self._u_history),
            config=self.config,
            topology_override=self.topology,
            now_utc=now_utc,
            decision_batch_id=decision_batch_id,
            apply_deadline_utc=apply_deadline_utc,
            hold_down_overrides=self._hold_down_overrides,
            tick_id=tick_id,
            tick_at_utc=tick_at_utc,
        )
        if res["status"] in ("DATA_NOT_SYNCED", "SKIPPED_IN_FLIGHT"):
            if self._u_history:
                self._u_history.pop()
        elif res["status"] in _HISTORY_RESET_STATUSES:
            self._u_history.clear()

        self._emit_audit(res, forecast_data, routing_state, now_utc, tick_id)
        return res

    def ingest_apply_result(
        self,
        apply_result: Mapping[str, Any],
        now_utc: datetime | None = None,
        published_routing_state: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Xác nhận ApplyResult đủ 48 flow sau khi RoutingState mới được công bố."""
        ts_now = now_utc.astimezone(timezone.utc) if now_utc is not None else datetime.now(timezone.utc)
        batch_status = str(apply_result.get("status", "")).upper()
        batch_completed_at = apply_result.get("completed_at")

        rows = (
            apply_result.get("results")
            or apply_result.get("flow_results")
            or apply_result.get("rows")
            or apply_result.get("decision_rows")
            or apply_result.get("changes")
            or []
        )

        expected_flows = list(self.topology.get("F", ()))
        seen_flows: set[str] = set()
        duplicate_flows: list[str] = []
        locked_updated: list[str] = []
        unverified_flows: list[str] = []

        pub_routes = (
            normalize_routing_entries(published_routing_state)
            if published_routing_state is not None
            else {}
        )

        for item in rows:
            fg = str(item.get("flow_group_id", "")).strip()
            if not fg:
                continue
            if fg in seen_flows:
                duplicate_flows.append(fg)
            seen_flows.add(fg)

            old_p = item.get("old_path_id") or item.get("old_path")
            new_p = (
                item.get("actual_path_id")
                or item.get("new_path_id")
                or item.get("proposed_path_id")
                or item.get("path_id")
                or item.get("new_path")
            )
            flow_status = str(item.get("status", "")).upper()
            ver_status = str(item.get("verification_status") or flow_status).upper()

            if flow_status in ("UNKNOWN", "FAILED", "INCONSISTENT") or ver_status in (
                "UNKNOWN",
                "FAILED",
                "UNVERIFIED",
            ):
                unverified_flows.append(fg)
                continue

            if batch_status in ("REJECTED", "FAILED", "INCONSISTENT", "ROLLED_BACK"):
                continue

            actually_changed = bool(
                item.get("changed", old_p is not None and new_p is not None and str(old_p) != str(new_p))
            )
            if actually_changed and ver_status == "VERIFIED" and flow_status in ("", "APPLIED", "VERIFIED"):
                if pub_routes and fg in pub_routes:
                    pub_status = str(
                        pub_routes[fg].get("verification_status") or pub_routes[fg].get("status") or ""
                    ).upper()
                    pub_path = resolve_flow_path_id(pub_routes[fg])
                    if pub_status != "VERIFIED" or (new_p is not None and str(pub_path) != str(new_p)):
                        unverified_flows.append(fg)
                        continue

                verified_ts = str(
                    item.get("verified_at_utc")
                    or item.get("applied_at")
                    or batch_completed_at
                    or _to_rfc3339_utc(ts_now)
                )
                self._hold_down_overrides[fg] = verified_ts
                locked_updated.append(fg)

        missing_flows = [fg for fg in expected_flows if fg not in seen_flows] if expected_flows else []
        complete_48 = (len(missing_flows) == 0 and len(duplicate_flows) == 0)

        if (
            apply_result.get("reconcile_required") is True
            or batch_status in ("INCONSISTENT", "PARTIAL", "FAILED")
            or unverified_flows
        ):
            self._u_history.clear()

        return {
            "complete_48": complete_48,
            "total_received": len(seen_flows),
            "missing_flows": missing_flows,
            "duplicate_flows": duplicate_flows,
            "locked_updated": locked_updated,
            "unverified_flows": unverified_flows,
        }
