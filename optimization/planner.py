import inspect
from collections import deque
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from optimization.config_loader import compute_config_hash
from optimization.input_validator import (
    normalize_routing_entries,
    resolve_flow_path_id,
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
    if not merged_config.get("config_hash"):
        merged_config["config_hash"] = compute_config_hash(merged_config)

    ref_now = now_utc.astimezone(timezone.utc) if now_utc is not None else datetime.now(timezone.utc)
    effective_tick_id = tick_id if tick_id is not None else forecast_data.get("tick_id")
    effective_tick_at = tick_at_utc if tick_at_utc is not None else forecast_data.get("tick_at_utc")
    F = topology["F"]
    routes = normalize_routing_entries(routing_state)

    state_ok, state_status, state_msg = validate_routing_state(routing_state, F, topology["P"])
    if not state_ok:
        return {"status": state_status, "reason": state_msg}

    effective_deadline = resolve_apply_deadline_utc(
        forecast_data, merged_config, ref_now, override_deadline=apply_deadline_utc
    )
    fc_ok, fc_status, fc_msg = validate_forecast(
        forecast_data,
        F,
        deadline_ms=int(merged_config.get("deadline_ms", 200)),
        apply_guard_ms=int(merged_config.get("apply_guard_ms", 200)),
        now_utc=ref_now,
        apply_deadline_utc=effective_deadline,
    )
    if not fc_ok:
        return {"status": fc_status, "reason": fc_msg}

    if not check_trigger(U_current_history, float(merged_config["TH"]), int(merged_config["N"])):
        return {"status": "NO_TRIGGER"}

    b_f = calculate_mean_demand(forecast_data, F)
    default_ce = float(merged_config.get("C_e", 100.0))
    U_old = evaluate_routing_utilization(topology, b_f, routes, default_ce=default_ce)
    J_old = U_old

    state_ver = int(routing_state.get("state_version", routing_state.get("routing_version", 1)))
    run_id = str(forecast_data.get("run_id") or routing_state.get("run_id") or "run_default")
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
        return _build_kept_result(f"Solver status: {status}", solver_status=status, solve_ms=solve_ms)

    n_changes = sum(y_vals.values())
    if not check_hysteresis(J_old, J_new, n_changes, float(merged_config["h"])):
        return _build_kept_result(
            "NO_IMPROVEMENT",
            solver_status=status,
            u_after=U_new,
            j_after=J_new,
            solve_ms=solve_ms,
        )

    selected_paths: dict[str, str] = {}
    for f in F:
        chosen = current_selected[f]
        for p, val in x_vals.get(f, {}).items():
            if int(round(val)) == 1:
                chosen = p
                break
        selected_paths[f] = chosen

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
        n_win = max(1, int(self.config.get("N", 3)))
        self._u_history: deque[float] = deque(maxlen=n_win)
        self._hold_down_overrides: dict[str, str] = {}

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
        elif res["status"] in (
            "STATE_UNKNOWN",
            "STATE_INCOMPLETE",
            "DATA_STALE",
            "FORECAST_INVALID",
            "FORECAST_NOT_READY",
            "INVALID_FORECAST_OFFSET",
            "INVALID_FORECAST_WINDOW",
        ):
            self._u_history.clear()
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