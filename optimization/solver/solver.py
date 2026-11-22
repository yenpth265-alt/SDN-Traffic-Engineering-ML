import time
from typing import Any, Mapping
import pulp


def _build_solver(time_limit_s: float):
    """Khởi tạo bộ giải PuLP nếu môi trường có sẵn (PuLP 2.x/3.x CBC hoặc PuLP 4.x + HiGHS)."""
    if hasattr(pulp, "PULP_CBC_CMD"):
        return pulp.PULP_CBC_CMD(timeLimit=time_limit_s, msg=False)

    candidates: list[str] = []
    if hasattr(pulp, "listSolvers"):
        try:
            avail = pulp.listSolvers(onlyAvailable=True)
            if avail:
                candidates.extend(avail)
        except Exception:
            pass

    for default_name in ("HIGHS", "HIGHS_CMD", "PULP_CBC_CMD", "CBC"):
        if default_name not in candidates:
            candidates.append(default_name)

    if hasattr(pulp, "getSolver"):
        for name in candidates:
            for kwargs in (
                {"timeLimit": time_limit_s, "msg": False},
                {"time_limit": time_limit_s, "msg": False},
                {"msg": False},
                {},
            ):
                try:
                    solver = pulp.getSolver(name, **kwargs)
                    if solver is not None:
                        return solver
                except TypeError:
                    continue
                except Exception:
                    break

    return None


def _read_var_float(var) -> float | None:
    val = getattr(var, "varValue", None)
    if val is None and hasattr(var, "value"):
        try:
            val = var.value()
        except Exception:
            val = None
    if val is None:
        try:
            val = pulp.value(var)
        except Exception:
            val = None
    return float(val) if val is not None else None


def _read_status(prob) -> str:
    raw = prob.status
    if isinstance(raw, str):
        return raw
    if isinstance(pulp.LpStatus, Mapping):
        return pulp.LpStatus.get(raw, str(raw))
    return getattr(raw, "name", str(raw))


def _solve_exact_branch_and_bound(
    topology: Mapping[str, Any],
    b_f: Mapping[str, float],
    routing_state: Mapping[str, Mapping[str, Any]],
    locked_groups: set[str] | list[str] | tuple[str, ...],
    config: Mapping[str, Any],
    start_time: float,
    time_limit_s: float,
) -> tuple[str, float, float, dict[str, int], dict[str, dict[str, int]], float]:
    """
    Bộ giải nhánh-và-cận (Exact Branch-and-Bound) tìm nghiệm tối ưu toàn cục cho bài toán MILP:
    min U + lambda * sum(y_f) với ràng buộc R1, R3, R4, R5 (sum y_f <= B), R6 (hold-down).
    """
    F: list[str] = list(topology["F"])
    P: Mapping[str, list[str]] = topology["P"]
    E: list[str] = list(topology["E"])
    default_ce = float(config.get("C_e", 100.0))
    C_map: Mapping[str, float] = topology.get("C", {})
    a_fpe = topology["a_fpe"]
    k_e = topology.get("k_e", {})

    B = int(config.get("B", 4))
    lam = float(config.get("lambda_weight", 0.01))
    locked_set = set(locked_groups)

    num_e = len(E)
    e_index = {e: idx for idx, e in enumerate(E)}
    cap = [float(C_map.get(e, default_ce)) for e in E]
    k_vec = [float(k_e.get(e, 1.0)) for e in E]

    # Cấu hình tuyến ban đầu của từng nhóm luồng
    cur_path: dict[str, str] = {}
    loads = [0.0] * num_e
    flow_path_weights: dict[str, dict[str, list[tuple[int, float]]]] = {}

    for f in F:
        p0 = routing_state[f].get("current_path") or routing_state[f].get("actual_path_id")
        cur_path[f] = p0
        flow_path_weights[f] = {}
        demand = float(b_f[f])
        for p in P[f]:
            w_list: list[tuple[int, float]] = []
            for e in E:
                if a_fpe[f][p][e] == 1:
                    idx = e_index[e]
                    w = k_vec[idx] * demand
                    w_list.append((idx, w))
                    if p == p0:
                        loads[idx] += w
            flow_path_weights[f][p] = w_list

    def calc_u(cur_loads: list[float]) -> float:
        m = 0.0
        for i in range(num_e):
            if cap[i] > 0:
                u = cur_loads[i] / cap[i]
                if u > m:
                    m = u
        return m

    best_U = calc_u(loads)
    best_changes = 0
    best_J = best_U
    best_choice: dict[str, str] = dict(cur_path)

    # Danh sách ứng viên được phép đổi tuyến (không bị hold-down và có >= 2 tuyến)
    candidates: list[tuple[float, str, str, list[str]]] = []
    for f in F:
        if f in locked_set:
            continue
        alt_paths = [p for p in P[f] if p != cur_path[f]]
        if not alt_paths:
            continue
        # Ưu tiên duyệt các luồng đang đè nặng lên các liên kết tải cao nhất
        impact = max((loads[i] / cap[i] for i, _ in flow_path_weights[f][cur_path[f]] if cap[i] > 0), default=0.0)
        candidates.append((impact, f, cur_path[f], alt_paths))

    candidates.sort(key=lambda item: item[0], reverse=True)
    n_cand = len(candidates)

    # Mức giảm tải tối đa mỗi bước đổi tuyến có thể mang lại trên từng liên kết e (dùng để cắt nhánh)
    max_drop_on_link = [[0.0] * n_cand for _ in range(num_e)]
    for c_idx, (_, f, p0, alt_paths) in enumerate(candidates):
        w0 = dict(flow_path_weights[f][p0])
        for i, w_old in w0.items():
            best_after = min(dict(flow_path_weights[f][p_alt]).get(i, 0.0) for p_alt in alt_paths)
            if w_old > best_after:
                max_drop_on_link[i][c_idx] = w_old - best_after

    suffix_drops: list[list[list[float]]] = [[[] for _ in range(n_cand + 1)] for _ in range(num_e)]
    for i in range(num_e):
        for pos in range(n_cand + 1):
            arr = sorted(max_drop_on_link[i][pos:], reverse=True)
            suffix_drops[i][pos] = arr

    cur_choice = dict(cur_path)
    timed_out = False

    def dfs(pos: int, used: int) -> None:
        nonlocal best_U, best_changes, best_J, best_choice, timed_out
        if timed_out:
            return
        if (time.time() - start_time) > time_limit_s:
            timed_out = True
            return

        u_now = calc_u(loads)
        j_now = u_now + lam * used
        if j_now < best_J - 1e-12:
            best_U = u_now
            best_changes = used
            best_J = j_now
            best_choice = dict(cur_choice)

        rem_budget = B - used
        if rem_budget <= 0 or pos >= n_cand:
            return

        # Cận dưới của hàm mục tiêu từ vị trí pos trở đi
        lb_u = 0.0
        for i in range(num_e):
            if cap[i] <= 0:
                continue
            drops = suffix_drops[i][pos]
            max_possible_drop = sum(drops[:rem_budget])
            min_u_i = max(0.0, loads[i] - max_possible_drop) / cap[i]
            if min_u_i > lb_u:
                lb_u = min_u_i

        if lb_u + lam * (used + 1) >= best_J - 1e-12:
            return

        _, f, p0, alt_paths = candidates[pos]
        w0 = flow_path_weights[f][p0]

        # Nhánh 1: Đổi luồng f sang tuyến ứng viên p_alt
        for p_alt in alt_paths:
            w_alt = flow_path_weights[f][p_alt]
            for i, w in w0:
                loads[i] -= w
            for i, w in w_alt:
                loads[i] += w
            cur_choice[f] = p_alt

            dfs(pos + 1, used + 1)

            cur_choice[f] = p0
            for i, w in w_alt:
                loads[i] -= w
            for i, w in w0:
                loads[i] += w

        # Nhánh 2: Giữ nguyên luồng f
        dfs(pos + 1, used)

    dfs(0, 0)

    y_vals = {f: (1 if best_choice[f] != cur_path[f] else 0) for f in F}
    x_vals = {f: {p: (1 if p == best_choice[f] else 0) for p in P[f]} for f in F}
    solve_ms = (time.time() - start_time) * 1000.0
    status = "Feasible" if timed_out else "Optimal"
    return status, best_U, best_J, y_vals, x_vals, solve_ms


def solve_model(
    prob,
    x,
    y,
    U,
    F,
    P_dict,
    deadline_ms,
    topology: Mapping[str, Any] | None = None,
    b_f: Mapping[str, float] | None = None,
    routing_state: Mapping[str, Mapping[str, Any]] | None = None,
    locked_groups: set[str] | list[str] | tuple[str, ...] | None = None,
    config: Mapping[str, Any] | None = None,
):
    """Gọi bộ giải MILP qua PuLP (nếu có solver) và tự động fallback sang Branch-and-Bound chính xác."""
    start_time = time.time()
    time_limit_s = max(0.05, float(deadline_ms) / 1000.0)

    try:
        solver = _build_solver(time_limit_s)
        if solver is not None:
            prob.solve(solver)
        else:
            prob.solve()

        solve_ms = (time.time() - start_time) * 1000.0
        status = _read_status(prob)

        if status in ["Optimal", "Feasible"]:
            u_raw = _read_var_float(U)
            U_val = u_raw if u_raw is not None else 0.0
            obj_raw = _read_var_float(prob.objective)
            J_val = obj_raw if obj_raw is not None else 0.0

            y_vals: dict[str, int] = {}
            x_vals: dict[str, dict[str, int]] = {}
            for f in F:
                y_raw = _read_var_float(y[f])
                y_vals[f] = int(round(y_raw)) if y_raw is not None else 0
                x_vals[f] = {}
                for p in P_dict[f]:
                    x_raw = _read_var_float(x[f][p])
                    x_vals[f][p] = int(round(x_raw)) if x_raw is not None else 0

            return status, U_val, J_val, y_vals, x_vals, solve_ms
    except Exception:
        pass

    if topology is not None and b_f is not None and routing_state is not None and config is not None:
        return _solve_exact_branch_and_bound(
            topology=topology,
            b_f=b_f,
            routing_state=routing_state,
            locked_groups=locked_groups or set(),
            config=config,
            start_time=start_time,
            time_limit_s=time_limit_s,
        )

    solve_ms = (time.time() - start_time) * 1000.0
    return "NoSolver", 0.0, 0.0, {}, {}, solve_ms