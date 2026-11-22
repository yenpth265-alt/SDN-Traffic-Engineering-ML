import pulp
from .objective import set_objective


def build_milp(topology, b_f, locked_groups, routing_state, config):
    F = topology["F"]
    P = topology["P"]
    E = topology["E"]
    default_ce = float(config.get("C_e", 100.0))
    C_map = topology.get("C", {})
    a_fpe = topology["a_fpe"]
    k_e = topology.get("k_e", {link: 1.0 for link in E})

    prob = pulp.LpProblem("SDN_Traffic_Engineering_MILP", pulp.LpMinimize)

    x = {}
    for f in F:
        x[f] = {}
        for p in P[f]:
            x[f][p] = pulp.LpVariable(f"x_{f}_{p}", cat="Binary")

    y = pulp.LpVariable.dicts("y", F, cat="Binary")
    U = pulp.LpVariable("U", lowBound=0, cat="Continuous")

    set_objective(prob, U, y, F, float(config.get("lambda_weight", 0.01)))

    for f in F:
        paths_f = P[f]
        # R1: Mỗi nhóm luồng chọn đúng 1 tuyến ứng viên
        prob += pulp.lpSum([x[f][p] for p in paths_f]) == 1, f"R1_UniquePath_{f}"

        # R4: Chỉ báo đổi tuyến y_f = 1 - x_{f, p_old}
        current_p = routing_state[f].get("current_path") or routing_state[f].get("actual_path_id")
        if current_p not in x[f]:
            raise ValueError(f"Tuyến hiện tại {current_p} của luồng {f} không nằm trong tập ứng viên {paths_f}")
        prob += y[f] == 1 - x[f][current_p], f"R4_ChangeDetect_{f}"

        # R6: Khóa tuyến trong thời gian Hold-down
        if f in locked_groups:
            prob += x[f][current_p] == 1, f"R6_HoldDown_{f}"

    # R5: Ngân sách số nhóm luồng tối đa được phép chuyển hướng trong 1 đợt
    prob += pulp.lpSum([y[f] for f in F]) <= int(config.get("B", 4)), "R5_Budget"

    # R3: Tải cực đại chuẩn hóa trên từng liên kết có hướng e
    for e in E:
        cap_e = float(C_map.get(e, default_ce))
        if cap_e <= 0:
            raise ValueError(f"Dung lượng liên kết {e} không hợp lệ: {cap_e}")
        L_e = float(k_e.get(e, 1.0)) * pulp.lpSum(
            [b_f[f] * a_fpe[f][p][e] * x[f][p] for f in F for p in P[f]]
        )
        prob += L_e <= U * cap_e, f"R3_Minimax_{e}"

    return prob, x, y, U