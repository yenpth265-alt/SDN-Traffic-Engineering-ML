import time
import pulp

def solve_model(prob, x, y, U, F, P_dict, deadline_ms):
    """Gọi bộ giải CBC với giới hạn thời gian thực thi (deadline)."""
    start_time = time.time()

    solver = pulp.PULP_CBC_CMD(timeLimit=deadline_ms / 1000.0, msg=False)
    prob.solve(solver)

    solve_ms = (time.time() - start_time) * 1000
    status = pulp.LpStatus[prob.status]

    y_vals, x_vals = {}, {}
    U_val, J_val = 0.0, 0.0

    if status in ['Optimal', 'Feasible']:
        U_val = float(pulp.value(U))
        J_val = float(pulp.value(prob.objective))
        for f in F:
            y_vals[f] = int(y[f].varValue) if y[f].varValue is not None else 0
            x_vals[f] = {}
            for p in P_dict[f]:
                val = x[f][p].varValue
                x_vals[f][p] = int(val) if val is not None else 0

    return status, U_val, J_val, y_vals, x_vals, solve_ms