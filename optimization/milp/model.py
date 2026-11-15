import pulp
from objective import set_objective

def build_milp(topology, b_f, locked_groups, routing_state, config):
    F = topology['F']         # 48 nhóm lưu lượng (vd: '0x103')
    P = topology['P']         # Candidate paths {'0x103': ['0x103_P1', '0x103_P2']}
    E = topology['E']         # 16 directed fabric links (vd: 'e1>c1')
    C_e = config['C_e']       # Dung lượng liên kết (10 Mbps)
    a_fpe = topology['a_fpe'] # Ma trận incidence
    k_e = topology.get('k_e', {link: 1.0 for link in E}) # Hệ số hiệu chuẩn
    
    prob = pulp.LpProblem("SDN_Traffic_Engineering_MILP", pulp.LpMinimize)
    
    # Biến quyết định
    x = pulp.LpVariable.dicts("x", (F, pulp.Lp的可选... if needed, list(P.values()) for f in F...), cat='Binary')
    # Khai báo biến x_fp cẩn thận theo từng tập path của flow f
    x = {}
    for f in F:
        x[f] = {}
        for p in P[f]:
            x[f][p] = pulp.LpVariable(f"x_{f}_{p}", cat='Binary')
            
    y = pulp.LpVariable.dicts("y", F, cat='Binary')
    U = pulp.LpVariable("U", lowBound=0, cat='Continuous')
    
    # 1. Hàm mục tiêu
    set_objective(prob, U, y, F, config['lambda_weight'])
    
    # 2. Ràng buộc 
    for f in F:
        paths_f = P[f]
        # R1: 
        prob += pulp.lpSum([x[f][p] for p in paths_f]) == 1, f"R1_UniquePath_{f}"
        
        current_p = routing_state[f]['current_path']
        # R4: 
        prob += y[f] == 1 - x[f][current_p], f"R4_ChangeDetect_{f}"
        
        # R6: 
        if f in locked_groups:
            prob += x[f][current_p] == 1, f"R6_HoldDown_{f}"
            
    # 3. R5: 
    prob += pulp.lpSum([y[f] for f in F]) <= config['B'], "R5_Budget"
    
    # 4. R2 & R3:
    for e in E:
        L_e = k_e.get(e, 1.0) * pulp.lpSum([
            b_f[f] * a_fpe[f][p][e] * x[f][p] 
            for f in F for p in P[f]
        ])
        prob += L_e <= U * C_e, f"R3_Minimax_{e}"
        
    return prob, x, y, U