import pulp

def set_objective(prob, U, y, F, lambda_weight):
    """Thiết lập hàm mục tiêu: min J = U + (lambda / |F|) * sum(y_f)"""
    prob += U + (lambda_weight / len(F)) * pulp.lpSum([y[f] for f in F]), "Objective" 