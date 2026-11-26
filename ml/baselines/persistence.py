import numpy as np
from ml.common import numbers, require


def predict(history, horizon):
    a = numbers(history, "history")
    require(a.ndim == 2 and a.shape[1] >= 1 and horizon >= 1, "Invalid baseline shape")
    return np.repeat(a[:, -1:], horizon, axis=1)
