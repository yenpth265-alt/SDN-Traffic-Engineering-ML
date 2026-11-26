import numpy as np
from ml.common import numbers, require


def predict(history, horizon, window):
    a = numbers(history, "history")
    require(a.ndim == 2 and 1 <= window <= a.shape[1], "MA requires 1 <= W <= L")
    # Core bins are contiguous and exactly one second, hence equal duration.
    return np.repeat(a[:, -window:].mean(1, keepdims=True), horizon, axis=1)
