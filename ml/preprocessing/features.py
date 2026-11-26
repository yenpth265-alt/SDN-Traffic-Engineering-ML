"""Same causal feature builder used offline and online."""
import numpy as np
from ml.common import FEATURE_VERSION, numbers, require


def feature_names(lookback, flow_ids):
    return ([f"demand_lag_{i}" for i in range(1, lookback + 1)]
            + ["history_mean", "history_std", "history_min", "history_max", "history_trend"]
            + [f"flow_{fg}" for fg in flow_ids])


def build_features(history, group_ids, flow_ids, lookback):
    """history columns are oldest -> newest. lag_1 is the newest COMPLETED bin."""
    a = numbers(history, "history")
    require(a.ndim == 2 and a.shape[1] >= lookback, "Insufficient lookback")
    require(len(group_ids) == len(a), "Feature/group row mismatch")
    a = a[:, -lookback:]
    indexes = {fg: i for i, fg in enumerate(flow_ids)}
    require(all(fg in indexes for fg in group_ids), "Unknown flow ID")
    onehot = np.zeros((len(a), len(flow_ids)))
    onehot[np.arange(len(a)), [indexes[fg] for fg in group_ids]] = 1
    trend = (a[:, -1] - a[:, 0]) / max(1, lookback - 1)
    stats = np.column_stack((a.mean(1), a.std(1), a.min(1), a.max(1), trend))
    # No seed, scenario, schedule ID, future parameters or labels in features.
    return np.column_stack((a[:, ::-1], stats, onehot)).astype(np.float32)
