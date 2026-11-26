import numpy as np
from ml.common import numbers, require


def calibrate(raw, epsilon):
    raw = numbers(raw, "raw prediction", nonnegative=False)
    require(np.isfinite(epsilon) and epsilon >= 0, "Invalid epsilon")
    return numbers(np.maximum(0, (1 + epsilon) * raw), "final prediction")


def choose_epsilon(y, raw, candidates):
    require(0 in candidates, "epsilon candidates must include zero")
    scores = [(float(np.mean(np.abs(calibrate(raw, e) - y))), e)
              for e in sorted(set(candidates))]
    # Exact/near ties choose the smaller epsilon; zero wins against no improvement.
    best = scores[0]
    for score in scores[1:]:
        if score[0] < best[0] - 1e-12:
            best = score
    return best[1], best[0]


def errors(y, prediction):
    y, p = numbers(y, "target"), numbers(prediction, "prediction")
    require(y.ndim == 2 and y.shape == p.shape and len(y), "Metric shape mismatch")
    e = p - y
    mean_error = e.mean(axis=1)
    return {
        "samples": len(y), "horizon": y.shape[1],
        "pooled_mae_mbps": float(np.abs(e).mean()),
        "pooled_rmse_mbps": float(np.sqrt(np.square(e).mean())),
        "per_step_mae_mbps": np.abs(e).mean(0).tolist(),
        "per_step_rmse_mbps": np.sqrt(np.square(e).mean(0)).tolist(),
        "mean_h_mae_mbps": float(np.abs(mean_error).mean()),
        "mean_h_rmse_mbps": float(np.sqrt(np.square(mean_error).mean())),
        "bias_mbps": float(e.mean()),
        "underprediction_fraction": float((e < 0).mean()),
    }
