import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

try:
    import yaml
except ImportError:
    yaml = None

DEFAULT_OPTIMIZATION_CONFIG: dict[str, Any] = {
    "config_version": "1.0",
    "topology_version": "1.0",
    "network_config_dir": "configs/network",
    "H": 1,
    "TH": 0.85,
    "N": 3,
    "h": 0.05,
    "T_hold": 10.0,
    "B": 4,
    "lambda_weight": 0.01,
    "C_e": 100.0,
    "deadline_ms": 200,
    "apply_guard_ms": 200,
}

_RUNTIME_EXCLUDED_KEYS = frozenset(
    {
        "config_hash",
        "topology_hash",
        "run_id",
        "audit_log_path",
        "network_config_dir",
        "optimizer_config_hash",
        "ml_deployment_config",
        "journal_path",
    }
)


def _parse_scalar(raw: str) -> Any:
    val = raw.strip()
    if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
        return val[1:-1]
    low = val.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    try:
        if "." in val or "e" in low:
            return float(val)
        return int(val)
    except ValueError:
        return val


def _simple_yaml_load(text: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for line in text.splitlines():
        stripped = line.split("#", 1)[0].strip()
        if not stripped or ":" not in stripped:
            continue
        k, v = stripped.split(":", 1)
        out[k.strip()] = _parse_scalar(v)
    return out


def compute_config_hash(config: Mapping[str, Any]) -> str:
    canonical = {k: v for k, v in config.items() if k not in _RUNTIME_EXCLUDED_KEYS}
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def load_optimization_config(config_path: str | Path = "configs/optimization/optimization.yaml") -> dict[str, Any]:
    cfg = dict(DEFAULT_OPTIMIZATION_CONFIG)
    path = Path(config_path)
    if path.exists():
        text = path.read_text(encoding="utf-8")
        loaded = yaml.safe_load(text) if yaml is not None else _simple_yaml_load(text)
        if isinstance(loaded, Mapping):
            cfg.update(loaded)
    if cfg.get("ml_deployment_config"):
        from ml.deployment import verify_deployment
        selected = verify_deployment(cfg["ml_deployment_config"])
        if "H" in (loaded if path.exists() and isinstance(loaded, Mapping) else {}):
            if cfg["H"] != selected["horizon_H"]:
                raise ValueError("Optimizer H differs from frozen ML deployment")
        cfg.update(H=selected["horizon_H"], model_sha256=selected["model_sha256"],
                   ml_config_sha256=selected["ml_config_sha256"], delta_s=selected["delta_s"])
    # Run config_hash is supplied by the registered Network/Runner context.
    # Optimizer parameters have their own namespace and must never replace it.
    cfg["optimizer_config_hash"] = compute_config_hash(cfg)
    return cfg
