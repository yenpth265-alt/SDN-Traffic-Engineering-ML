"""
CF-27 Reproducible SDN v1.4 Integration Preflight & Runner Readiness Gate.

Pure-Python CLI for Runner / QA operators to:
1. Validate fail-closed `data_v1` integration environment variables without ever logging secrets.
2. Validate `TopologyRegistry` (`topology_version`, `topology_hash`, CSV cross-checks, 6 switches, 48 flows).
3. Optionally probe Data API `/health/ready` (`--check-data-health`).
4. Optionally poll SDN Northbound `GET /health` (`--wait-ready`) until:
   - `6/6` OVS switches connected
   - `48/48` flows `VERIFIED`
   - `reconcile_required == False`
   - `data_sync_gate.optimization_tick_eligible == True`

Legacy standalone defaults in `S0AppConfig` are NOT modified.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sdn.apps.ryu_s0_app import S0AppConfig
from sdn.controller.topology_registry import TopologyRegistry


def _http_get_json(
    url: str,
    headers: Mapping[str, str],
    timeout_s: float,
) -> tuple[int, dict[str, Any]]:
    req = urllib.request.Request(url=url, headers=dict(headers), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return int(resp.status), json.loads(raw) if raw.strip() else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw) if raw.strip() else {"detail": raw}
        except json.JSONDecodeError:
            body = {"detail": raw}
        return int(exc.code), body


def run_preflight(
    env: Optional[Mapping[str, str]] = None,
    *,
    expected_topology_version: Optional[str] = None,
    expected_topology_hash: Optional[str] = None,
    expected_config_version: Optional[str] = None,
    expected_config_hash: Optional[str] = None,
    check_data_health: bool = False,
    http_get_fn: Callable[[str, Mapping[str, str], float], tuple[int, dict[str, Any]]] = _http_get_json,
) -> dict[str, Any]:
    """
    Validate all CF-27 fail-closed requirements for the `data_v1` integration profile.
    Raises `ValueError` on any invalid/missing setting. Never includes secret values in output.
    """
    orig_env: Optional[dict[str, str]] = None
    if env is not None:
        orig_env = dict(os.environ)
        os.environ.clear()
        os.environ.update(env)
    try:
        cfg = S0AppConfig.from_env()
    finally:
        if orig_env is not None:
            os.environ.clear()
            os.environ.update(orig_env)

    errors: list[str] = []
    if not cfg.strict_integration_profile:
        errors.append("SDN_STRICT_INTEGRATION_PROFILE must be set to '1' for Runner integration launch.")
    if not cfg.northbound_enable:
        errors.append("SDN_NORTHBOUND_ENABLE must be set to '1' for Runner integration launch.")
    try:
        cfg.validate_integration_profile()
    except ValueError as exc:
        errors.append(str(exc))

    topo = TopologyRegistry.from_bundle_file(
        cfg.topology_bundle_path,
        csv_config_dir=cfg.csv_config_dir,
        verify_hash=True,
    )
    if expected_topology_version and topo.topology_version != expected_topology_version:
        errors.append(
            f"topology_version mismatch: bundle has {topo.topology_version!r}, expected {expected_topology_version!r}."
        )
    if expected_topology_hash and topo.topology_hash != expected_topology_hash:
        errors.append(
            f"topology_hash mismatch: bundle has {topo.topology_hash!r}, expected {expected_topology_hash!r}."
        )
    env_src = env if env is not None else os.environ
    actual_cfg_ver = str(env_src.get("SDN_CONFIG_VERSION", "1.0")).strip()
    actual_cfg_hash = str(env_src.get("SDN_CONFIG_HASH", "")).strip()
    if expected_config_version and actual_cfg_ver != expected_config_version:
        errors.append(
            f"config_version mismatch: env has {actual_cfg_ver!r}, expected {expected_config_version!r}."
        )
    if expected_config_hash and actual_cfg_hash != expected_config_hash:
        errors.append(
            f"config_hash mismatch: env has {actual_cfg_hash!r}, expected {expected_config_hash!r}."
        )

    data_health: Optional[dict[str, Any]] = None
    if check_data_health and cfg.data_endpoint_url:
        url = f"{cfg.data_endpoint_url.rstrip('/')}/health/ready"
        code, body = http_get_fn(url, {"Accept": "application/json"}, max(1.0, cfg.data_timeout_s))
        data_health = {"http_status": code, "body": body}
        if code != 200:
            errors.append(f"Data API /health/ready failed with HTTP {code}: {body}")

    if errors:
        raise ValueError("SDN CF-27 preflight failed: " + " | ".join(errors))

    return {
        "preflight_ok": True,
        "strict_integration_profile": cfg.strict_integration_profile,
        "data_api_mode": cfg.data_api_mode,
        "demand_provider_mode": cfg.demand_provider_mode,
        "data_endpoint_url": cfg.data_endpoint_url,
        "data_api_key_configured": bool(cfg.data_api_key),
        "northbound_enable": cfg.northbound_enable,
        "northbound_bind": f"{cfg.northbound_host}:{cfg.northbound_port}",
        "northbound_token_configured": bool(cfg.northbound_auth_token),
        "data_outbox_dir": str(cfg.data_outbox_dir),
        "idempotency_db_path": str(cfg.idempotency_db_path),
        "topology_version": topo.topology_version,
        "topology_hash": topo.topology_hash,
        "switch_count": len(topo.get_all_switches()),
        "flow_group_count": len(topo.get_all_flow_groups()),
        "data_health": data_health,
    }


def evaluate_sdn_readiness_payload(health: Mapping[str, Any]) -> tuple[bool, list[str]]:
    """
    Evaluate SDN `GET /health` JSON against CF-27 Runner readiness gates:
    - 6/6 switches connected
    - 48/48 flows VERIFIED
    - reconcile_required == False
    - data_sync_gate.optimization_tick_eligible == True
    """
    reasons: list[str] = []
    ready_block = health.get("sdn_readiness") or {}
    if isinstance(ready_block, Mapping) and ready_block:
        switches = int(ready_block.get("connected_switches", 0) or 0)
        verified = int(ready_block.get("verified_flow_groups", 0) or 0)
        reconcile = bool(ready_block.get("reconcile_required", False))
    else:
        switches = int(health.get("connected_switches", 0) or 0)
        verified = int(health.get("verified_flow_groups", 0) or 0)
        reconcile = bool(health.get("reconcile_required", False))

    if switches < 6:
        reasons.append(f"switches_connected={switches}/6")
    if verified < 48:
        reasons.append(f"flows_verified={verified}/48")
    if reconcile:
        reasons.append("ovs_reconcile_required=true")

    gate = health.get("data_sync_gate")
    if not isinstance(gate, Mapping):
        reasons.append("data_sync_gate_missing")
    else:
        if gate.get("api_mode") != "data_v1":
            reasons.append(f"data_sync_gate.api_mode={gate.get('api_mode')!r}")
        if not bool(gate.get("optimization_tick_eligible", False)):
            reasons.append(
                f"optimization_tick_eligible=false (reason={gate.get('optimization_tick_block_reason')!r})"
            )
    return (len(reasons) == 0), reasons


def wait_for_sdn_readiness(
    sdn_base_url: str,
    *,
    auth_token: str,
    timeout_s: float = 30.0,
    poll_interval_s: float = 0.5,
    http_get_fn: Callable[[str, Mapping[str, str], float], tuple[int, dict[str, Any]]] = _http_get_json,
) -> dict[str, Any]:
    """Poll SDN `GET /health` until all CF-27 gates pass or raise `TimeoutError`."""
    if not auth_token or not auth_token.strip():
        raise ValueError("SDN_NORTHBOUND_TOKEN is required to poll SDN /health.")
    deadline = time.monotonic() + max(0.1, float(timeout_s))
    url = f"{sdn_base_url.rstrip('/')}/health"
    headers = {"X-SDN-Token": auth_token, "Accept": "application/json"}
    last_reasons: list[str] = ["no_poll_completed"]
    last_payload: dict[str, Any] = {}

    while True:
        try:
            code, payload = http_get_fn(url, headers, min(2.0, max(0.5, timeout_s)))
            last_payload = payload
            if code == 200:
                ok, reasons = evaluate_sdn_readiness_payload(payload)
                if ok:
                    return {"ready": True, "health": payload}
                last_reasons = reasons
            else:
                last_reasons = [f"http_status={code}"]
        except Exception as exc:
            last_reasons = [f"{type(exc).__name__}: {exc}"]

        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"SDN readiness gate not satisfied within {timeout_s:.1f}s: {', '.join(last_reasons)}"
            )
        time.sleep(max(0.01, float(poll_interval_s)))


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="SDN v1.4 CF-27 Integration Preflight & Readiness Gate")
    parser.add_argument("--expected-topology-version", default=None)
    parser.add_argument("--expected-topology-hash", default=None)
    parser.add_argument("--expected-config-version", default=None)
    parser.add_argument("--expected-config-hash", default=None)
    parser.add_argument("--check-data-health", action="store_true")
    parser.add_argument("--wait-ready", action="store_true")
    parser.add_argument("--sdn-url", default=None, help="Override SDN base URL (default from env bind host/port)")
    parser.add_argument("--wait-timeout-s", type=float, default=30.0)
    parser.add_argument("--poll-interval-s", type=float, default=0.5)
    args = parser.parse_args(argv)

    try:
        report = run_preflight(
            expected_topology_version=args.expected_topology_version,
            expected_topology_hash=args.expected_topology_hash,
            expected_config_version=args.expected_config_version,
            expected_config_hash=args.expected_config_hash,
            check_data_health=args.check_data_health,
        )
        if args.wait_ready:
            sdn_url = args.sdn_url or f"http://{report['northbound_bind']}"
            token = os.environ.get("SDN_NORTHBOUND_TOKEN", "")
            ready = wait_for_sdn_readiness(
                sdn_url,
                auth_token=token,
                timeout_s=args.wait_timeout_s,
                poll_interval_s=args.poll_interval_s,
            )
            report["sdn_readiness"] = ready
    except Exception as exc:
        print(json.dumps({"preflight_ok": False, "error": str(exc)}, indent=2), file=sys.stderr)
        return 1

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
