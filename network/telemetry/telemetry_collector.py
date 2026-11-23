#!/usr/bin/env python3
"""
Network OVS Telemetry Collector (v1.4 Contract Compliant)
Polls OVS TX port counters every 1 second across all 6 switches (e1-e4, c1-c2),
computes port_tx_mbps for all 32 directed links (marking the 16 fabric links),
writes `link_samples.csv`, and emits `LinkSampleBatch` JSONL/HTTP events.

Usage:
    python3 telemetry_collector.py --manifest run_manifest.json --config-dir artifacts/network
    python3 telemetry_collector.py --manifest run_manifest.json --dry-run   # test without OVS
"""

import argparse
import csv
import json
import os
import signal
import sys
import time
import urllib.error
import urllib.request

from counter_parser import compute_link_delta, read_switch_ports

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
for candidate in (SCRIPT_DIR, os.path.join(PARENT_DIR, "traffic_generator")):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from common import iso_utc, load_manifest

STOP = False
SWITCH_IDS = ["e1", "e2", "e3", "e4", "c1", "c2"]


def _stop(*_):
    global STOP
    STOP = True


def load_links(config_dir: str) -> list[dict]:
    path = os.path.join(config_dir, "links.csv")
    if not os.path.exists(path):
        sys.exit(f"Error: links.csv not found at {path}")
    links = []
    with open(path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            links.append({
                "link_id": r["link_id"].strip(),
                "from_node": r["from_node"].strip(),
                "to_node": r["to_node"].strip(),
                "from_port": int(r["from_port"]),
                "to_port": int(r["to_port"]),
                "link_type": r["link_type"].strip(),
                "is_fabric": int(r["is_fabric"]),
                "capacity_mbps": float(r["capacity_mbps"]),
            })
    return links


def _post_batch_with_retry(
    url: str,
    body_bytes: bytes,
    *,
    api_key: str | None = None,
    timeout: float = 1.0,
    max_retries: int = 3,
    backoff_base: float = 0.15,
) -> tuple[bool, int | None, str]:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if api_key:
        headers["X-API-Key"] = api_key

    last_err = ""
    for attempt in range(max_retries + 1):
        req = urllib.request.Request(
            url=url,
            data=body_bytes,
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status = resp.getcode()
                resp_text = resp.read().decode("utf-8", errors="replace")
                if 200 <= status < 300:
                    return True, status, resp_text
                last_err = f"HTTP {status}: {resp_text}"
        except urllib.error.HTTPError as err:
            status = err.code
            err_text = err.read().decode("utf-8", errors="replace") if err.fp else ""
            last_err = f"HTTP {status}: {err_text}"
            if status != 429 and not (500 <= status < 600):
                return False, status, last_err
        except (urllib.error.URLError, TimeoutError, OSError) as err:
            last_err = f"Network/Timeout error: {err}"

        if attempt < max_retries and not STOP:
            time.sleep(backoff_base * (2 ** attempt))

    return False, None, last_err


def emit_batch_event(
    batch_event: dict,
    jsonl_fp=None,
    telemetry_url: str | None = None,
    *,
    api_key: str | None = None,
    timeout: float = 1.0,
    max_retries: int = 3,
    backoff_base: float = 0.15,
):
    payload = json.dumps(batch_event, separators=(",", ":"))
    if jsonl_fp is not None:
        jsonl_fp.write(payload + "\n")
        jsonl_fp.flush()
    if telemetry_url:
        ok, status_code, detail = _post_batch_with_retry(
            url=telemetry_url,
            body_bytes=payload.encode("utf-8"),
            api_key=api_key,
            timeout=timeout,
            max_retries=max_retries,
            backoff_base=backoff_base,
        )
        if not ok:
            print(
                f"[!] Warning: failed to POST LinkSampleBatch {batch_event['batch_id']} "
                f"(status={status_code}): {detail}",
                file=sys.stderr,
            )


def poll_all_switches(dry_run: bool = False) -> dict[str, dict[int, dict[str, int]]]:
    if dry_run:
        return {
            sw: {p: {"tx_packets": 0, "tx_bytes": 0, "tx_drops": 0, "tx_errors": 0} for p in range(1, 5)}
            for sw in SWITCH_IDS
        }
    results = {}
    for sw in SWITCH_IDS:
        try:
            ports = read_switch_ports(sw)
            if ports is not None:
                results[sw] = ports
            else:
                print(f"[!] Warning: read_switch_ports({sw}) returned None", file=sys.stderr)
        except Exception as e:
            print(f"[!] Warning: failed reading ports for switch {sw}: {e}", file=sys.stderr)
    return results


def _default_missing_sample(reason: str) -> tuple[int, dict]:
    return 0, {
        "delta_bytes": 0,
        "delta_t_s": 0.0,
        "port_tx_mbps": 0.0,
        "utilization": 0.0,
        "tx_drops_delta": 0,
        "counter_status": reason,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", required=True, help="Path to run_manifest.json")
    ap.add_argument("--config-dir", default="artifacts/network", help="Directory containing links.csv")
    ap.add_argument("--outdir", default=None, help="Output directory (default: results/<run_id>)")
    ap.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds (default: 1.0)")
    ap.add_argument("--telemetry-url", default=None, help="Optional HTTP endpoint to push LinkSampleBatch events")
    ap.add_argument("--api-key", default=None, help="Optional X-API-Key header for Data HTTP endpoint (fallback: DATA_API_KEY)")
    ap.add_argument("--telemetry-timeout", type=float, default=1.0, help="HTTP timeout per attempt in seconds (default: 1.0)")
    ap.add_argument("--telemetry-retries", type=int, default=3, help="Max HTTP retry attempts for LinkSampleBatch (default: 3)")
    ap.add_argument("--telemetry-backoff", type=float, default=0.15, help="Base backoff in seconds between retries (default: 0.15)")
    ap.add_argument("--dry-run", action="store_true", help="Simulate zero counters without calling ovs-ofctl")
    args = ap.parse_args()

    man = load_manifest(args.manifest)
    links = load_links(args.config_dir)
    api_key = args.api_key or os.environ.get("DATA_API_KEY")
    outdir = args.outdir or os.path.join("results", man["run_id"])
    os.makedirs(outdir, exist_ok=True)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    t0 = man["t0"]
    total_bins = man["warmup"] + man["measurement"] + man["drain"]
    stop_at = t0 + total_bins

    if t0 > time.time():
        print(f"[*] Waiting until {iso_utc(t0)} ({t0 - time.time():.1f} s remaining) to start telemetry...")
        while time.time() < t0 and not STOP:
            time.sleep(min(0.2, max(0.0, t0 - time.time())))

    prev_state: dict[str, dict] = {}
    base_ts = time.time()
    base_counters = poll_all_switches(dry_run=args.dry_run)
    for lk in links:
        sw = lk["from_node"]
        if sw in base_counters:
            port_stats = base_counters[sw].get(lk["from_port"])
            if port_stats:
                prev_state[lk["link_id"]] = {
                    "ts": base_ts,
                    "tx_bytes": port_stats["tx_bytes"],
                    "tx_packets": port_stats["tx_packets"],
                    "tx_drops": port_stats["tx_drops"],
                }

    csv_path = os.path.join(outdir, "link_samples.csv")
    jsonl_path = os.path.join(outdir, "link_samples.jsonl")

    with open(csv_path, "w", encoding="utf-8", newline="") as fcsv, \
         open(jsonl_path, "w", encoding="utf-8") as fjson:
        writer = csv.writer(fcsv, lineterminator="\n")
        writer.writerow([
            "time_offset_s", "sampled_at_utc", "link_id", "from_node", "to_node",
            "measure_switch", "measure_port", "is_fabric", "capacity_mbps",
            "tx_bytes_total", "delta_bytes", "delta_t_s", "port_tx_mbps",
            "utilization", "tx_drops_delta", "counter_status"
        ])

        for bin_idx in range(total_bins):
            if STOP or time.time() >= stop_at:
                break

            target_ts = t0 + (bin_idx + 1) * args.interval
            while time.time() < target_ts and not STOP:
                time.sleep(min(0.05, max(0.0, target_ts - time.time())))

            sample_ts = time.time()
            sampled_at_utc = iso_utc(sample_ts)
            sw_counters = poll_all_switches(dry_run=args.dry_run)

            batch_samples = []
            missing_links = []
            for lk in links:
                lid = lk["link_id"]
                sw = lk["from_node"]
                port = lk["from_port"]

                pstats = sw_counters[sw].get(port) if sw in sw_counters else None
                if sw not in sw_counters:
                    missing_links.append(f"{lid}({sw}:{port}:missing_switch)")
                    tx_bytes_total, delta_info = _default_missing_sample("missing_switch")
                elif pstats is None:
                    missing_links.append(f"{lid}({sw}:{port}:missing_port)")
                    tx_bytes_total, delta_info = _default_missing_sample("missing_port")
                else:
                    tx_bytes_total = pstats["tx_bytes"]
                    delta_info = compute_link_delta(
                        prev_sample=prev_state.get(lid),
                        curr_tx_bytes=pstats["tx_bytes"],
                        curr_tx_packets=pstats["tx_packets"],
                        curr_tx_drops=pstats["tx_drops"],
                        curr_ts=sample_ts,
                        capacity_mbps=lk["capacity_mbps"],
                    )
                    prev_state[lid] = {
                        "ts": sample_ts,
                        "tx_bytes": pstats["tx_bytes"],
                        "tx_packets": pstats["tx_packets"],
                        "tx_drops": pstats["tx_drops"],
                    }

                drops_delta = delta_info.get("tx_drops_delta", delta_info.get("delta_drops", 0))

                writer.writerow([
                    bin_idx, sampled_at_utc, lid, sw, lk["to_node"],
                    sw, port, lk["is_fabric"], lk["capacity_mbps"],
                    tx_bytes_total, delta_info["delta_bytes"], delta_info["delta_t_s"],
                    delta_info["port_tx_mbps"], delta_info["utilization"],
                    drops_delta, delta_info["counter_status"],
                ])

                batch_samples.append({
                    "link_id": lid,
                    "from_node": sw,
                    "to_node": lk["to_node"],
                    "measure_switch": sw,
                    "measure_port": port,
                    "is_fabric": bool(lk["is_fabric"]),
                    "port_tx_mbps": delta_info["port_tx_mbps"],
                    "utilization": delta_info["utilization"],
                    "delta_bytes": delta_info["delta_bytes"],
                    "delta_t_s": delta_info["delta_t_s"],
                    "tx_drops_delta": drops_delta,
                    "counter_status": delta_info["counter_status"],
                })

            if missing_links:
                print(
                    f"[!] Warning: bin {bin_idx} missing {len(missing_links)}/{len(links)} links: "
                    f"{', '.join(missing_links)}",
                    file=sys.stderr,
                )

            fcsv.flush()

            batch_event = {
                "batch_id": f"network:{man['run_id']}:telemetry:{bin_idx}",
                "run_id": man["run_id"],
                "bin_index": bin_idx,
                "sampled_at_utc": sampled_at_utc,
                "interval_s": args.interval,
                "sample_count": len(batch_samples),
                "samples": batch_samples,
            }
            emit_batch_event(
                batch_event,
                jsonl_fp=fjson,
                telemetry_url=args.telemetry_url,
                api_key=api_key,
                timeout=args.telemetry_timeout,
                max_retries=args.telemetry_retries,
                backoff_base=args.telemetry_backoff,
            )

    print(f"[+] Telemetry collection finished: {csv_path}")


if __name__ == "__main__":
    main()
