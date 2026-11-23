#!/usr/bin/env python3
"""Correlates sender and receiver traffic logs by cohort (send_bin) and sequence,
writes `correlated_traffic.csv` and `correlated_traffic.jsonl`, and optionally
POSTs Data-contract `TrafficBatch` payloads (<= 4096 rows per batch) to `/v1/network/traffic`.

Usage:
    python3 correlate_traffic.py --manifest run_manifest.json --config-dir configs/network --indir results/<run_id>
    python3 correlate_traffic.py --manifest run_manifest.json --indir results/<run_id> \
        --traffic-url http://<host>:<port>/v1/network/traffic --api-key "$DATA_API_KEY"
"""

import argparse
import csv
from datetime import datetime, timedelta, timezone
import glob
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from common import iso_utc, load_flow_groups, load_manifest

MAX_TRAFFIC_BATCH_ROWS = 4096


def percentile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def _fmt_utc_1s_window(t0: float, bin_idx: int) -> tuple[str, str]:
    start_dt = datetime.fromtimestamp(t0 + bin_idx, tz=timezone.utc)
    end_dt = start_dt + timedelta(seconds=1)
    return (
        start_dt.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        end_dt.isoformat(timespec="microseconds").replace("+00:00", "Z"),
    )


def _iter_chunks(items: list[dict], chunk_size: int):
    for i in range(0, len(items), chunk_size):
        yield i // chunk_size, items[i : i + chunk_size]


def _post_with_retry(
    url: str,
    body_bytes: bytes,
    *,
    api_key: str | None = None,
    timeout: float = 2.0,
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

        if attempt < max_retries:
            time.sleep(backoff_base * (2 ** attempt))

    return False, None, last_err


def load_requested_logs(indir: str) -> dict[tuple[int, str], dict]:
    req_map: dict[tuple[int, str], dict] = {}
    for path in sorted(glob.glob(os.path.join(indir, "requested_*.csv"))):
        with open(path, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                t = int(float(r["time_offset_s"]))
                fg = r["flow_group_id"].strip()
                req_map[(t, fg)] = {
                    "requested_mbps": float(r["requested_mbps"]),
                    "available_at_utc": r.get("available_at_utc", "").strip(),
                }
    return req_map


def load_sent_logs(indir: str) -> dict[tuple[int, str], dict]:
    sent_map: dict[tuple[int, str], dict] = {}
    for path in sorted(glob.glob(os.path.join(indir, "sent_*.csv"))):
        m = re.match(r"^sent_(.+)\.csv$", os.path.basename(path))
        src_host = m.group(1) if m else ""
        with open(path, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                t = int(float(r["time_offset_s"]))
                fg = r["flow_group_id"].strip()
                key = (t, fg)
                entry = sent_map.setdefault(
                    key,
                    {
                        "src_host": src_host,
                        "packets_sent": 0,
                        "bytes_sent": 0,
                        "late_packets": 0,
                    },
                )
                entry["packets_sent"] += int(r["packets_sent"])
                entry["bytes_sent"] += int(r["bytes_sent"])
                entry["late_packets"] += int(r.get("late_packets", 0))
    return sent_map


def load_recv_bysend_logs(indir: str) -> dict[tuple[int, str], dict]:
    """Loads packets and bytes grouped by sender cohort (send_bin / time_offset_s) and flow_group_id."""
    cohort_stats: dict[tuple[int, str], dict] = {}
    for path in sorted(glob.glob(os.path.join(indir, "recv_bysend_*.csv"))):
        with open(path, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                t = int(float(r["time_offset_s"]))
                fg = r["flow_group_id"].strip()
                key = (t, fg)
                pkts = int(r["packets_received"])
                has_bytes = "bytes_received" in r and r["bytes_received"] not in (None, "")
                b_val = int(r["bytes_received"]) if has_bytes else None
                entry = cohort_stats.setdefault(
                    key,
                    {"packets_received": 0, "bytes_received": 0, "has_exact_bytes": False},
                )
                entry["packets_received"] += pkts
                if b_val is not None:
                    entry["bytes_received"] += b_val
                    entry["has_exact_bytes"] = True
    return cohort_stats


def load_recv_packet_logs(indir: str) -> dict[tuple[int, str], dict]:
    """Correlates per-packet logs (flow_group_id, seq, send_bin, recv_offset_s, delay_ms)
    by cohort (send_bin) and sequence number when recvpkts_*.csv files are present.
    """
    raw_seqs: dict[tuple[int, str], set[int]] = {}
    delays_by_cohort: dict[tuple[int, str], list[float]] = {}
    total_pkts: dict[tuple[int, str], int] = {}

    for path in sorted(glob.glob(os.path.join(indir, "recvpkts_*.csv"))):
        with open(path, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                fg = r["flow_group_id"].strip()
                seq = int(r["seq"])
                send_bin = int(float(r["send_bin"]))
                delay_ms = float(r["delay_ms"])
                key = (send_bin, fg)
                raw_seqs.setdefault(key, set()).add(seq)
                delays_by_cohort.setdefault(key, []).append(delay_ms)
                total_pkts[key] = total_pkts.get(key, 0) + 1

    seq_stats: dict[tuple[int, str], dict] = {}
    all_keys = set(raw_seqs) | set(delays_by_cohort) | set(total_pkts)
    for key in all_keys:
        seqs = raw_seqs.get(key, set())
        delays = sorted(delays_by_cohort.get(key, []))
        pkt_n = total_pkts.get(key, 0)
        uniq_n = len(seqs)
        seq_stats[key] = {
            "has_packet_log": True,
            "packets_received_pktlog": pkt_n,
            "unique_seqs_received": uniq_n,
            "duplicate_or_retrans_packets": max(0, pkt_n - uniq_n),
            "seq_min": min(seqs) if seqs else None,
            "seq_max": max(seqs) if seqs else None,
            "delay_ms_mean": round(sum(delays) / len(delays), 3) if delays else 0.0,
            "delay_ms_p95": round(percentile(delays, 0.95), 3) if delays else 0.0,
            "delay_ms_max": round(delays[-1], 3) if delays else 0.0,
        }
    return seq_stats


def build_correlated_rows(
    man: dict,
    groups: list[dict],
    req_map: dict[tuple[int, str], dict],
    sent_map: dict[tuple[int, str], dict],
    bysend_map: dict[tuple[int, str], dict],
    seq_stats_map: dict[tuple[int, str], dict],
) -> tuple[list[dict], list[dict]]:
    fg_meta = {g["flow_group_id"]: g for g in groups}
    total_bins = man["warmup"] + man["measurement"] + man["drain"]
    t0 = float(man["t0"])

    all_keys = set(sent_map) | set(bysend_map) | set(seq_stats_map)
    for t in range(total_bins):
        for g in groups:
            all_keys.add((t, g["flow_group_id"]))

    detail_rows: list[dict] = []
    traffic_rows: list[dict] = []

    for t, fg in sorted(all_keys, key=lambda item: (item[0], item[1])):
        win_start_utc, win_end_utc = _fmt_utc_1s_window(t0, t)
        meta = fg_meta.get(fg, {})
        req = req_map.get((t, fg), {})
        sent = sent_map.get(
            (t, fg),
            {
                "src_host": meta.get("src_host", ""),
                "packets_sent": 0,
                "bytes_sent": 0,
                "late_packets": 0,
            },
        )
        seq_info = seq_stats_map.get((t, fg))
        bysend_info = bysend_map.get((t, fg))
        if bysend_info is not None:
            recv_cohort = int(bysend_info["packets_received"])
        elif seq_info:
            recv_cohort = int(seq_info["unique_seqs_received"])
        else:
            recv_cohort = 0

        pkts_sent = int(sent["packets_sent"])
        bytes_sent = int(sent["bytes_sent"])
        if bysend_info is not None and bysend_info.get("has_exact_bytes"):
            recv_bytes_val = int(bysend_info["bytes_received"])
        else:
            avg_pkt_bytes = (bytes_sent / pkts_sent) if pkts_sent > 0 else 1000.0
            recv_bytes_val = int(round(recv_cohort * avg_pkt_bytes))

        traffic_actual_mbps = round((recv_bytes_val * 8.0) / 1e6, 6)

        lost_cohort = max(0, pkts_sent - recv_cohort)
        loss_ratio = round(lost_cohort / pkts_sent, 6) if pkts_sent > 0 else 0.0

        detail_rows.append({
            "run_id": man["run_id"],
            "cohort_bin": t,
            "window_start_utc": win_start_utc,
            "window_end_utc": win_end_utc,
            "flow_group_id": fg,
            "src_host": sent.get("src_host") or meta.get("src_host", ""),
            "dst_host": meta.get("dst_host", ""),
            "requested_mbps": req.get("requested_mbps", 0.0),
            "traffic_actual_mbps": traffic_actual_mbps,
            "available_at_utc": req.get("available_at_utc", ""),
            "packets_sent": pkts_sent,
            "bytes_sent": bytes_sent,
            "late_packets": int(sent["late_packets"]),
            "packets_received_cohort": int(recv_cohort),
            "bytes_received": recv_bytes_val,
            "packets_lost_cohort": int(lost_cohort),
            "loss_ratio_cohort": loss_ratio,
            "unique_seqs_received": int(seq_info["unique_seqs_received"]) if seq_info else int(recv_cohort),
            "duplicate_or_retrans_packets": int(seq_info["duplicate_or_retrans_packets"]) if seq_info else 0,
            "seq_min": seq_info["seq_min"] if seq_info else None,
            "seq_max": seq_info["seq_max"] if seq_info else None,
            "delay_ms_mean": seq_info["delay_ms_mean"] if seq_info else 0.0,
            "delay_ms_p95": seq_info["delay_ms_p95"] if seq_info else 0.0,
            "delay_ms_max": seq_info["delay_ms_max"] if seq_info else 0.0,
        })

        traffic_rows.append({
            "flow_group_id": fg,
            "window_start_utc": win_start_utc,
            "window_end_utc": win_end_utc,
            "sent_bytes": bytes_sent,
            "received_bytes": recv_bytes_val,
            "sent_packets": pkts_sent,
            "received_packets": int(recv_cohort),
        })

    return detail_rows, traffic_rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", required=True, help="Path to run_manifest.json")
    ap.add_argument("--config-dir", default="configs/network", help="Directory containing flow_groups.csv")
    ap.add_argument("--indir", default=None, help="Input directory containing sender/receiver logs (default: results/<run_id>)")
    ap.add_argument("--outdir", default=None, help="Output directory (default: same as --indir)")
    ap.add_argument("--traffic-url", default=None, help="Optional HTTP endpoint (e.g., http://.../v1/network/traffic)")
    ap.add_argument("--api-key", default=None, help="Optional X-API-Key header (fallback: DATA_API_KEY)")
    ap.add_argument("--timeout", type=float, default=2.0, help="HTTP timeout in seconds per attempt (default: 2.0)")
    ap.add_argument("--retries", type=int, default=3, help="Max HTTP retry attempts (default: 3)")
    ap.add_argument("--backoff", type=float, default=0.15, help="Base retry backoff in seconds (default: 0.15)")
    args = ap.parse_args()

    man = load_manifest(args.manifest)
    groups = load_flow_groups(args.config_dir)
    indir = args.indir or os.path.join("results", man["run_id"])
    outdir = args.outdir or indir
    os.makedirs(outdir, exist_ok=True)
    api_key = args.api_key or os.environ.get("DATA_API_KEY")

    req_map = load_requested_logs(indir)
    sent_map = load_sent_logs(indir)
    bysend_map = load_recv_bysend_logs(indir)
    seq_stats_map = load_recv_packet_logs(indir)

    detail_rows, traffic_rows = build_correlated_rows(
        man, groups, req_map, sent_map, bysend_map, seq_stats_map
    )

    csv_path = os.path.join(outdir, "correlated_traffic.csv")
    jsonl_path = os.path.join(outdir, "correlated_traffic.jsonl")

    fieldnames = [
        "run_id", "cohort_bin", "window_start_utc", "window_end_utc",
        "flow_group_id", "src_host", "dst_host", "requested_mbps", "traffic_actual_mbps",
        "available_at_utc", "packets_sent", "bytes_sent", "late_packets",
        "packets_received_cohort", "bytes_received", "packets_lost_cohort",
        "loss_ratio_cohort", "unique_seqs_received", "duplicate_or_retrans_packets",
        "seq_min", "seq_max", "delay_ms_mean", "delay_ms_p95", "delay_ms_max",
    ]
    with open(csv_path, "w", encoding="utf-8", newline="") as fcsv:
        writer = csv.DictWriter(fcsv, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for r in detail_rows:
            writer.writerow(r)

    chunks = list(_iter_chunks(traffic_rows, MAX_TRAFFIC_BATCH_ROWS))
    with open(jsonl_path, "w", encoding="utf-8") as fjson:
        for _, chunk in chunks:
            payload_line = json.dumps(
                {"run_id": man["run_id"], "rows": chunk},
                separators=(",", ":"),
            )
            fjson.write(payload_line + "\n")

    print(
        f"[+] Correlated {len(detail_rows)} records -> {csv_path} "
        f"(TrafficBatch chunks: {len(chunks)}, max {MAX_TRAFFIC_BATCH_ROWS} rows/batch)"
    )

    if args.traffic_url:
        if not chunks:
            print("[!] Warning: skipping POST /v1/network/traffic because rows is empty", file=sys.stderr)
            return
        for chunk_idx, chunk in chunks:
            chunk_bytes = json.dumps(
                {"run_id": man["run_id"], "rows": chunk},
                separators=(",", ":"),
            ).encode("utf-8")
            ok, status_code, detail = _post_with_retry(
                url=args.traffic_url,
                body_bytes=chunk_bytes,
                api_key=api_key,
                timeout=args.timeout,
                max_retries=args.retries,
                backoff_base=args.backoff,
            )
            if ok:
                print(
                    f"[+] POST /v1/network/traffic chunk {chunk_idx} ({len(chunk)} rows) "
                    f"succeeded (status={status_code})"
                )
            else:
                print(
                    f"[!] Warning: failed to POST /v1/network/traffic chunk {chunk_idx} "
                    f"(status={status_code}): {detail}",
                    file=sys.stderr,
                )


if __name__ == "__main__":
    main()