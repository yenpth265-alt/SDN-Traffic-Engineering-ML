#!/usr/bin/env python3
"""Correlates sender and receiver traffic logs by cohort (send_bin) and sequence,
writes `correlated_traffic.csv` and `correlated_traffic.jsonl`, and optionally
POSTs Data-contract `TrafficBatch` payloads (<= 4096 rows per batch) to
`/v1/network/traffic` with persistent outbox replay when Data is unavailable.

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
import signal
import sys
import time
import urllib.error
import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from common import load_flow_groups, load_manifest

MAX_TRAFFIC_BATCH_ROWS = 4096
STOP = False


def _stop(*_):
    global STOP
    STOP = True


def _parse_utc_to_epoch(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        pass
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def _resolve_traffic_origin_ts(manifest: dict) -> float:
    for key in ("traffic_started_at_utc", "traffic_start_utc", "t0"):
        ts = _parse_utc_to_epoch(manifest.get(key))
        if ts is not None:
            return ts
    raise KeyError("Manifest missing valid traffic_started_at_utc or t0")


def _parse_optional_int(raw_val) -> int | None:
    if raw_val is None:
        return None
    text = str(raw_val).strip()
    if not text or text.lower() in ("null", "none", "nan", "na"):
        return None
    return int(float(text))


def _parse_optional_float(raw_val) -> float | None:
    if raw_val is None:
        return None
    text = str(raw_val).strip()
    if not text or text.lower() in ("null", "none", "nan", "na"):
        return None
    return float(text)


def _percentile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    pos = (len(sorted_vals) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (pos - lo)


def _fmt_utc_1s_window(origin_ts: float, bin_idx: int) -> tuple[str, str]:
    start_dt = datetime.fromtimestamp(origin_ts + bin_idx, tz=timezone.utc)
    end_dt = start_dt + timedelta(seconds=1)
    return (
        start_dt.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        end_dt.isoformat(timespec="microseconds").replace("+00:00", "Z"),
    )


def _iter_chunks(items: list[dict], chunk_size: int):
    for start_idx in range(0, len(items), chunk_size):
        yield start_idx // chunk_size, items[start_idx : start_idx + chunk_size]


class TrafficOutbox:
    """Persists unacknowledged TrafficBatch chunks so replays preserve exact content."""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._pending: dict[str, dict] = {}
        self._load()
        if not os.path.exists(self.path):
            self._rewrite()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                batch_key = rec.get("batch_key")
                payload = rec.get("payload")
                if batch_key and isinstance(payload, dict):
                    self._pending[batch_key] = payload

    def _rewrite(self) -> None:
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            for batch_key, payload in self._pending.items():
                f.write(
                    json.dumps({"batch_key": batch_key, "payload": payload}, separators=(",", ":"))
                    + "\n"
                )
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self.path)

    def add_if_missing(self, batch_key: str, payload: dict) -> None:
        if batch_key in self._pending and self._pending[batch_key] != payload:
            raise ValueError(f"Traffic batch ID reused with different content: {batch_key}")
        if batch_key not in self._pending:
            self._pending[batch_key] = json.loads(json.dumps(payload))
            self._rewrite()

    def ack(self, batch_key: str) -> None:
        if batch_key in self._pending:
            del self._pending[batch_key]
            self._rewrite()

    def items(self) -> list[tuple[str, dict]]:
        return list(self._pending.items())


def _post_batch_once(
    url: str,
    body_bytes: bytes,
    *,
    api_key: str | None = None,
    timeout: float = 2.0,
) -> tuple[str, int | None, str]:
    """Returns ('acked' | 'conflict_409' | 'invalid_422' | 'retryable', status_code, detail)."""
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if api_key:
        headers["X-API-Key"] = api_key

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
                return "acked", status, resp_text
            return "retryable", status, f"HTTP {status}: {resp_text}"
    except urllib.error.HTTPError as err:
        status = err.code
        err_text = err.read().decode("utf-8", errors="replace") if err.fp else ""
        if status == 409:
            return "conflict_409", status, err_text
        if status == 422:
            return "invalid_422", status, err_text
        if status == 429 or (500 <= status < 600):
            return "retryable", status, err_text
        return "retryable", status, f"HTTP {status}: {err_text}"
    except (urllib.error.URLError, TimeoutError, OSError) as err:
        return "retryable", None, f"Network/Timeout error: {err}"


def _send_or_queue_batch(
    batch_key: str,
    payload: dict,
    outbox: TrafficOutbox,
    traffic_url: str,
    *,
    api_key: str | None = None,
    timeout: float = 2.0,
    max_retries: int = 3,
    backoff_base: float = 0.15,
) -> None:
    outbox.add_if_missing(batch_key, payload)
    body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    last_outcome = "retryable"
    last_status: int | None = None
    last_detail = ""

    for attempt in range(max_retries + 1):
        outcome, status_code, detail = _post_batch_once(
            url=traffic_url,
            body_bytes=body_bytes,
            api_key=api_key,
            timeout=timeout,
        )
        last_outcome, last_status, last_detail = outcome, status_code, detail
        if outcome == "acked":
            outbox.ack(batch_key)
            print(
                f"[+] POST /v1/network/traffic {batch_key} ({len(payload['rows'])} rows) "
                f"succeeded (status={status_code})"
            )
            return
        if outcome in ("conflict_409", "invalid_422"):
            break
        if attempt < max_retries and not STOP:
            time.sleep(backoff_base * (2 ** attempt))

    if last_outcome == "conflict_409":
        print(f"[!] 409 Conflict for TrafficBatch {batch_key}: {last_detail}", file=sys.stderr)
    elif last_outcome == "invalid_422":
        print(f"[!] 422 Invalid payload for TrafficBatch {batch_key}: {last_detail}", file=sys.stderr)
    else:
        outbox.add_if_missing(batch_key, payload)
        print(
            f"[!] Queued TrafficBatch {batch_key} in outbox "
            f"(Data unavailable, status={last_status}): {last_detail}",
            file=sys.stderr,
        )


def flush_traffic_outbox(
    outbox: TrafficOutbox,
    traffic_url: str | None,
    *,
    api_key: str | None = None,
    timeout: float = 2.0,
    backoff_base: float = 0.15,
) -> None:
    if not traffic_url:
        return
    for idx, (batch_key, payload) in enumerate(outbox.items()):
        if STOP:
            break
        body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        outcome, status_code, detail = _post_batch_once(
            url=traffic_url,
            body_bytes=body_bytes,
            api_key=api_key,
            timeout=timeout,
        )
        if outcome == "acked":
            outbox.ack(batch_key)
            print(f"[+] Replayed TrafficBatch {batch_key} successfully (status={status_code})")
        elif outcome == "conflict_409":
            print(f"[!] Outbox 409 Conflict for {batch_key}: {detail}", file=sys.stderr)
        elif outcome == "invalid_422":
            print(f"[!] Outbox 422 Invalid for {batch_key}: {detail}", file=sys.stderr)
        else:
            print(
                f"[!] Traffic outbox still pending for {batch_key} (status={status_code}): {detail}",
                file=sys.stderr,
            )
            time.sleep(min(1.0, backoff_base * (2 ** min(idx, 4))))
            break


def load_requested_logs(indir: str) -> dict[tuple[int, str], dict]:
    req_map: dict[tuple[int, str], dict] = {}
    for path in sorted(glob.glob(os.path.join(indir, "requested_*.csv"))):
        with open(path, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                t = int(float(r["time_offset_s"]))
                fg = r["flow_group_id"].strip()
                req_map[(t, fg)] = {
                    "requested_mbps": _parse_optional_float(r.get("requested_mbps")),
                    "reason": r.get("reason", "").strip(),
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
                        "packets_sent": None,
                        "bytes_sent": None,
                        "late_packets": None,
                        "has_sent_log": True,
                    },
                )
                entry["has_sent_log"] = True
                pkts_sent = _parse_optional_int(r.get("packets_sent"))
                bytes_sent = _parse_optional_int(r.get("bytes_sent"))
                late_pkts = _parse_optional_int(r.get("late_packets"))
                if pkts_sent is not None:
                    entry["packets_sent"] = (entry["packets_sent"] or 0) + pkts_sent
                if bytes_sent is not None:
                    entry["bytes_sent"] = (entry["bytes_sent"] or 0) + bytes_sent
                if late_pkts is not None:
                    entry["late_packets"] = (entry["late_packets"] or 0) + late_pkts
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
                pkts = _parse_optional_int(r.get("packets_received"))
                b_val = _parse_optional_int(r.get("bytes_received"))

                entry = cohort_stats.setdefault(
                    key,
                    {"packets_received": None, "bytes_received": None, "has_recv_log": True},
                )
                entry["has_recv_log"] = True
                if pkts is not None:
                    entry["packets_received"] = (entry["packets_received"] or 0) + pkts
                if b_val is not None:
                    entry["bytes_received"] = (entry["bytes_received"] or 0) + b_val
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
            "delay_ms_mean": round(sum(delays) / len(delays), 3) if delays else None,
            "delay_ms_p95": round(_percentile(delays, 0.95), 3) if delays else None,
            "delay_ms_max": round(delays[-1], 3) if delays else None,
        }
    return seq_stats


def _resolve_recv_metrics(
    bysend_info: dict | None,
    seq_info: dict | None,
    *,
    has_any_recv_source: bool,
) -> tuple[int | None, int | None]:
    # CF-29: Missing receiver logs -> None (never confuse missing evidence with observed 0)
    if not has_any_recv_source:
        return None, None

    # CF-30: Prefer unique sequence count from packet log to avoid duplicate/retrans inflation
    if seq_info is not None and seq_info.get("unique_seqs_received") is not None:
        recv_pkts: int | None = int(seq_info["unique_seqs_received"])
    elif bysend_info is not None and bysend_info.get("packets_received") is not None:
        recv_pkts = int(bysend_info["packets_received"])
    else:
        recv_pkts = None

    # CF-31: Keep bytes_received as None when byte counter is missing (do not coerce to 0)
    if bysend_info is not None and bysend_info.get("bytes_received") is not None:
        recv_bytes: int | None = int(bysend_info["bytes_received"])
    else:
        recv_bytes = None

    return recv_pkts, recv_bytes


def build_correlated_rows(
    man: dict,
    groups: list[dict],
    req_map: dict[tuple[int, str], dict],
    sent_map: dict[tuple[int, str], dict],
    bysend_map: dict[tuple[int, str], dict],
    seq_stats_map: dict[tuple[int, str], dict],
    *,
    include_drain: bool = False,
) -> tuple[list[dict], list[dict]]:
    fg_meta = {g["flow_group_id"]: g for g in groups}
    warmup_s = int(man.get("warmup", 60))
    measurement_s = int(man.get("measurement", 360))
    drain_s = int(man.get("drain", 10))
    active_bins = warmup_s + measurement_s
    total_bins = active_bins + drain_s
    batch_bins = total_bins if include_drain else active_bins
    origin_ts = _resolve_traffic_origin_ts(man)

    all_keys = set(sent_map) | set(bysend_map) | set(seq_stats_map)
    for t in range(total_bins):
        for g in groups:
            all_keys.add((t, g["flow_group_id"]))

    has_any_recv_source = bool(bysend_map or seq_stats_map)
    detail_rows: list[dict] = []
    traffic_rows: list[dict] = []

    for t, fg in sorted(all_keys, key=lambda item: (item[0], item[1])):
        win_start_utc, win_end_utc = _fmt_utc_1s_window(origin_ts, t)
        meta = fg_meta.get(fg, {})
        req = req_map.get((t, fg), {})
        sent = sent_map.get(
            (t, fg),
            {
                "src_host": meta.get("src_host", ""),
                "packets_sent": None,
                "bytes_sent": None,
                "late_packets": None,
                "has_sent_log": False,
            },
        )
        seq_info = seq_stats_map.get((t, fg))
        bysend_info = bysend_map.get((t, fg))

        recv_pkts, recv_bytes = _resolve_recv_metrics(
            bysend_info,
            seq_info,
            has_any_recv_source=has_any_recv_source,
        )

        # CF-29: Missing sender logs -> None instead of fabricating 0
        if sent.get("has_sent_log"):
            pkts_sent: int | None = sent.get("packets_sent")
            bytes_sent: int | None = sent.get("bytes_sent")
            late_pkts: int | None = sent.get("late_packets")
        else:
            pkts_sent, bytes_sent, late_pkts = None, None, None

        actual_mbps = round((recv_bytes * 8.0) / 1e6, 6) if recv_bytes is not None else None
        # CF-30: Denominator == 0 -> loss_ratio is None (N/A), never 0.0
        cohort_status = "MISSING_EVIDENCE"
        if pkts_sent is not None and recv_pkts is not None and recv_pkts > pkts_sent:
            cohort_status = "INVALID_RECEIVE_COUNT"
        elif pkts_sent is not None and recv_pkts is not None and pkts_sent > 0:
            cohort_status = "VALID"
        elif pkts_sent == 0 and recv_pkts == 0:
            cohort_status = "NO_SENT_PACKETS"
        if cohort_status == "VALID":
            lost_cohort: int | None = pkts_sent - recv_pkts
            loss_ratio: float | None = round(lost_cohort / pkts_sent, 6)
        else:
            lost_cohort = None
            loss_ratio = None

        detail_rows.append({
            "run_id": man["run_id"],
            "cohort_bin": t,
            "window_start_utc": win_start_utc,
            "window_end_utc": win_end_utc,
            "flow_group_id": fg,
            "src_host": sent.get("src_host") or meta.get("src_host", ""),
            "dst_host": meta.get("dst_host", ""),
            "requested_mbps": req.get("requested_mbps"),
            "traffic_actual_mbps": actual_mbps,
            "packets_sent": pkts_sent,
            "bytes_sent": bytes_sent,
            "late_packets": late_pkts,
            "packets_received_cohort": recv_pkts,
            "bytes_received": recv_bytes,
            "packets_lost_cohort": lost_cohort,
            "loss_ratio_cohort": loss_ratio,
            "cohort_status": cohort_status,
            "unique_seqs_received": int(seq_info["unique_seqs_received"]) if seq_info else None,
            "duplicate_or_retrans_packets": int(seq_info["duplicate_or_retrans_packets"]) if seq_info else None,
            "seq_min": seq_info["seq_min"] if seq_info else None,
            "seq_max": seq_info["seq_max"] if seq_info else None,
            "delay_ms_mean": seq_info["delay_ms_mean"] if seq_info else None,
            "delay_ms_p95": seq_info["delay_ms_p95"] if seq_info else None,
            "delay_ms_max": seq_info["delay_ms_max"] if seq_info else None,
        })

        if t < batch_bins:
            traffic_rows.append({
                "flow_group_id": fg,
                "window_start_utc": win_start_utc,
                "window_end_utc": win_end_utc,
                "sent_bytes": bytes_sent,
                "received_bytes": recv_bytes,
                "sent_packets": pkts_sent,
                "received_packets": recv_pkts,
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
    ap.add_argument("--include-drain", action="store_true", help="Include drain bins in emitted TrafficBatch rows")
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

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
        man,
        groups,
        req_map,
        sent_map,
        bysend_map,
        seq_stats_map,
        include_drain=args.include_drain,
    )

    csv_path = os.path.join(outdir, "correlated_traffic.csv")
    jsonl_path = os.path.join(outdir, "correlated_traffic.jsonl")
    outbox_path = os.path.join(outdir, "traffic_outbox.jsonl")
    outbox = TrafficOutbox(outbox_path)

    fieldnames = [
        "run_id", "cohort_bin", "window_start_utc", "window_end_utc",
        "flow_group_id", "src_host", "dst_host", "requested_mbps", "traffic_actual_mbps",
        "packets_sent", "bytes_sent", "late_packets",
        "packets_received_cohort", "bytes_received", "packets_lost_cohort",
        "loss_ratio_cohort", "cohort_status", "unique_seqs_received", "duplicate_or_retrans_packets",
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
        flush_traffic_outbox(
            outbox,
            args.traffic_url,
            api_key=api_key,
            timeout=args.timeout,
            backoff_base=args.backoff,
        )
        if not chunks:
            print("[!] Warning: skipping POST /v1/network/traffic because rows is empty", file=sys.stderr)
            return
        for chunk_idx, chunk in chunks:
            if STOP:
                break
            batch_key = f"{man['run_id']}:traffic:{chunk_idx}"
            payload = {"run_id": man["run_id"], "rows": chunk}
            _send_or_queue_batch(
                batch_key=batch_key,
                payload=payload,
                outbox=outbox,
                traffic_url=args.traffic_url,
                api_key=api_key,
                timeout=args.timeout,
                max_retries=args.retries,
                backoff_base=args.backoff,
            )

        remaining = len(outbox.items())
        if remaining:
            print(
                f"[!] Completed with {remaining} unacked TrafficBatch chunk(s) in {outbox_path}",
                file=sys.stderr,
            )


if __name__ == "__main__":
    main()
