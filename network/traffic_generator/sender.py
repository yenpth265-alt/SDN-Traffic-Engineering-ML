#!/usr/bin/env python3
"""UDP Traffic Generator: sends scheduled traffic for flow groups originating from ONE host (v1.4 compliant).

Usage (run on virtual source host, e.g., h1):
    python3 sender.py --host h1 --manifest run_manifest.json --schedule demand_plan.csv --schedule-id S1_seed01

Outputs in results/<run_id>/:
    requested_<host>.csv         : time_offset_s,flow_group_id,requested_mbps,reason,recorded_at_utc
    sent_<host>.csv              : time_offset_s,flow_group_id,packets_sent,bytes_sent,late_packets
    releases_<host>.jsonl        : Online DemandRelease events emitted strictly after each 1s bin completes
    demand_outbox_<host>.jsonl   : Pending unacked DemandRelease events for replay with original ID & payload
"""

import argparse
import csv
from datetime import datetime, timezone
import json
import math
import os
import signal
import socket
import sys
import time
import urllib.error
import urllib.request

from common import HEADER, iso_utc, load_flow_groups, load_manifest

STOP = False


def _stop(*_):
    global STOP
    STOP = True


def _parse_utc_to_epoch(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        pass
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        return None


def _resolve_traffic_origin_ts(man: dict) -> float:
    for key in ("traffic_started_at_utc", "traffic_start_utc", "t0"):
        ts = _parse_utc_to_epoch(man.get(key))
        if ts is not None:
            return ts
    raise KeyError("Manifest missing valid traffic_started_at_utc or t0")


def _to_rfc3339_utc(ts_or_str: float | str) -> str:
    if isinstance(ts_or_str, (int, float)):
        dt = datetime.fromtimestamp(float(ts_or_str), tz=timezone.utc)
        return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")
    s = str(ts_or_str).strip()
    if s.endswith("+00:00"):
        return s[:-6] + "Z"
    return s


def _parse_optional_mbps(raw_val) -> float | None:
    if raw_val is None:
        return None
    s = str(raw_val).strip()
    if not s or s.lower() in ("null", "none", "nan", "na"):
        return None
    val = float(s)
    if not math.isfinite(val) or val < 0.0:
        return None
    return float(val)


def load_schedule(
    path: str,
    wanted: set[str],
    schedule_id: str | None = None,
) -> dict[tuple[int, str], float | None]:
    """Loads demand schedule for wanted flow_group_ids; returns None when value is missing/unavailable."""
    sched: dict[tuple[int, str], float | None] = {}
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        has_sched_col = reader.fieldnames and "schedule_id" in reader.fieldnames
        if has_sched_col and not schedule_id:
            sys.exit("Error: schedule CSV contains 'schedule_id' column; specify --schedule-id or set it in manifest.")
        for r in reader:
            if has_sched_col and r["schedule_id"].strip() != schedule_id:
                continue
            fg = r["flow_group_id"].strip()
            if fg in wanted:
                t = int(float(r["time_offset_s"]))
                sched[(t, fg)] = _parse_optional_mbps(r.get("requested_mbps"))
    return sched


class DemandOutbox:
    """Persists unacknowledged DemandRelease payloads so retries keep exact event_id and content."""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._pending: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                eid = rec.get("event_id")
                payload = rec.get("payload")
                if eid and isinstance(payload, dict):
                    self._pending[eid] = payload

    def _rewrite(self) -> None:
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            for eid, payload in self._pending.items():
                f.write(json.dumps({"event_id": eid, "payload": payload}, separators=(",", ":")) + "\n")
        os.replace(tmp_path, self.path)

    def add_if_missing(self, payload: dict) -> None:
        eid = payload["event_id"]
        if eid not in self._pending:
            self._pending[eid] = payload
            self._rewrite()

    def ack(self, event_id: str) -> None:
        if event_id in self._pending:
            del self._pending[event_id]
            self._rewrite()

    def items(self) -> list[dict]:
        return list(self._pending.values())


def build_demand_event(
    *,
    run_id: str,
    flow_group_id: str,
    bin_index: int,
    window_start_utc: str,
    window_end_utc: str,
    released_at_utc: str,
    requested_mbps: float | None,
    missing_reason: str = "missing_demand_value",
) -> dict:
    """Builds DemandRelease payload strictly conforming to online_models.Demand."""
    event = {
        "event_id": f"network:{run_id}:demand:{flow_group_id}:{bin_index}",
        "run_id": run_id,
        "flow_group_id": flow_group_id,
        "bin_index": int(bin_index),
        "source": "network",
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
        "requested_mbps": float(requested_mbps) if requested_mbps is not None else None,
        "released_at_utc": released_at_utc,
    }
    if requested_mbps is None:
        event["reason"] = missing_reason
    return event


def _post_demand_once(
    url: str,
    body_bytes: bytes,
    *,
    api_key: str | None = None,
    timeout: float = 1.0,
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


def _send_or_queue_event(
    event: dict,
    outbox: DemandOutbox,
    release_fp=None,
    release_url: str | None = None,
    *,
    api_key: str | None = None,
    timeout: float = 1.0,
    max_retries: int = 3,
    backoff_base: float = 0.15,
) -> None:
    """Appends event to local release log and POSTs with exact same event_id & payload."""
    payload_str = json.dumps(event, separators=(",", ":"))
    if release_fp is not None:
        release_fp.write(payload_str + "\n")
        release_fp.flush()

    if not release_url:
        return

    body_bytes = payload_str.encode("utf-8")
    last_outcome = "retryable"
    last_status: int | None = None
    last_detail = ""

    for attempt in range(max_retries + 1):
        outcome, status_code, detail = _post_demand_once(
            url=release_url,
            body_bytes=body_bytes,
            api_key=api_key,
            timeout=timeout,
        )
        last_outcome, last_status, last_detail = outcome, status_code, detail
        if outcome == "acked":
            outbox.ack(event["event_id"])
            return
        if outcome in ("conflict_409", "invalid_422"):
            break
        if attempt < max_retries and not STOP:
            time.sleep(backoff_base * (2 ** attempt))

    if last_outcome == "conflict_409":
        print(
            f"[!] 409 Conflict for {event['event_id']} (existing record differs): {last_detail}",
            file=sys.stderr,
        )
    elif last_outcome == "invalid_422":
        print(
            f"[!] 422 Invalid payload for {event['event_id']}: {last_detail}",
            file=sys.stderr,
        )
    else:
        outbox.add_if_missing(event)
        print(
            f"[!] Queued {event['event_id']} in outbox (Data unavailable, status={last_status}): {last_detail}",
            file=sys.stderr,
        )


def flush_outbox(
    outbox: DemandOutbox,
    release_url: str | None,
    *,
    api_key: str | None = None,
    timeout: float = 1.0,
    backoff_base: float = 0.15,
) -> None:
    """Replays unacked items in outbox preserving exact event_id and payload."""
    if not release_url:
        return
    for idx, payload in enumerate(outbox.items()):
        if STOP:
            break
        body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        outcome, status_code, detail = _post_demand_once(
            url=release_url,
            body_bytes=body_bytes,
            api_key=api_key,
            timeout=timeout,
        )
        if outcome == "acked":
            outbox.ack(payload["event_id"])
        elif outcome == "conflict_409":
            print(f"[!] Outbox 409 Conflict for {payload['event_id']}: {detail}", file=sys.stderr)
        elif outcome == "invalid_422":
            print(f"[!] Outbox 422 Invalid for {payload['event_id']}: {detail}", file=sys.stderr)
        else:
            print(
                f"[!] Outbox still pending for {payload['event_id']} (status={status_code}): {detail}",
                file=sys.stderr,
            )
            time.sleep(min(1.0, backoff_base * (2 ** min(idx, 4))))
            break


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", required=True, help="Source host name, e.g., h1")
    ap.add_argument("--manifest", required=True, help="Path to run_manifest.json")
    ap.add_argument("--schedule", required=True, help="Path to demand_plan.csv")
    ap.add_argument("--schedule-id", default=None, help="Schedule ID filter (e.g., S1_seed01); falls back to manifest")
    ap.add_argument("--config-dir", default="config", help="Directory containing flow_groups.csv")
    ap.add_argument("--outdir", default=None, help="Output directory (default: results/<run_id>)")
    ap.add_argument("--payload-size", type=int, default=1000, help="UDP payload size in bytes (default: 1000)")
    ap.add_argument("--flow-group", default=None, help="Optional single flow_group_id (e.g., 0x103) for testing")
    ap.add_argument("--dst-override", default=None, help="Override destination IP for single-machine testing")
    ap.add_argument("--host-cap-mbps", type=float, default=10.0, help="Host send capacity cap in Mbps (default: 10.0)")
    ap.add_argument("--release-url", default=None, help="Optional HTTP endpoint (e.g., .../v1/network/demand)")
    ap.add_argument("--api-key", default=None, help="Optional X-API-Key header for HTTP endpoints (fallback: DATA_API_KEY)")
    ap.add_argument("--release-timeout", type=float, default=1.0, help="HTTP timeout in seconds per attempt (default: 1.0)")
    ap.add_argument("--release-retries", type=int, default=3, help="Max HTTP retry attempts for DemandRelease (default: 3)")
    ap.add_argument("--release-backoff", type=float, default=0.15, help="Base backoff in seconds between retries (default: 0.15)")
    ap.add_argument("--missing-reason", default="missing_demand_value", help="Reason sent when requested_mbps is null")
    args = ap.parse_args()

    if args.payload_size < HEADER.size:
        sys.exit(f"payload-size must be >= {HEADER.size}")

    man = load_manifest(args.manifest)
    schedule_id = args.schedule_id or man.get("schedule_id")
    api_key = args.api_key or os.environ.get("DATA_API_KEY")

    groups = [g for g in load_flow_groups(args.config_dir) if g["src_host"] == args.host]
    if args.flow_group:
        target_fg = args.flow_group.strip()
        groups = [g for g in groups if g["flow_group_id"] == target_fg]
    if not groups:
        sys.exit("No matching flow groups found for this source host")

    sched = load_schedule(args.schedule, {g["flow_group_id"] for g in groups}, schedule_id=schedule_id)
    warmup_s = int(man.get("warmup", 60))
    measurement_s = int(man.get("measurement", 360))
    drain_s = int(man.get("drain", 10))
    active_bins = warmup_s + measurement_s

    outdir = args.outdir or os.path.join("results", man["run_id"])
    os.makedirs(outdir, exist_ok=True)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)
    pad = bytes(args.payload_size - HEADER.size)

    state = {}
    for k, g in enumerate(groups):
        state[g["flow_group_id"]] = {
            "stt": int(g["stt"]),
            "addr": (args.dst_override or g["dst_ip"], int(g["udp_port"])),
            "seq": 0,
            "carry": 0.0,
            "phase": k / len(groups),
        }

    missing = sum(1 for g in groups for t in range(active_bins) if sched.get((t, g["flow_group_id"])) is None)
    if missing:
        print(
            f"[!] Warning: schedule missing {missing} (second, flow_group) entries; "
            f"sending requested_mbps=null with reason='{args.missing_reason}'",
            file=sys.stderr,
        )

    over = [
        t for t in range(active_bins)
        if sum((sched.get((t, g["flow_group_id"])) or 0.0) for g in groups) > args.host_cap_mbps * 1.001
    ]
    if over:
        print(
            f"[!] Warning: {len(over)} seconds exceed {args.host_cap_mbps} Mbps host cap (e.g., t={over[0]})",
            file=sys.stderr,
        )

    t0 = _resolve_traffic_origin_ts(man)
    if t0 > time.time():
        print(f"[*] Waiting until {iso_utc(t0)} ({t0 - time.time():.1f} s remaining)")
        while time.time() < t0 and not STOP:
            time.sleep(min(0.2, max(0.0, t0 - time.time())))
    elif time.time() - t0 > 1.0:
        sys.exit("traffic origin has already passed; generate a new manifest")
    elif time.time() - t0 > 0.2:
        print(f"[!] Starting {time.time() - t0:.2f} s late relative to traffic origin", file=sys.stderr)

    req_path = os.path.join(outdir, f"requested_{args.host}.csv")
    sent_path = os.path.join(outdir, f"sent_{args.host}.csv")
    rel_path = os.path.join(outdir, f"releases_{args.host}.jsonl")
    outbox_path = os.path.join(outdir, f"demand_outbox_{args.host}.jsonl")
    outbox = DemandOutbox(outbox_path)

    with open(req_path, "w", encoding="utf-8", newline="") as fr, \
         open(sent_path, "w", encoding="utf-8", newline="") as fs, \
         open(rel_path, "w", encoding="utf-8") as frel:
        wr = csv.writer(fr, lineterminator="\n")
        ws = csv.writer(fs, lineterminator="\n")
        wr.writerow(["time_offset_s", "flow_group_id", "requested_mbps", "reason", "recorded_at_utc"])
        ws.writerow(["time_offset_s", "flow_group_id", "packets_sent", "bytes_sent", "late_packets"])

        flush_outbox(
            outbox,
            args.release_url,
            api_key=api_key,
            timeout=args.release_timeout,
            backoff_base=args.release_backoff,
        )

        for t in range(active_bins):
            if STOP:
                break
            sec_start = t0 + t
            events, counts, late = [], {}, {}
            for fg, st in state.items():
                raw_mbps = sched.get((t, fg))
                mbps = float(raw_mbps) if raw_mbps is not None else 0.0
                st["carry"] += mbps * 1e6 / 8 / args.payload_size
                n = int(st["carry"])
                st["carry"] -= n
                counts[fg], late[fg] = 0, 0
                for i in range(n):
                    events.append((sec_start + (i + st["phase"]) / n, fg))
            events.sort()

            for when, fg in events:
                if STOP:
                    break
                delta = when - time.time()
                if delta > 0.0004:
                    time.sleep(delta - 0.0002)
                elif delta < -0.005:
                    late[fg] += 1
                st = state[fg]
                st["seq"] += 1
                now = time.time()
                try:
                    sock.sendto(HEADER.pack(st["stt"], t, st["seq"], now) + pad, st["addr"])
                    counts[fg] += 1
                except OSError as e:
                    print(f"[!] Send error {fg} seq {st['seq']}: {e}", file=sys.stderr)

            while time.time() < sec_start + 1.0 and not STOP:
                time.sleep(min(0.05, max(0.0, sec_start + 1.0 - time.time())))

            win_end_ts = sec_start + 1.0
            release_ts = max(time.time(), win_end_ts)
            released_at = _to_rfc3339_utc(release_ts)
            win_start_utc = _to_rfc3339_utc(sec_start)
            win_end_utc = _to_rfc3339_utc(win_end_ts)

            for fg in state:
                raw_req = sched.get((t, fg))
                req_val = float(round(raw_req, 6)) if raw_req is not None else None
                reason_str = "" if req_val is not None else args.missing_reason
                wr.writerow([
                    t,
                    fg,
                    f"{req_val:.6f}" if req_val is not None else "",
                    reason_str,
                    released_at,
                ])
                ws.writerow([t, fg, counts[fg], counts[fg] * args.payload_size, late[fg]])

                release_event = build_demand_event(
                    run_id=man["run_id"],
                    flow_group_id=fg,
                    bin_index=t,
                    window_start_utc=win_start_utc,
                    window_end_utc=win_end_utc,
                    released_at_utc=released_at,
                    requested_mbps=req_val,
                    missing_reason=args.missing_reason,
                )
                _send_or_queue_event(
                    release_event,
                    outbox,
                    release_fp=frel,
                    release_url=args.release_url,
                    api_key=api_key,
                    timeout=args.release_timeout,
                    max_retries=args.release_retries,
                    backoff_base=args.release_backoff,
                )

            fr.flush()
            fs.flush()

            if outbox.items() and not STOP:
                flush_outbox(
                    outbox,
                    args.release_url,
                    api_key=api_key,
                    timeout=args.release_timeout,
                    backoff_base=args.release_backoff,
                )

        # Drain phase: no new UDP traffic generation; wait out drain window and flush pending outbox
        if drain_s > 0 and not STOP:
            drain_end = t0 + active_bins + drain_s
            while time.time() < drain_end and not STOP:
                if outbox.items():
                    flush_outbox(
                        outbox,
                        args.release_url,
                        api_key=api_key,
                        timeout=args.release_timeout,
                        backoff_base=args.release_backoff,
                    )
                time.sleep(min(0.25, max(0.0, drain_end - time.time())))

    sock.close()
    remaining_outbox = len(outbox.items())
    if remaining_outbox:
        print(
            f"[!] Completed host {args.host} with {remaining_outbox} unacked demand item(s) in {outbox_path}",
            file=sys.stderr,
        )
    else:
        print(f"[+] Completed host {args.host}: logs saved in {outdir}")


if __name__ == "__main__":
    main()
```[cite: 4]

---

### 2. `correlate_traffic.py` (cập nhật)

```python
#!/usr/bin/env python3
"""Correlates sender and receiver traffic logs by cohort (send_bin) and sequence,
writes `correlated_traffic.csv` and `correlated_traffic.jsonl`, and optionally
POSTs Data-contract `TrafficBatch` payloads (<= 4096 rows per batch) to `/v1/network/traffic`
with persistent outbox replay when Data is unavailable.

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


def _parse_utc_to_epoch(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        pass
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        return None


def _resolve_traffic_origin_ts(man: dict) -> float:
    for key in ("traffic_started_at_utc", "traffic_start_utc", "t0"):
        ts = _parse_utc_to_epoch(man.get(key))
        if ts is not None:
            return ts
    raise KeyError("Manifest missing valid traffic_started_at_utc or t0")


def percentile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def _fmt_utc_1s_window(origin_ts: float, bin_idx: int) -> tuple[str, str]:
    start_dt = datetime.fromtimestamp(origin_ts + bin_idx, tz=timezone.utc)
    end_dt = start_dt + timedelta(seconds=1)
    return (
        start_dt.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        end_dt.isoformat(timespec="microseconds").replace("+00:00", "Z"),
    )


def _iter_chunks(items: list[dict], chunk_size: int):
    for i in range(0, len(items), chunk_size):
        yield i // chunk_size, items[i : i + chunk_size]


class TrafficOutbox:
    """Persists unacknowledged TrafficBatch payloads so retries keep exact content."""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._pending: dict[str, dict] = {}
        self._load()

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
                f.write(json.dumps({"batch_key": batch_key, "payload": payload}, separators=(",", ":")) + "\n")
        os.replace(tmp_path, self.path)

    def add_if_missing(self, batch_key: str, payload: dict) -> None:
        if batch_key not in self._pending:
            self._pending[batch_key] = payload
            self._rewrite()

    def ack(self, batch_key: str) -> None:
        if batch_key in self._pending:
            del self._pending[batch_key]
            self._rewrite()

    def items(self) -> list[tuple[str, dict]]:
        return list(self._pending.items())


def _post_once(
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
    body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    last_outcome = "retryable"
    last_status: int | None = None
    last_detail = ""

    for attempt in range(max_retries + 1):
        outcome, status_code, detail = _post_once(
            url=traffic_url,
            body_bytes=body_bytes,
            api_key=api_key,
            timeout=timeout,
        )
        last_outcome, last_status, last_detail = outcome, status_code, detail
        if outcome == "acked":
            outbox.ack(batch_key)
            print(f"[+] POST /v1/network/traffic {batch_key} ({len(payload['rows'])} rows) succeeded (status={status_code})")
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
            f"[!] Queued TrafficBatch {batch_key} in outbox (Data unavailable, status={last_status}): {last_detail}",
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
        outcome, status_code, detail = _post_once(
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
                raw_req = r.get("requested_mbps", "").strip()
                req_val = float(raw_req) if raw_req not in ("", "null", "None", "nan", "NaN") else None
                req_map[(t, fg)] = {
                    "requested_mbps": req_val,
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
                        "packets_sent": 0,
                        "bytes_sent": 0,
                        "late_packets": 0,
                        "has_sent_log": True,
                    },
                )
                entry["packets_sent"] += int(r["packets_sent"])
                entry["bytes_sent"] += int(r["bytes_sent"])
                entry["late_packets"] += int(r.get("late_packets", 0))
                entry["has_sent_log"] = True
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
                raw_pkts = str(r.get("packets_received", "")).strip()
                pkts = int(raw_pkts) if raw_pkts not in ("", "null", "None", "nan", "NaN") else None
                raw_bytes = str(r.get("bytes_received", "")).strip()
                b_val = int(raw_bytes) if raw_bytes not in ("", "null", "None", "nan", "NaN") else None

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
                "packets_sent": 0,
                "bytes_sent": 0,
                "late_packets": 0,
                "has_sent_log": False,
            },
        )
        seq_info = seq_stats_map.get((t, fg))
        bysend_info = bysend_map.get((t, fg))

        if bysend_info is not None and bysend_info.get("packets_received") is not None:
            recv_cohort: int | None = int(bysend_info["packets_received"])
        elif seq_info is not None:
            recv_cohort = int(seq_info["unique_seqs_received"])
        elif has_any_recv_source:
            recv_cohort = None
        else:
            recv_cohort = 0

        if bysend_info is not None and bysend_info.get("bytes_received") is not None:
            recv_bytes_val: int | None = int(bysend_info["bytes_received"])
        elif recv_cohort is None:
            recv_bytes_val = None
        else:
            recv_bytes_val = 0

        pkts_sent: int | None = int(sent["packets_sent"]) if sent.get("has_sent_log") else (None if sent_map else 0)
        bytes_sent: int | None = int(sent["bytes_sent"]) if sent.get("has_sent_log") else (None if sent_map else 0)

        traffic_actual_mbps = round((recv_bytes_val * 8.0) / 1e6, 6) if recv_bytes_val is not None else None
        if pkts_sent is not None and recv_cohort is not None:
            lost_cohort: int | None = max(0, pkts_sent - recv_cohort)
            loss_ratio: float | None = round(lost_cohort / pkts_sent, 6) if pkts_sent > 0 else 0.0
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
            "traffic_actual_mbps": traffic_actual_mbps,
            "packets_sent": pkts_sent,
            "bytes_sent": bytes_sent,
            "late_packets": int(sent["late_packets"]) if sent.get("has_sent_log") else None,
            "packets_received_cohort": recv_cohort,
            "bytes_received": recv_bytes_val,
            "packets_lost_cohort": lost_cohort,
            "loss_ratio_cohort": loss_ratio,
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
                "received_bytes": recv_bytes_val,
                "sent_packets": pkts_sent,
                "received_packets": recv_cohort,
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
```[cite: 4]

Nếu bạn cũng muốn cập nhật tương tự cho `telemetry_collector.py` (ưu tiên đọc `traffic_started_at_utc` hoặc mốc gốc tương ứng khi tính `t0`) hoặc cần hỗ trợ gì thêm cho giai đoạn chốt G0/G1, cứ bảo mình nhé.
