#!/usr/bin/env python3
"""UDP Traffic Generator: sends scheduled traffic for flow groups originating from ONE host (v1.4 compliant).

Usage (run on virtual source host, e.g., h1):
    python3 sender.py --host h1 --manifest run_manifest.json --schedule demand_plan.csv --schedule-id S1_seed01

Outputs in results/<run_id>/:
    requested_<host>.csv : time_offset_s,flow_group_id,requested_mbps,available_at_utc
    sent_<host>.csv      : time_offset_s,flow_group_id,packets_sent,bytes_sent,late_packets
    releases_<host>.jsonl: Online DemandRelease events emitted strictly after each 1s bin completes
"""

import argparse
import csv
from datetime import datetime, timezone
import json
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


def _to_rfc3339_utc(ts_or_str: float | str) -> str:
    if isinstance(ts_or_str, (int, float)):
        dt = datetime.fromtimestamp(float(ts_or_str), tz=timezone.utc)
        return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")
    s = str(ts_or_str).strip()
    if s.endswith("+00:00"):
        return s[:-6] + "Z"
    return s


def load_schedule(path: str, wanted: set[str], schedule_id: str | None = None) -> dict[tuple[int, str], float]:
    """Loads demand schedule for wanted flow_group_ids, filtering by schedule_id if present."""
    sched = {}
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
                sched[(int(float(r["time_offset_s"])), fg)] = float(r["requested_mbps"])
    return sched


def _post_with_retry(
    url: str,
    body_bytes: bytes,
    *,
    api_key: str | None = None,
    timeout: float = 1.0,
    max_retries: int = 3,
    backoff_base: float = 0.15,
) -> tuple[bool, int | None, str]:
    """POSTs JSON payload with bounded retry on transient/network/5xx/429 errors."""
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


def emit_release_event(
    event: dict,
    release_fp=None,
    release_url: str | None = None,
    *,
    api_key: str | None = None,
    timeout: float = 1.0,
    max_retries: int = 3,
    backoff_base: float = 0.15,
):
    """Writes online DemandRelease event to local outbox and optionally POSTs with retry."""
    payload = json.dumps(event, separators=(",", ":"))
    if release_fp is not None:
        release_fp.write(payload + "\n")
        release_fp.flush()
    if release_url:
        ok, status_code, detail = _post_with_retry(
            url=release_url,
            body_bytes=payload.encode("utf-8"),
            api_key=api_key,
            timeout=timeout,
            max_retries=max_retries,
            backoff_base=backoff_base,
        )
        if not ok:
            print(
                f"[!] Warning: failed to POST DemandRelease {event['event_id']} "
                f"(status={status_code}): {detail}",
                file=sys.stderr,
            )


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
    ap.add_argument("--release-url", default=None, help="Optional HTTP endpoint to push DemandRelease events")
    ap.add_argument("--api-key", default=None, help="Optional X-API-Key header for HTTP endpoints (fallback: DATA_API_KEY)")
    ap.add_argument("--release-timeout", type=float, default=1.0, help="HTTP timeout in seconds per attempt (default: 1.0)")
    ap.add_argument("--release-retries", type=int, default=3, help="Max HTTP retry attempts for DemandRelease (default: 3)")
    ap.add_argument("--release-backoff", type=float, default=0.15, help="Base backoff in seconds between retries (default: 0.15)")
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
    total = man["warmup"] + man["measurement"]
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

    missing = sum(1 for g in groups for t in range(total) if (t, g["flow_group_id"]) not in sched)
    if missing:
        print(f"[!] Warning: schedule missing {missing} (second, flow_group) entries; treating as 0 Mbps", file=sys.stderr)

    over = [
        t for t in range(total)
        if sum(sched.get((t, g["flow_group_id"]), 0.0) for g in groups) > args.host_cap_mbps * 1.001
    ]
    if over:
        print(
            f"[!] Warning: {len(over)} seconds exceed {args.host_cap_mbps} Mbps host cap (e.g., t={over[0]})",
            file=sys.stderr,
        )

    t0 = float(man["t0"])
    if t0 > time.time():
        print(f"[*] Waiting until {iso_utc(t0)} ({t0 - time.time():.1f} s remaining)")
        while time.time() < t0 and not STOP:
            time.sleep(min(0.2, max(0.0, t0 - time.time())))
    elif time.time() - t0 > 1.0:
        sys.exit("run_start_utc has already passed; generate a new manifest")
    elif time.time() - t0 > 0.2:
        print(f"[!] Starting {time.time() - t0:.2f} s late relative to run_start_utc", file=sys.stderr)

    req_path = os.path.join(outdir, f"requested_{args.host}.csv")
    sent_path = os.path.join(outdir, f"sent_{args.host}.csv")
    rel_path = os.path.join(outdir, f"releases_{args.host}.jsonl")

    with open(req_path, "w", encoding="utf-8", newline="") as fr, \
         open(sent_path, "w", encoding="utf-8", newline="") as fs, \
         open(rel_path, "w", encoding="utf-8") as frel:
        wr = csv.writer(fr, lineterminator="\n")
        ws = csv.writer(fs, lineterminator="\n")
        wr.writerow(["time_offset_s", "flow_group_id", "requested_mbps", "available_at_utc"])
        ws.writerow(["time_offset_s", "flow_group_id", "packets_sent", "bytes_sent", "late_packets"])

        for t in range(total):
            if STOP:
                break
            sec_start = t0 + t
            events, counts, late = [], {}, {}
            for fg, st in state.items():
                mbps = sched.get((t, fg), 0.0)
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

            # Wait until bin [t, t+1) completes before releasing demand observation
            while time.time() < sec_start + 1.0 and not STOP:
                time.sleep(min(0.05, max(0.0, sec_start + 1.0 - time.time())))

            win_end_ts = sec_start + 1.0
            release_ts = max(time.time(), win_end_ts)
            released_at = _to_rfc3339_utc(release_ts)
            win_start_utc = _to_rfc3339_utc(sec_start)
            win_end_utc = _to_rfc3339_utc(win_end_ts)

            for fg in state:
                req_val = float(round(sched.get((t, fg), 0.0), 6))
                wr.writerow([t, fg, f"{req_val:.6f}", released_at])
                ws.writerow([t, fg, counts[fg], counts[fg] * args.payload_size, late[fg]])

                release_event = {
                    "event_id": f"network:{man['run_id']}:demand:{fg}:{t}",
                    "run_id": man["run_id"],
                    "flow_group_id": fg,
                    "bin_index": t,
                    "source": "network",
                    "window_start_utc": win_start_utc,
                    "window_end_utc": win_end_utc,
                    "requested_mbps": req_val,
                    "released_at_utc": released_at,
                }
                emit_release_event(
                    release_event,
                    release_fp=frel,
                    release_url=args.release_url,
                    api_key=api_key,
                    timeout=args.release_timeout,
                    max_retries=args.release_retries,
                    backoff_base=args.release_backoff,
                )

            fr.flush()
            fs.flush()

    sock.close()
    print(f"[+] Completed host {args.host}: logs saved in {outdir}")


if __name__ == "__main__":
    main()
