#!/usr/bin/env python3
"""UDP Traffic Receiver: receives traffic destined for ONE host and logs per-second metrics (v1.4 compliant).

Usage (run on virtual destination host, e.g., h5; start BEFORE sender):
    python3 receiver.py --host h5 --manifest run_manifest.json
    python3 receiver.py --host all --manifest run_manifest.json
    python3 receiver.py --host h5 --manifest run_manifest.json --packet-log

Outputs in results/<run_id>/:
  recv_<host>.csv        : time_offset_s,flow_group_id,packets_received,bytes_received,
                           goodput_mbps,delay_ms_mean,delay_ms_p95,delay_ms_max,seq_min,seq_max
  recv_bysend_<host>.csv : time_offset_s,flow_group_id,packets_received
  recvpkts_<host>.csv    : flow_group_id,seq,send_bin,recv_offset_s,delay_ms (when --packet-log is set)
"""

import argparse
import csv
import os
import selectors
import signal
import socket
import sys
import time

from common import HEADER, load_flow_groups, load_manifest

STOP = False
MAX_PKTS_PER_READY = 2000


def _stop(*_):
    global STOP
    STOP = True


def percentile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", required=True, help="Destination host, e.g., h5, or 'all'")
    ap.add_argument("--manifest", required=True, help="Path to run_manifest.json")
    ap.add_argument("--config-dir", default="config", help="Directory containing flow_groups.csv")
    ap.add_argument("--outdir", default=None, help="Output directory (default: results/<run_id>)")
    ap.add_argument("--bind-ip", default="0.0.0.0", help="IP address to bind UDP sockets")
    ap.add_argument("--grace", type=float, default=2.0, help="Extra seconds to wait after drain phase")
    ap.add_argument("--packet-log", action="store_true", help="Enable per-packet delay logging")
    args = ap.parse_args()

    man = load_manifest(args.manifest)
    groups = [g for g in load_flow_groups(args.config_dir) if args.host == "all" or g["dst_host"] == args.host]
    if not groups:
        sys.exit("No matching flow groups found for this destination host")

    outdir = args.outdir or os.path.join("results", man["run_id"])
    os.makedirs(outdir, exist_ok=True)
    t0 = man["t0"]
    stop_at = t0 + man["warmup"] + man["measurement"] + man["drain"] + args.grace

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    sel = selectors.DefaultSelector()
    by_stt = {}
    for g in groups:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        s.bind((args.bind_ip, int(g["udp_port"])))
        s.setblocking(False)
        sel.register(s, selectors.EVENT_READ, g)
        by_stt[int(g["stt"])] = g["flow_group_id"]

    print(f"[*] Listening on {len(groups)} UDP ports for host {args.host}; stops in {stop_at - time.time():.0f} s")

    bins = {}
    by_send = {}
    bad = 0

    path = os.path.join(outdir, f"recv_{args.host}.csv")
    fp = open(path, "w", encoding="utf-8", newline="")
    w = csv.writer(fp, lineterminator="\n")
    w.writerow([
        "time_offset_s", "flow_group_id", "packets_received", "bytes_received",
        "goodput_mbps", "delay_ms_mean", "delay_ms_p95", "delay_ms_max", "seq_min", "seq_max"
    ])

    pk_w = None
    pk_fp = None
    if args.packet_log:
        pk_fp = open(os.path.join(outdir, f"recvpkts_{args.host}.csv"), "w", encoding="utf-8", newline="")
        pk_w = csv.writer(pk_fp, lineterminator="\n")
        pk_w.writerow(["flow_group_id", "seq", "send_bin", "recv_offset_s", "delay_ms"])

    def flush(upto):
        for k in sorted(k for k in bins if upto is None or k[0] <= upto):
            b = bins.pop(k)
            d = sorted(b["delays"])
            goodput_mbps = (b["bytes"] * 8.0) / 1e6
            w.writerow([
                k[0], k[1], b["n"], b["bytes"], f"{goodput_mbps:.6f}",
                f"{sum(d) / len(d):.3f}", f"{percentile(d, 0.95):.3f}", f"{d[-1]:.3f}",
                b["smin"], b["smax"]
            ])
        fp.flush()

    try:
        while not STOP and time.time() < stop_at:
            for key, _ in sel.select(timeout=0.2):
                sock = key.fileobj
                for _ in range(MAX_PKTS_PER_READY):
                    try:
                        data, _addr = sock.recvfrom(65535)
                    except BlockingIOError:
                        break
                    now = time.time()
                    if len(data) < HEADER.size:
                        bad += 1
                        continue
                    stt, send_bin, seq, send_time = HEADER.unpack_from(data)
                    fg = by_stt.get(stt)
                    if fg is None or fg != key.data["flow_group_id"]:
                        bad += 1
                        continue
                    t = int(now - t0) if now >= t0 else -1
                    delay = (now - send_time) * 1000.0
                    b = bins.setdefault((t, fg), {"n": 0, "bytes": 0, "delays": [], "smin": seq, "smax": seq})
                    b["n"] += 1
                    b["bytes"] += len(data)
                    b["delays"].append(delay)
                    b["smin"] = min(b["smin"], seq)
                    b["smax"] = max(b["smax"], seq)
                    by_send[(send_bin, fg)] = by_send.get((send_bin, fg), 0) + 1
                    if pk_w:
                        pk_w.writerow([fg, seq, send_bin, f"{now - t0:.6f}", f"{delay:.3f}"])
            flush(int(time.time() - t0) - 2)
        flush(None)
    finally:
        fp.close()
        if pk_fp:
            pk_fp.close()
        with open(os.path.join(outdir, f"recv_bysend_{args.host}.csv"), "w", encoding="utf-8", newline="") as f2:
            w2 = csv.writer(f2, lineterminator="\n")
            w2.writerow(["time_offset_s", "flow_group_id", "packets_received"])
            for (t, fg), n in sorted(by_send.items()):
                w2.writerow([t, fg, n])

    print(f"[+] Completed host {args.host}: {path}" + (f" ({bad} invalid packets dropped)" if bad else ""))


if __name__ == "__main__":
    main()
