"""Recompute benchmark metrics from packet, sender and directed-link raw logs."""
import argparse
import csv
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import random
import statistics


def csv_rows(paths):
    for path in sorted(paths):
        with path.open(encoding="utf-8-sig", newline="") as stream:
            yield from csv.DictReader(stream)


def percentile(values, q):
    if not values:
        return None
    values = sorted(values)
    position = (len(values) - 1) * q
    lower = int(position)
    return values[lower] + (values[min(lower + 1, len(values) - 1)] - values[lower]) * (position - lower)


def recompute(run_dir, topology, *, packet_deadline_s, minimum_coverage=1.0):
    run_dir = Path(run_dir)
    manifest = json.loads((run_dir / "manifest.json").read_bytes())
    raw = run_dir / "raw"
    phases = manifest.get("phases", {})
    warmup = manifest.get("warmup_s", phases.get("warmup_seconds", 60))
    duration = manifest.get("measurement_s", phases.get("measurement_seconds", 360))
    if not (type(duration) is int and duration > 0 and 0 < minimum_coverage <= 1
            and math.isfinite(packet_deadline_s) and packet_deadline_s >= 0):
        raise ValueError("Invalid metric protocol")
    groups = {r["flow_group_id"] for r in topology["flow_groups"]}
    fabric = {r["link_id"] for r in topology["links"] if r["is_fabric"]}
    expected = {(t, fg) for t in range(warmup, warmup + duration) for fg in groups}
    sent, received, unique_bytes, delays, errors = {}, {}, {}, [], []
    for row in csv_rows(raw.glob("sent_*.csv")):
        key = (int(row["time_offset_s"]), row["flow_group_id"])
        if key not in expected:
            continue
        if key in sent:
            errors.append(f"Duplicate sender cohort {key}")
        count = int(row["packets_sent"])
        if count < 0:
            errors.append(f"Negative sent count {key}")
        sent[key] = count
    for row in csv_rows(raw.glob("recvpkts_*.csv")):
        key = (int(row["send_bin"]), row["flow_group_id"])
        if key not in expected:
            continue
        arrival, delay = float(row["recv_offset_s"]), float(row["delay_ms"])
        if not math.isfinite(arrival) or not math.isfinite(delay) or delay < 0:
            errors.append(f"Invalid packet timestamp {key}")
            continue
        if arrival > key[0] + 1 + packet_deadline_s:
            continue
        seq = int(row["seq"])
        by_seq = received.setdefault(key, {})
        if seq in by_seq:
            continue
        by_seq[seq] = arrival
        delays.append(delay)
        byte_count = row.get("received_bytes")
        if byte_count not in (None, ""):
            byte_count = int(byte_count)
            if byte_count < 0:
                errors.append(f"Invalid packet length {key}")
            unique_bytes[(key, seq)] = byte_count
    packet_files = list(raw.glob("recvpkts_*.csv"))
    covered = set(sent) & expected
    sent_total, received_total = 0, 0
    for key in covered:
        count = len(received.get(key, {}))
        if count > sent[key]:
            errors.append(f"Received unique packets exceed sent packets {key}")
        sent_total += sent[key]
        received_total += count
    # A missing receiver file cannot establish observed zero for that host.
    hosts = {r["dst_host"] for r in topology["flow_groups"]}
    receiver_hosts = {p.stem.removeprefix("recvpkts_") for p in packet_files}
    packet_coverage = len(covered) / len(expected) if expected else 0
    if not hosts <= receiver_hosts:
        packet_coverage = 0
    valid_packets = packet_coverage >= minimum_coverage and not errors
    loss = ((sent_total - received_total) / sent_total) if valid_packets and sent_total > 0 else None
    received_unique_n = sum(len(v) for v in received.values())
    goodput = (sum(unique_bytes.values()) * 8 / 1e6 / duration
               if valid_packets and len(unique_bytes) == received_unique_n else None)

    origin = datetime.fromisoformat(manifest["traffic_started_at_utc"].replace("Z", "+00:00"))
    mlu_by_bin = {}
    telemetry = raw / "link_samples.jsonl"
    if telemetry.exists():
        for line in telemetry.read_text(encoding="utf-8").splitlines():
            batch = json.loads(line)
            if batch.get("run_id") != manifest["run_id"]:
                errors.append("Foreign run in telemetry")
                continue
            as_of = datetime.fromisoformat(batch["sampled_at_utc"].replace("Z", "+00:00"))
            index = batch.get("bin_index", round((as_of - origin).total_seconds()) - 1)
            if not warmup <= index < warmup + duration:
                continue
            samples = [r for r in batch["samples"] if r["link_id"] in fabric]
            values = [r.get("utilization") for r in samples]
            valid = (len(samples) == len(fabric) and {r["link_id"] for r in samples} == fabric
                     and all(type(v) in (int, float) and math.isfinite(v) and v >= 0 for v in values))
            if index in mlu_by_bin:
                errors.append(f"Duplicate telemetry bin {index}")
            if valid:
                mlu_by_bin[index] = max(values)
    link_coverage = len(mlu_by_bin) / duration
    mlu = statistics.mean(mlu_by_bin.values()) if mlu_by_bin and link_coverage >= minimum_coverage else None
    checksums = {str(p.relative_to(run_dir)): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in sorted(raw.rglob("*")) if p.is_file()}
    return {"run_id": manifest["run_id"], "schedule_id": manifest.get("schedule_id"),
            "method_id": manifest.get("method_id", manifest.get("method")),
            "status": "INVALID" if errors else "VALID" if valid_packets and link_coverage >= minimum_coverage else "INCOMPLETE",
            "errors": errors, "coverage": {"packets": packet_coverage, "fabric": link_coverage},
            "sent_packets": sent_total, "received_unique_packets": received_total,
            "loss_ratio": loss, "unique_goodput_mbps": goodput, "mlu_mean": mlu,
            "mlu_p95": percentile(list(mlu_by_bin.values()), .95) if link_coverage >= minimum_coverage else None,
            "packet_delay_p95_ms": percentile(delays, .95) if valid_packets else None,
            "protocol": {"packet_deadline_s": packet_deadline_s, "minimum_coverage": minimum_coverage},
            "raw_sha256": checksums}


def paired_differences(results, metric, *, baseline="M0", seed=2026, resamples=2000):
    blocks = {}
    for result in results:
        key = result.get("schedule_id")
        method = result["method_id"]
        if method in blocks.setdefault(key, {}):
            raise ValueError("Repeated schedule/method needs an explicit attempt-selection protocol")
        blocks[key][method] = result
    rng = random.Random(seed)
    out = {}
    for method in ("M1", "M2", "M3", "M4"):
        pairs = []
        for block in blocks.values():
            a, b = block.get(baseline), block.get(method)
            if (a and b and a["status"] == b["status"] == "VALID"
                    and a.get(metric) is not None and b.get(metric) is not None):
                pairs.append(b[metric] - a[metric])
        bootstrap = [statistics.mean(rng.choices(pairs, k=len(pairs))) for _ in range(resamples)] if len(pairs) > 1 else []
        out[method] = {"n": len(pairs), "missing_blocks": len(blocks) - len(pairs),
                       "mean_difference": statistics.mean(pairs) if pairs else None,
                       "bootstrap_ci95": [percentile(bootstrap, .025), percentile(bootstrap, .975)] if bootstrap else None}
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--topology", type=Path, default=Path("configs/network/topology_bundle.json"))
    parser.add_argument("--packet-deadline-s", required=True, type=float)
    parser.add_argument("--minimum-coverage", type=float, default=1)
    args = parser.parse_args()
    report = recompute(args.run_dir, json.loads(args.topology.read_bytes()),
                       packet_deadline_s=args.packet_deadline_s, minimum_coverage=args.minimum_coverage)
    (args.run_dir / "validation.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    if report["status"] != "VALID":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
