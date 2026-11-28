"""Freeze M0-M4 run order and execute an explicit launcher sequentially."""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import random
import subprocess
from uuid import uuid4


def build_matrix(schedules, *, order_seed, faults=()):
    methods = ["M0", "M1", "M2", "M3", "M4"]
    rng = random.Random(order_seed)
    rng.shuffle(methods)
    groups = defaultdict(list)
    seen = set()
    for schedule in schedules:
        sid = schedule["schedule_id"]
        if sid in seen or schedule["split"] != "test" or not schedule.get("schedule_sha256"):
            raise ValueError("Benchmark schedules must be distinct locked test schedules with hashes")
        seen.add(sid)
        groups[(schedule["scenario_id"], schedule.get("scenario_config", "default"))].append(schedule)
    if not groups:
        raise ValueError("Schedule matrix is empty")
    matrix = []
    for group in sorted(groups):
        block = groups[group]
        rng.shuffle(block)
        offsets = list(range(len(methods)))
        rng.shuffle(offsets)
        for index, schedule in enumerate(block):
            shift = offsets[index % 5]
            for method in methods[shift:] + methods[:shift]:
                matrix.append({**schedule, "method_id": method, "fault_id": None,
                               "fault_seed": None, "order_seed": order_seed})
    for fault in faults:
        if fault["fault_id"] not in {f"F{i:02}" for i in range(1, 13)}:
            raise ValueError("Unknown fault hook")
        schedule = next((s for s in schedules if s["schedule_id"] == fault["schedule_id"]), None)
        if schedule is None:
            raise ValueError("Fault schedule is not in the locked schedule matrix")
        for fault_seed in fault["seeds"]:
            matrix.append({**schedule, "method_id": "M3", "fault_id": fault["fault_id"],
                           "fault_seed": fault_seed, "order_seed": order_seed})
    return [{**row, "order": index} for index, row in enumerate(matrix)]


def execute_matrix(matrix, command, output, *, timeout_s):
    if not command or not all(isinstance(arg, str) for arg in command):
        raise ValueError("Launcher must be a nonempty JSON array of arguments")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    raw = json.dumps(matrix, sort_keys=True, separators=(",", ":")).encode()
    (output / "locked_matrix.json").write_bytes(raw)
    (output / "locked_matrix.sha256").write_text(hashlib.sha256(raw).hexdigest())
    results = []
    for row in matrix:
        run_id = f"bench-{row['order']:04}-{uuid4().hex[:12]}"
        run_dir = (output / run_id).resolve()
        run_dir.mkdir()
        context = {**row, "run_id": run_id, "run_dir": str(run_dir),
                   "mode": "oracle" if row["method_id"] == "M4" else "benchmark"}
        (run_dir / "plan.json").write_text(json.dumps(context, indent=2), encoding="utf-8")
        args = [arg.format_map(context) for arg in command]
        result = {**context, "command": args}
        with (run_dir / "stdout.log").open("wb") as stdout, (run_dir / "stderr.log").open("wb") as stderr:
            try:
                completed = subprocess.run(args, stdout=stdout, stderr=stderr, timeout=timeout_s, check=False)
                result.update(exit_code=completed.returncode, status="COMPLETED" if completed.returncode == 0 else "FAILED")
            except subprocess.TimeoutExpired:
                result.update(exit_code=None, status="TIMEOUT")
            except OSError as exc:
                result.update(exit_code=None, status="LAUNCH_FAILED", reason=str(exc))
        # A completed launcher is not a PASS_SYSTEM claim. Raw validation is separate.
        results.append(result)
        (output / "execution.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("protocol", type=Path, help="JSON with schedules, order_seed, optional faults and launcher")
    parser.add_argument("output", type=Path)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    protocol = json.loads(args.protocol.read_bytes())
    matrix = build_matrix(protocol["schedules"], order_seed=protocol["order_seed"], faults=protocol.get("faults", []))
    if args.execute:
        results = execute_matrix(matrix, protocol["launcher"], args.output, timeout_s=protocol["run_timeout_s"])
        if any(r["status"] != "COMPLETED" for r in results):
            raise SystemExit(1)
    else:
        args.output.write_text(json.dumps(matrix, indent=2), encoding="utf-8")
    print(f"Prepared {len(matrix)} sequential runs")


if __name__ == "__main__":
    main()
