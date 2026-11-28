import csv
import json
from pathlib import Path
import sys

import pytest

from testing.scripts.run_benchmarks import build_matrix, execute_matrix
from testing.scripts.validate_results import paired_differences, recompute


def write_csv(path, fields, rows):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def test_latin_order_and_fault_runs_are_separate():
    schedules = [{"schedule_id": str(i), "scenario_id": "S3", "seed": i, "split": "test",
                  "schedule_sha256": "a" * 64} for i in range(5)]
    matrix = build_matrix(schedules, order_seed=1,
                          faults=[{"schedule_id": "0", "fault_id": "F05", "seeds": [10, 11, 12]}])
    assert len(matrix) == 28
    for position in range(5):
        assert {matrix[i * 5 + position]["method_id"] for i in range(5)} == {f"M{i}" for i in range(5)}
    assert all(r["method_id"] == "M3" and r["fault_id"] == "F05" for r in matrix[25:])


def test_executor_preserves_failed_run_and_exact_launcher_arguments(tmp_path):
    launcher = tmp_path / "launcher.py"
    launcher.write_text("import sys; print(sys.argv[1:]); raise SystemExit(3)")
    rows = [{"order": 0, "method_id": "M3", "schedule_id": "with spaces"}]
    result = execute_matrix(rows, [sys.executable, str(launcher), "{schedule_id}"], tmp_path / "results", timeout_s=5)
    assert result[0]["status"] == "FAILED" and result[0]["exit_code"] == 3
    assert "with spaces" in (Path(result[0]["run_dir"]) / "stdout.log").read_text()
    assert (tmp_path / "results/locked_matrix.sha256").exists()


def test_recompute_100_sent_90_unique_excludes_duplicate_late_packet(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    manifest = {"run_id": "run", "schedule_id": "S3_1", "method_id": "M3",
                "warmup_s": 0, "measurement_s": 1, "traffic_started_at_utc": "2026-11-28T00:00:00Z"}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    write_csv(raw / "sent_h1.csv", ["time_offset_s", "flow_group_id", "packets_sent"],
              [{"time_offset_s": 0, "flow_group_id": "0x103", "packets_sent": 100}])
    packets = [{"send_bin": 0, "flow_group_id": "0x103", "seq": seq, "recv_offset_s": .5,
                "delay_ms": 1, "received_bytes": 100} for seq in range(90)]
    write_csv(raw / "recvpkts_h3.csv", list(packets[0]),
              packets + [packets[0], {**packets[0], "seq": 100, "recv_offset_s": 2}])
    topology = {"flow_groups": [{"flow_group_id": "0x103", "dst_host": "h3"}],
                "links": [{"link_id": "e1>c1", "is_fabric": True}, {"link_id": "c1>e2", "is_fabric": True}]}
    batch = {"run_id": "run", "sampled_at_utc": "2026-11-28T00:00:01Z", "bin_index": 0,
             "samples": [{"link_id": "e1>c1", "utilization": .8}, {"link_id": "c1>e2", "utilization": .4}]}
    (raw / "link_samples.jsonl").write_text(json.dumps(batch) + "\n")
    result = recompute(tmp_path, topology, packet_deadline_s=.5)
    assert result["status"] == "VALID"
    assert result["loss_ratio"] == .1 and result["received_unique_packets"] == 90
    assert result["unique_goodput_mbps"] == .072 and result["mlu_mean"] == .8
    batch["samples"].pop()
    (raw / "link_samples.jsonl").write_text(json.dumps(batch) + "\n")
    incomplete = recompute(tmp_path, topology, packet_deadline_s=.5)
    assert incomplete["status"] == "INCOMPLETE" and incomplete["mlu_mean"] is None
    (raw / "recvpkts_h3.csv").unlink()
    missing = recompute(tmp_path, topology, packet_deadline_s=.5)
    assert missing["loss_ratio"] is None and missing["unique_goodput_mbps"] is None


def test_paired_units_are_schedules_and_missing_blocks_are_reported():
    results = [{"schedule_id": sid, "method_id": method, "status": "VALID", "loss_ratio": value}
               for sid, method, value in [("s1", "M0", .3), ("s1", "M3", .1), ("s2", "M0", .5)]]
    paired = paired_differences(results, "loss_ratio")
    assert paired["M3"]["n"] == 1 and paired["M3"]["missing_blocks"] == 1
    assert paired["M3"]["mean_difference"] == pytest.approx(-.2)
    assert paired["M3"]["bootstrap_ci95"] is None
