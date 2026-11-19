"""Offline plan import: preserve labels, reject incomplete data, retain provenance."""
import csv
from pathlib import Path
import zipfile

import pytest
import yaml

from ml.__main__ import main
from ml.common import ContractError, catalog, read_json, write_json
from ml.preprocessing.schedule import import_schedule

CATALOG = Path(__file__).resolve().parents[2] / "configs/network/flow_groups.csv"


@pytest.fixture
def schedule(tmp_path):
    rows = []
    allocations = []
    for seed in range(1, 21):
        split = "train" if seed <= 14 else "validation" if seed <= 17 else "test"
        allocations.append({"seed": seed, "split": split})
        for t in range(8):
            for fg in catalog(CATALOG):
                rows.append([f"S1_seed{seed:02}", "S1", seed, split, t, fg,
                             0.1 + seed / 100 + t / 1000 + int(fg, 16) / 10000])
    source = tmp_path / "schedule.csv"
    write_rows(source, rows)
    write_json(tmp_path / "manifest.json", {"seed_allocations": allocations})
    return source, tmp_path / "manifest.json", rows


def write_rows(path, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["schedule_id", "scenario_id", "seed", "split", "time_offset_s",
                         "flow_group_id", "requested_mbps"])
        writer.writerows(rows)


def test_zip_import_keeps_demand_split_and_marks_artificial_origins(schedule, tmp_path):
    source, manifest, rows = schedule
    archive = tmp_path / "demand plan.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.write(source, "folder/demand_plan.csv")
    report = import_schedule(archive, manifest, CATALOG, tmp_path / "import", 3, 5)
    assert report["rows"] == 20 * 8 * 48
    assert report["schedules"] == 20
    assert report["provenance"]["online_eligible"] is False
    runs = read_json(tmp_path / "import/runs.json")["runs"]
    assert runs[0]["measurement_started_at_utc"] == "2000-01-01T00:00:03.000000Z"
    with (tmp_path / "import/history.csv").open(newline="") as f:
        imported = list(csv.DictReader(f))
    assert float(imported[0]["requested_mbps"]) == rows[0][-1]
    assert runs[-1]["split"] == "test"
    assert not (tmp_path / "import/history.csv.partial").exists()


@pytest.mark.parametrize("mutation", ["split", "missing", "duplicate", "unknown_flow",
                                     "nan", "time", "scenario_seed", "family"])
def test_bad_schedule_does_not_publish_import(schedule, tmp_path, mutation):
    source, manifest, rows = schedule
    if mutation == "split": rows[0][3] = "test"
    elif mutation == "missing": rows.pop()
    elif mutation == "duplicate": rows.append(rows[0])
    elif mutation == "unknown_flow": rows[0][5] = "h1_h3"
    elif mutation == "nan": rows[0][-1] = "NaN"
    elif mutation == "time": rows[0][4] = 8
    elif mutation == "scenario_seed":
        for row in rows:
            if row[2] == 20: row[1] = "S2"
    else:
        # One schedule cannot change seed/family metadata halfway through.
        rows[0][2] = 2
    write_rows(source, rows)
    out = tmp_path / "import"
    with pytest.raises(ContractError):
        import_schedule(source, manifest, CATALOG, out, 3, 5)
    assert not (out / "runs.json").exists()
    assert not (out / "history.csv").exists()
    assert not (out / "history.csv.partial").exists()


def test_70_second_warmup_is_not_silently_shortened(schedule, tmp_path):
    source, manifest, _ = schedule
    with pytest.raises(ContractError, match="needs"):
        import_schedule(source, manifest, CATALOG, tmp_path / "import", 4, 5)


def test_from_schedule_cli_completes_offline_pipeline(schedule, tmp_path):
    source, manifest, _ = schedule
    config = {
        "lookback_candidates": [2], "horizon_candidates": [1, 2],
        "epsilon_candidates": [0, .1], "ma_window_candidates": [1, 2],
        "random_state": 42, "n_jobs": 1, "max_train_samples": 100,
        "rf_candidates": [{"n_estimators": 3, "max_depth": 3}], "baseline_epsilon": 0,
    }
    cfg = tmp_path / "search.yaml"
    cfg.write_text(yaml.safe_dump(config), encoding="utf-8")
    out = tmp_path / "pipeline"
    assert main(["from-schedule", "--schedule", str(source), "--seed-manifest", str(manifest),
                 "--config", str(cfg), "--warmup-seconds", "3", "--measurement-seconds", "5",
                 "--stride", "1", "--out", str(out)]) == 0
    report = read_json(out / "evaluation/evaluation.json")
    model = read_json(out / "model/metadata.json")
    assert report["data_provenance"]["kind"] == "offline_network_schedule"
    assert model["data_provenance"]["online_eligible"] is False
    assert model["test_used_for_selection"] is False
    assert len(report["by_flow"]) == 48 and len(report["by_run"]) == 3
    assert report["test_seeds"] == ["18", "19", "20"]
    assert (out / "evaluation/REPORT.md").is_file()
