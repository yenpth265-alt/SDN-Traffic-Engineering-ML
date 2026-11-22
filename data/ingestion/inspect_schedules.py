
import csv
import zipfile
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[2]
zip_path = ROOT / "network/traffic_schedules/demand_plan.zip"

# Thống kê theo từng schedule
schedules = defaultdict(lambda: {
    "min_offset": float("inf"),
    "max_offset": float("-inf"),
    "rows": 0,
    "flows": set(),
    "scenarios": set(),
    "seeds": set(),
    "splits": set()
})

with zipfile.ZipFile(zip_path) as z:
    with z.open("demand_plan.csv") as f:
        import io
        reader = csv.DictReader(
            io.TextIOWrapper(f, encoding="utf-8-sig")
        )

        for row in reader:
            sid = row["schedule_id"]
            offset = int(row["time_offset_s"])

            s = schedules[sid]
            s["min_offset"] = min(s["min_offset"], offset)
            s["max_offset"] = max(s["max_offset"], offset)
            s["rows"] += 1
            s["flows"].add(row["flow_group_id"])
            s["scenarios"].add(row["scenario_id"])
            s["seeds"].add(row["seed"])
            s["splits"].add(row["split"])

print("Tổng số schedules:", len(schedules))

for sid, s in list(sorted(schedules.items()))[:15]:
    print(
        sid,
        "| offset:", s["min_offset"], "->", s["max_offset"],
        "| flows:", len(s["flows"]),
        "| rows:", s["rows"],
        "| scenario:", sorted(s["scenarios"]),
        "| seed:", sorted(s["seeds"]),
        "| split:", sorted(s["splits"])
    )


from collections import defaultdict

seed_splits = defaultdict(set)
scenario_splits = defaultdict(set)

for sid, s in schedules.items():
    seed = next(iter(s["seeds"]))
    scenario = next(iter(s["scenarios"]))

    seed_splits[int(seed)].update(s["splits"])
    scenario_splits[scenario].update(s["splits"])

print("\nPhân chia theo seed:")
for seed, splits in sorted(seed_splits.items()):
    print(f"Seed {seed:02d}: {sorted(splits)}")

print("\nPhân chia theo scenario:")
for scenario, splits in sorted(scenario_splits.items()):
    print(f"{scenario}: {sorted(splits)}")

print("\nTổng số dòng:", sum(s["rows"] for s in schedules.values()))
