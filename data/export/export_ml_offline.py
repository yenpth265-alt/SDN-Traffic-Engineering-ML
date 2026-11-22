"""Export the offline ML dataset from PostgreSQL into a new directory."""

import argparse
import csv
import getpass
import json
import math
import shutil
from collections import Counter
from contextlib import ExitStack, closing
from itertools import groupby
from pathlib import Path
from tempfile import TemporaryDirectory


COLUMNS = ("schedule_id", "flow_group_id", "time_offset_s", "requested_mbps", "split")
DEFAULT_OUTPUT = Path(__file__).resolve().parents[2] / "ml" / "data"
SCHEDULE_COUNTS = {"train": 60, "validation": 30, "test": 30}
FIRST_BIN, LAST_BIN = -60, 359
GROUP_COUNT = 48
LOOKBACK, HORIZON = 60, 5
QUERY = """
SELECT s.schedule_id, d.flow_group_id, d.time_offset_s, d.requested_mbps, a.split,
       s.seed, s.split_manifest_id, s.file_hash, s.topology_id
FROM public.schedules AS s
LEFT JOIN public.split_assignments AS a USING (split_manifest_id, seed_family)
LEFT JOIN public.demand_plan AS d
  ON d.schedule_id = s.schedule_id AND d.topology_id = s.topology_id
ORDER BY s.schedule_id, d.flow_group_id, d.time_offset_s
"""


def make_sample(schedule_id, flow_group_id, values):
    return {
        "schedule_id": schedule_id,
        "flow_group_id": flow_group_id,
        "split": "train",
        "L": LOOKBACK,
        "H": HORIZON,
        "first_target_offset_s": 0,
        "X_offsets_s": list(range(-LOOKBACK, 0)),
        "X": values[:LOOKBACK],
        "y_offsets_s": list(range(HORIZON)),
        "y": values[LOOKBACK:],
    }


def write_dataset(rows, flow_groups, destination):
    counts = Counter()
    seed_splits = {}
    manifest_ids, source_hashes = set(), set()
    sample = None
    with ExitStack() as files:
        writers = {}
        for split in SCHEDULE_COUNTS:
            stream = files.enter_context((destination / f"{split}.csv").open(
                "w", encoding="utf-8", newline=""
            ))
            writers[split] = csv.writer(stream)
            writers[split].writerow(COLUMNS)

        for schedule_id, schedule in groupby(rows, key=lambda row: row[0]):
            seen_groups = set()
            for flow_id, series in groupby(schedule, key=lambda row: row[1]):
                values = []
                offset = FIRST_BIN - 1
                for offset_expected, row in enumerate(series, FIRST_BIN):
                    _, _, offset, mbps, split, seed, manifest_id, file_hash, topology_id = row
                    if (offset != offset_expected or offset_expected > LAST_BIN
                            or mbps is None or not math.isfinite(mbps) or mbps < 0):
                        raise ValueError(f"Bin hoặc demand không hợp lệ: {schedule_id}/{flow_id}, "
                                         f"cần bin {offset_expected}, nhận {offset}.")
                    if offset_expected == FIRST_BIN:
                        expected_split = ("train" if 1 <= seed <= 10 else
                                          "validation" if 11 <= seed <= 15 else
                                          "test" if 16 <= seed <= 20 else None)
                        if (expected_split is None or split != expected_split
                                or seed_splits.setdefault(seed, split) != split):
                            raise ValueError(f"Split không hợp lệ: {schedule_id}, seed {seed}, {split}.")
                        if flow_id not in flow_groups.get(topology_id, set()):
                            raise ValueError(f"Flow group không hợp lệ: {schedule_id}/{flow_id}.")
                    writers[split].writerow(row[:len(COLUMNS)])
                    if sample is None and split == "train" and -LOOKBACK <= offset < HORIZON:
                        values.append(mbps)
                if offset != LAST_BIN:
                    raise ValueError(f"Thiếu bin cuối: {schedule_id}/{flow_id}.")
                if values:
                    sample = make_sample(schedule_id, flow_id, values)
                seen_groups.add(flow_id)

            if len(seen_groups) != GROUP_COUNT or seen_groups != flow_groups[topology_id]:
                raise ValueError(f"{schedule_id} không có đủ {GROUP_COUNT} flow group của topology.")
            counts[split] += 1
            manifest_ids.add(manifest_id)
            source_hashes.add(file_hash)

    if (counts != SCHEDULE_COUNTS or set(seed_splits) != set(range(1, 21))
            or len(manifest_ids) != 1 or len(source_hashes) != 1 or sample is None):
        raise ValueError(f"Dữ liệu không khớp bộ Network đã chốt. Số lịch: {dict(counts)}.")
    sample["split_manifest_id"] = next(iter(manifest_ids))
    sample["source_file_hash"] = next(iter(source_hashes))
    (destination / "sample_xy.json").write_text(
        json.dumps(sample, indent=2) + "\n", encoding="utf-8"
    )
    return counts


def publish_dataset(stage, output):
    output.mkdir()
    created_files = []
    try:
        for source in sorted(stage.iterdir()):
            target = output / source.name
            with source.open("rb") as incoming, target.open("xb") as outgoing:
                created_files.append(target)
                shutil.copyfileobj(incoming, outgoing)
    except BaseException:
        for target in reversed(created_files):
            target.unlink()
        if not any(output.iterdir()):
            output.rmdir()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT,
        help="Thư mục đích chưa tồn tại (mặc định là ml/data trong repo)",
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=5432, type=int)
    parser.add_argument("--database", default="sdn_te")
    parser.add_argument("--user", default="postgres")
    args = parser.parse_args()
    output = args.output.absolute()
    if output.exists():
        parser.error(f"Thư mục đích đã tồn tại: {output}")

    import psycopg2

    try:
        with closing(psycopg2.connect(
            host=args.host, port=args.port, dbname=args.database, user=args.user,
            password=getpass.getpass("PostgreSQL password: "),
        )) as connection:
            connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
            output.parent.mkdir(parents=True, exist_ok=True)
            with TemporaryDirectory(prefix=".ml-export-", dir=output.parent) as temporary:
                stage = Path(temporary) / "dataset"
                stage.mkdir()
                with connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT topology_id, flow_group_id FROM public.flow_groups")
                        flow_groups = {}
                        for topology_id, flow_group_id in cursor:
                            flow_groups.setdefault(topology_id, set()).add(flow_group_id)
                    with connection.cursor(name="ml_offline_export") as cursor:
                        cursor.itersize = 10_000
                        cursor.execute(QUERY)
                        counts = write_dataset(cursor, flow_groups, stage)
                publish_dataset(stage, output)
    except (psycopg2.Error, OSError, ValueError) as error:
        parser.exit(1, f"Xuất dữ liệu thất bại: {error}\n")

    for split, count in counts.items():
        print(f"{split}: {count} lịch, {count * GROUP_COUNT * (LAST_BIN - FIRST_BIN + 1):,} dòng")
    print(f"Đã xuất CSV và sample_xy.json tại {output}")


if __name__ == "__main__":
    main()
