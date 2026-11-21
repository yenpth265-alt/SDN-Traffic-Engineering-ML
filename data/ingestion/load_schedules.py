import argparse
import getpass
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from prepare_network import prepare_delivery

ROOT = Path(__file__).resolve().parents[2]


@contextmanager
def staged_delivery():
    with TemporaryDirectory(prefix="sdn-network-") as directory:
        yield prepare_delivery(ROOT, Path(directory))


@contextmanager
def database_cursor():
    import psycopg2

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--database", default="sdn_te")
    parser.add_argument("--user", default="postgres")
    args = parser.parse_args()
    connection = psycopg2.connect(
        host=args.host, port=args.port, dbname=args.database, user=args.user,
        password=getpass.getpass("PostgreSQL password: "),
        options="-c search_path=public",
    )
    try:
        with connection, connection.cursor() as cursor:
            yield cursor
    except (psycopg2.errors.UndefinedTable, psycopg2.errors.UndefinedColumn) as error:
        raise SystemExit(f"Schema public không khớp bản DDL PostgreSQL hiện có: {error}") from error
    finally:
        connection.close()


def schedule_rows(delivery):
    return [
        (schedule_id, delivery["topology_id"], scenario, seed, delivery["manifest_id"],
         str(seed), delivery["file_hash"], -delivery["warmup"], delivery["measurement"])
        for schedule_id, (scenario, seed, _) in sorted(delivery["metadata"].items())
    ]


def check_schedules(cursor, delivery):
    expected = schedule_rows(delivery)
    cursor.execute("""
        SELECT s.schedule_id, s.topology_id, s.scenario_id, s.seed, s.split_manifest_id,
               s.seed_family, s.file_hash, s.first_offset_s, s.duration_s, a.split
        FROM public.schedules s
        JOIN public.split_assignments a USING (split_manifest_id, seed_family)
        WHERE s.schedule_id = ANY(%s)
    """, ([row[0] for row in expected],))
    actual = {row[0]: row[1:] for row in cursor.fetchall()}
    for row in expected:
        if actual.get(row[0]) != row[1:] + (delivery["metadata"][row[0]][2],):
            raise ValueError(f"Lịch {row[0]} chưa nạp hoặc khác dữ liệu Network trong public.")


def main():
    from psycopg2.extras import execute_values

    with database_cursor() as cursor, staged_delivery() as delivery:
        cursor.execute(
            "SELECT topology_id FROM public.topologies WHERE topology_id = %s AND topology_hash = %s",
            (delivery["topology_id"], delivery["topology_id"]),
        )
        if cursor.fetchone() is None:
            raise ValueError("Chạy load_topology.py để nạp đúng topology trước.")
        assignments = sorted({(delivery["manifest_id"], str(seed), split)
                              for _, seed, split in delivery["metadata"].values()})
        execute_values(cursor, """
            INSERT INTO public.split_assignments (split_manifest_id, seed_family, split)
            VALUES %s ON CONFLICT (split_manifest_id, seed_family) DO NOTHING
        """, assignments)
        execute_values(cursor, """
            INSERT INTO public.schedules (
                schedule_id, topology_id, scenario_id, seed, split_manifest_id, seed_family,
                file_hash, first_offset_s, duration_s
            ) VALUES %s ON CONFLICT (schedule_id) DO NOTHING
        """, schedule_rows(delivery))
        check_schedules(cursor, delivery)
    print(f"Đã xác nhận {len(delivery['metadata'])} lịch trong public.schedules.")


if __name__ == "__main__":
    main()

