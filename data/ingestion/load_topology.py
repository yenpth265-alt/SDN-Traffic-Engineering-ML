
import json
import getpass
from pathlib import Path

import psycopg2

# Đường dẫn tới thư mục gốc dự án
ROOT = Path(__file__).resolve().parents[2]
file_path = ROOT / "configs/network/topology_bundle.json"

# Đọc nội dung JSON gốc
raw_data = file_path.read_bytes()
data = json.loads(raw_data)

# Dùng topology_hash làm topology_id
topology_id = data["topology_hash"]

# Kết nối PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="sdn_te",
    user="postgres",
    password=getpass.getpass("PostgreSQL password: ")
)

try:
    with conn:
        with conn.cursor() as cur:

            # 1. Import bảng topologies
            cur.execute("""
                INSERT INTO public.topologies
                    (topology_id, topology_version, topology_hash, bundle)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (topology_id) DO NOTHING
            """, (
                topology_id,
                data["topology_version"],
                data["topology_hash"],
                psycopg2.Binary(raw_data)
            ))

            # 2. Import danh sách hosts
            for host in data["hosts"]:
                cur.execute("""
                    INSERT INTO public.topology_nodes
                        (topology_id, node_id, role, dpid)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (topology_id, node_id) DO NOTHING
                """, (
                    topology_id,
                    host["host_id"],
                    "host",
                    None
                ))

            # 3. Import danh sách switches
            for switch in data["switches"]:
                cur.execute("""
                    INSERT INTO public.topology_nodes
                        (topology_id, node_id, role, dpid)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (topology_id, node_id) DO NOTHING
                """, (
                    topology_id,
                    switch["switch_id"],
                    switch["role"],
                    switch["dpid"]
                ))

    print("Import thành công!")
    print("Topology ID:", topology_id)
    print("Số hosts:", len(data["hosts"]))
    print("Số switches:", len(data["switches"]))

finally:
    conn.close()
