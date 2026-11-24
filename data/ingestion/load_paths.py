
import json
import getpass
from pathlib import Path

import psycopg2

# Đọc file topology
ROOT = Path(__file__).resolve().parents[2]
file_path = ROOT / "configs/network/topology_bundle.json"

data = json.loads(file_path.read_text(encoding="utf-8"))
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

            for path in data["candidate_paths"]:
                flow_group_id = path["flow_group_id"]
                path_id = path["path_id"]

                # 1. Import candidate_paths
                cur.execute("""
                    INSERT INTO public.candidate_paths (
                        topology_id, flow_group_id,
                        path_id, path_label
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (topology_id, flow_group_id, path_id)
                    DO NOTHING
                """, (
                    topology_id,
                    flow_group_id,
                    path_id,
                    path["path_label"]
                ))

                # 2. Import path_links
                for hop, link_id in enumerate(
                    path["ordered_link_ids"], start=1
                ):
                    cur.execute("""
                        INSERT INTO public.path_links (
                            topology_id, flow_group_id,
                            path_id, hop, link_id
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (
                            topology_id, flow_group_id,
                            path_id, hop
                        ) DO NOTHING
                    """, (
                        topology_id,
                        flow_group_id,
                        path_id,
                        hop,
                        link_id
                    ))

            expected_paths = {
                (path["flow_group_id"], path["path_id"], path["path_label"])
                for path in data["candidate_paths"]
            }
            cur.execute("""
                SELECT flow_group_id, path_id, path_label
                FROM public.candidate_paths WHERE topology_id = %s
            """, (topology_id,))
            if set(cur.fetchall()) != expected_paths:
                raise ValueError("Candidate paths trong DB khác với topology bundle nguồn.")

            expected_path_links = {
                (path["flow_group_id"], path["path_id"], hop, link_id)
                for path in data["candidate_paths"]
                for hop, link_id in enumerate(path["ordered_link_ids"], start=1)
            }
            cur.execute("""
                SELECT flow_group_id, path_id, hop, link_id
                FROM public.path_links WHERE topology_id = %s
            """, (topology_id,))
            if set(cur.fetchall()) != expected_path_links:
                raise ValueError("Path links trong DB khác với topology bundle nguồn.")

    print("Import thành công!")
    print("Số candidate paths:", len(data["candidate_paths"]))
    print(
        "Số path links:",
        sum(len(p["ordered_link_ids"]) for p in data["candidate_paths"])
    )

finally:
    conn.close()
