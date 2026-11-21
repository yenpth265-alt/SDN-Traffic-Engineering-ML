
import json
import getpass
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[2]
file_path = ROOT / "configs/network/topology_bundle.json"

data = json.loads(file_path.read_text(encoding="utf-8"))
topology_id = data["topology_hash"]

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

            # 1. Import links
            for link in data["links"]:
                cur.execute("""
                    INSERT INTO public.links (
                        topology_id, link_id,
                        from_node, to_node,
                        from_port, to_port,
                        is_fabric, capacity_mbps, delay_ms
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (topology_id, link_id) DO NOTHING
                """, (
                    topology_id,
                    link["link_id"],
                    link["from_node"],
                    link["to_node"],
                    link["from_port"],
                    link["to_port"],
                    link["is_fabric"],
                    link["capacity_mbps"],
                    link["delay_ms"]
                ))

            # 2. Import flow_groups
            for flow in data["flow_groups"]:
                cur.execute("""
                    INSERT INTO public.flow_groups (
                        topology_id, flow_group_id, flow_cookie,
                        src_host, dst_host, src_ip, dst_ip,
                        src_edge, dst_edge, udp_port
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (topology_id, flow_group_id) DO NOTHING
                """, (
                    topology_id,
                    flow["flow_group_id"],
                    flow["flow_cookie"],
                    flow["src_host"],
                    flow["dst_host"],
                    flow["src_ip"],
                    flow["dst_ip"],
                    flow["src_edge"],
                    flow["dst_edge"],
                    flow["udp_port"]
                ))

    print("Import thành công!")
    print("Số links trong JSON:", len(data["links"]))
    print("Số flow groups trong JSON:", len(data["flow_groups"]))

finally:
    conn.close()
