#!/usr/bin/env python3
"""
Network TopologyBundle Exporter (v1.4 Contract Compliant)
Reads canonical network CSV catalogs (links.csv, flow_groups.csv, candidate_paths.csv, initial_paths.csv),
verifies topological invariants (8 hosts, 6 switches, 32 directed links, 16 fabric links, 48 flows, 96 paths,
192 path-fabric-link incidences), and exports a standardized TopologyBundle JSON artifact with SHA-256 hash.

Usage:
    python3 exporter.py --config-dir artifacts/network --out artifacts/network/topology_bundle.json
"""

import argparse
import csv
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

# Canonical switch & host definitions per v1.4 Section 3.1
SWITCHES = [
    {"switch_id": "e1", "role": "edge", "dpid": 1, "dpid_hex": "0x1", "openflow_version": "1.3"},
    {"switch_id": "e2", "role": "edge", "dpid": 2, "dpid_hex": "0x2", "openflow_version": "1.3"},
    {"switch_id": "e3", "role": "edge", "dpid": 3, "dpid_hex": "0x3", "openflow_version": "1.3"},
    {"switch_id": "e4", "role": "edge", "dpid": 4, "dpid_hex": "0x4", "openflow_version": "1.3"},
    {"switch_id": "c1", "role": "core", "dpid": 5, "dpid_hex": "0x5", "openflow_version": "1.3"},
    {"switch_id": "c2", "role": "core", "dpid": 6, "dpid_hex": "0x6", "openflow_version": "1.3"},
]

HOSTS = [
    {"host_id": f"h{i}", "ip": f"10.0.0.{i}", "mac": f"00:00:00:00:00:{i:02x}",
     "edge_switch": f"e{(i - 1) // 2 + 1}", "edge_port": (i - 1) % 2 + 1}
    for i in range(1, 9)
]


def load_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        sys.exit(f"Error: required file not found: {path}")
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def build_topology_bundle(config_dir: str, topology_version: str = "1.0", verified_ovs: bool = False) -> dict:
    links_raw = load_csv(os.path.join(config_dir, "links.csv"))
    flows_raw = load_csv(os.path.join(config_dir, "flow_groups.csv"))
    paths_raw = load_csv(os.path.join(config_dir, "candidate_paths.csv"))
    init_raw = load_csv(os.path.join(config_dir, "initial_paths.csv"))

    links = []
    for r in links_raw:
        links.append({
            "link_id": r["link_id"].strip(),
            "from_node": r["from_node"].strip(),
            "to_node": r["to_node"].strip(),
            "from_port": int(r["from_port"]),
            "to_port": int(r["to_port"]),
            "link_type": r["link_type"].strip(),
            "is_fabric": bool(int(r["is_fabric"])),
            "capacity_mbps": float(r["capacity_mbps"]),
            "delay_ms": float(r["delay_ms"]),
        })

    flow_groups = []
    for r in flows_raw:
        fg_id = r["flow_group_id"].strip()
        flow_groups.append({
            "stt": int(r["stt"]),
            "flow_group_id": fg_id,
            "flow_cookie": r.get("flow_cookie", fg_id).strip(),
            "src_host": r["src_host"].strip(),
            "dst_host": r["dst_host"].strip(),
            "src_ip": r["src_ip"].strip(),
            "dst_ip": r["dst_ip"].strip(),
            "src_edge": r["src_edge"].strip(),
            "dst_edge": r["dst_edge"].strip(),
            "udp_port": int(r["udp_port"]),
        })

    candidate_paths = []
    fabric_incidences = 0
    for r in paths_raw:
        ordered = [x.strip() for x in r["ordered_link_ids"].split(";") if x.strip()]
        fabric = [x.strip() for x in r["fabric_link_ids"].split(";") if x.strip()]
        fabric_incidences += len(fabric)
        candidate_paths.append({
            "path_id": r["path_id"].strip(),
            "flow_group_id": r["flow_group_id"].strip(),
            "path_label": r["path_label"].strip(),
            "core_switch": r["core_switch"].strip(),
            "hop_count": int(r["hop_count"]),
            "nominal_delay_ms": float(r["nominal_delay_ms"]),
            "ordered_link_ids": ordered,
            "fabric_link_ids": fabric,
        })

    initial_paths = [
        {
            "flow_group_id": r["flow_group_id"].strip(),
            "initial_path_id": r["initial_path_id"].strip(),
            "path_label": r["path_label"].strip(),
        }
        for r in init_raw
    ]

    # Contract Invariant Checks (v1.4 Section 3.1, 3.2, 5.6)
    fabric_links = [lk for lk in links if lk["is_fabric"]]
    assert len(HOSTS) == 8, f"Expected 8 hosts, got {len(HOSTS)}"
    assert len(SWITCHES) == 6, f"Expected 6 switches, got {len(SWITCHES)}"
    assert len(links) == 32, f"Expected 32 directed links, got {len(links)}"
    assert len(fabric_links) == 16, f"Expected 16 fabric directed links, got {len(fabric_links)}"
    assert len(flow_groups) == 48, f"Expected 48 flow groups, got {len(flow_groups)}"
    assert len(candidate_paths) == 96, f"Expected 96 candidate paths, got {len(candidate_paths)}"
    assert len(initial_paths) == 48, f"Expected 48 initial paths, got {len(initial_paths)}"
    assert fabric_incidences == 192, f"Expected 192 path-fabric-link incidences, got {fabric_incidences}"

    core_payload = {
        "schema_version": "1.0",
        "topology_version": topology_version,
        "hosts": HOSTS,
        "switches": SWITCHES,
        "links": links,
        "flow_groups": flow_groups,
        "candidate_paths": candidate_paths,
        "initial_paths": initial_paths,
    }

    canonical_bytes = json.dumps(core_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    topology_hash = hashlib.sha256(canonical_bytes).hexdigest()

    bundle = {
        "schema_version": "1.0",
        "producer": "network",
        "produced_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "topology_version": topology_version,
        "topology_hash": topology_hash,
        "ovs_port_mapping_verified": verified_ovs,
        "summary": {
            "host_count": len(HOSTS),
            "switch_count": len(SWITCHES),
            "directed_link_count": len(links),
            "fabric_directed_link_count": len(fabric_links),
            "flow_group_count": len(flow_groups),
            "candidate_path_count": len(candidate_paths),
            "path_fabric_incidence_count": fabric_incidences,
        },
        **core_payload,
    }
    return bundle


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config-dir", default="artifacts/network", help="Directory containing network CSV catalogs")
    ap.add_argument("--out", default="artifacts/network/topology_bundle.json", help="Output JSON path")
    ap.add_argument("--topology-version", default="1.0", help="Topology version string")
    ap.add_argument("--verified-ovs", action="store_true", help="Mark OVS DPID/port mapping as verified via ovs-ofctl")
    args = ap.parse_args()

    bundle = build_topology_bundle(
        config_dir=args.config_dir,
        topology_version=args.topology_version,
        verified_ovs=args.verified_ovs,
    )

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2)

    print(f"[+] Exported TopologyBundle to {args.out}")
    print(f"    topology_version = {bundle['topology_version']}")
    print(f"    topology_hash    = {bundle['topology_hash']}")
    print(f"    ovs_verified     = {bundle['ovs_port_mapping_verified']}")


if __name__ == "__main__":
    main()