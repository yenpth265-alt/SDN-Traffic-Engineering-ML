import csv
import json
import os
from typing import Any, Mapping, Sequence


def _parse_links_field(raw_links: Any) -> list[str]:
    if isinstance(raw_links, list):
        return [str(x).strip() for x in raw_links if str(x).strip()]
    if not isinstance(raw_links, str):
        return []
    text = raw_links.strip()
    if not text:
        return []
    if text.startswith("["):
        return [str(x).strip() for x in json.loads(text) if str(x).strip()]
    sep = ";" if ";" in text else ","
    return [x.strip() for x in text.split(sep) if x.strip()]


def load_topology_from_configs(config_dir: str = "configs/network") -> dict[str, Any]:
    """
    Nạp topology chuẩn từ configs/network/topology_bundle.json (ưu tiên)
    hoặc fallback sang links.csv, flow_groups.csv, candidate_paths.csv.
    Khớp 100% với TopologyRegistry của SDN (32 links, 48 flows, 96 paths).
    """
    F: list[str] = []
    P: dict[str, list[str]] = {}
    E: list[str] = []
    C: dict[str, float] = {}
    fabric_links: set[str] = set()
    raw_path_links: dict[tuple[str, str], set[str]] = {}
    topology_version = "1.0"
    topology_hash: str | None = None

    bundle_path = os.path.join(config_dir, "topology_bundle.json")
    if os.path.exists(bundle_path):
        with open(bundle_path, mode="r", encoding="utf-8") as fb:
            bundle = json.load(fb)

        topology_version = str(bundle.get("topology_version", "1.0")).lstrip("v")
        topology_hash = bundle.get("topology_hash") or bundle.get("sha256")

        # 1. Links (hỗ trợ cả dạng list và dict)
        raw_links = bundle.get("links", [])
        link_items = raw_links.values() if isinstance(raw_links, Mapping) else raw_links
        for item in link_items:
            link_id = str(item["link_id"]).strip()
            cap = float(item.get("capacity_mbps") or item.get("capacity") or 100.0)
            E.append(link_id)
            C[link_id] = cap
            if item.get("is_fabric", True):
                fabric_links.add(link_id)

        # 2. Flow groups (48 nhóm luồng)
        raw_fgs = bundle.get("flow_groups", [])
        fg_items = raw_fgs.values() if isinstance(raw_fgs, Mapping) else raw_fgs
        for item in fg_items:
            fg_id = str(item["flow_group_id"]).strip()
            F.append(fg_id)
            P[fg_id] = []

        # 3. Candidate paths (96 tuyến)
        raw_cps = bundle.get("candidate_paths", [])
        cp_items = raw_cps.values() if isinstance(raw_cps, Mapping) else raw_cps
        for item in cp_items:
            fg_id = str(item["flow_group_id"]).strip()
            path_id = str(item["path_id"]).strip()
            links_on_path = _parse_links_field(
                item.get("links") or item.get("link_ids") or item.get("directed_links")
            )
            P.setdefault(fg_id, []).append(path_id)
            raw_path_links[(fg_id, path_id)] = set(links_on_path)
    else:
        # Fallback đọc từ bộ 3 file CSV
        with open(os.path.join(config_dir, "links.csv"), mode="r", encoding="utf-8") as f_links:
            for row in csv.DictReader(f_links):
                link_id = row["link_id"].strip()
                cap = float(row.get("capacity_mbps") or row.get("capacity") or 100.0)
                E.append(link_id)
                C[link_id] = cap
                if str(row.get("is_fabric", "true")).lower() in ("1", "true", "yes"):
                    fabric_links.add(link_id)

        with open(os.path.join(config_dir, "flow_groups.csv"), mode="r", encoding="utf-8") as f_fg:
            for row in csv.DictReader(f_fg):
                fg_id = row["flow_group_id"].strip()
                F.append(fg_id)
                P[fg_id] = []

        with open(os.path.join(config_dir, "candidate_paths.csv"), mode="r", encoding="utf-8") as f_cp:
            for row in csv.DictReader(f_cp):
                fg_id = row["flow_group_id"].strip()
                path_id = row["path_id"].strip()
                links_on_path = _parse_links_field(
                    row.get("links") or row.get("link_ids") or row.get("path_links")
                )
                P.setdefault(fg_id, []).append(path_id)
                raw_path_links[(fg_id, path_id)] = set(links_on_path)

    # Dựng ma trận chỉ thị 3 chiều a_fpe[f][p][e]
    a_fpe: dict[str, dict[str, dict[str, int]]] = {}
    for f in F:
        a_fpe[f] = {}
        for p in P[f]:
            link_set = raw_path_links.get((f, p), set())
            if not link_set:
                raise ValueError(f"Tuyến {p} của luồng {f} không ánh xạ được danh sách liên kết nào.")
            a_fpe[f][p] = {e: (1 if e in link_set else 0) for e in E}

    return {
        "F": F,
        "P": P,
        "E": E,
        "C": C,
        "fabric_links": fabric_links,
        "a_fpe": a_fpe,
        "topology_version": topology_version,
        "topology_hash": topology_hash,
    }


def calculate_mean_demand(forecast_data: Mapping[str, Any], flow_groups: Sequence[str]) -> dict[str, float]:
    """Tính nhu cầu trung bình b_f (Mbps) trên cửa sổ dự báo H* bước."""
    pred_map = forecast_data.get("predictions", forecast_data)
    b_f: dict[str, float] = {}
    for f in flow_groups:
        rec = pred_map[f]
        h_values = rec["predicted_mbps_array"] if isinstance(rec, Mapping) else rec
        b_f[f] = sum(float(v) for v in h_values) / len(h_values)
    return b_f


def evaluate_routing_utilization(
    topology: Mapping[str, Any],
    b_f: Mapping[str, float],
    routes: Mapping[str, Mapping[str, Any]],
    default_ce: float = 100.0,
) -> float:
    """
    Tính mức sử dụng liên kết cực đại U trên tải dự báo b_f cho phương án định tuyến hiện tại.
    Dùng làm mốc J_old chuẩn xác khi so sánh Hysteresis với nghiệm MILP J_new.
    """
    E = topology["E"]
    C_map = topology.get("C", {})
    a_fpe = topology["a_fpe"]
    k_e = topology.get("k_e", {})

    max_u = 0.0
    for e in E:
        cap = float(C_map.get(e, default_ce))
        if cap <= 0:
            continue
        load_e = 0.0
        for f in topology["F"]:
            cur_p = routes[f].get("current_path") or routes[f].get("actual_path_id")
            if a_fpe[f][cur_p][e] == 1:
                load_e += float(b_f[f])
        u_e = (float(k_e.get(e, 1.0)) * load_e) / cap
        if u_e > max_u:
            max_u = u_e
    return max_u