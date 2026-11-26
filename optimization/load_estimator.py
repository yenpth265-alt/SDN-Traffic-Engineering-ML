import csv
import json
import os
from typing import Any, Mapping, Sequence

from optimization.input_validator import extract_forecast_series


def _parse_token_list(raw_val: Any) -> list[str]:
    if raw_val is None:
        return []
    if isinstance(raw_val, list):
        out: list[str] = []
        for item in raw_val:
            if isinstance(item, Mapping):
                val = item.get("link_id") or item.get("edge_id") or item.get("node_id") or item.get("id")
                if val is not None and str(val).strip():
                    out.append(str(val).strip())
            elif item is not None and str(item).strip():
                out.append(str(item).strip())
        return out
    if not isinstance(raw_val, str):
        return []
    text = raw_val.strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            return _parse_token_list(json.loads(text))
        except Exception:
            pass
    for sep in (";", "->", "|", ","):
        if sep in text:
            return [x.strip() for x in text.split(sep) if x.strip()]
    return [text]


def _extract_links_from_item(
    item: Mapping[str, Any],
    valid_links: set[str],
    pair_to_link: Mapping[tuple[str, str], str],
) -> list[str]:
    preferred_link_keys = (
        "links",
        "link_ids",
        "directed_links",
        "path_links",
        "edges",
        "edge_ids",
        "link_sequence",
        "route_links",
        "link_list",
        "segments",
    )
    for key in preferred_link_keys:
        if key in item:
            tokens = [t for t in _parse_token_list(item[key]) if t in valid_links]
            if tokens:
                return tokens

    for key, val in item.items():
        if key in ("flow_group_id", "path_id", "id", "name"):
            continue
        tokens = [t for t in _parse_token_list(val) if t in valid_links]
        if tokens:
            return tokens

    node_keys = ("nodes", "node_ids", "node_sequence", "path_nodes", "hops", "switches")
    for key in node_keys:
        if key in item:
            nodes = _parse_token_list(item[key])
            if len(nodes) >= 2:
                mapped = [
                    pair_to_link[(nodes[i], nodes[i + 1])]
                    for i in range(len(nodes) - 1)
                    if (nodes[i], nodes[i + 1]) in pair_to_link
                ]
                if mapped:
                    return mapped

    return []


def load_topology_from_configs(config_dir: str = "configs/network") -> dict[str, Any]:
    """
    Nạp topology chuẩn từ configs/network/topology_bundle.json và các tệp CSV.
    Khớp 100% với TopologyRegistry của SDN (32 links, 48 flows, 96 paths).
    """
    F: list[str] = []
    P: dict[str, list[str]] = {}
    E: list[str] = []
    C: dict[str, float] = {}
    fabric_links: set[str] = set()
    pair_to_link: dict[tuple[str, str], str] = {}
    raw_path_links: dict[tuple[str, str], list[str]] = {}
    topology_version = "1.0"
    topology_hash: str | None = None

    bundle_path = os.path.join(config_dir, "topology_bundle.json")
    cp_csv_path = os.path.join(config_dir, "candidate_paths.csv")

    if os.path.exists(bundle_path):
        with open(bundle_path, mode="r", encoding="utf-8") as fb:
            bundle = json.load(fb)

        topology_version = str(bundle.get("topology_version", "1.0")).lstrip("v")
        topology_hash = bundle.get("topology_hash") or bundle.get("sha256")

        raw_links = bundle.get("links", [])
        link_items = raw_links.values() if isinstance(raw_links, Mapping) else raw_links
        for item in link_items:
            link_id = str(item.get("link_id") or item.get("id")).strip()
            cap = float(item.get("capacity_mbps") or item.get("capacity") or 100.0)
            E.append(link_id)
            C[link_id] = cap
            if item.get("is_fabric", True):
                fabric_links.add(link_id)

            u = item.get("src") or item.get("src_node") or item.get("from_node") or item.get("u") or item.get("source")
            v = item.get("dst") or item.get("dst_node") or item.get("to_node") or item.get("v") or item.get("target")
            if u is not None and v is not None:
                pair_to_link[(str(u).strip(), str(v).strip())] = link_id

        valid_links = set(E)

        raw_fgs = bundle.get("flow_groups", [])
        fg_items = raw_fgs.values() if isinstance(raw_fgs, Mapping) else raw_fgs
        for item in fg_items:
            fg_id = str(item.get("flow_group_id") or item.get("id")).strip()
            F.append(fg_id)
            P[fg_id] = []

        raw_cps = bundle.get("candidate_paths", [])
        cp_items = raw_cps.values() if isinstance(raw_cps, Mapping) else raw_cps
        for item in cp_items:
            fg_id = str(item["flow_group_id"]).strip()
            path_id = str(item["path_id"]).strip()
            if path_id not in P.setdefault(fg_id, []):
                P[fg_id].append(path_id)
            links_on_path = _extract_links_from_item(item, valid_links, pair_to_link)
            if links_on_path:
                raw_path_links[(fg_id, path_id)] = links_on_path

    if not E:
        with open(os.path.join(config_dir, "links.csv"), mode="r", encoding="utf-8") as f_links:
            for row in csv.DictReader(f_links):
                link_id = row["link_id"].strip()
                cap = float(row.get("capacity_mbps") or row.get("capacity") or 100.0)
                E.append(link_id)
                C[link_id] = cap
                if str(row.get("is_fabric", "true")).lower() in ("1", "true", "yes"):
                    fabric_links.add(link_id)
                u = row.get("src") or row.get("src_node") or row.get("from_node") or row.get("u")
                v = row.get("dst") or row.get("dst_node") or row.get("to_node") or row.get("v")
                if u and v:
                    pair_to_link[(u.strip(), v.strip())] = link_id

    if not F:
        with open(os.path.join(config_dir, "flow_groups.csv"), mode="r", encoding="utf-8") as f_fg:
            for row in csv.DictReader(f_fg):
                fg_id = row["flow_group_id"].strip()
                F.append(fg_id)
                P[fg_id] = []

    valid_links = set(E)
    if os.path.exists(cp_csv_path) and len(raw_path_links) < sum(len(v) for v in P.values()):
        with open(cp_csv_path, mode="r", encoding="utf-8") as f_cp:
            for row in csv.DictReader(f_cp):
                fg_id = row["flow_group_id"].strip()
                path_id = row["path_id"].strip()
                if path_id not in P.setdefault(fg_id, []):
                    P[fg_id].append(path_id)
                if (fg_id, path_id) not in raw_path_links:
                    links_on_path = _extract_links_from_item(row, valid_links, pair_to_link)
                    if links_on_path:
                        raw_path_links[(fg_id, path_id)] = links_on_path

    a_fpe: dict[str, dict[str, dict[str, int]]] = {}
    for f in F:
        a_fpe[f] = {}
        for p in P[f]:
            link_set = set(raw_path_links.get((f, p), ()))
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
    """
    Tính nhu cầu trung bình b_f (Mbps) trên cửa sổ dự báo H bước.
    Theo A07: epsilon đã được ML áp dụng đúng 1 lần trước khi xuất ForecastBatch READY,
    Optimization không cộng thêm epsilon.
    """
    series_map = extract_forecast_series(forecast_data)
    b_f: dict[str, float] = {}
    for f in flow_groups:
        h_values = series_map[f]
        b_f[f] = sum(float(v) for v in h_values) / len(h_values)
    return b_f


def evaluate_routing_utilization(
    topology: Mapping[str, Any],
    b_f: Mapping[str, float],
    routes: Mapping[str, Mapping[str, Any]],
    default_ce: float = 100.0,
) -> float:
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