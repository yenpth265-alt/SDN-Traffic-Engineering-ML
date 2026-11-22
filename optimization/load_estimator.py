import csv
import json
import os
from typing import Any, Mapping, Sequence


def load_topology_from_configs(config_dir: str = "configs/network") -> dict[str, Any]:
    """
    Nạp dữ liệu topology chuẩn từ configs/network/ và dựng ma trận a_fpe[f][p][e]
    khớp 100% với milp/model.py.
    """
    F: list[str] = []
    P: dict[str, list[str]] = {}
    E: list[str] = []
    C: dict[str, float] = {}
    raw_path_links: dict[tuple[str, str], set[str]] = {}

    # 1. Đọc danh sách liên kết và băng thông (links.csv)
    links_file = os.path.join(config_dir, "links.csv")
    with open(links_file, mode="r", encoding="utf-8") as f_links:
        for row in csv.DictReader(f_links):
            link_id = row["link_id"].strip()
            cap = float(row.get("capacity_mbps") or row.get("capacity") or 100.0)
            E.append(link_id)
            C[link_id] = cap

    # 2. Đọc danh sách 48 nhóm luồng (flow_groups.csv)
    fg_file = os.path.join(config_dir, "flow_groups.csv")
    with open(fg_file, mode="r", encoding="utf-8") as f_fg:
        for row in csv.DictReader(f_fg):
            fg_id = row["flow_group_id"].strip()
            F.append(fg_id)
            P[fg_id] = []

    # 3. Đọc 96 tuyến ứng viên (candidate_paths.csv)
    cp_file = os.path.join(config_dir, "candidate_paths.csv")
    with open(cp_file, mode="r", encoding="utf-8") as f_cp:
        for row in csv.DictReader(f_cp):
            fg_id = row["flow_group_id"].strip()
            path_id = row["path_id"].strip()
            raw_links = row.get("links") or row.get("link_ids") or row.get("path_links") or ""
            if raw_links.startswith("["):
                links_on_path = json.loads(raw_links)
            else:
                sep = ";" if ";" in raw_links else ","
                links_on_path = [x.strip() for x in raw_links.split(sep) if x.strip()]

            P.setdefault(fg_id, []).append(path_id)
            raw_path_links[(fg_id, path_id)] = {x.strip() for x in links_on_path}

    # 4. Dựng chỉ thị 3 chiều a_fpe[f][p][e] khớp với model.py
    a_fpe: dict[str, dict[str, dict[str, int]]] = {}
    for f in F:
        a_fpe[f] = {}
        for p in P[f]:
            link_set = raw_path_links.get((f, p), set())
            a_fpe[f][p] = {e: (1 if e in link_set else 0) for e in E}

    return {
        "F": F,
        "P": P,
        "E": E,
        "C": C,
        "a_fpe": a_fpe,
    }


def calculate_mean_demand(forecast_data: Mapping[str, Any], flow_groups: Sequence[str]) -> dict[str, float]:
    """Tự tính trung bình cộng b_f từ mảng predicted_mbps_array gồm H bước."""
    pred_map = forecast_data.get("predictions", forecast_data)
    b_f: dict[str, float] = {}
    for f in flow_groups:
        rec = pred_map[f]
        h_values = rec["predicted_mbps_array"] if isinstance(rec, Mapping) else rec
        b_f[f] = sum(float(v) for v in h_values) / len(h_values)
    return b_f