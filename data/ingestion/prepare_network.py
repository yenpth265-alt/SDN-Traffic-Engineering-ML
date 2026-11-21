import csv
import hashlib
import json
import math
import shutil
import zipfile
from collections import Counter

HEADER = "schedule_id scenario_id seed split time_offset_s flow_group_id requested_mbps pre_clip_mbps".split()
CORE = "schema_version topology_version hosts switches links flow_groups candidate_paths initial_paths".split()
CSV_SHA256 = "d8d915c198caedefc62d94e5a9d543d0a6394e7b24f79b550d33622f7997131a"
SCENARIOS = {"S1", "S2a", "S2b", "S3", "S4", "S5"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique(rows, key):
    result = {key(row): row for row in rows}
    require(len(result) == len(rows), "Duplicate topology key")
    return result


def validate_topology(bundle):
    data = {key: bundle[key] for key in CORE}
    digest = hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    require(digest == bundle["topology_hash"], "Topology hash mismatch")
    hosts = unique(bundle["hosts"], lambda h: h["host_id"])
    switches = unique(bundle["switches"], lambda s: s["switch_id"])
    links = unique(bundle["links"], lambda l: l["link_id"])
    flows = unique(bundle["flow_groups"], lambda g: g["flow_group_id"])
    paths = unique(bundle["candidate_paths"], lambda p: (p["flow_group_id"], p["path_id"]))
    initial = unique(bundle["initial_paths"], lambda p: p["flow_group_id"])
    require(tuple(map(len, (hosts, switches, links, flows, paths, initial))) ==
            (8, 6, 32, 48, 96, 48), "Unexpected topology counts")
    require(not hosts.keys() & switches.keys(), "Host/switch IDs overlap")
    require(sum(l["is_fabric"] for l in links.values()) == 16, "Unexpected fabric count")
    for link_id, link in links.items():
        src, dst = link["from_node"], link["to_node"]
        require(src in hosts or src in switches, f"Unknown source: {link_id}")
        require(dst in hosts or dst in switches, f"Unknown destination: {link_id}")
        require(link_id == f"{src}>{dst}" and link["capacity_mbps"] > 0 and link["delay_ms"] >= 0,
                f"Invalid link: {link_id}")

    require(Counter(fg for fg, _ in paths) == {fg: 2 for fg in flows}, "Expected two paths per group")
    for (fg, path_id), path in paths.items():
        group = flows[fg]
        hops = [links[lid] for lid in path["ordered_link_ids"]]
        require(len(hops) == path["hop_count"] == 4, f"{path_id}: invalid hop count")
        require(hops[0]["from_node"] == group["src_host"] and hops[-1]["to_node"] == group["dst_host"],
                f"{path_id}: invalid endpoints")
        require(all(a["to_node"] == b["from_node"] for a, b in zip(hops, hops[1:])),
                f"{path_id}: disconnected hops")
        fabric = [h["link_id"] for h in hops if h["is_fabric"]]
        require(len(fabric) == 2 and fabric == path["fabric_link_ids"], f"{path_id}: invalid fabric hops")
    for fg, row in initial.items():
        path = paths[(fg, row["initial_path_id"])]
        require(path["path_label"] == row["path_label"], f"{fg}: initial path label mismatch")
    return flows


def prepare_delivery(root, output):
    bundle = json.loads((root / "configs/network/topology_bundle.json").read_bytes())
    flows = validate_topology(bundle)
    indexes = {fg: i for i, fg in enumerate(flows)}
    manifest = json.loads((root / "configs/experiments/runner_manifest.json").read_bytes())
    allocations = unique(manifest["seed_allocations"], lambda a: a["seed"])
    allocation = {seed: a["split"] for seed, a in allocations.items()}
    require(set(allocation) == set(range(1, 21)), "Manifest must cover seeds 1–20")
    require(set(allocation.values()) == {"train", "validation", "test"}, "Invalid manifest split")
    phases = manifest["phases"]
    warmup = phases["warmup_seconds"]
    measured = phases["measurement_seconds"]
    require((warmup, measured) == (60, 360), "Use the GitHub manifest for this delivery")
    bins = warmup + measured

    folder = root / "network/traffic_schedules"
    archive = folder / "demand_plan.zip"
    output.mkdir(parents=True, exist_ok=True)
    temporary = output / "demand_stage.csv.part"
    digest = hashlib.sha256()
    metadata, seen = {}, {}
    with zipfile.ZipFile(archive) as z:
        require(z.namelist() == ["demand_plan.csv"], "Unexpected ZIP contents")
        with z.open("demand_plan.csv") as raw, temporary.open("wb") as dest:
            shutil.copyfileobj(raw, dest)
    with temporary.open("rb") as raw:
        for chunk in iter(lambda: raw.read(1024 * 1024), b""):
            digest.update(chunk)
    require(digest.hexdigest() == CSV_SHA256, "CSV hash differs from current Network delivery")
    with temporary.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        require(next(reader) == HEADER, "Unexpected CSV header")
        for line, row in enumerate(reader, 2):
            sid, scenario, seed_text, split, offset, fg, req, pre = row
            seed, pos = int(seed_text), int(offset)
            require(scenario in SCENARIOS and seed in allocation and sid == f"{scenario}_seed{seed:02d}",
                    f"Line {line}: unexpected schedule metadata")
            expected_split = allocation[seed]
            require(split == expected_split, f"Line {line}: unexpected source split")
            require(0 <= pos < bins and fg in indexes, f"Line {line}: invalid offset or unknown group")
            requested, before_clip = float(req), float(pre)
            require(all(math.isfinite(x) and x >= 0 for x in (requested, before_clip)),
                    f"Line {line}: invalid Mbps")
            if sid not in seen:
                metadata[sid] = (scenario, seed, split)
                seen[sid] = bytearray(bins * len(flows))
            require(metadata[sid] == (scenario, seed, split), f"Line {line}: inconsistent metadata")
            i = pos * len(flows) + indexes[fg]
            require(not seen[sid][i], f"Line {line}: duplicate schedule/flow/bin")
            seen[sid][i] = 1
    require(len(seen) == 120 and all(all(mask) for mask in seen.values()), "Missing schedules or bins")

    demand_file = temporary.replace(output / "demand_stage.csv")
    return {
        "topology_id": bundle["topology_hash"],
        "manifest_id": hashlib.sha256(json.dumps(allocation, sort_keys=True).encode()).hexdigest(),
        "file_hash": digest.hexdigest(),
        "warmup": warmup,
        "measurement": measured,
        "metadata": metadata,
        "flow_ids": set(flows),
        "demand_file": demand_file,
    }
