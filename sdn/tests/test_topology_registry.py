"""
Unit and contract tests for sdn.controller.topology_registry (Phase 1).

Tests run 100% on Windows in pure Python without Ryu, Mininet, or Open vSwitch.
Original Network catalog files under configs/network/ are read-only and never modified;
all negative/error test cases use deep-copied in-memory fixtures or pytest tmp_path copies.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from sdn.controller.topology_registry import (
    TopologyLookupError,
    TopologyRegistry,
    TopologyValidationError,
    compute_network_topology_hash,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
NETWORK_CONFIG_DIR = REPO_ROOT / "configs" / "network"
BUNDLE_PATH = NETWORK_CONFIG_DIR / "topology_bundle.json"


@pytest.fixture(scope="module")
def raw_bundle_dict() -> dict:
    """Load the raw TopologyBundle JSON dictionary once per test module."""
    return json.loads(BUNDLE_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def registry() -> TopologyRegistry:
    """Load and validate TopologyRegistry from the official Network bundle and CSVs."""
    return TopologyRegistry.from_bundle_file(
        BUNDLE_PATH,
        csv_config_dir=NETWORK_CONFIG_DIR,
        verify_hash=True,
    )


def _resign_bundle(bundle: dict) -> dict:
    """Recompute and update topology_hash after mutating a deep-copied bundle fixture."""
    bundle["topology_hash"] = compute_network_topology_hash(bundle)
    return bundle


# -----------------------------------------------------------------------------
# 1. Load Bundle & Verify Topology Invariants (AC-N01, AC-N02, TC-N01)
# -----------------------------------------------------------------------------


def test_load_bundle_and_csv_consistency(registry: TopologyRegistry, raw_bundle_dict: dict) -> None:
    """
    Verify TopologyBundle loads cleanly and matches all 4 Network CSV catalogs.
    Distinguishes historical bundle (`1.0`, `2fa43f87...`) from canonical `v1.0` (`ec6c6969...`)
    without ever hardcoding `ovs_port_mapping_verified=True` without supporting OVS evidence.
    """
    assert registry.schema_version == "1.0"
    assert registry.topology_version in ("1.0", "v1.0")
    assert registry.topology_hash == compute_network_topology_hash(raw_bundle_dict)
    assert len(registry.topology_hash) == 64
    assert registry.ovs_port_mapping_verified is bool(raw_bundle_dict.get("ovs_port_mapping_verified", False))


def test_cf05_canonical_v10_bundle_contract(raw_bundle_dict: dict, tmp_path: Path) -> None:
    """
    CF-05 Canonical Network `v1.0` Contract Check:
    Verifies that a `v1.0` bundle with `ovs_port_mapping_verified=False` (Colab export)
    recomputes the exact approved raw 64-hex digest `ec6c6969...` and passes `verify_hash=True`
    against the 4 matching Network CSV catalogs.
    """
    v10_bundle = copy.deepcopy(raw_bundle_dict)
    v10_bundle["topology_version"] = "v1.0"
    v10_bundle["ovs_port_mapping_verified"] = False
    v10_bundle["topology_hash"] = compute_network_topology_hash(v10_bundle)
    assert (
        v10_bundle["topology_hash"]
        == "ec6c6969019b1c36744339fb54f0f608c95402ae972859b30710dc44b5dc3956"
    )
    bpath = tmp_path / "topology_bundle.json"
    bpath.write_text(json.dumps(v10_bundle), encoding="utf-8")
    reg = TopologyRegistry.from_bundle_file(bpath, csv_config_dir=NETWORK_CONFIG_DIR, verify_hash=True)
    assert reg.topology_version == "v1.0"
    assert reg.topology_hash == "ec6c6969019b1c36744339fb54f0f608c95402ae972859b30710dc44b5dc3956"
    assert reg.ovs_port_mapping_verified is False
    assert len(reg.get_all_switches()) == 6
    assert len(reg.get_all_flow_groups()) == 48


def test_topology_counts_and_invariants(registry: TopologyRegistry) -> None:
    """
    Verify exact counts:
    - 8 hosts (h1..h8)
    - 6 switches (e1..e4, c1..c2)
    - 32 directed links
    - 16 directed fabric links
    - 48 flow groups (inter-edge, sorted by ascending numeric hex ID)
    - 96 candidate paths (P1 via c1, P2 via c2)
    - 48 initial paths (all P1)
    - 192 path-fabric-link incidences (12 per fabric link)
    """
    hosts = registry.get_all_hosts()
    switches = registry.get_all_switches()
    all_links = registry.get_all_links()
    fabric_links = registry.get_fabric_links()
    flows = registry.get_all_flow_groups()

    assert len(hosts) == 8
    assert [h.host_id for h in hosts] == [f"h{i}" for i in range(1, 9)]

    assert len(switches) == 6
    assert [(s.switch_id, s.dpid, s.role) for s in switches] == [
        ("e1", 1, "edge"),
        ("e2", 2, "edge"),
        ("e3", 3, "edge"),
        ("e4", 4, "edge"),
        ("c1", 5, "core"),
        ("c2", 6, "core"),
    ]

    assert len(all_links) == 32
    assert len(fabric_links) == 16
    assert len(flows) == 48

    # Check ascending numeric order of hex flow IDs
    numeric_ids = [f.numeric_id for f in flows]
    assert numeric_ids == sorted(numeric_ids)
    assert flows[0].flow_group_id == "0x103"
    assert flows[-1].flow_group_id == "0x806"

    # Count candidate paths, initial paths, and fabric incidences
    total_paths = 0
    total_incidences = 0
    for fg in flows:
        p1, p2 = registry.get_candidate_paths_for_flow(fg.flow_group_id)
        assert p1.path_id == f"{fg.flow_group_id}_P1"
        assert p2.path_id == f"{fg.flow_group_id}_P2"
        assert p1.core_switch == "c1"
        assert p2.core_switch == "c2"
        total_paths += 2
        total_incidences += len(p1.fabric_link_ids) + len(p2.fabric_link_ids)

        init_entry = registry.get_initial_path(fg.flow_group_id)
        assert init_entry.initial_path_id == f"{fg.flow_group_id}_P1"
        assert init_entry.path_label == "P1"

    assert total_paths == 96
    assert total_incidences == 192


# -----------------------------------------------------------------------------
# 2. Canonical Lookups for Flow 0x103, 0x103_P1, 0x103_P2, Links & Switch Hops
# -----------------------------------------------------------------------------


def test_lookup_flow_0x103(registry: TopologyRegistry) -> None:
    """Verify detailed lookup of canonical flow 0x103 (h1 -> h3)."""
    fg = registry.get_flow_group("0x103")
    assert fg.stt == 1
    assert fg.flow_group_id == "0x103"
    assert fg.flow_cookie == "0x103"
    assert fg.numeric_id == 0x103 == 259
    assert fg.cookie_int == 0x103
    assert fg.src_host == "h1"
    assert fg.dst_host == "h3"
    assert fg.src_ip == "10.0.0.1"
    assert fg.dst_ip == "10.0.0.3"
    assert fg.src_edge == "e1"
    assert fg.dst_edge == "e2"
    assert fg.udp_port == 5001

    assert registry.get_flow_by_hosts("h1", "h3") == fg


def test_lookup_candidate_paths_0x103_p1_and_p2(registry: TopologyRegistry) -> None:
    """Verify lookup and attributes of 0x103_P1 and 0x103_P2."""
    p1 = registry.get_candidate_path("0x103_P1")
    assert p1.path_id == "0x103_P1"
    assert p1.flow_group_id == "0x103"
    assert p1.path_label == "P1"
    assert p1.core_switch == "c1"
    assert p1.hop_count == 4
    assert p1.nominal_delay_ms == pytest.approx(12.0)
    assert p1.ordered_link_ids == ("h1>e1", "e1>c1", "c1>e2", "e2>h3")
    assert p1.fabric_link_ids == ("e1>c1", "c1>e2")

    p2 = registry.get_candidate_path("0x103_P2")
    assert p2.path_id == "0x103_P2"
    assert p2.flow_group_id == "0x103"
    assert p2.path_label == "P2"
    assert p2.core_switch == "c2"
    assert p2.hop_count == 4
    assert p2.nominal_delay_ms == pytest.approx(12.0)
    assert p2.ordered_link_ids == ("h1>e1", "e1>c2", "c2>e2", "e2>h3")
    assert p2.fabric_link_ids == ("e1>c2", "c2>e2")

    assert registry.get_path_by_label("0x103", "P1") == p1
    assert registry.get_path_by_label("0x103", "P2") == p2
    assert registry.get_initial_candidate_path("0x103") == p1


def test_switch_hops_and_port_mapping_for_0x103(registry: TopologyRegistry) -> None:
    """Verify compiled per-switch (in_port, out_port) hops for 0x103_P1 and 0x103_P2."""
    hops_p1 = registry.get_switch_hops("0x103_P1")
    assert len(hops_p1) == 3

    # Hop 0: e1 (dpid=1), in_port=1 (from h1), out_port=3 (to c1)
    assert (hops_p1[0].switch_id, hops_p1[0].dpid, hops_p1[0].in_port, hops_p1[0].out_port) == (
        "e1",
        1,
        1,
        3,
    )
    # Hop 1: c1 (dpid=5), in_port=1 (from e1), out_port=2 (to e2)
    assert (hops_p1[1].switch_id, hops_p1[1].dpid, hops_p1[1].in_port, hops_p1[1].out_port) == (
        "c1",
        5,
        1,
        2,
    )
    # Hop 2: e2 (dpid=2), in_port=3 (from c1), out_port=1 (to h3)
    assert (hops_p1[2].switch_id, hops_p1[2].dpid, hops_p1[2].in_port, hops_p1[2].out_port) == (
        "e2",
        2,
        3,
        1,
    )

    hops_p2 = registry.get_switch_hops("0x103_P2")
    assert len(hops_p2) == 3
    # Hop 0: e1 (dpid=1), in_port=1 (from h1), out_port=4 (to c2)
    assert (hops_p2[0].switch_id, hops_p2[0].dpid, hops_p2[0].in_port, hops_p2[0].out_port) == (
        "e1",
        1,
        1,
        4,
    )
    # Hop 1: c2 (dpid=6), in_port=1 (from e1), out_port=2 (to e2)
    assert (hops_p2[1].switch_id, hops_p2[1].dpid, hops_p2[1].in_port, hops_p2[1].out_port) == (
        "c2",
        6,
        1,
        2,
    )
    # Hop 2: e2 (dpid=2), in_port=4 (from c2), out_port=1 (to h3)
    assert (hops_p2[2].switch_id, hops_p2[2].dpid, hops_p2[2].in_port, hops_p2[2].out_port) == (
        "e2",
        2,
        4,
        1,
    )

    # Directed link lookup
    lk_e1_c1 = registry.get_link("e1>c1")
    assert lk_e1_c1.from_port == 3
    assert lk_e1_c1.to_port == 1
    assert lk_e1_c1.is_fabric is True
    assert registry.get_port_between("e1", "c1") == (3, 1)
    assert registry.get_port_between("c1", "e1") == (1, 3)


# -----------------------------------------------------------------------------
# 3. Reject Legacy & Non-Canonical IDs (Section 3.2, AC-D01, AC-N02)
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "legacy_flow_id",
    ["h1_h3", "h1-h3", "h1->h3", "f13", "103", "0x101", "0x102", "0x999", ""],
)
def test_reject_legacy_and_invalid_flow_ids(
    registry: TopologyRegistry, legacy_flow_id: str
) -> None:
    """Ensure legacy flow IDs (e.g. h1_h3, h1->h3) or same-edge IDs (0x102) are rejected."""
    with pytest.raises(TopologyLookupError):
        registry.get_flow_group(legacy_flow_id)


@pytest.mark.parametrize(
    "legacy_path_id",
    ["P1", "P2", "h1_h3_c1", "h1_h3_P1", "0x103_c1", "0x103_P3", ""],
)
def test_reject_legacy_and_bare_path_ids(
    registry: TopologyRegistry, legacy_path_id: str
) -> None:
    """Ensure bare path labels (P1/P2) and legacy path IDs (h1_h3_c1) are rejected."""
    with pytest.raises(TopologyLookupError):
        registry.get_candidate_path(legacy_path_id)


@pytest.mark.parametrize(
    "legacy_link_id",
    ["e1_to_c1", "e1-c1", "e1->c1", "e1_c1", ""],
)
def test_reject_legacy_link_ids(
    registry: TopologyRegistry, legacy_link_id: str
) -> None:
    """Ensure legacy link IDs (e.g. e1_to_c1) are rejected."""
    with pytest.raises(TopologyLookupError):
        registry.get_link(legacy_link_id)


# -----------------------------------------------------------------------------
# 4. Detect Duplicate IDs, Invalid Paths, Invalid Ports & Invalid Hash
# -----------------------------------------------------------------------------


def test_detect_invalid_topology_hash(raw_bundle_dict: dict) -> None:
    """Tampering with topology_hash or payload without updating hash must fail."""
    corrupted = copy.deepcopy(raw_bundle_dict)
    corrupted["topology_hash"] = "0" * 64
    with pytest.raises(TopologyValidationError, match="hash mismatch"):
        TopologyRegistry.from_bundle_dict(corrupted, verify_hash=True)


def test_detect_duplicate_flow_group_id(raw_bundle_dict: dict) -> None:
    """Duplicate flow_group_id must be rejected."""
    corrupted = copy.deepcopy(raw_bundle_dict)
    corrupted["flow_groups"][1]["flow_group_id"] = "0x103"
    corrupted["flow_groups"][1]["flow_cookie"] = "0x103"
    _resign_bundle(corrupted)
    with pytest.raises(TopologyValidationError):
        TopologyRegistry.from_bundle_dict(corrupted)


def test_detect_duplicate_path_id(raw_bundle_dict: dict) -> None:
    """Duplicate path_id in candidate_paths must be rejected."""
    corrupted = copy.deepcopy(raw_bundle_dict)
    corrupted["candidate_paths"][1]["path_id"] = "0x103_P1"
    corrupted["candidate_paths"][1]["path_label"] = "P1"
    _resign_bundle(corrupted)
    with pytest.raises(TopologyValidationError, match="Duplicate"):
        TopologyRegistry.from_bundle_dict(corrupted)


def test_detect_duplicate_switch_dpid(raw_bundle_dict: dict) -> None:
    """Duplicate switch DPID must be rejected."""
    corrupted = copy.deepcopy(raw_bundle_dict)
    corrupted["switches"][1]["dpid"] = 1
    corrupted["switches"][1]["dpid_hex"] = "0x1"
    _resign_bundle(corrupted)
    with pytest.raises(TopologyValidationError, match="Duplicate switch dpid"):
        TopologyRegistry.from_bundle_dict(corrupted)


def test_detect_path_discontinuity_and_invalid_hop(raw_bundle_dict: dict) -> None:
    """Broken link sequence in ordered_link_ids must be rejected."""
    corrupted = copy.deepcopy(raw_bundle_dict)
    # Replace c1>e2 with c1>e3 on path 0x103_P1 (which goes to h3 on e2)
    corrupted["candidate_paths"][0]["ordered_link_ids"] = [
        "h1>e1",
        "e1>c1",
        "c1>e3",
        "e2>h3",
    ]
    corrupted["candidate_paths"][0]["fabric_link_ids"] = ["e1>c1", "c1>e3"]
    _resign_bundle(corrupted)
    with pytest.raises(TopologyValidationError, match="discontinuity"):
        TopologyRegistry.from_bundle_dict(corrupted)


def test_detect_invalid_switch_port_mapping(raw_bundle_dict: dict) -> None:
    """Wrong port number on a fabric link (e.g. e1>c1 using port 4 instead of 3) must fail."""
    corrupted = copy.deepcopy(raw_bundle_dict)
    for lk in corrupted["links"]:
        if lk["link_id"] == "e1>c1":
            lk["from_port"] = 4  # collides with e1>c2 and violates e1->c1=port 3
    _resign_bundle(corrupted)
    with pytest.raises(TopologyValidationError):
        TopologyRegistry.from_bundle_dict(corrupted)


def test_detect_csv_bundle_mismatch_in_tmp_dir(
    raw_bundle_dict: dict, tmp_path: Path
) -> None:
    """
    Using a temporary directory copy of CSV files, verify that any discrepancy
    between topology_bundle.json and the CSV catalogs is caught without touching
    configs/network/.
    """
    for csv_name in (
        "links.csv",
        "flow_groups.csv",
        "candidate_paths.csv",
        "initial_paths.csv",
    ):
        src = NETWORK_CONFIG_DIR / csv_name
        dst = tmp_path / csv_name
        dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")

    # Corrupt initial_paths.csv in tmp_path so 0x103 starts on P2 instead of P1
    init_csv = tmp_path / "initial_paths.csv"
    init_text = init_csv.read_text(encoding="utf-8").replace(
        "0x103,0x103_P1,P1", "0x103,0x103_P2,P2"
    )
    init_csv.write_text(init_text, encoding="utf-8")

    tmp_bundle = tmp_path / "topology_bundle.json"
    tmp_bundle.write_text(json.dumps(raw_bundle_dict), encoding="utf-8")

    with pytest.raises(TopologyValidationError, match="initial_paths.csv"):
        TopologyRegistry.from_bundle_file(tmp_bundle, csv_config_dir=tmp_path)

