# SDN Controller — Ubuntu / OVS / Mininet / Ryu Live Readiness Guide (`UBUNTU_REQUIRED`)

**Project:** `SDN-Traffic-Engineering-ML`  
**Role:** SDN Controller (`sdn/`)  
**Current Verification Status:** `PASS_MOCK` (`97/97` core/contract tests) + `PASS_MOCK_ADAPTER` (`5/5` OpenFlow/S0 adapter tests = `102/102` on Windows)  
**Target Live Status:** `PENDING_UBUNTU_VALIDATION` $\rightarrow$ `PASS_LIVE_OVS` (Requires Ubuntu Linux + Mininet + Open vSwitch + Ryu / OS-Ken OpenFlow 1.3)

---

## 1. Ubuntu System & Python Dependencies

Before running live data-plane tests on Ubuntu VirtualBox / Linux VM, ensure the following system packages and Python packages are installed:

### 1.1 System Packages (`apt`)
```bash
sudo apt update
sudo apt install -y \
    mininet \
    openvswitch-switch \
    openvswitch-common \
    iperf3 \
    iputils-ping \
    iproute2 \
    tcpdump \
    net-tools
```

### 1.2 Service Verification
```bash
sudo systemctl status openvswitch-switch
sudo ovs-vsctl --version
sudo mn --version
```

### 1.3 Python Environment (Ryu / OpenFlow 1.3 Runtime)
- **Note on Python version for Ryu:** Classic `ryu` (`4.34`) with `eventlet` typically runs best on Python `3.8`–`3.10` (or `os-ken` on Python `3.10+`). Because the pure-Python core (`sdn/`) has zero import-time dependency on `ryu`, it can either run inside the Ryu process (via a thin `RyuOpenFlowDriver`) or communicate with a thin Ryu bridge process.

---

## 2. Canonical Topology & OpenFlow 1.3 Switch Configuration

All Open vSwitch bridges must be configured for **OpenFlow 1.3** (`protocols=OpenFlow13`) with canonical DPIDs from `configs/network/topology_bundle.json`:

| Switch ID | Role | Canonical DPID (Int) | Canonical DPID (16-hex) | Port Mapping (`configs/network/links.csv`) |
| :--- | :--- | :---: | :---: | :--- |
| **`e1`** | Edge Switch 1 | `1` | `0000000000000001` | `p1 <-> h1`, `p2 <-> h2`, `p3 <-> c1:p1`, `p4 <-> c2:p1` |
| **`e2`** | Edge Switch 2 | `2` | `0000000000000002` | `p1 <-> h3`, `p2 <-> h4`, `p3 <-> c1:p2`, `p4 <-> c2:p2` |
| **`e3`** | Edge Switch 3 | `3` | `0000000000000003` | `p1 <-> h5`, `p2 <-> h6`, `p3 <-> c1:p3`, `p4 <-> c2:p3` |
| **`e4`** | Edge Switch 4 | `4` | `0000000000000004` | `p1 <-> h7`, `p2 <-> h8`, `p3 <-> c1:p4`, `p4 <-> c2:p4` |
| **`c1`** | Core Switch 1 (`P1`) | `5` | `0000000000000005` | `p1 <-> e1:p3`, `p2 <-> e2:p3`, `p3 <-> e3:p3`, `p4 <-> e4:p3` |
| **`c2`** | Core Switch 2 (`P2`) | `6` | `0000000000000006` | `p1 <-> e1:p4`, `p2 <-> e2:p4`, `p3 <-> e3:p4`, `p4 <-> e4:p4` |

### 2.1 OpenFlow 1.3 Rule Structure (`RouteCompiler`)
For every canonical inter-edge flow `0x{src_host}{dst_host}` (48 flows, priority `100`, table `0`):
- **`cookie`**: `flow_cookie == int(flow_group_id, 16)` (e.g., `0x103 = 259`), `cookie_mask = 0xFFFFFFFFFFFFFFFF` for `OFPFC_MODIFY_STRICT` / `OFPFC_DELETE_STRICT` and `OFPMP_FLOW` queries.
- **`match_fields`** (default `FlowMatchPolicy.L3_IPV4_PAIR` in `FlowMatcherConfig`, serialized via `FlowMatchSpec.to_openflow_match_dict()`):
  - `in_port`: Expected ingress switch port on that hop (`e_src`, `c1`/`c2`, `e_dst`)
  - `eth_type`: `0x0800` (IPv4)
  - `ipv4_src`: Source host IPv4 (`10.0.0.{src_host}`)
  - `ipv4_dst`: Destination host IPv4 (`10.0.0.{dst_host}`)
  - (Optional when `FlowMatchPolicy.L4_UDP_DPORT` is selected: `ip_proto=17`, `udp_dst`)
- **`actions`**: `[OFPActionOutput(rule.out_port)]`
- **ARP Handling on Live Mininet:** Either pre-populate static ARP tables on `h1`–`h8` (`net.staticArp()`) or install a low-priority ARP rule (`eth_type=0x0806`) so host-to-host `ping` and `iperf3` never block on broadcast ARP storms across the leaf-spine fabric.

---

## 3. `RyuOpenFlowDriver` Implementation Blueprint

When transitioning to Ubuntu, implement `RyuOpenFlowDriver(SwitchDriverProtocol)` with `evidence_mode = EvidenceMode.LIVE_OVS`:

1. **`send_flow_mod(rule: OpenFlowRuleSpec, command: FlowModCommand, *, phase: SwitchPhaseTag) -> FlowModReceipt`:**
   - Map `command` (`FlowModCommand.ADD` $\rightarrow$ `ofproto.OFPFC_ADD`, `FlowModCommand.MODIFY_STRICT` $\rightarrow$ `ofproto.OFPFC_MODIFY_STRICT`, `FlowModCommand.DELETE_STRICT` $\rightarrow$ `ofproto.OFPFC_DELETE_STRICT`).
   - Build `OFPMatch(**rule.match.to_openflow_match_dict())` and `OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, [OFPActionOutput(rule.out_port)])`.
   - Send `OFPFlowMod` with `cookie=rule.cookie_int`, `priority=rule.priority`, `table_id=rule.table_id` to `datapath` (`rule.dpid`) and record wire latency.
2. **`send_barrier(dpid: int, *, phase: SwitchPhaseTag) -> BarrierReceipt`:**
   - Send `OFPBarrierRequest(datapath)` and await `EventOFPBarrierReply` within `barrier_timeout_ms`.
3. **`read_flow_stats(dpid: int, *, flow_group_id: Optional[str] = None, cookie_int: Optional[int] = None) -> tuple[SwitchFlowStatsEntry, ...]`:**
   - Send `OFPFlowStatsRequest(datapath, table_id=0, cookie=cookie_int or 0, cookie_mask=0xFFFFFFFFFFFFFFFF if cookie_int is not None else 0)` and parse `EventOFPFlowStatsReply` into `SwitchFlowStatsEntry` (`packet_count`, `byte_count`, `out_port`, `match`, `cookie_int`).
4. **`verify_forwarding_counter_or_probe(compiled_plan: CompiledPathPlan, *, probe_id: Optional[str] = None) -> ForwardingCheckResult`:**
   - Confirm positive `packet_count` / `byte_count` delta across `compiled_plan.hop_rules` or execute a lightweight control/data-plane probe along `compiled_plan`.
5. **`get_operation_log() -> tuple[SwitchOperationLogEntry, ...]`:**
   - Return chronological `SwitchOperationLogEntry` audit records for all `FLOW_MOD`, `BARRIER`, `FLOW_STATS`, and `FORWARDING_CHECK` calls.

Because `RuleInstaller`, `RuleVerifier`, `StateManager`, `DecisionHandler`, and `SnapshotCreator` depend strictly on `SwitchDriverProtocol`, **zero lines of business logic need to change**.

---

## 4. Live Test Execution Matrix (`S0`–`S7`, `F09`–`F12`)

Execute the following scenarios on Ubuntu with Mininet + OVS + Ryu and collect raw log artifacts before claiming `PASS_LIVE_OVS`:

| Test ID | Scenario Description | Commands / Verification Method on Ubuntu | Required Evidence for `PASS_LIVE_OVS` |
| :--- | :--- | :--- | :--- |
| **`S0`** | **Topology, Baseline P1 Bootstrap & Overhead Calibration (`k_e`)** | Launch Mininet 8-host 6-switch topology; bootstrap 48 flows on `P1` (`c1`); run `pingall` and baseline `iperf3` flows; dump `ovs-ofctl -O OpenFlow13 dump-flows` on `e1`–`e4`, `c1`, `c2`. | `0%` ping loss; all 48 flows verified on `P1` (`state_version == 1`); `ovs-ofctl` flow dumps matching `RouteCompiler`; calibrated `k_e` recorded. |
| **`S1`–`S2`** | **Causal Snapshot Creation & Closed-Loop Trigger** | Generate traffic via `iperf3`; feed causal `released_demand` to `SnapshotCreator`; verify `Snapshot` JSON (`is_valid == True`) stored by Data. | Raw `Snapshot` JSON artifact with `evidence_mode == "LIVE_OVS"` and zero future data leakage. |
| **`S3`** | **Two-Phase Downstream-First Route Switch (`P1 -> P2` & `P2 -> P1`)** | Submit `ApplyRequest` flipping active `iperf3` flows (`0x103: P1 -> P2`); capture OpenFlow wire order (`c2`/`e2` + `Barrier` $\rightarrow$ `e1` + `Barrier`). | Zero transient blackhole drops during path flip; `ovs-ofctl dump-flows e1` shows `out_port=4`; `c2` `packet_count` increments; `state_version` increments by `+1`. |
| **`S4`** | **Pre-Write Validation & Rejection (`AC-S02`)** | Submit stale snapshot, wrong `expected_state_version`, `len(changes) > B`, and active hold-down requests against live controller. | `ApplyResponse.status == REJECTED`, `rule_count == 0`, zero `OFPFlowMod` packets on wire (`tcpdump` / OVS logs), `state_version` unchanged. |
| **`S5`** | **Idempotency & Conflict Detection (`AC-S08`)** | Re-send identical `(run_id, decision_batch_id)` and conflicting payload to live controller. | Replay returns cached `ApplyResponse` (`idempotent_replay == True`, `0` new `FlowMod`s); conflicting payload returns `BATCH_ID_CONFLICT`. |
| **`S6` / `F09`–`F10`** | **Switch / Barrier Timeout & Verified Rollback (`AC-S04`)** | Inject delay/drop on `e_src` (`dpid=1`) `FlowMod` or `Barrier` during `P1 -> P2` switch. | Controller rolls back downstream rules, verifies `0x103_P1` via `ovs-ofctl` & counters, returns `ROLLED_BACK` with `actual_path_id="0x103_P1"`. |
| **`S7` / `F11`–`F12`** | **`FlowStats` Mismatch, `UNKNOWN` & Reconciliation (`AC-S04`, `AC-S10`)** | Tamper with a flow entry on `e1` via `ovs-ofctl` or block `FlowStatsReply`; then run `recover_and_reconcile_run()`. | Flow marked `UNKNOWN` (`actual_path_id=None`), batch `INCONSISTENT`, subsequent `ApplyRequest` rejected with `RECONCILIATION_REQUIRED` until `recover_and_reconcile_run()` restores `VERIFIED` state. |

---

## 5. Mandatory Criteria to Upgrade `PASS_MOCK` $\rightarrow$ `PASS_LIVE_OVS`

A test or acceptance criterion (`AC-S01`–`AC-S10`) may only be marked **`PASS_LIVE_OVS`** when ALL of the following artifacts are captured from an actual Ubuntu + Mininet + OVS + Ryu run:
1. Raw `ovs-vsctl show` and `ovs-ofctl -O OpenFlow13 dump-flows <switch>` outputs before and after route changes.
2. Live `FlowStatsReply` (`packet_count`, `byte_count`) and OVS port counter dumps (`ovs-ofctl -O OpenFlow13 dump-ports <switch>`).
3. `ApplyResponse` and `Snapshot` JSON artifacts carrying `evidence_mode: "LIVE_OVS"`.
4. Zero regressions in the 102 Windows tests (`pytest sdn/tests/`).

---

## 6. S0 Ryu / OS-Ken Live Execution & Network Handoff (`PENDING_UBUNTU_VALIDATION`)

> **Important:** All commands below are prepared for Ubuntu Linux + Mininet + Open vSwitch and have **not** been executed on Windows. Their live status is **`PENDING_UBUNTU_VALIDATION`**, never `PASS_LIVE_OVS`.

### 6.1 Implemented Runtime Files
- **OpenFlow 1.3 Driver:** `sdn/flow_management/ryu_openflow_driver.py` (`RyuOpenFlowDriver(SwitchDriverProtocol)`).
- **S0 Ryu / OS-Ken Application:** `sdn/apps/ryu_s0_app.py` (`RyuS0App` + pure-Python `S0ControllerOrchestrator`).

### 6.2 Python & Ryu / OS-Ken Compatibility
- **Ryu (`ryu==4.34`) + `eventlet`:** Recommended on Python `3.8`–`3.10`.
- **OS-Ken (`os-ken`):** Recommended on Python `3.10+` (`osken-manager` CLI).
- **Port Alignment with Network (`network/topology/topology.py`):**
  `network/topology/topology.py` connects all 6 OVS switches (`e1`–`e4`, `c1`–`c2`, `protocols='OpenFlow13'`) to `RemoteController('c0', ip='127.0.0.1', port=6633)`. Because Ryu/OS-Ken default to port `6653`, **always pass `--ofp-tcp-listen-port 6633`** (unless Network updates `port=6653`).

### 6.3 Exact Launch Commands (`PENDING_UBUNTU_VALIDATION`)

1. **Terminal 1 — Start SDN S0 Controller (choose Ryu or OS-Ken):**
   ```bash
   # Option A: Ryu (Python 3.8-3.10)
   PYTHONPATH=. ryu-manager --ofp-tcp-listen-port 6633 sdn/apps/ryu_s0_app.py

   # Option B: OS-Ken (Python 3.10+)
   PYTHONPATH=. osken-manager --ofp-tcp-listen-port 6633 sdn/apps/ryu_s0_app.py
   ```
   Optional environment flags (defaults are `0` / strict):
   - `SDN_RUN_ID`: default `s0_live_run`
   - `SDN_ENABLE_INTRA_EDGE_L3=1`: install helper rules for the 8 intra-edge host pairs (`INTRA_EDGE_HOST_PAIRS`, cookie `0xFFFF0001`, separate from the 48 canonical TE flows)
   - `SDN_ENABLE_ARP_EDGE_LOCAL_FLOOD=1`: install controlled edge-local ARP helper rules (cookie `0xFFFF0002`) if Mininet `staticArp()` is not used
   - `SDN_PREINSTALL_BOTH_DOWNSTREAM=1`: default `1` (pre-installs both `P1` and `P2` downstream hops)

2. **Terminal 2 — Start Network's Mininet Topology:**
   ```bash
   sudo mn -c
   sudo python3 network/topology/topology.py
   ```
   *(Verify `RemoteController` connects to `127.0.0.1:6633` and all 6 DPIDs `1..6` appear in controller logs.)*

3. **Terminal 3 — Inspect OVS Switches & Flow Tables (`PENDING_UBUNTU_VALIDATION`):**
   ```bash
   sudo ovs-vsctl show
   for sw in e1 e2 e3 e4 c1 c2; do
       echo "=== $sw ==="
       sudo ovs-ofctl -O OpenFlow13 dump-flows "$sw"
   done
   ```

4. **Data-Plane & Counter Verification (`PENDING_UBUNTU_VALIDATION`):**
   - Generate inter-edge traffic (e.g., `mininet> h1 ping -c 5 10.0.0.3` or `iperf3` between inter-edge pairs).
   - Verify OVS counters increment on `P1` hops:
     ```bash
     sudo ovs-ofctl -O OpenFlow13 dump-flows e1 | grep cookie=0x103
     sudo ovs-ofctl -O OpenFlow13 dump-ports e1
     ```
   - Trigger/confirm `orchestrator.reconcile_after_reconnect_or_traffic()` (or active probe) so the 48 flows only transition to `VERIFIED` (`LIVE_OVS`) after real `packet_count` delta / active probe evidence.

### 6.4 `pingall` Scope: 48 Inter-Edge TE Flows vs. 8 Intra-Edge Pairs & ARP
- **Canonical TE Scope (`configs/network/flow_groups.csv`):** Exactly **48 directed inter-edge flows** (`h_i -> h_j` where `src_edge != dst_edge`).
- **Outside the 48 Canonical Flows (8 directed intra-edge pairs):**
  - `e1`: `h1 -> h2`, `h2 -> h1`
  - `e2`: `h3 -> h4`, `h4 -> h3`
  - `e3`: `h5 -> h6`, `h6 -> h5`
  - `e4`: `h7 -> h8`, `h8 -> h7`
- **ARP Requirement:** `network/topology/topology.py` currently does **not** call `net.staticArp()`. Without either:
  1. `net.staticArp()` in Mininet (recommended by Network/SDN integration), **or**
  2. opt-in `SDN_ENABLE_ARP_EDGE_LOCAL_FLOOD=1` + `SDN_ENABLE_INTRA_EDGE_L3=1` in `RyuS0App`,
  a full `mininet> pingall` will drop intra-edge pings and/or ARP resolution even when all 48 inter-edge flows are installed and working.

### 6.5 S0 Evidence Checklist (Required from Ubuntu Run)
- [ ] `PENDING_UBUNTU_VALIDATION`: Controller log showing all 6 switches (`DPID 1..6`) connected via OpenFlow 1.3 (`0x04`) and topology hash verified.
- [ ] `PENDING_UBUNTU_VALIDATION`: `ovs-ofctl -O OpenFlow13 dump-flows` on `e1`–`e4`, `c1`–`c2` confirming downstream-first + Barrier + source-edge `P1` rules.
- [ ] `PENDING_UBUNTU_VALIDATION`: Inter-edge `ping` / `iperf3` raw output + non-zero `packet_count` in `dump-flows` / `FlowStatsReply`.
- [ ] `PENDING_UBUNTU_VALIDATION`: `S0BootstrapReport.verified_48_flows == True` only after real counter/probe evidence (`EvidenceMode.LIVE_OVS`).
