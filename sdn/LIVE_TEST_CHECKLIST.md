# SDN Controller — Ubuntu Live OVS / Mininet Verification Checklist (`UBUNTU_REQUIRED`)

**Project:** `SDN-Traffic-Engineering-ML`  
**Role:** SDN Controller (`sdn/`)  
**Current Status of All Live Items Below:** **`PENDING_UBUNTU_VALIDATION`** (Not yet executed on Ubuntu Linux; never claim `PASS_LIVE_OVS` until raw command outputs are captured on Ubuntu).

---

## 1. Environment & Dependency Checklist

- [ ] **`PENDING_UBUNTU_VALIDATION` — OS & Kernel:** Ubuntu 20.04 / 22.04 / 24.04 LTS (`uname -a`).
- [ ] **`PENDING_UBUNTU_VALIDATION` — Open vSwitch & Mininet Services:**
  - `sudo systemctl is-active openvswitch-switch` returns `active`.
  - `sudo ovs-vsctl --version` and `sudo mn --version` succeed.
- [ ] **`PENDING_UBUNTU_VALIDATION` — Python & OpenFlow Manager:**
  - Python `3.8`–`3.10` with `ryu==4.34` (`ryu-manager --version`), **or**
  - Python `3.10+` with `os-ken` (`osken-manager --version`).

---

## 2. Step-by-Step Live S0 & Closed-Loop Verification Checklist

### Step A — Controller Startup & Port `6633` Listening
- [ ] **`PENDING_UBUNTU_VALIDATION`:** Launch `RyuS0App` listening on TCP port `6633` (matching `network/topology/topology.py`):
  ```bash
  SDN_VERIFY_PORT_DESC=1 SDN_ENABLE_INTRA_EDGE_L3=1 SDN_ENABLE_ARP_EDGE_LOCAL_FLOOD=1 \
  PYTHONPATH=. ryu-manager --ofp-tcp-listen-port 6633 sdn/apps/ryu_s0_app.py
  ```
  *Expected Evidence:* Controller log binds `0.0.0.0:6633` without import or eventlet errors.

### Step B — Mininet Topology Connection & 6 Datapaths (`DPID 1..6`)
- [ ] **`PENDING_UBUNTU_VALIDATION`:** Start `network/topology/topology.py`:
  ```bash
  sudo mn -c
  sudo python3 network/topology/topology.py
  ```
  *Expected Evidence:* All 6 switches (`e1`–`e4` DPID `1..4`, `c1`–`c2` DPID `5..6`) negotiate OpenFlow 1.3 (`0x04`) and pass `PortDesc` validation (`port_desc_validated=True`).

### Step C — Initial Two-Phase Bootstrap (`RULES_INSTALLED` / `VERIFICATION_PENDING`)
- [ ] **`PENDING_UBUNTU_VALIDATION`:** Dump flows across all 6 OVS bridges immediately after switch connection (before traffic):
  ```bash
  for sw in e1 e2 e3 e4 c1 c2; do
      echo "=== $sw ==="
      sudo ovs-ofctl -O OpenFlow13 dump-flows "$sw"
  done
  ```
  *Expected Evidence:*
  - `S0BootstrapReport.readiness_state == S0ReadinessState.VERIFICATION_PENDING`
  - `bootstrapped_48_inter_edge_installed == True`, `rules_readback_ok == True`
  - `verified_48_flows == False` (because `n_packets=0` before traffic — proving zero false `VERIFIED`).
  - Downstream rules (`c1`/`c2` and `e_dst`) and source-edge `P1` rules (`out_port=3` toward `c1`) present with canonical cookies `0x103..0x806`.

### Step D — `pingall` & Full 48-Flow Data-Plane Traffic Verification (`VERIFIED`)
- [ ] **`PENDING_UBUNTU_VALIDATION`:** Run `mininet> pingall` (with `SDN_ENABLE_INTRA_EDGE_L3=1 SDN_ENABLE_ARP_EDGE_LOCAL_FLOOD=1` or `net.staticArp()`).
  *Expected Evidence:*
  - `0% dropped` across all 56 directed host pairs (`48` canonical inter-edge flows + `8` intra-edge helper pairs).
  - Non-zero `n_packets` across all 3 hops (`e_src`, `c1`, `e_dst`) of all 48 `P1` paths in `ovs-ofctl -O OpenFlow13 dump-flows`.
  - Controller log prints: `S0 Reconciled & VERIFIED all 48 flows (state_version=2)` and `readiness_state` transitions to `VERIFIED`.

### Step E — Two-Phase Route Switch (`P1 -> P2` and `P2 -> P1`) (`S3`, `AC-S01`, `AC-S03`)
- [ ] **`PENDING_UBUNTU_VALIDATION`:** While `iperf3` UDP traffic flows from `h1 -> h3` (`0x103`), submit an `ApplyRequest` switching `0x103: 0x103_P1 -> 0x103_P2`.
  *Expected Evidence:*
  - Downstream `c2` (`dpid=6`) and `e2` (`dpid=2`) are updated and Barrier-confirmed before `e1` (`dpid=1`) uplink changes from `output:3` to `output:4`.
  - `sudo ovs-ofctl -O OpenFlow13 dump-flows e1 | grep cookie=0x103` shows `actions=output:4`.
  - `sudo ovs-ofctl -O OpenFlow13 dump-flows c2 | grep cookie=0x103` shows incrementing `n_packets`.
  - `ApplyResponse.status == "APPLIED"`, `state_version` increments by `+1`.

### Step F — Rollback, Switch Disconnect/Reconnect & Reconciliation (`S6`, `S7`, `AC-S04`, `AC-S10`)
- [ ] **`PENDING_UBUNTU_VALIDATION`:** Simulate switch disconnect/reconnect or flow-table wipe on `e1`:
  ```bash
  sudo ovs-ofctl -O OpenFlow13 del-flows e1
  ```
  *Expected Evidence:*
  - Controller detects missing rules / reconnect, locks `reconcile_required=True` (rejecting any new `ApplyRequest` with `RECONCILIATION_REQUIRED` and `rule_count=0`), reinstalls missing `P1` + helper rules on `e1`, and restores `VERIFIED` state only after live traffic increments `n_packets`.

---

## 3. Required Artifacts for Final `PASS_LIVE_OVS` Sign-Off
1. Raw terminal log of `ryu-manager` / `osken-manager` showing `S0 Bootstrap Report` and `S0 Reconciled & VERIFIED all 48 flows`.
2. Raw outputs of `sudo ovs-vsctl show`, `sudo ovs-ofctl -O OpenFlow13 dump-flows <switch>`, and `sudo ovs-ofctl -O OpenFlow13 dump-ports-desc <switch>` for `e1..e4, c1..c2`.
3. Raw `mininet> pingall` and `iperf3` logs before and after `P1 -> P2` route switch.
4. Saved JSON outputs of `Snapshot.to_json()` and `ApplyResponse.to_json()` with `"evidence_mode": "LIVE_OVS"`.
