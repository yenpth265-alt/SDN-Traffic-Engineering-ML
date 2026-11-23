# Phase 10.1 — SDN Dynamic Routing Live Validation Report

**Verdict:** `PASS_DYNAMIC_ROUTING_LIVE`  
**Date (UTC):** `2026-11-21`  
**Repository:** `SDN-Traffic-Engineering-ML`  
**Branch:** `feature/sdn-controller`  
**Baseline Commit:** `d6bf9ad4851383a981de0743c3501e015924f240`  
**Live Runtime Environment:** Ubuntu `22.04.5 LTS` (kernel `6.8.0-40-generic`), Python `3.10.12`, OS-Ken `2.8.1`, Eventlet `0.35.2`, Mininet `2.3.0`, Open vSwitch `2.17.12` (OpenFlow 1.3).

---

## 1. Root Cause Analysis (Phase 10 Live Investigation)

During Phase 10 initial live dynamic routing on real Open vSwitch (`0x103: 0x103_P1 -> 0x103_P2`):

1. **FlowMod & Barrier execution were already correct:** `OFPFC_ADD` on `c2` and `e2` (`PRIMARY_DOWNSTREAM`) -> `Barrier` -> `OFPFC_MODIFY_STRICT` on `e1` (`actions=output:4`, `PRIMARY_SOURCE_EDGE`) -> `Barrier` completed cleanly with `0%` packet loss (`250/250` ICMP packets).
2. **Rules on `e1`, `c2`, `e2` existed with exact match/cookie/out_port:** `ovs-ofctl -O OpenFlow13 dump-flows` confirmed valid `cookie=0x103` rules on all 3 hops (`e1 output:4`, `c2 output:2`, `e2 in_port=4 output:1`).
3. **Actual Root Cause:** `RyuOpenFlowDriver.verify_forwarding_counter_or_probe()` issued only a **single** `OFPFlowStatsRequest` immediately (~10 ms) after `e1`'s `BarrierReply`. Because `OFPFF_RESET_COUNTS` zeroes OVS packet counters on `FlowMod` and OVS kernel datapath revalidators synchronize `n_packets` asynchronously (typically 200–750 ms depending on packet arrival cadence and revalidator interval), the single immediate read observed `delta = 0` on one or more hops and immediately failed with `NO_REAL_COUNTER_DELTA_OR_ACTIVE_PROBE`, triggering an unnecessary rollback.
4. **Secondary Audit Discovery (Fixed in Phase 10.1):** During live Task 5D (`e1` disconnect immediately after a microsecond-precision `ApplyRequest` in the same wall-clock second), `_utc_now_rfc3339()` in `sdn/apps/ryu_s0_app.py` truncated microseconds (`%Y-%m-%dT%H:%M:%SZ`), producing a timestamp earlier than `last_transition_at_utc` (`StateValidationError: Backward timestamp in mark_controller_restart`). Fixed by preserving microseconds (`%Y-%m-%dT%H:%M:%S.%fZ`).

---

## 2. Files Modified (Strictly Inside `sdn/`)

| File | Summary of Phase 10.1 Changes |
| :--- | :--- |
| `sdn/flow_management/ryu_openflow_driver.py` | Added `counter_verify_timeout_s` and `counter_verify_poll_interval_s` to `RyuDriverConfig`, eventlet-safe `_default_sleep()` + injectable `sleep_fn`, `set_verification_budget_deadline()`, and bounded cooperative polling in `verify_forwarding_counter_or_probe()` with immediate abort on `conn_epoch` change, `rule_generation` change, missing hop rule, or `COUNTER_RESET_DETECTED`, issuing 0 FlowMods during polling. |
| `sdn/controller/decision_handler.py` | Wired `set_verification_budget_deadline()` around `install_batch()` (with `finally` reset to `None`) so bounded verification polling never exceeds `min(apply_deadline - decision_time, forecast_end - decision_time)`. |
| `sdn/apps/ryu_s0_app.py` | Added `SDN_COUNTER_VERIFY_TIMEOUT_S` (default `1.0s`) and `SDN_COUNTER_VERIFY_POLL_INTERVAL_S` (default `0.1s`) to `S0AppConfig.from_env()`; scoped S0 bootstrap/reconcile ticks to `0.0s` polling budget; updated `_utc_now_rfc3339()` to preserve microsecond precision (`%Y-%m-%dT%H:%M:%S.%fZ`). |
| `sdn/tests/test_ryu_openflow_driver_and_s0.py` | Added 3 regression tests (`test_15`, `test_16`, `test_17`) covering immediate pass, delayed asynchronous counter increment, timeout without false positive, partial-hop failure, counter reset, switch reconnect/rule-generation change during polling, rollback verification polling, and `_utc_now_rfc3339()` microsecond monotonicity. |
| `sdn/DYNAMIC_ROUTING_LIVE_VALIDATION_REPORT.md` | This Phase 10.1 live validation report. |

*(Files outside `sdn/` were not modified.)*

---

## 3. Automated Regression & Compile Verification

- **Bytecode compilation (`compileall`):**
  - Windows (`.venv/bin/python.exe -m compileall -q sdn/`): `rc = 0`
  - Ubuntu (`.venv_ubuntu/bin/python -m compileall -q sdn/`): `rc = 0`
- **Unit & Mock Regression Suite (`pytest -q -p no:cacheprovider sdn/tests/`):**
  - Windows (`PASS_MOCK`): **115/115 passed** (`0 failed, 0 skipped`).
  - Ubuntu (`PASS_MOCK` on Linux Python 3.10.12 + OS-Ken 2.8.1): **115/115 passed** (`0 failed, 0 skipped`).

---

## 4. Live Dynamic Routing Results (Real Ubuntu + Mininet + OVS 2.17.12 + OS-Ken 2.8.1)

All live tests below were executed through the real `DecisionHandler` -> `RuleInstaller` -> `RuleVerifier` -> `RyuOpenFlowDriver` -> `OS-Ken 2.8.1` -> `Open vSwitch 2.17.12` stack (no manual `ovs-ofctl` path switching).

### 4.1 Task 4A — Baseline S0 (`48/48 P1 VERIFIED`)
- Connected switches: `6/6` (`DPID 1..6`: `e1, e2, e3, e4, c1, c2`), `PortDesc` validated.
- `state_version = 2`, `readiness_state = VERIFIED`, `p1_count = 48`, `p2_count = 0`, `verified_count = 48`, `unknown_count = 0`.
- Baseline `0x103` (`10.0.0.1 -> 10.0.0.3`) OVS counters:
  - `e1`: `actions=output:3`, `n_packets=2`
  - `c1`: `actions=output:2`, `n_packets=2`
  - `c2`: `actions=output:2`, `n_packets=0`
  - `e2`: `in_port=3 actions=output:1` (`n_packets=2`), `in_port=4 actions=output:1` (`n_packets=0`)

### 4.2 Task 4B — Live Switch `0x103: P1 -> P2`
- `decision_batch_id`: `batch_p101_p1_to_p2_001`
- **Two-phase OpenFlow sequence verified from driver op log:**
  1. `PRIMARY_DOWNSTREAM`: `OFPFC_ADD` on `c2` (`out_port=2`) and `e2` (`out_port=1`) -> `Barrier` on `c2` and `e2` (both `ok=True`).
  2. `PRIMARY_SOURCE_EDGE`: `OFPFC_MODIFY_STRICT` on `e1` (`out_port=4`) -> `Barrier` on `e1` (`ok=True`).
  3. Bounded `OFPFlowStatsRequest` polling (`33` FlowStats reads across 3 hops, `0` extra FlowMods) verified `counter_delta >= 1` on `e1`, `c2`, `e2`.
- **`ApplyResponse`:** `status = APPLIED`, `actual_path_id = 0x103_P2`, `verification_status = VERIFIED`, `state_version: 2 -> 3`, `rule_count = 3`, `deadline_missed = False`, `apply_latency_ms = 770.23 ms`.
- **OVS live counters post-switch (`0x103`):**
  - `e1`: `actions=output:4`, `n_packets=89`, `n_bytes=8722`
  - `c1`: `actions=output:2`, `n_packets=13`, `n_bytes=1274` *(pre-switch packets only)*
  - `c2`: `actions=output:2`, `n_packets=89`, `n_bytes=8722` *(live traffic shifted to `c2`)*
  - `e2`: `in_port=4 actions=output:1`, `n_packets=89`, `n_bytes=8722` *(exact 3-hop match)*
- **Live continuous ICMP traffic (`h1 -> h3`):** `100 packets transmitted, 100 received, 0% packet loss`, `rtt min/avg/max/mdev = 26.413/33.000/88.206/12.409 ms`.
- **State:** `p1_count = 47`, `p2_count = 1`, `verified_count = 48`, `unknown_count = 0`.

### 4.3 Task 4C — Live Switch Back `0x103: P2 -> P1` (After `T_hold = 10s`)
- `decision_batch_id`: `batch_p101_p2_to_p1_002`
- **Two-phase OpenFlow sequence:** `PRIMARY_DOWNSTREAM` (`c1`, `e2` + `Barrier`) -> `PRIMARY_SOURCE_EDGE` (`e1 output:3` + `Barrier`) -> bounded `FlowStats` verification (`30` FlowStats reads, `0` extra FlowMods).
- **`ApplyResponse`:** `status = APPLIED`, `actual_path_id = 0x103_P1`, `verification_status = VERIFIED`, `state_version: 3 -> 4`, `rule_count = 3`, `deadline_missed = False`, `apply_latency_ms = 738.20 ms`.
- **OVS live counters post-switch (`0x103`):**
  - `e1`: `actions=output:3`, `n_packets=85`, `n_bytes=8330`
  - `c1`: `actions=output:2`, `n_packets=84`, `n_bytes=8232` *(live traffic returned to `c1`)*
  - `e2`: `in_port=3 actions=output:1`, `n_packets=84`, `n_bytes=8232`
- **Live continuous ICMP traffic (`h1 -> h3`):** `100 packets transmitted, 100 received, 0% packet loss`, `rtt min/avg/max/mdev = 27.526/88.585/1386.392/211.452 ms`.
- **State:** `p1_count = 48`, `p2_count = 0`, `verified_count = 48`, `unknown_count = 0`.

---

## 5. Independent Negative, Idempotency & Fault-Injection / Recovery Results

### 5.1 Task 5A — Pre-Write Guard & Idempotency Tests (All Executed Live at `sv=3`)

| Test Case | `status` | `reason` | `state_version` | `rule_count` | `idempotent_replay` | Driver Ops | Result |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| Duplicate replay (same batch_id + hash) | `APPLIED` | `None` | `3` (unchanged) | `3` (cached) | `True` | `0` | **PASS** |
| Batch ID conflict (same batch_id, diff hash) | `REJECTED` | `BATCH_ID_CONFLICT` | `3` (unchanged) | `0` | `False` | `0` | **PASS** |
| Hold-down active (`< 10s` on `0x103`) | `REJECTED` | `HOLDDOWN` | `3` (unchanged) | `0` | `False` | `0` | **PASS** |
| Stale snapshot (`> T_stale`) | `REJECTED` | `STALE` | `3` (unchanged) | `0` | `False` | `0` | **PASS** |
| Version mismatch (`expected_sv != 3`) | `REJECTED` | `VERSION_MISMATCH` | `3` (unchanged) | `0` | `False` | `0` | **PASS** |

### 5.2 Task 5B — Live Barrier Timeout & Verified Rollback (`0x104: h1 -> h4`)
- Injected deterministic Barrier timeout on `e1` (`PRIMARY_SOURCE_EDGE`).
- Controller immediately executed 2-phase rollback (`ROLLBACK_DOWNSTREAM` on `c1`, `e2` + `Barrier` -> `ROLLBACK_SOURCE_EDGE` on `e1 output:3` + `Barrier`) and verified live forwarding on `0x104_P1`.
- **`ApplyResponse`:** `status = ROLLED_BACK`, `actual_path_id = 0x104_P1`, `verification_status = VERIFIED`, `reason = VERIFY_TIMEOUT`, `rule_count = 6`, `state_version: 4 -> 5`, `reconcile_required = False`.
- **OVS live counters post-rollback (`0x104`):** `e1 actions=output:3` (`n_packets=91`), `c1` (`n_packets=90`), `e2 in_port=3` (`n_packets=90`).
- **Live continuous ICMP traffic (`h1 -> h4`):** `140 packets transmitted, 140 received, 0% packet loss`, `rtt min/avg/max/mdev = 26.938/38.684/127.677/17.289 ms`.

### 5.3 Task 5C — Live Missing Rule (`0x105: h1 -> h5`) -> `INCONSISTENT/UNKNOWN` -> Reconcile
- Deleted `cookie=0x105` on `e1` and injected `e1` rollback barrier failure during `0x105` apply.
- **`ApplyResponse`:** `status = INCONSISTENT`, `actual_path_id = None`, `flow_status = UNKNOWN`, `reason = ROLLBACK_FAILED`, `rule_count = 6`, `state_version: 5 -> 6`, `reconcile_required = True`.
- Controller re-installed missing `0x105` P1 rule, observed live traffic (`n_packets=3` on `e1`, `c1`, `e3 in_port=3`), and reconciled back to `state_version = 7`, `readiness_state = VERIFIED`, `verified_count = 48`, `reconcile_required = False`.

### 5.4 Task 5D — Live Switch `e1` Disconnect During Apply (`0x106: h1 -> h6`) -> Reconcile
- Disconnected `e1` (`ovs-vsctl del-controller e1`) during `0x106` apply.
- **`ApplyResponse`:** `status = INCONSISTENT`, `actual_path_id = None`, `flow_status = UNKNOWN`, `reason = ROUTE_STATE_UNKNOWN`, `state_version: 7 -> 8 -> 9`, `reconcile_required = True`.
- Reconnected `e1` (`ovs-vsctl set-controller e1 tcp:127.0.0.1:6633`), generated verification traffic, and reconciled cleanly back to `6/6` switches connected, `state_version = 10`, `readiness_state = VERIFIED`, `p1_count = 48`, `p2_count = 0`, `verified_count = 48`, `unknown_count = 0`, `reconcile_required = False`.
- **Controller log audit:** **0 tracebacks, 0 exceptions, 0 `Backward timestamp` errors.**

---

## 6. Timestamp, `routing_history` & `state_version` Audit

- **Monotonic `state_version`:** `1 (bootstrap) -> 2 (48/48 P1) -> 3 (0x103 P2) -> [3 unchanged across 5 negative tests] -> 4 (0x103 P1) -> 5 (0x104 ROLLED_BACK) -> 6 (0x105 INCONSISTENT) -> 7 (reconciled) -> 8/9 (0x106 disconnect INCONSISTENT) -> 10 (reconciled)`.
- **Contiguous half-open `routing_history` for `0x103`:**
  1. `0x103_P1`: `[2026-11-21T06:20:48.671675Z, 2026-11-21T06:20:51.946454Z)` (`RECONCILED`)
  2. `0x103_P2`: `[2026-11-21T06:20:51.946454Z, 2026-11-21T06:21:04.963602Z)` (`APPLIED`)
  3. `0x103_P1`: `[2026-11-21T06:21:04.963602Z, None)` (`APPLIED`)
- **Causality & ordering:** For every batch, `snapshot_as_of_utc <= decision_time_utc <= applied_at <= completed_at < apply_deadline_utc < forecast_end`.

---

## 7. Preserved Evidence & Remaining Risks

- **Preserved raw artifacts on Ubuntu VM:**
  - `/tmp/sdn_s0_live_artifacts/` (Phase 9 / 9.1 S0 live logs)
  - `/tmp/sdn_phase10_artifacts/` (Phase 10 initial investigation logs)
  - `/tmp/sdn_phase10_1_artifacts/` (Phase 10.1 controller log and live run outputs)
- **Known operational notes (non-blocking for SDN live routing):**
  - `verify_forwarding_counter_or_probe()` requires active traffic (at least 1 packet traversing the new path within `SDN_COUNTER_VERIFY_TIMEOUT_S`, default `1.0s`, capped by `apply_deadline`) unless an active probe is injected; silent flows with 0 packets will honestly fail verification rather than falsely reporting `VERIFIED`.
  - Inter-process transport between Data / ML / Optimization and SDN (`REQ-03` `released_demand_to_sdn` and RPC/REST envelope) remains an `[OPEN / MUST-LOCK]` team-level integration item; the Phase 10.1 temporary WSGI harness lived only in `/tmp/phase10_1_live_app.py` and was not committed into `sdn/`.
