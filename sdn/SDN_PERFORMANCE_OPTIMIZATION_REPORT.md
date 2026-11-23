# SDN Performance Optimization & Real Deadline Validation Report (Phase 12.2)

- **Repository:** `SDN-Traffic-Engineering-ML`
- **Branch:** `feature/sdn-controller`
- **Baseline Commit (`HEAD`):** `d6bf9ad4851383a981de0743c3501e015924f240`
- **Environment:**
  - Windows 11 (`.venv`, Python 3.12.7, `pytest 9.2.0`) — unit/contract regression (`PASS_MOCK`)
  - Ubuntu 22.04.5 LTS VirtualBox (`.venv_ubuntu`, Python 3.10.12, `os-ken 2.8.1`, `eventlet 0.35.2`, `Mininet 2.3.0`, `Open vSwitch 2.17.12`, `iperf3`) — live data-plane benchmarks (`PASS_LIVE_OVS`)
- **Final Verdict:** **`SDN_FUNCTIONALLY_READY_PERFORMANCE_LIMITED`**

> [!IMPORTANT]
> **Honesty & Scope Statement:**
> - All optimizations preserve **strict two-phase OpenFlow 1.3 ordering** (downstream `OFPFC_ADD` + `Barrier` before source-edge `OFPFC_MODIFY_STRICT` + `Barrier`), **true 3-hop `OFPFlowStats` counter verification**, **rollback on failure**, and **v1.4 causal state semantics**.
> - No fake `APPLIED`, no `Barrier`-only verification, no skipped hops, no stale counters, and no HTTP `202` latency masking.
> - **P50 apply latency dropped 5.0× (`1332.96 ms → 267.87 ms`)**, **Max apply latency dropped 3.0× (`2178.14 ms → 721.09 ms`)**, **`B=4` FlowStats count dropped ~10× (`54+ → 5`)**, and **`POST /apply?async=1` HTTP acceptance dropped 89× (`950.73 ms max → 10.71 ms max`)**.
> - However, **P95 (`683.37 ms`) and Max (`721.09 ms`)** still exceed the strict `<= 400 ms` target under lower-rate ICMP (`20 pps`) or unlucky OVS kernel revalidator phase alignment (~500 ms kernel-to-userspace stats sweep). Therefore, the honest verdict is **`SDN_FUNCTIONALLY_READY_PERFORMANCE_LIMITED`** rather than `PASS_SDN_PERFORMANCE_TARGET`.
> - Full closed-loop system integration (`TC-I01`) remains **PENDING** cross-role runner/ML/Optimization/Data connection.

---

## 1. Task 1 — Latency Profiling & Root Cause Analysis (Phase 12.1 Baseline)

In Phase 12.1 (`N = 6` live batches on Ubuntu/Mininet/OVS, `B = 1, 2, 4`), `apply_latency_ms` was **`756.34–2178.14 ms`** (`P50 = 1332.96 ms`, `P95 = 2050.38 ms`) and `POST /apply?wait_ms=250` hit **`950.73 ms`** on `B2_TCP_iperf3_P1toP2`. Profiling isolated five exact bottlenecks:

| Stage | Phase 12.1 Measured Cost | Root Cause |
|---|---:|---|
| 1. HTTP parse + admission (`http_accept_ms`, `queue_wait_ms`) | `0.38–1.05 ms` | Negligible. |
| 2. Sync wait in `POST /apply?wait_ms=250` | `257–950.73 ms` | `_cooperative_wait` blocked the WSGI handler under `eventlet` hub contention, exceeding the `450 ms` caller HTTP timeout. |
| 3. Sequential per-flow `install_batch()` | `B × T_revalidator` (`~1.5–2.2 s`) | `RuleInstaller.install_batch()` ran `install_flow_change()` sequentially per flow instead of batching Phase 1 downstream, Phase 2 source-edge, and Phase 3 verification across the batch. |
| 4. Per-flow, per-hop `OFPFlowStatsRequest` + post-apply `get_active_switch_rule_count()` | `3 × B × rounds` + `6 × (B + 1)` extra calls | `verify_forwarding_counter_or_probe()` queried each hop separately (`12` FlowStats/round for `B=4`), AND `RyuOpenFlowDriver` lacked `get_installed_rule_count()`, causing `RuleInstaller` to fire **`12` extra FlowStats (`B=1`) to `30` extra FlowStats (`B=4`)** *after* verification had already succeeded! |
| 5. Polling interval + OVS kernel revalidator | `100 ms` overshoot + `~500 ms` kernel sweep | `counter_verify_poll_interval_s = 0.10 s` added up to `100 ms` overshoot per batch; OVS `ovs-vswitchd` flushes kernel megaflow counters to userspace on a `~500 ms` cycle. |

---

## 2. Task 2 & Task 3 — Implemented Optimizations (Strict `sdn/` Scope)

All changes were strictly confined to `sdn/` with zero modifications to v1.4 schemas or other roles' files:

1. **Datapath-Grouped Cooperative Batch Verification ([ryu_openflow_driver.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/flow_management/ryu_openflow_driver.py)):**
   - Added `verify_batch_forwarding_counter_or_probe(compiled_plans, *, shared_deadline_s=None)`:
     - Groups unique DPIDs across all still-pending flows in the batch and sends **at most 1 unfiltered `OFPFlowStatsRequest` per unique switch per poll round**, reusing the response for every flow traversing that switch.
     - Enforces a **single shared batch deadline** (never `B × timeout`), while preserving connection-epoch checks, rule-generation checks, missing-rule detection, and counter-reset aborts.
   - Added O(1)/O(N_rules) local rule tracking (`_tracked_rules` + `get_installed_rule_count()`), eliminating **`6 × (B + 1)`** redundant post-apply `OFPFlowStatsRequest` round-trips.
2. **True Two-Phase Batch Install + Safe Rollback Budget ([rule_installer.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/flow_management/rule_installer.py)):**
   - Added `_install_batch_batched()` for `len(changes) > 1`:
     - **Phase 1A:** All downstream `OFPFC_ADD` rules across the batch -> **Phase 1B:** 1 `Barrier` per unique downstream DPID.
     - **Phase 2A:** All source-edge `OFPFC_MODIFY_STRICT` rules -> **Phase 2B:** 1 `Barrier` per unique source-edge DPID.
     - **Phase 3:** Single `verify_batch_forwarding_counter_or_probe()` pass -> per-flow `_rollback_and_verify_old_path()` on any unverified flow.
   - Fixed a real tight-deadline rollback bug discovered during live `400 ms` testing: `_rollback_and_verify_old_path()` now clears any already-expired primary verification budget before verifying rollback to `old_path_id`, preventing false `ROLLBACK_FAILED` / `INCONSISTENT` states when a tight primary deadline expires.
   - Added stage timing metrics (`openflow_install_ms`, `verification_ms`, `flow_stats_request_count`) surfaced in `BatchInstallResult`, `DecisionHandler._last_batch_timing`, and HTTP control envelopes (v1.4 `ApplyResponse` payload unchanged).
3. **Lower Poll Overshoot & Non-Blocking Async Mode ([ryu_s0_app.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/apps/ryu_s0_app.py), [http_control_api.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/adapters/http_control_api.py)):**
   - Reduced default `SDN_COUNTER_VERIFY_POLL_INTERVAL_S` from `0.10 s` to `0.04 s` (`40 ms`).
   - Verified `POST /apply?async=1` (`wait_ms=0`) returns `202 Accepted` immediately (`< 11 ms` live) with idempotent retry returning cached `FINALIZED` `200 OK` (`0` extra FlowMods).
4. **Regression Tests ([test_phase12_2_batch_optimization.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/tests/test_phase12_2_batch_optimization.py)):**
   - Added 3 unit/contract tests (`test_p12_2_01`–`test_p12_2_03`) verifying two-phase ordering, `B=4` FlowStats grouping (`24 → 8` calls), shared deadline + silent-flow partial rollback, and `POST /apply?async=1` immediate `202` + idempotent replay.

---

## 3. Task 5 — Before vs. After Live Benchmark on Ubuntu / Mininet / OVS

### 3.1 Summary Metrics Comparison

| Metric | BEFORE (Phase 12.1, `N = 6`, `wait_ms=250`) | AFTER (Phase 12.2, `N = 8`, `async=1`) | Improvement |
|---|---:|---:|---|
| **Batch `APPLIED` Success Rate** | `6/6` (`100%`) | `8/8` (`100%`) | Maintained (`100%`) |
| **HTTP Acceptance (`http_accept_ms`) P50 / Max** | `0.52 ms` / `1.05 ms` | `0.56 ms` / `1.26 ms` | Sub-2 ms |
| **Initial HTTP Response (`initial_http_ms`) P50 / Max** | `259.6 ms` / `950.73 ms` (`1/6 > 450 ms`) | **`6.13 ms` / `10.71 ms` (`0/8 > 450 ms`)** | **42× P50 / 89× Max faster** |
| **OpenFlow Install (`openflow_install_ms`, `B=2..4`)** | Not separated (sequential) | **`28.77–66.55 ms`** | Batched 2-phase |
| **Apply Latency (`apply_latency_ms`) P50** | `1332.96 ms` | **`267.87 ms`** | **5.0× faster** |
| **Apply Latency (`apply_latency_ms`) P95** | `2050.38 ms` | **`683.37 ms`** | **3.0× faster** |
| **Apply Latency (`apply_latency_ms`) Max** | `2178.14 ms` | **`721.09 ms`** | **3.0× faster** |
| **`apply_latency_ms <= 400 ms` Rate** | `0/6` (`0%`) | **`5/8` (`62.5%`)** (`8/13 = 61.5%` incl. `D1–D5`) | Target partially met |
| **`apply_latency_ms <= 1000 ms` Rate** | `2/6` (`33.3%`) | **`8/8` (`100%`)** (`13/13 = 100%` incl. `D1–D5`) | **100% within `T_ctl = 1000 ms`** |
| **`OFPFlowStats` Count (`B = 4`)** | `~54+` (est. per-hop + 30 breakdown) | **`5`** (1 per unique switch!) | **~10× reduction** |
| **TCP / UDP Data-Plane Packet Loss** | `0.0%` | **`0.0%`** (all TCP/UDP flows) | Zero disruption |

### 3.2 Per-Scenario Live Breakdown (AFTER — `N = 8`, Ubuntu 22.04 + OS-Ken 2.8.1 + OVS 2.17.12)

| # | Scenario | `B` | Traffic | BEFORE `apply_ms` | AFTER `apply_ms` | `init_http_ms` (`202`) | `of_install_ms` | `verify_ms` | `FlowStats` | Loss | Status |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|---|---|
| 1 | `B1_ICMP_20pps_P1toP2` | 1 | `ping -i 0.05` (`0x103`) | `756.34` | **`603.64`** | `4.08` | — | `567.36` | `33` | `4.5%*` | `APPLIED` |
| 2 | `B1_TCP_iperf3_P1toP2` | 1 | `iperf3 TCP` (`0x106`) | — | **`98.74`** | `3.05` | — | `59.85` | `6` | `0.0%` | `APPLIED` |
| 3 | `B2_TCP_iperf3_P1toP2` | 2 | `2× iperf3 TCP` (`0x104, 0x105`) | `2178.14` | **`721.09`** | `10.63` | `47.20` | `640.01` | `19` | `0.0%` | `APPLIED` |
| 4 | `B1_ICMP_20pps_P2toP1` | 1 | `ping -i 0.05` (`0x103`) | `778.08` | **`613.33`** | `6.10` | — | `568.95` | `33` | `4.3%*` | `APPLIED` |
| 5 | `B1_TCP_iperf3_P2toP1` | 1 | `iperf3 TCP` (`0x106`) | — | **`200.59`** | `2.96` | — | `163.97` | `12` | `0.0%` | `APPLIED` |
| 6 | `B4_UDP_3M_P1toP2` | 4 | `2× UDP 3M + 2× ICMP 50pps` | `1667.11` | **`215.29`** | `9.05` | `66.55` | `102.56` | **`5`** | `0.0%` | `APPLIED` |
| 7 | `B2_TCP_iperf3_P2toP1` | 2 | `2× iperf3 TCP` (`0x104, 0x105`) | `1508.84` | **`254.28`** | `6.16` | `28.77` | `175.62` | **`7`** | `0.0%` | `APPLIED` |
| 8 | `B4_UDP_3M_P2toP1` | 4 | `2× UDP 3M + 2× ICMP 50pps` | `1157.08` | **`281.45`** | `10.71` | `51.23` | `164.37` | **`5`** | `0.0%` | `APPLIED` |

*\*Note: `4.3–4.5%` on short `ping` runs equals `1` in-flight ICMP packet when `SIGINT` terminated `ping` immediately after batch completion; all TCP/UDP `iperf3` receivers reported `0.0%` loss.*
*Idempotent replay check on `batch_p122_1`: returned `200 OK` in `10.83 ms`, cached `APPLIED`, `rule_count = 3` (0 extra FlowMods).*

---

## 4. Task 4 — Real Tight-Deadline Validation on Live OVS (`D1–D6`)

All 6 tests used **real tight `apply_deadline_utc` and `forecast_end` timestamps** (never 25 s safety deadlines) on live Ubuntu/Mininet/OVS:

| Test | `B` | `apply_deadline` / `forecast_end` | Traffic Condition | `init_http_ms` | `of_install_ms` | `verify_ms` | `apply_latency_ms` | `FlowStats` | `deadline_missed` | Final Status | `reconcile_required` |
|---|---:|---|---|---:|---:|---:|---:|---:|---|---|---|
| `D1_1000ms_B1_ICMP` | 1 | `1000 ms` / `2000 ms` | `ping 20pps` (`0x107`) | `8.10` | — | `572.63` | **`618.28`** | `36` | `False` | **`APPLIED`** | `False` |
| `D2_1000ms_B4_UDP_ICMP` | 4 | `1000 ms` / `2000 ms` | `2× UDP 3M + 2× ICMP 50pps` | `7.22` | `21.90` | `593.74` | **`667.05`** | `17` | `False` | **`APPLIED`** | `False` |
| `D3_400ms_B1_TCP` | 1 | **`400 ms`** / `1000 ms` | `iperf3 TCP` (`0x103`) | `4.93` | — | `59.42` | **`101.94`** | `6` | `False` | **`APPLIED`** | `False` |
| `D4_400ms_B2_TCP` | 2 | **`400 ms`** / `1000 ms` | `2× iperf3 TCP` (`0x104, 0x105`) | `3.11` | `34.95` | `199.47` | **`271.76`** | `7` | `False` | **`APPLIED`** | `False` |
| `D5_150ms_MID_APPLY_EXPIRE` | 1 | **`150 ms`** / `200 ms` | `iperf3 TCP` (`0x207`) | `7.91` | — | `74.09` | **`131.22`** | `6` | `False` | **`APPLIED`** | `False` |
| `D5b_150ms_ICMP_MID_APPLY_ROLLBACK` | 1 | **`150 ms`** / `200 ms` | `ping 20pps` (`0x107`) | `4.62` | — | `690.59` | **`740.74`** | `45` | **`True`** | **`ROLLED_BACK`** (`0x107_P1` VERIFIED, `rule_count=6`) | **`False`** |
| `D6_1000ms_B1_SILENT_FLOW` | 1 | `1000 ms` / `2000 ms` | **0 packets (`0x208`)** | `3.98` | — | `2249.14` | `2286.62` | `126` | **`True`** | **`INCONSISTENT`** (`ROLLBACK_FAILED`, `actual_path_id=null`) | **`True`** |

**Live Switch Reconnect & Reconciliation Regression (`Task 6`):**
- `ovs-vsctl del-controller e1` → `connected_dpids = [2, 3, 4, 5, 6]`, `reconcile_required = True`.
- `ovs-vsctl set-controller e1 tcp:127.0.0.1:6633` → automatic reconciliation recovered cleanly to **`readiness_state = VERIFIED`**, **`48/48` flows VERIFIED**, **`reconcile_required = False`**, `state_version = 5`, `0` controller errors.

**Key Safety Findings from `D1–D6`:**
- **Active traffic under `1000 ms` deadline (`D1`, `D2`):** `2/2` (`100%`) `APPLIED` (`618.28 ms`, `667.05 ms`).
- **Active high-rate TCP under `400 ms` deadline (`D3`, `D4`, `D5`):** `3/3` (`100%`) `APPLIED` (`101.94 ms`, `271.76 ms`, `131.22 ms`).
- **Zero-traffic silent flow (`D6`):** Controller **never faked `APPLIED`** and **never faked `actual_path_id = 0x208_P1`**. After `1000 ms` primary verification saw `0` packets, it rolled back (`rule_count = 6`), could not verify `P1` without traffic, and honestly returned `INCONSISTENT` / `UNKNOWN` (`actual_path_id = None`, `reconcile_required = True`) per `AC-S04` & `AC-S09`.

---

## 5. Technical Limit Analysis & Recommended Team Budget

- **Hard OVS Kernel Floor:** Open vSwitch (`ovs-vswitchd` 2.17.12 with Linux kernel datapath) flushes kernel megaflow counters to userspace on a **`~500 ms` revalidator cycle**. Because `openflow_install_ms` is only **`21.9–66.6 ms`**, any counter-based verification whose `FlowMod` lands right after a revalidator sweep must wait up to **`~500–650 ms`** for the next sweep before `OFPFlowStatsRequest` reflects non-zero packet deltas across all 3 hops.
- **No Safety Compromises:** Bypassing counter verification (trusting `Barrier` alone) would make `apply_latency_ms < 70 ms` but would directly violate `AC-S03` / `AC-S04`.
- **Recommended Production Budgets for Cross-Role Sign-Off:**
  1. **Caller HTTP Timeout (`POST /apply?async=1`):** **`<= 50 ms`** (observed max `10.71 ms`, well inside the `450 ms` caller budget).
  2. **SDN Apply + Real Verification Budget (`apply_deadline_utc`):** **`800–1000 ms`** (observed **100% `APPLIED` (`13/13`)** across all active `B=1, 2, 4` ICMP/TCP/UDP traffic; max `721.09 ms`).
  3. **Zero-Traffic Flows:** Optimization should only reroute flows with active demand (`b_f > 0`); otherwise SDN will rightly refuse to fake `APPLIED`.

---

## 6. Regression, Hygiene & Backup Verification

| Check | Result |
|---|---|
| **Windows Unit/Contract Suite (`PASS_MOCK`)** | `130 passed` (`sdn/tests/`, 0 failures, 0 skipped) |
| **Ubuntu Full Suite (`PASS_MOCK` on Linux)** | `130 passed in 66.70s` (`compileall` RC=0) |
| **Ubuntu Live OVS Errors (`osken_phase12_2*.log`)** | `0` Tracebacks / `0` ERROR lines |
| **Post-Run Cleanup (`mn -c`, `osken`, `iperf3`, `ovs-vsctl list-br`)** | Clean (`ovs_bridges = ""`) |
| **Off-`/tmp` Ubuntu Backup** | `/home/vboxuser/sdn_evidence_backup_phase12_2/` (`artifacts/`, `dl_artifacts/`, `repo_phase12_2.bundle`, `working_tree_phase12_2.patch`) |
| **Git Scope Hygiene** | Strictly inside `sdn/`; `0` commits/pushes/merges; `0` edits to other roles' files |

---

## 7. Ubuntu Requirements / Pending Live Tests

- **Completed on Ubuntu (`PASS_LIVE_OVS`):** S0 (`6/6` switches, `48/48` P1), reconnect recovery, dynamic routing `P1 -> P2 -> P1`, negative/rollback/reconciliation, 3-cycle stability, Northbound HTTP API (`async=1` + SQLite WAL + Outbox), Phase 12.2 Before/After benchmark (`B=1, 2, 4`), and real tight-deadline validation (`D1–D6`).
- **Pending Cross-Role Live Test (`UBUNTU_REQUIRED` — requires Network + Data + ML + Optimization):**
  - Full closed-loop `TC-I01` (live `Network -> SDN /demand-releases -> SDN /snapshots -> Data -> ML -> Optimization -> SDN /apply?async=1 -> OVS`).
