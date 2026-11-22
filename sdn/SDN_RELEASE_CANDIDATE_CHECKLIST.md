# SDN Release Candidate Checklist (`Phase 11.1`)

**Component:** SDN Controller (`sdn/`)  
**Branch:** `feature/sdn-controller`  
**Baseline Commit:** `d6bf9ad4851383a981de0743c3501e015924f240`  
**Standalone RC Verdict:** `SDN_RC_VERIFIED_STANDALONE`  
**Cross-Role System Verdict:** `PARTIALLY_READY_FOR_INTEGRATION`

---

## 1. Code Preservation & Integrity Gate
- [x] Working tree modifications strictly inside `sdn/` (`8` modified tracked files + SDN validation/gate `.md` reports).
- [x] Zero edits to `network/`, `configs/`, `data/`, `ml/`, `optimization/`, `integration/`, `testing/`.
- [x] No unapproved `git commit`, `git push`, `git merge`, or destructive Git operations.
- [x] Local backup branch `backup/sdn-phase7-d6bf9ad` intact.
- [x] Git bundle (`sdn_controller_phase11.bundle`) verified with `git bundle verify` (`rc=0`).
- [x] Dry-run full restore verified in clean temp directory: `git clone -b feature/sdn-controller <bundle>` (`HEAD=d6bf9ad...`) + `git apply --check working_tree_phase8_to_10_1.patch` (`rc=0`).
- [x] Off-`/tmp` evidence archives preserved on Ubuntu (`/home/vboxuser/sdn_evidence_backup_phase11/`) and Windows (all Phase 9, 9.1, 10, 10.1, 11 logs intact).
- [x] `0` SHA-256 mismatches across Windows and Ubuntu.

## 2. SDN Verification & Live Stability Gate
- [x] `compileall -q sdn/` passes (`rc=0`) on Windows & Ubuntu.
- [x] `115/115` unit/contract/regression tests PASS on Windows (`PASS_MOCK`) and Ubuntu (`Python 3.10.12 + OS-Ken 2.8.1`).
- [x] S0 live bootstrap (`6/6` OVS switches, `PortDesc`, `pingall 56/56`, `48/48 P1 VERIFIED`) — `PASS_LIVE_OVS`.
- [x] Switch disconnect & reconnect recovery (`e1`, `0` tracebacks) — `PASS_LIVE_OVS`.
- [x] Dynamic routing `0x103: P1 -> P2 -> P1` via real `DecisionHandler` + `RyuOpenFlowDriver` + bounded `FlowStats` polling — `PASS_DYNAMIC_ROUTING_LIVE`.
- [x] Live negative & fault-injection suite (replay, conflict, hold-down, stale, version mismatch, expired deadline, Barrier timeout rollback, missing rule reconcile, switch disconnect during apply reconcile) — `PASS_DYNAMIC_ROUTING_LIVE`.
- [x] Live 3-cycle stability (`6` consecutive transitions under `ping` 20/50 pps + `iperf3` TCP `8.27/7.12 Mbps` 0 retransmits + `iperf3` UDP `5 Mbps` 0% loss, constant `1` thread, `0` exceptions, clean `mn -c` teardown) — `PASS_LIVE_OVS`.

## 3. Final Hardening & Cross-Role Integration Gate
- [x] SDN implemented all 4 production adapters (`http_control_api.py`, `sqlite_idempotency_store.py`, `sqlite_demand_ledger.py`, `data_event_publisher.py` with supervised auto-flush worker).
- [x] Separated `/health` readiness gates (`process_liveness`, `sdn_readiness`, `data_delivery_readiness`, `sdn_integration_readiness`, and honest `full_system_readiness.ready=False` until external roles & `TC-I01` are verified).
- [x] `136/136` regression & hardening tests PASS on Windows (`61.06s`) and Ubuntu (`73.89s`) + live OS-Ken/Mininet/OVS hardening smoke (`PASS_LIVE_OVS` / `PASS_ADAPTER_TEST` for mock Data receiver).
- [ ] Team approves unified config conflict resolution (`topology_version="1.0"`, `topology_hash` in `RunManifest`, ARP mode lock).
- [ ] Team locks `MUST-LOCK #1` (`sender.py` POST to `/demand-releases` + retry/ack).
- [ ] Team locks `MUST-LOCK #2` (realistic apply deadline budget `800–1000 ms` + `async=1` caller timeout `<= 50 ms`).
- [ ] Team lands `data/` HTTP receiver + DB, `optimization/` online tick, `integration/` runner on `origin`, and merges `origin/feat-ml`.
- [ ] Team executes live closed-loop `TC-I01` on Ubuntu and collects raw provenance evidence.
