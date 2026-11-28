# Tiêu chí nghiệm thu
**Baseline:** `main@29b95149e728078fd8f1840ffd021afb37536c7f` — 28/11/2026. **Trạng thái:** bản đồng bộ QA để owner rà soát; chưa sign-off toàn hệ thống. Nguồn phiên bản: [baseline_manifest.json](baseline_manifest.json).

**Quy tắc:** PASS khi expected = actual với evidence định danh checkout và test scope. PASS_CONTRACT không chứng minh live OVS; PASS_UNIT không chứng minh API delivery. Thiếu run/evidence là NOT_RUN/BLOCKED; owner confirmation riêng. Đối với ca âm, expected 401/409/422 đúng là PASS. Release/system chỉ khi các ca bắt buộc đều PASS_LIVE/PASS phù hợp, zero blocker/major còn mở, sign-off rõ vai trò.

| ID | Module | Điều kiện PASS (nếu không đạt: FAIL) | Bằng chứng | Hiện trạng |
|---|---|---|---|---|
| AC-N01 | Network | 8 hosts,6 switches,48 hex flows,96 paths,32 directed links nhất quán; 16 fabric; OVS ports đúng | topology_bundle + live dump/probe | CONTRACT_PASS; PORT_LIVE_PENDING |
| AC-N02 | Network | Seed/schedule hash tái lập; split/parent family disjoint, test không được dùng chọn model | registry/content fingerprints | OWNER_PENDING |
| AC-N03 | Network | Demand/telemetry 24 link đúng bin, availability DB, ACK verified/read-back, outbox persist/fsync/replay; missing không zero | TC-D01/D04; producer logs + SQL | CONTRACT_PASS; LIVE_PENDING |
| AC-N04 | Network | Raw sender/receiver unique seq, cohort deadline, received_bytes; reset/missing counter invalid, overcount loss NULL | TC-E01; live capture/calibration | UNIT_PASS; LIVE_PENDING |
| AC-D01 | Data | Migrations 1–4; API/schema same checkout; role and route enforcement | CI schema_migrations [1,2,3,4], tests | PASS_CONTRACT |
| AC-D02 | Data | Identical replay one logical record (HTTP200 inserted/updated0); changed same ID 409; invalid 422; no partial writes | TC-D02/D03/R01/R02 | PASS_CONTRACT |
| AC-D03 | Data | as_of/cutoff causal; missing/future/lateness exclusion, no demand_plan fill; valid snapshot bytes/hash round-trip | TC-I02, 95→96 history/97 DB; two snapshots | PASS_CONTRACT |
| AC-D04 | Data | H1 = 48 complete rows; H5 regression = 240; strict int bins, atomic READY, float invalid, no partial writes | TC-I03/M01/I05 | PASS_CONTRACT (H1 handoff + H5 SQL; native live H1 pending) |
| AC-D05 | Data | routing version/deadline rejected, routing precedes ApplyResult, 48 rows/store stable; audit events persist/replay | TC-I04/I05/D06 | PASS_CONTRACT |
| AC-M01 | ML | frozen artifact hash/load platform stable; L30/H1/W5/eps0, feature order, exact seven fields, 48 rows | model metadata, portability test, ML integration tests | PASS_UNIT/CONTRACT |
| AC-M02 | ML | causal snapshot (VALID only), M1–M3 invariant to future schedule, M4 separately oracle; split no leakage | TC-I02/M02; split registry | CONTRACT_PASS; FAMILY_OWNER_PENDING |
| AC-M03 | ML | missing/invalid/timeout skip/audit (current policy), durable forecast/event replay exact bytes across restarts, expiry guard | test_online.py, TC-M03; latency pilot | UNIT_PASS; LIVE_PENDING |
| AC-O01 | Optimization | VERIFIED state + sync gate, READY 48×H, IDs/hash/version/window integer and H×Δ; no default IDs | input_validator, tests | PASS_UNIT |
| AC-O02 | Optimization | strict trigger U>TH N=3, hold-down 10, B≤4, hysteresis h=.05, objective J and exact solver/feasible incumbent; epsilon once, mean final H | toy cases, validator + solver audit | PASS_UNIT; pilot parameter sign-off pending |
| AC-O03 | Optimization | 48-row Data decision written first, strict changed-only SDN request, poll run/batch, stale/expired blocked | TC-I04, serializer/service tests | PASS_CONTRACT; live pending |
| AC-O04 | Optimization | pending journal cannot be overwritten, corrupt fail-closed, recovery before next batch, audit durable | tests + shutdown/restart trace | PASS_UNIT; LIVE_PENDING |
| AC-S01 | SDN | native controller 6/6 switch, 48/48 VERIFIED and sync gate eligible; actual run topology verified | GET health + rule/route data | MODULE_LIVE_HISTORICAL; current NOT_RUN |
| AC-S02 | SDN | snapshot v1.4 causal L30=1440, hash/bytes; STALE decision gets 409 | SDN PG three cases | PASS_CONTRACT |
| AC-S03 | SDN | apply verify actual paths, unchanged outside B, publish routing then 48 result; APPLIED not merely FlowMod/Barrier; UNKNOWN no path | SDN PG 1/47 fixture; live OVS proof required | PASS_CONTRACT; LIVE_PENDING |
| AC-S04 | SDN | version once for verified changes, rollback/partial/unknown reflect actual, no overlap, auth, reconcile audit; sync gate closed till ACK | schema/controller tests, F09–F12 | PASS_UNIT/CONTRACT; LIVE_PENDING |
| AC-R01 | Runner | manifest mapper real origins/hash/model; register identical replay; separate explicit modes and mandatory Network URLs/env key | TC-R01/R03, CI + external 18 unit | CONTRACT_PASS; preflight not merged/launch pending |
| AC-R02 | Runner | readiness gate before launch/tick, first cutoff≥30, skip audit, deadline, lock/recovery, close only all outboxes drained | Runner tests; TC-I01 | UNIT_PASS; LIVE_PENDING; owner tick policy |
| AC-E01 | QA/Eval | trace raw with hash, recompute unique loss/goodput/MLU coverage; invalid infra distinguished from degraded method | test_benchmark_tools / raw | UNIT_PASS; live raw pending |
| AC-E02 | QA/Eval | same test schedule/config, method matrix, paired CI independent schedule, separate faults, retain failed executions | locked protocol + run_order + raw results | NOT_RUN |
| AC-I01 | All | TC-I01 full register→actual network→DB→ML→G0→OVS→DB→metrics→terminal no gaps | real shared baseline full run | NOT_RUN_LIVE |
| AC-I02 | All | causal snapshot, M1–M3 future-invariant, data not backfilled; M4 only oracle | SQL + predictor TC-I02 | PASS_CONTRACT; M4 live pending |
| AC-I03 | All | warm-up map traffic160=measurement100, H5→240 (regression), H1→48 deployment | SQL check + ML handoff | PASS_CONTRACT |
| AC-I04 | All | version/expired/replay invariants; decision and routing order; actual [2,1,47] | SQL read-back | PASS_CONTRACT |
| AC-I05 | All | SDN real PG 3/3 no skips: VALID path, STALE reject, L30 1440; no live conflation | CI g1 job | PASS_CONTRACT |

## Gate acceptance roll-up
G1 evidence PASS (CI success, 289 + 19 + 3, migrations 1–4); formal G1 pending owner. G2 current live topology/ACK/calibration; G3 current H1 measured budget/family confirmation; G4 full closed-loop/fault runs; G5 protocol/benchmark; G6 paired analysis; not yet signed. Specific checkpoints in readiness_checklist.

## Trace and threshold decisions
At every run record baseline, case_id/AC/IF/owner, fixture SHA, command, expected, actual, status, raw evidence, defect/fix/retest, UTC. Runtime values (TH/N/h/hold/B/lambda, timeout, T_stale=2s in snapshot fixtures) are code/profile, not measured SLA. Lock packet deadline/drain, max acceptable lag/coverage, k_e/calibration, latency budget H1, retry/retention, exclusion and benchmark repeat before any official comparisons. Keep missing metrics NULL, no optimistic clipping or fake fill. Fault-run scope F01–F12 separate from performance sample; see test_cases.


Khi đọc hồ sơ v2, đối chiếu [crosswalk ID lịch sử](reference/legacy_id_crosswalk.md); không ghép trạng thái theo mã trùng nhưng khác nghĩa.
