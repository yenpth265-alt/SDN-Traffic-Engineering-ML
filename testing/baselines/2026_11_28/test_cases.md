# Danh mục ca kiểm thử và trạng thái
**Baseline:** `main@29b95149e728078fd8f1840ffd021afb37536c7f` — 28/11/2026. **Trạng thái:** bản đồng bộ QA để owner rà soát; chưa sign-off toàn hệ thống. Nguồn phiên bản: [baseline_manifest.json](baseline_manifest.json).

**Evidence** [CI job](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/actions/runs/46131298984/job/140936807608) and [excerpt](evidence/ci_46131298984_excerpt.log). PASS below means the identified scope passed; no current real Mininet/OVS acceptance. Use exactly baseline and test DB; fixtures are controlled producer/synthetic test samples, not live traffic. Case IDs canonical to this package; legacy C/REQ trace in decision_log.

| Case | Gate / AC / interface | Action / expected | Actual / status | Evidence / entry point / owner |
|---|---|---|---|---|
| TC-B01 | G1 AC-D01 | Same-checkout paths, migrate 1–4, 289 module +19 Data/QA +3 SDN PostgreSQL zero skip | checkout 29b95149; versions [1,2,3,4]; 289 passed, 19 OK, 3 OK; PASS_CONTRACT | workflow/job; QA/Data |
| TC-R01 | G1 AC-R01/D02 IF01,02 | Register with bound actual origins; replay same payload→HTTP200 inserted0 & one row; invalid origins 422, conflicting content rejected | 1 run, 200 inserted0; PASS_CONTRACT | test_runner_registration_replay, management_api.RunCreate; Runner/Data |
| TC-R02 | G1/G3 AC-R02 IF14 | event replay one row; wrong role 401, differing same ID 409, terminal run not reopened; first cutoff≥30, no backlog, audit skips | one event row, 401/409, terminal guard; runner tests PASS; first tick live NOT_RUN | test_runner_and_event_replay, integration/tests/test_orchestration.py; Runner |
| TC-R03 | G2 AC-R01 IF03/05 | explicit mode; missing URLs/key fails before launch; correct config not delivery; actual run ACK/db | external 18/18 unit, CLI valid exit0 delivery=false, missing URL exit2; CONFIG_PASS only; native launch NOT_RUN | evidence/runner_delivery; Runner/Network/Data |
| TC-N01 | G2 AC-N01/S01 IF02 | canonical 8/6/48/96/32; real port map/routes 48 VERIFIED + health | bundle summary matches, flag false; NOT_RUN_LIVE | topology_bundle/TopologyRegistry and SDN /health; Network/SDN |
| TC-N02 | G3 AC-N02/M02 | hashes and parent families across split; test only benchmark | sample registry 1–10/11–15/16–20; actual family confirmation not received | Network schedule + ML selected config; OWNER_PENDING |
| TC-N03 | G2 AC-N03/N04 IF03/04/05 | pacing/raw sequence/counter points, network delivery & calibration; ACK and exact read-back | dry-run telemetry contract only, NOT_RUN_LIVE | sender.py/correlate/collector + Data SQL; Network |
| TC-D01 | G1 AC-D02/D03 IF03 | invalid demand bin/timestamp/null semantics; nonnegative rates; release DB clock | Data suite PASS; late case I02 | test_network_concurrent_retry_and_invalid_timestamps, Demand / ingest_demand; Data/Network |
| TC-D02 | G1 AC-D02 IF03 | simultaneous same ID replay one logical row; changed same ID 409 | PASS_CONTRACT; I02 SQL demand count 95 | Data suite, test_i02...; Data |
| TC-D03 | G1 AC-D02 IF04 | traffic batch invalid FK 422 and no partial rows | PASS_CONTRACT | test_traffic_fk_rolls_back_batch, TrafficBatch/ingest_traffic; Data |
| TC-D04 | G1 AC-N03 IF05 | dry-run 24 links, duplicate returns0, empty invalid, bad port/count/conflict reject | SQL24 after replay and invalid/conflict; PASS_CONTRACT | test_telemetry_retry_and_topology, LinkBatch/ingest_links, fixture telemetry; Data/Network |
| TC-D05 | G1 AC-D03 IF08 | exact snapshot bytes + header digest, hash mismatch/invalid shape 422, same ID mismatch 409; missing history not VALID | Data online/QA suite PASS_CONTRACT; I02 | Snapshot/ingest_snapshot, tests test_routing_and_snapshot_causality; Data/SDN |
| TC-D06 | G1 AC-D05 IF14 | RoutingHistory + VerificationEvidence stored unmodified, duplicate no new event | PASS_CONTRACT | test_sdn_history_and_verification_events_persist_and_replay; SDN/Data |
| TC-M01 | G3 AC-M01/D04 IF09 | artifact/hash portability, seven fields, L30 H1 48 rows; native Predictor, Data schema accept | 289 suite including test_artifact_portability, test_data_handoff/native_sdn_fixture PASS; 48 rows handoff | fixtures/ml_data_v14, selected.yaml; ML/Data |
| TC-M02 | G3 AC-M02 IF08/09 | future isolation, invalid snapshot/model/NaN/shape rejects; strict int bins | suite PASS; I02 M1–M3 invariant; M4 live separate | ML tests, forecast float negative fixture; ML |
| TC-M03 | G3 AC-M03 IF09/14 | missing/timeout skip & audit; replay bytes before POST, restarts/competing journal; expiry guards | unit tests PASS; H1 pilot pending | ml/tests/test_online.py + replay.py; ML/Runner |
| TC-O01 | G1 AC-O01 IF10 | READY, state VERIFIED/sync, no default IDs, matching run/snapshot/hash/horizon/time windows | Optimization + Runner tests PASS_UNIT | input_validator/ready_forecast_context; G0 |
| TC-O02 | G4 AC-O02 IF10/11 | exact toy load/objective, strict threshold trigger, hold-down/B/hysteresis, no double eps, deadline; safe feasible incumbent | suite PASS_UNIT; no network pilot yet | optimization/tests, selected.yaml, optimization.yaml; G0 |
| TC-O03 | G4 AC-O03/O04 IF11/12 | 48 decision rows before apply, changed-only, no-op, bounded polling, durable unresolved journal block | unit/contract PASS, live recovery pending | test_optimization_sdn_contract/test_conflict_regressions; G0 |
| TC-S01 | G2/G4 AC-S01/S03 IF07/12 | apply and verify changed real path, same-edge not disturbed, data sync eligible | old SDN standalone live report not same baseline; NOT_RUN_LIVE | /health/rule dump/probe; SDN |
| TC-S02 | G4 AC-S03/S04 IF12/13 | verification fail/rollback/partial/unknown must reflect actual; no Barrier-only APPLIED | mock/contract tests PASS, F09–12 live pending | decision_handler, test_phase12*.py; SDN |
| TC-S03 | G4 AC-S04/O04 IF12 | reject expired prewrite, stale version/non-VALID, duplicate replay/recover with run/batch; no overlap | regression + I04/05 PASS_CONTRACT; live pending | SDN schema/controller/adapter, G0 service_client; SDN/G0 |
| TC-I01 | G4 AC-I01 IF01–15 | closed-loop actual Network→Data→SDN→ML→G0→OVS→Data with raw and terminal | NOT_RUN_LIVE (no PASS_SYSTEM) | launch/actual-run evidence; all |
| TC-I02 | G1 AC-I02/D03 IF03/06/08/09 | late/future excluded, missing bin 95/96, identical history with schedule edit, M1–M3 stable; reject invalid forecast and forced VALID | first 95 rows [158,159]; refresh 96 vs DB97; same history=true; two snapshots after replay; M1–M3 invariant; PASS_CONTRACT | test_i02_cutoff_late_missing_and_future_schedule; Data/ML/SDN/QA |
| TC-I03 | G1 AC-I03 IF03/06/08/09 | UTC cutoff traffic160=measurement100, H5=240 strict-int rows; reject float/missing; invalid→[0,0], replay one batch | [160,100], float reject=true, invalid [0,0], replay [1,240]; PASS_CONTRACT | test_i03_warmup_and_forecast_horizon; H5 only regression; QA |
| TC-I04 | G1 AC-I04 IF07/11/12/13 | stale/changed version reject, expired decision no DB tick/batch, then 1 APPLIED/47 KEPT and v2, replay same | expired [0,0]; actual [2,1,47]; routing versions [1,2]; PASS_CONTRACT | test_version_change_deadline_and_apply_replay; QA/Data/SDN/G0 |
| TC-I05 | G1 AC-I05 IF08/11/13 | true PG SDN VALID chain → 1 APPLIED +47 KEPT; STALE decision 409; L30 DB exactly1440 rows+48 verified | 3/3 PASS zero skipped; assertions in integration test; no live OVS | sdn.tests.integration_postgres_v14 (3 methods below); SDN/Data/QA |
| TC-E01 | G5 AC-E01 IF04/05/15 | raw packet cohort, dedup, overcount unknown, missing link MLU NULL/coverage, received_bytes; 100/90×1200 bytes T1→goodput .864 Mbps, loss10% | module tests PASS_UNIT; no benchmark raw | network/tests/test_conflict_regressions.py, testing/tests/test_benchmark_tools.py; QA/Network |
| TC-E02 | G5 AC-E02 IF15 | same schedule bytes, disjoint test seeds, Latin order, M0–M4 exactly, separate fault runs | executor available, tests PASS_UNIT, real protocol pending | run_benchmarks.py; QA/Runner |
| TC-E03 | G6 AC-E02 IF15 | paired schedule metrics, bootstrap, n/missing, raw run hashes and exclusions retained; no selection by outcome | validator tests PASS_UNIT; real aggregate NOT_RUN | validate_results.py; QA |

### SDN PG assertion detail (G1)
- `test_end_to_end_contracts_1_2_3_against_real_postgres`: register/actual DB history 48, snapshot VALID, Data decision 48, dry/mock driver result and count {"APPLIED":1,"KEPT":47}; verified by unittest.
- `test_stale_snapshot_rejected_for_optimization_decision`: expected HTTP409; assertion PASS.
- `test_cf21_ml_l30_snapshot_48x30_1440_observations`: 48×30 from PostgreSQL; expected/valid/history=1440; routing_state48; PASS.
- CI unittest runner fails on skip, zero skip confirmed. SQL read-back for I02/I03/I04 is logged explicitly; for SDN tests assertion evidence is source + test result.

## Fault pilot (NOT_RUN_LIVE; use isolated testbed)
| Fault | Owner / case | Expected behavior |
|---|---|---|
| F01 | Data/SDN TC-I02 | missing history → INCOMPLETE/null; block forecast |
| F02 | Data TC-D01 | invalid negative/type/unit inconsistency → reject/invalid; no silent scale correction |
| F03 | Network/Data/SDN TC-I05 | stale sample/release → STALE; block optimization, audit |
| F04 | ML/Runner TC-M03/R02 | insufficient L, no padding → skip audit |
| F05 | ML TC-M03 | inference timeout/error → skip with reason, keep method |
| F06 | ML TC-M02 | invalid numeric/shape → reject, finite >capacity not automatically invalid |
| F07 | ML TC-M01 | wrong model hash/feature order → reject startup or skip fail-closed |
| F08 | G0 TC-O02 | solver timeout → only feasible validated incumbent if within deadline, else keep/skip |
| F09 | SDN TC-S02 | verification timeout → reconcile actual, UNKNOWN if not verified; no false APPLIED |
| F10 | SDN TC-S02 | failed write → FAILED/PARTIAL/rollback status reflects verified actual |
| F11 | SDN/Data TC-S02/I04 | partial batch → 48 per-flow outcomes, version and actual consistent |
| F12 | SDN TC-S02 | discrepant installed path → verification failure, no APPLIED until correct proof |

Each fault run isolated from benchmark, record hook trace, run_id, timestamps, expected/actual/observed SQL/OVS, recovery, raw hashes. No injection into shared production.

## Record schema
`case_id, ac_ids, if_ids, owner, gate, scope, git_commit, config_hash, fixture_path, fixture_sha256, expected, actual, status, evidence_uri, tested_at_utc, defect_id, fix_commit, retest_status`. Source of actual above is exact CI log/source tests or clearly labeled external preflight unit; newly generated negative fixtures not re-executed. Live cases not run; no invented result. For every future attempt retain separate record and raw evidence. 


## Bổ sung regression chi tiết từ protocol v2
Các case bổ sung dưới đây bảo toàn kiểm tra định lượng; chưa có log assertion riêng trong gói này, không tự ghi PASS độc lập dù suite module đã PASS. Giá trị toy không phải profile chạy.

| Case | Tiêu chí | Fixture/expected | Status |
|---|---|---|---|
| TC-M04 | AC-O02/AC-M01 | raw=4, epsilon=.1 (toy), final=4.4; G0 dùng 4.4, không 4.84; deployed eps0. H5 toy vector mean kiểm separate vs H1 deploy. | regression scope, individual evidence pending |
| TC-O04 | AC-O02 | J=U+λ×n_changes/F; cùng context/coeff, strict trigger >TH đủ N, J_new≤(1−h)J_old theo tolerance; test biên ±δ, B=0, no change and zero case; toy λ=.1 F2 U_new=.6 một đổi → J=.65; profile λ=.01 F48. | regression scope, individual evidence pending |
| TC-O05 | AC-O02/O03 | timeout incumbent feasible/integer/validated before deadline only; otherwise skip/keep; reject stale state, ensure pending worker does not block health. | regression scope, individual evidence pending |
| TC-S01X | AC-S01/S03 | real rule dump/probe cả 2 P1/P2, 0-demand probe, chiều ngược/nhóm ngoài batch giữ nguyên; verified hold-down timestamps, no false APPLIED. | NOT_RUN_LIVE |
| TC-E01X | AC-N04/E01 | epoch reset, Δt≤0 invalid, packet duplication/reordering/late, no received_bytes → NULL; counter before shaping flagged offered load; denominator zero→N/A. | live calibration pending |
