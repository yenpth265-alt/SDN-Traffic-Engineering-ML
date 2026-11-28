# Sổ quyết định và xung đột tích hợp
**Baseline:** `main@29b95149e728078fd8f1840ffd021afb37536c7f` — 28/11/2026. **Trạng thái:** bản đồng bộ QA để owner rà soát; chưa sign-off toàn hệ thống. Nguồn phiên bản: [baseline_manifest.json](baseline_manifest.json).

## 1. Quy ước trạng thái
CODE_FIXED/TEST_FIXED = thay đổi nằm trong baseline hoặc suite đã chạy; CONTRACT_PASS = evidence CI đúng SHA; OWNER_PENDING = chưa có approval record; LIVE_PENDING/PILOT_REQUIRED = thiếu thực nghiệm. Không tự điền người/ký duyệt; owner dưới đây là vai trò và đầu mối tham chiếu từ sổ quyết định API, không phải người đã xác nhận.

**Lịch sử được bảo toàn:** `e9948d1` merge SDN v1.4 (d7b9bce); `a64916e` merge G0 (b3a85bb) và repair; `fa53213` ghi evidence và capture; `29b95149` fix frozen artifact portability. CI job 140936807608 pass, thay thế trạng thái local-only/CI chưa chạy trong báo cáo repair. Repo main đã chứa hai nhánh bàn giao; không merge lại.

## 2. C01–C16 / REQ-01–16

| Quyết định | Hợp đồng hiện hành / commit | Owner (đầu mối) | File/module / test | Status và phần cần chốt |
|---|---|---|---|---|
| C01 | Canonical ID topology v1.0/raw hash; a2d9768,d7b9bce,a64916e | Network (daylauyen2006-alt), SDN | topology_bundle, topology_registry; TC-N01 | CODE_FIXED; live ports pending |
| C02 | Forecast vector strict bins, H deploy1, mean H tại G0; baa4967/a64916e | Data/ML (VanNgHong,yenpth265-alt), G0 | selected.yaml, online_models, input_validator; TC-M01/I03 | CONTRACT_PASS; H1 latency pending |
| C03 | SDN reads demand history from Data, DB available_at, no future; a64916e | Data/SDN/ML | online_store, snapshot_creator; TC-I02 | CONTRACT_PASS; native live |
| C04 | Data Decision 48 before changed-only /apply, poll with run+batch; b3a85bb/a64916e | G0/SDN (dangtquang0509,PhamAn56) | serializer, apply_models; TC-I04/S03 | CONTRACT_PASS; live |
| C05 | ApplyResult 48 + verified routing first, events lossless; a64916e,004 | Data/SDN | management_api, data publisher; TC-I04/D06 | CONTRACT_PASS |
| C06 | UTC real origins, warm-up60; traffic160=measurement100; 003/a64916e | Data/Network/Runner | manifest_to_run, store; TC-I03 | CONTRACT_PASS; launcher origin live |
| C07 | Epsilon ML once, G0 mean final; frozen eps0; baa4967/a64916e | ML/G0 | selected.yaml, ml_contract/load_estimator; TC-M01/O02 | UNIT_PASS; no second epsilon |
| C08 | Trigger/MILP feasible verified, objective with change penalty; b3a85bb | G0 | milp, solution_validator; TC-O02 | UNIT_PASS; owner check formula/calibration |
| C09 | TH=.85,N3,h=.05,T_hold10,B4,lambda=.01; a64916e | G0/QA | optimization.yaml, trigger; TC-O02 | CODE_FIXED; pilot lock pending |
| C10 | Warm-up60, L30 cutoff≥30, seed split example; a64916e | QA/Network/ML/Runner (ndtchumoa) | runner/config, selected; TC-R02/E02 | owner tick/family/benchmark lock pending |
| C11 | 24 measured links; 16 fabric for MLU; unknown/reset/null; fe6ad50/a64916e | Network/SDN/Data/G0 | collector, validator; TC-D04/N03/E01 | CONTRACT_PASS; k_e & live calibration pending |
| C12 | canonical initial paths P1, static ARP, same-edge separate; a2d9768/d7b9bce | Network/SDN | topology/SDN app; TC-N01/S01 | historical S0, current live pending |
| C13 | Role keys, API deadlines, no overlapping pending, durable audit/replay; b3a85bb/a64916e | G0/SDN/Runner/Data | service_client, runner; TC-I04/S03/R03 | CONTRACT_PASS, pilot H1/retry pending |
| C14 | M0–M4 provider, oracle isolated, metrics raw/paired; a64916e | QA/all | run_benchmarks, validate_results; TC-E01/E02/E03 | UNIT_PASS; protocol/benchmark pending |
| C15 | READY completeness 48×H, int bins, same ID exact replay; 4d632b3/f812559/a64916e | Data/ML | online_store, 003; TC-I03/M01 | CONTRACT_PASS |
| C16 | VERIFIED actual state, UNKNOWN no path, version once/batch, sync gate; a64916e | SDN/Data/G0 | state_manager, management_api; TC-I04/I05/S02 | CONTRACT_PASS; live faults pending |

Formal confirmation for each C: PENDING; owner review, date/time, chosen config/hash and evidence reference required. REQ numbering follows corresponding C number; prior draft v2 must not override current runtime wire.

## 3. CF repairs from integration findings

Source: `testing/reports/cf-repair-2026-11-28/cf-status.json` (29 items). File retained in reference/. The resolution text below separates code convergence/CI from live evidence.

| CF | Finding / owner | Source status at repair | Current QA disposition / test |
|---|---|---|---|
| CF-01 | Còn8file merge conflict G0–main/ML — Optimization | RESOLVED_LOCAL | MERGED_MAIN a64916e — TC-B01, regression 289 |
| CF-02 | SDN/G0 mới chưa được hội tụ vào main — QA | LOCAL_ONLY | MERGED_MAIN (not local-only): e9948d1/a64916e — TC-B01 |
| CF-03 | Merge trực tiếp G0–SDN có20modify/delete conflict — QA | RESOLVED_LOCAL | MERGED_MAIN a64916e; SDN retained — TC-B01 |
| CF-06 | run config_hash và optimizer parameter hash vẫn lẫn namespace — QA/Runner | CODE_FIXED | CODE_FIXED, binding hash — TC-R01/O01 |
| CF-07 | Thiếu mapper manifest Network → RunCreate/runtime context — QA/Runner | CODE_FIXED | CODE_FIXED mapper — TC-R01 |
| CF-08 | Runner đóng vòng vẫn chưa triển khai — QA/Runner | IMPLEMENTED_PENDING_LIVE | RUNNER_IMPLEMENTED; TC-I01 live still pending |
| CF-11 | Thiếu ID vẫn sinh run_default/snap_default — Optimization | CODE_FIXED | UNIT_PASS guards — TC-O01 |
| CF-12 | H1/Δ1 vẫn nhận window UTC5giây; fixture test dùng Δ10 — Optimization | CODE_FIXED | UNIT_PASS bounds — TC-O01/I03 |
| CF-14 | Data READY forecast chưa có bounds/origin/deadline cho planner — QA/Runner | CODE_FIXED | UNIT_PASS READY envelope — TC-O01 |
| CF-16 | Audit tick đã có code nhưng chưa có sink bền vững được cấu hình chung — Optimization | CODE_FIXED | UNIT_PASS durable audit — TC-R02/O03 |
| CF-18 | Journal có thể ghi đè batch chưa resolve; recovery chưa bắt buộc — Optimization | CODE_FIXED | UNIT_PASS recovery — TC-O03/S03 |
| CF-19 | G0 chưa nạp/verify selected.yaml như hợp đồng ML trên main — Optimization | CODE_FIXED | UNIT_PASS frozen artifact — TC-M01/O02 |
| CF-22 | Chốt lịch tick đầu và audit các tick thiếu lịch sử — QA/Runner | OWNER_CONFIRMATION_PENDING | OWNER_PENDING first cutoff L30 — TC-R02 |
| CF-23 | Chưa nghiệm thu H1/latency/timeout/fallback/replay sau expiry — QA/Runner | PILOT_REQUIRED | PILOT_REQUIRED H1, retention, latency — TC-I01 |
| CF-26 | M8: routing history/verification evidence còn CONTRACT_BLOCKED — Data | CODE_FIXED | CONTRACT_PASS events — TC-D06 |
| CF-28 | Test SDN–PostgreSQL vẫn có thể STALE và suite timeout — SDN | TEST_FIXED | CONTRACT_PASS SDN 3/3 CI — TC-I05 |
| CF-30 | Loss còn âm khi thiếu packet log/receive raw count vượt sent — Network | CODE_FIXED | UNIT_PASS overcount invalid — TC-E01 |
| CF-32 | Telemetry chưa write-ahead vào outbox trước HTTP — Network | CODE_FIXED | UNIT_PASS write-ahead — TC-D04/N03 |
| CF-35 | 4test snapshot/capture còn payload compact cũ — QA | TEST_FIXED | CONTRACT_PASS refreshed captures — TC-I02/I03/I04 |
| CF-36 | 2test telemetry còn trái coverage Data — QA | TEST_FIXED | CONTRACT_PASS 24 links + empty reject — TC-D04 |
| CF-37 | Test dùng clock thật/client–DB nên không ổn định theo tải — QA | TEST_FIXED | CONTRACT_PASS DB clock — TC-I02/I05 |
| CF-38 | CI chưa chạy main/PR và producer pins còn cũ — QA | CI_UPDATED_NOT_RUN_ON_GITHUB | CI_PASS exact SHA run 46131298984 — TC-B01 |
| CF-39 | Chưa có TC-I01/fault vòng kín trên baseline chung — QA | LIVE_EVIDENCE_REQUIRED | LIVE_EVIDENCE_REQUIRED TC-I01/F01–F12 |
| CF-40 | Benchmark executor/results validator vẫn0byte — QA | IMPLEMENTED_BENCHMARK_NOT_RUN | IMPLEMENTED; benchmark NOT_RUN — TC-E02/E03 |
| CF-41 | ID/warmup/gate trong tài liệu vẫn mâu thuẫn — QA | DOC_FIXED | DOC_FIXED canonical runtime — TC-I03/R01 |
| CF-43 | Profile môi trường/lệnh test chưa thống nhất cho triển khai — QA | RUNBOOK_ADDED_LINUX_LAUNCH_PENDING | ENV_RUNBOOK; target Linux launch pending — TC-N01/I01 |
| CF-44 | README G0 còn endpoint cũ và code fence chưa đóng — Optimization | DOC_FIXED | DOC_FIXED native /apply — TC-I04/S03 |
| CF-45 | Main/ML còn9file pyc đã track — Optimization | RESOLVED_LOCAL | MERGED_MAIN cleanup — TC-B01 |
| CF-46 | Sổ quyết định/calibration/deployment chưa cập nhật đầy đủ — QA | OWNER_CONFIRMATION_REQUIRED | OWNER_CONFIRMATION_REQUIRED/calibration — all gates |

Additional handoff items not in the 29 open-finding repair list:

| CF / item | Current disposition | Owner and evidence |
|---|---|---|
| CF-05 | topology v1.0 raw 64 hex = ec6c6969019b1c36744339fb54f0f608c95402ae972859b30710dc44b5dc3956; not legacy 1.0; port mapping false → live pending | Network/SDN; a2d9768,d7b9bce; TC-N01 |
| CF-20 | ML durable forecast/event replay introduced b903706; frozen portability 29b95149 | ML; TC-M01/M03, CI exact sha |
| CF-21 | L30 snapshot 1440 and 48 VERIFIED; native SDN PostgreSQL case passes | SDN/ML/Data; TC-I05 |
| CF-27 | strict SDN preflight profile with Data health/readiness, native runner launch pending | SDN/Runner; TC-R03 |
| CF-29/31/33 | Network telemetry/loss/config repairs fe6ad50; no live delivery proof | Network; TC-N03/E01 |
| Runner delivery preflight (R05/R06/R11) | external package based on baa4967; 18 PASS_UNIT, CLI config validation. Not in current main tree; incorporate in launcher, then verify actual ACK+DB. Explicit same-origin policy needs review if dev deployment separates endpoints. | Runner/Network/Data; TC-R03 |
| Baseline portability | model bytes config LF/platform consistent; 289 vs 288 tests due portability regression | ML; 29b95149, TC-M01 |

## 4. Open operational decisions
- D-01 (Runner/Network/Data): endpoint/host/port, role env injection, preflight placement, actual ACK and drain/retry terminal semantics. Evidence TC-R03/TC-N03.
- D-02 (Runner/ML/SDN/G0): L30 first tick and skip policy; measured H1 budget, timeout/fallback/expiry and reconciliation. Evidence TC-R02/I01.
- D-03 (Network/SDN/G0): live mapping, counter location, k_e/calibration, pacing/headroom/probe/queue; packet deadline and drain. Evidence TC-N01/N03/E01.
- D-04 (Network/ML/QA): seed family independence, split, benchmark scenario/repeat/order/boot/fault seeds. Evidence TC-E02.
- D-05 (All): TC-I01, real fault pilot F01–F12, final QA sign-off and benchmark unlock. Evidence TC-I01/E03.

Owner confirmation record required: decision ID, named role owner, approved contract, commit/config hash, UTC confirmed_at, evidence, signature/approval link, disposition. All approval records are NOT_RECEIVED in this package; no inferred agreement from commit author.

