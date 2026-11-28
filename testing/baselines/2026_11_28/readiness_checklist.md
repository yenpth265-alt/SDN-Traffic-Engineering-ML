# Checklist sẵn sàng kiểm thử tích hợp
**Baseline:** `main@29b95149e728078fd8f1840ffd021afb37536c7f` — 28/11/2026. **Trạng thái:** bản đồng bộ QA để owner rà soát; chưa sign-off toàn hệ thống. Nguồn phiên bản: [baseline_manifest.json](baseline_manifest.json).

Ký hiệu [x] có bằng chứng; [ ] còn thiếu/chưa sign-off. Không coi trạng thái code complete là live acceptance.

## G1 — Contract QA
- [x] main pin 29b95149; SDN d7b9bce và G0 b3a85bb là ancestors qua merge e9948d1/a64916e; ML portability 29b95149.
- [x] CI đúng checkout, module pytest 289 pass; 19 Data/QA + 3 SDN–PG, zero skip SDN; four migration versions 1–4.
- [x] TC-I02 95 history rows→96 sau refresh (97 DB); TC-I03 160→100 / [1,240]; registration replay HTTP200 inserted0/one row; 24 telemetry rows; version/expired [0,0], apply [2,1,47].
- [x] Canonical fixture L30 1440, VALID/STALE, runtime/replay tested in controlled PostgreSQL; hash and producer provenance indexed.
- [ ] QA + Network/Data/ML/G0/SDN/Runner review baseline and accept C/CF decisions (no assumed sign-off).

## G2 — Network/S0 and launcher
- [ ] Owner cung cấp Ubuntu/Mininet/OVS/controller profile, Data host/port/DB test, process commands; time sync and actual UTC; no secrets in evidence.
- [ ] Canonical topology source hash, cross-check ports from real OVS (bundle flag false), IP/MAC/ARP, same-edge helper; 6/6/48 VERIFIED and data sync gate eligible. Confirm catalog 24 measured links /16 fabric.
- [ ] Launcher uses actual manifest, registers run before traffic; starts 8 receiver, 8 sender, collector; correct run/path/hash for each process.
- [ ] Integration-mode preflight incorporated into launcher; mandatory release/telemetry URLs; NETWORK key injected env. Missing URL/key → no launch. Offline/dry-run impossible to confuse with delivery.
- [ ] Verify Data health + ACK + read-back same run/flow/bin/24 links, traffic correlation during drain, outbox reconciliation. Run terminal status ok only after all deliveries resolved.
- [ ] S0 pacing, raw seq/received_bytes, counter placement before/after shaping, k_e/headroom/unit/delay/queue/probe/calibration version and hash. Verify packet/log/counter loss semantics.

## G3 — ML data, causality, H1 runtime
- [x] frozen model+config hashes and environment lock; L30/H1/W5/eps0; offline selection excludes test; H5 only regression.
- [ ] Confirm schedule family independence seed registry and split/fingerprint; native L30/H1 end-to-end using live Data, no padding/no future.
- [ ] Owner signs first tick policy wait_for_measurement_history; all earlier ticks audited; skip policy and no fabricated forecast.
- [ ] Measure H1 end-to-end budgets, freshness, timeouts/HTTP202/poll, drain/late ACK retention and failure/recovery before expiry. Development profile not SLA.

## G4 — Optimization/SDN closed loop
- [x] Tests for context binding, READY, deadline/version, journal/recovery, 48 Decision rows / changed-only apply; PG fixture VALID→result, stale reject, L30 real PG.
- [ ] Lock parameters/coefficients and verify measured utilization/reconcile routing actual; run current TC-I01 full chain from launcher to OVS and SQL read-back, one real controlled reroute and no-op.
- [ ] Pilot fault F01–F12 with true controlled hook/evidence, no false APPLIED, no stale application or concurrent unresolved batch; record every tick and terminal.
- [ ] Check raw artifacts/quality/error logs and explicit root-cause defects; no blocker/major remaining.

## G5 — Benchmark lock/execution
- [ ] Owner-approved config/model/seed registry/schedule hash/parent families, protocol (case/scenario, repeat/order_seed, fault_seed, max infra retry, packet deadline, drain, metric min coverage, latency budget).
- [ ] Calibration verified and locked; benchmark matrix M0–M4 identical schedule per block, independent seeds; explicit launcher argv and reset, fault lane separately.
- [ ] All G1–G4 sign-off; execution logs and raw hashes, final state for each run; exclude only predeclared infra failure, not real poor performance.

## G6 — Evaluation & release
- [ ] validate_results.recompute exact raw cohort/MLU etc.; verify coverage and N/A; paired schedule summaries and CI with locked seed/resamples.
- [ ] Acceptance record and defect/retest links, compare M3 vs M1/M2, disclose uncertainty/limits and failure/degraded runs.
- [ ] All owners confirm gate decisions with named owner/date/commit/evidence; formal PASS_SYSTEM only after successful scope.

## Điểm chặn ưu tiên
| Gate | Owner | Input cần cung cấp để đóng |
|---|---|---|
| G1 sign-off | QA + all role owners | confirm baseline and C/CF decision log |
| G2 | Runner/Network/Data/SDN | native launcher, URLs + role env setup, ACK + DB; live OVS mapping, calibration |
| G3 | ML/Network/Runner | family registry + measured H1 timing/skip/retention |
| G4 | Optimization/SDN/Runner/Data/QA | live TC-I01 and F01–F12 evidence, verify/reconcile |
| G5/6 | QA/all | locked benchmark protocol and raw paired results |

Kết luận: **READY_FOR_CONTROLLED_INTEGRATION_PREPARATION, NOT_READY_FOR_PASS_SYSTEM_OR_BENCHMARK**. CI G1 evidence đạt; có thể chuẩn bị G2/pilot, không mở benchmark hoặc tuyên bố G1 sign-off tự động.

