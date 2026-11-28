# Kế hoạch kiểm thử và thực nghiệm SDN
**Baseline:** `main@29b95149e728078fd8f1840ffd021afb37536c7f` — 28/11/2026. **Trạng thái:** bản đồng bộ QA để owner rà soát; chưa sign-off toàn hệ thống. Nguồn phiên bản: [baseline_manifest.json](baseline_manifest.json).

## 1. Mục tiêu, đối tượng, phạm vi

Kiểm đúng tính liên thông, tính nhân quả của dự báo, toàn vẹn dữ liệu, điều phối phiên bản định tuyến và khả năng tái lập thực nghiệm trước khi đánh giá lợi ích SDN Traffic Engineering ML. Sáu đầu mối: Network (topology, sender/receiver, telemetry); Data (PostgreSQL, API); ML (preprocessing, inference, frozen artifacts); Optimization (trigger, MILP, wire serializer); SDN (snapshot, áp dụng/kiểm chứng); Runner (lifecycle, readiness, audit, replay, drain). QA kiểm tra độc lập, không tự sửa output producer nhằm ép PASS.

Phạm vi G1 gồm schema, validator, HTTP, SQL read-back/replay và contract producer trong môi trường kiểm soát; G2–G4 gồm S0/Mininet/OVS, pilot H1, vòng kín và fault; G5–G6 gồm benchmark, phân tích dữ liệu và bàn giao. Hạ tầng benchmark không được xác nhận chỉ bằng mock hoặc SQL.

## 2. Baseline và nguyên tắc kiểm thử
- Topology leaf-spine, 8 host/6 switch, 32 directed links (16 fabric), 48 nhóm liên-edge, 96 candidate paths, 24 measurable links/batch. Canonical hex IDs/path/link; topology v1.0, raw hash trong manifest; export OVS mapping flag false phải xác minh live.
- Warm-up 60 s, measurement 360 s, drain 10 s là profile; delta 1 s; dùng UTC thực do launcher cấp, measurement = traffic +60. Tick thiếu L30 phải audit skip, không đọc lịch trước origin hoặc padding.
- Frozen ML RF v1: L30/H1/W5/epsilon0, feature schema demand_lags_stats_flow_v1, hash khóa. H5 chỉ là regression 240 rows; model hiện hành 48 rows.
- Optimization TH=.85 N=3 h=.05 T_hold=10 s B=4 lambda=.01 C_e default=100; deadline 200 ms/apply guard 200 ms là giá trị cấu hình, không phải SLA. C_e theo catalog từng link; không thay bằng 100 nếu catalog khác.
- Không điền giá trị thiếu thành zero; UNKNOWN không thành VERIFIED. Chỉ dữ liệu thực có available_at ≤ as_of và history trước cutoff được dùng; M4 oracle tách riêng.
- QA phân biệt PASS_UNIT / PASS_CONTRACT (including real PostgreSQL) / PASS_LIVE / PASS_SYSTEM; 4xx đúng expected của case âm là PASS, không phải lỗi sản phẩm.

## 3. Các cấp kiểm thử và gate

| Gate | Hoạt động | Điều kiện thoát | Hiện trạng |
|---|---|---|---|
| G1 QA | Freeze baseline, schema/role, tests module, producer captures và PostgreSQL API | Migrations 1–4; suite 289+19+3 PASS; trace đủ; accepted contract/owner | EVIDENCE_PASS, SIGN_OFF_PENDING |
| G2 QA | Network/S0, topology và đo lường | 6/6 switch, 48/48 VERIFIED, catalog mapping, capture packet/counter/probe; delivery read-back; calibration | NOT_RUN trên baseline này |
| G3 QA | ML leakage/model + native forecast; pilot dữ liệu | L30, H1, hash, split registry, M1–M3 không dùng tương lai; latency/timing thực | CONTRACT_PASS; PILOT_PENDING |
| G4 QA | MILP, SDN apply/reconcile, closed loop TC-I01 và faults F01–F12 | đủ tick trace/SQL, all required live cases; no blocker/major; no APPLIED giả | NOT_RUN_LIVE |
| G5 QA | Khóa protocol/seed/calibration/latency budget, chạy M0–M4 | benchmark matrix, fault lane, evidence raw, exclusion rule trước khi run | BLOCKED_G2_G4 |
| G6 QA | Recompute metrics, paired analysis, publication/handoff | dữ liệu/version/hash đầy đủ, kết luận giới hạn + dấu xác nhận | BLOCKED |

G1 Network/S0 trong tài liệu SDN là gate nội bộ, không phải G1 QA. Hành động chốt gate thuộc QA và owner đã xác nhận, không suy từ Git merge hoặc tình trạng CI.

## 4. Kịch bản và thứ tự

1. Khóa checkout SHA ghi trong manifest; cài dependencies theo process (Data requirements-online, SDN osken/Ubuntu, Network Mininet/OVS, ML/Runner Python 3.11 artifact lock). Tạo DB test riêng, migration 1–4, assert same checkout. Không dùng DB production, không ghi API key.
2. G1 regression theo workflow `.github/workflows/postgresql_interface_contracts.yml`: pytest các module; unittest management/online/QA; verify SDN canonical fixtures; SDN integration zero skipped. Chạy TC-I02/I03, version/deadline, replay, NULL/late/missing, read-back.
3. G2 cấu hình launcher, preflight bắt buộc đúng URLs và role key (xem interfaces). Tạo run_id mới, đăng ký trước traffic, kiểm topology/config/hash; khởi controller, Mininet, 8 receiver, 8 sender, collector; xác minh 6 switch/48 routes và data sync.
4. G3 kiểm frozen artifacts, split parent families và native history; run pilot H1, đo snapshot→forecast→plan→apply, xác nhận budget hợp lệ; không kéo dài H để che latency.
5. G4 chạy vòng kín TC-I01: register → demand/link → history → Snapshot → Forecast READY → decision trước apply → routing version → ApplyResult → event/evidence → terminal run. Lặp controlled failures theo F01–F12, giữ trace cả skip/reconcile; không apply batch chưa resolve.
6. G5 thực nghiệm chỉ khi G1–G4 được nghiệm thu. Mỗi run dùng schedule nguyên bytes, fresh run ID, reset namespace/rules/counter/outbox an toàn, đo 360 s; giữ warm-up và drain riêng. Mọi tick được audit, không queue quá hạn.
7. G6 kiểm manifest raw log/checksum, tính metric độc lập, ghép cặp schedule, giữ failed attempts/cases cùng lý do.

## 5. Đối chứng và thiết kế ma trận

| Method | Provider / policy | Quy tắc |
|---|---|---|
| M0 | Static | Giữ initial paths, vẫn thu telemetry và packet metrics |
| M1 | Persistence | History hợp lệ mới nhất, cùng Optimization/SDN core |
| M2 | Moving average W5 | Cùng window/timing frozen và core |
| M3 | Frozen RF L30/H1/epsilon0 | Không dùng lịch tương lai; skip khi không đủ điều kiện theo runtime profile hiện tại, audit degraded/skip; không đổi nhãn method |
| M4 | Oracle | mode=oracle, schedule_file/id/hash khóa, chỉ OracleProvider đọc future; cùng constraints/core |

Manifest mẫu phân seed train 1–10, validation 11–15, test 16–20. Không coi danh sách mẫu là seed registry đã nghiệm thu: Network xác nhận parent_family_id, lịch độc lập, split không trùng content fingerprint (bỏ ID/timestamp khi so), replay giữ split. Benchmark chỉ từ split=test. Cùng schedule/scenario replay M0–M4, run_id riêng, Latin square 5×5 theo order_seed, seed model 2026, split/order/bootstrap/fault/solver RNG tách namespace. Chưa khóa số scenario, số repeat, tổng run; không dùng số 72/225 như số đã chốt. Mỗi scenario × số schedule độc lập × 5 methods × repeat; fault pilot tách riêng.

## 6. Protocol metric/analysis

Cohort sent thành công trong [measurement_start, measurement_end); nhận đúng deadline trong drain, dedup theo run/group/sequence; warm-up không nhập cohort dù nhận lúc đo. Raw mất byte evidence → goodput NULL. Mẫu số 0 → N/A. Không trừ sent/received của hai arrival windows để tính loss.

| Metric | Công thức / yêu cầu |
|---|---|
| Goodput cohort | 8×unique on-time payload bytes/(10^6×T); tổng/group |
| Served và pacing | Q_rx/Q_requested; Q_tx/Q_requested; Q_requested tích phân demand Mbps theo thời gian; ghi denominator |
| Loss | (P_tx−P_rx_unique_on_time)/P_tx; overcount invalid/unknown, không âm hoặc clamp |
| Duplicate/reorder/late | Đếm riêng; không tăng unique bytes |
| RTT | p50/p95 + success rate + n; timeout không RTT=0 |
| Utilization link & MLU | 8Δcounter/(10^6Δt×C_e); max 16 fabric links; thiếu một link → MLU NULL, báo coverage; không clip >1; phân biệt offered load và egress |
| Congestion | Time above TH=.85 và .90 / valid duration; coverage separate |
| Apply cost | verified changes, FlowMods success/fail, rollback/recovery, attempts; N/A nếu 0 attempt |
| Latency/freshness | feature, forecast, solve, apply, total, data age, overrun, forecast remaining; p50/p95/max; ghi mọi eligible/skip tick |
| Dự báo | MAE/RMSE per horizon và pooled; mean-H riêng; raw/final, provider/fallback riêng |
| Resource | CPU/RAM, pending queue, generator lag/error, trace coverage |

Ưu tiên goodput, loss, MLU p95, chi phí đổi tuyến; đánh giá M3−M1/M2 theo cặp lịch trong scenario, không dùng từng giây là mẫu độc lập. Bootstrap theo schedule/parent family, CI percentile 95%, seed riêng và resample count khóa trước; scripts có default 2.000 nhưng protocol được duyệt mới là chuẩn. Không đặt trước mức tăng phần trăm M3; báo cả trade-off, missing, failure, CI chứa 0 không chứng minh tương đương. M0/M4 dùng tham chiếu bổ sung. Mọi exclusions dựa trên infra/log/config trước khi thấy outcome. Đề xuất tối đa 2 reruns infra, chọn first valid attempt, lưu mọi attempt; owner khóa trước benchmark. Không loại run do RF/solver chậm hoặc fallback/loss thật.

## 7. Evidence, command và bàn giao

- `python -m pytest ml/tests optimization/tests sdn/tests integration/tests testing/tests network/tests -q`
- Sau migrations và DB test: `python -m unittest -v data.ingestion.test_management_api data.ingestion.test_online_ingestion testing.scripts.test_postgresql_contracts sdn.tests.integration_postgres_v14`; kiểm riêng zero skips; fixture `python sdn/fixtures/data_v14_v10/verify_v10_fixtures.py --verify`.
- Runner map `python -m integration.orchestration.runner actual-manifest.json`; execute chỉ sau readiness và profile đã duyệt. `actual-manifest.json` cần mốc UTC thực; sample không phải run thật.
- Benchmark `python -m testing.scripts.run_benchmarks protocol.json run-order.json`; execute chỉ sau G5; validate raw `python -m testing.scripts.validate_results <run_dir> --packet-deadline-s <locked_value>`. Không dùng ví dụ deadline làm SLA.
- Raw append-only: run manifest/schedule hash, sent_h*.csv, recvpkts_h*.csv (received_bytes), link_samples.jsonl, topology, probe, routing/forecast/decision/apply audit, journals, execution.json, quality/checksums, metrics/series, exclusions, test record và defect log. Paths chính thức theo runtime runbook. Giữ full evidence khi thất bại; ghi version thực và artifact hash.
- Trạng thái PASS/FAIL/NOT_RUN/BLOCKED theo criterion; finding severity blocker (leakage, false APPLIED, state/metric sai, evidence thiếu nghiêm trọng), major (sai schema/time/unit/reproducibility), minor (trình bày). Bắt buộc 100% ca release PASS, zero blocker/major mở; không coi waiver thay PASS.

Xem [test_cases](test_cases.md), [criteria](acceptance_criteria.md), [readiness](readiness_checklist.md). Kết quả hiện tại là đọc lại CI đã chạy trên baseline, không phải một đợt thực nghiệm live mới.

