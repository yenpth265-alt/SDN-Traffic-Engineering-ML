# Rà soát Testing sau merge PR #8 — 28/11/2026

Phạm vi: `main@71f120a41f2eab9adfdd358a3feb91ef4d4a0656`, nhánh QA `a4da343`, baseline frozen `29b95149`. Git tree kiểm kê 92 file, hai cây nội dung đồng nhất. 78/78 source pins và 20/20 fixture pins khớp SHA Git blob. [CI main@71f120a](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/actions/runs/46134738871), job 140947393045: 289 pytest (7 warnings), 19 unittest, 3 SDN–PostgreSQL; workflow migration 1–4 và verifier PASS. TC-I02 95→96 (97 DB theo baseline), TC-I03 traffic160=measurement100, H5=240; H1 deploy=48. Đây là audit metadata/source và CI, không chạy lại live hoặc xác minh từng byte bằng SHA-256 độc lập.

## Kiểm kê từng file

| File (tương đối từ testing/) | Bytes | Đánh giá và xử lý |
|---|---:|---|
| `README.md` | 2016 | Nguồn QA hiện hành; theo frozen baseline, owner sign-off pending |
| `baselines/2026_11_28/.gitattributes` | 65 | Giữ quy tắc byte/hashing |
| `baselines/2026_11_28/README.md` | 2592 | Nguồn QA hiện hành; theo frozen baseline, owner sign-off pending |
| `baselines/2026_11_28/acceptance_criteria.md` | 7357 | Nguồn QA hiện hành; theo frozen baseline, owner sign-off pending |
| `baselines/2026_11_28/baseline_manifest.json` | 19415 | Nguồn QA hiện hành; theo frozen baseline, owner sign-off pending |
| `baselines/2026_11_28/decision_log.md` | 11029 | Nguồn QA hiện hành; theo frozen baseline, owner sign-off pending |
| `baselines/2026_11_28/evidence/README.md` | 1116 | Evidence CI frozen; CI mới có link riêng |
| `baselines/2026_11_28/evidence/ci_46131298984_excerpt.log` | 15496 | Evidence CI frozen; CI mới có link riêng |
| `baselines/2026_11_28/evidence/runner_delivery/README.md` | 6815 | Preflight unit evidence ngoài main; chưa chứng minh delivery |
| `baselines/2026_11_28/evidence/runner_delivery/cli_cases.json` | 1995 | Preflight unit evidence ngoài main; chưa chứng minh delivery |
| `baselines/2026_11_28/evidence/runner_delivery/unit_tests.txt` | 2382 | Preflight unit evidence ngoài main; chưa chứng minh delivery |
| `baselines/2026_11_28/fixtures/README.md` | 3223 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/ml_data_v14/README.md` | 1546 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/ml_data_v14/context.json` | 713 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/ml_data_v14/forecast_batch.json` | 12309 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/ml_data_v14/snapshot.json` | 656145 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/ml_data_v14/verification.json` | 984 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/negative/apply_result_one_row.json` | 306 | Negative fixture mới, NOT_RUN variant; cần chạy riêng |
| `baselines/2026_11_28/fixtures/negative/forecast_float_bin.json` | 12311 | Negative fixture mới, NOT_RUN variant; cần chạy riêng |
| `baselines/2026_11_28/fixtures/negative/forecast_missing_row.json` | 8745 | Negative fixture mới, NOT_RUN variant; cần chạy riêng |
| `baselines/2026_11_28/fixtures/negative/telemetry_empty.json` | 193 | Negative fixture mới, NOT_RUN variant; cần chạy riêng |
| `baselines/2026_11_28/fixtures/negative_manifest.json` | 1657 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/online_contract/README.md` | 1062 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/online_contract/apply.json` | 21126 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/online_contract/capture_provenance.json` | 881 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/online_contract/forecast.json` | 44162 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/online_contract/snapshot.json` | 69650 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/online_contract/telemetry.json` | 5554 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/optimization/wire/apply_request_reroute.json` | 1120 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/optimization/wire/decision_batch_wire_reroute.json` | 6628 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/optimization/wire/decision_batch_wire_zero_b.json` | 6614 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/sdn/data_v14_v10/README.md` | 1379 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/sdn/data_v14_v10/snapshot_incomplete_body.json` | 177718 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/sdn/data_v14_v10/snapshot_stale_body.json` | 177773 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/sdn/data_v14_v10/snapshot_valid_l10_body.json` | 177765 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/sdn/data_v14_v10/snapshot_valid_l30_body.json` | 483911 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/sdn/data_v14_v10/verify_v10_fixtures.py` | 12318 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/fixtures/source_manifest.json` | 7651 | Frozen fixture/index; nguồn 20/20 khớp; không ghi đè |
| `baselines/2026_11_28/interfaces.md` | 11326 | Nguồn QA hiện hành; theo frozen baseline, owner sign-off pending |
| `baselines/2026_11_28/readiness_checklist.md` | 5303 | Nguồn QA hiện hành; theo frozen baseline, owner sign-off pending |
| `baselines/2026_11_28/reference/configs/experiments/runner_manifest.json` | 2012 | Frozen reference/crosswalk; không làm config thực thi |
| `baselines/2026_11_28/reference/configs/experiments/runtime.yaml` | 572 | Frozen reference/crosswalk; không làm config thực thi |
| `baselines/2026_11_28/reference/configs/ml/selected.yaml` | 1628 | Frozen reference/crosswalk; không làm config thực thi |
| `baselines/2026_11_28/reference/configs/network/config.yaml` | 1030 | Frozen reference/crosswalk; không làm config thực thi |
| `baselines/2026_11_28/reference/configs/network/topology_bundle.json` | 61939 | Frozen reference/crosswalk; không làm config thực thi |
| `baselines/2026_11_28/reference/configs/optimization/optimization.yaml` | 382 | Frozen reference/crosswalk; không làm config thực thi |
| `baselines/2026_11_28/reference/legacy_id_crosswalk.md` | 2713 | Frozen reference/crosswalk; không làm config thực thi |
| `baselines/2026_11_28/reference/ml/saved_models/offline-L30-H1/deployment.yaml` | 1593 | Frozen reference/crosswalk; không làm config thực thi |
| `baselines/2026_11_28/reference/ml/saved_models/offline-L30-H1/metadata.json` | 2428 | Frozen reference/crosswalk; không làm config thực thi |
| `baselines/2026_11_28/reference/testing/reports/cf-repair-2026-11-28/cf-status.json` | 10189 | Frozen reference/crosswalk; không làm config thực thi |
| `baselines/2026_11_28/reference/testing/reports/cf-repair-2026-11-28/verification.json` | 1690 | Frozen reference/crosswalk; không làm config thực thi |
| `baselines/2026_11_28/test_cases.md` | 12260 | Nguồn QA hiện hành; theo frozen baseline, owner sign-off pending |
| `baselines/2026_11_28/test_plan.md` | 11256 | Nguồn QA hiện hành; theo frozen baseline, owner sign-off pending |
| `configs/.gitkeep` | 0 | Giữ placeholder; không phải bằng chứng |
| `docs/acceptance_criteria_v2.md` | 22748 | Lịch sử v2; gắn banner superseded; không dùng runtime |
| `docs/integration_requirements.md` | 36034 | Lịch sử v2; gắn banner superseded; không dùng runtime |
| `docs/interfaces_v2.md` | 31374 | Lịch sử v2; gắn banner superseded; không dùng runtime |
| `docs/test_plan_v2.md` | 38668 | Lịch sử v2; gắn banner superseded; không dùng runtime |
| `fixtures/ml_data_v14/README.md` | 1546 | Fixture thực thi; giữ dependency của script/test |
| `fixtures/ml_data_v14/context.json` | 713 | Fixture thực thi; giữ dependency của script/test |
| `fixtures/ml_data_v14/forecast_batch.json` | 12309 | Fixture thực thi; giữ dependency của script/test |
| `fixtures/ml_data_v14/snapshot.json` | 656145 | Fixture thực thi; giữ dependency của script/test |
| `fixtures/ml_data_v14/verification.json` | 984 | Fixture thực thi; giữ dependency của script/test |
| `fixtures/online_contract/README.md` | 1062 | Fixture thực thi; giữ dependency của script/test |
| `fixtures/online_contract/apply.json` | 21126 | Fixture thực thi; giữ dependency của script/test |
| `fixtures/online_contract/capture_provenance.json` | 881 | Fixture thực thi; giữ dependency của script/test |
| `fixtures/online_contract/forecast.json` | 44162 | Fixture thực thi; giữ dependency của script/test |
| `fixtures/online_contract/snapshot.json` | 69650 | Fixture thực thi; giữ dependency của script/test |
| `fixtures/online_contract/telemetry.json` | 5554 | Fixture thực thi; giữ dependency của script/test |
| `fixtures/snapshot_factory.py` | 5576 | Fixture thực thi; giữ dependency của script/test |
| `reports/.gitkeep` | 0 | Giữ placeholder; không phải bằng chứng |
| `reports/cf-repair-2026-11-28/README.md` | 3828 | Bằng chứng repair local; giữ nguyên log/JSON/XML; không thay CI mới |
| `reports/cf-repair-2026-11-28/capture-contract-retest.log` | 10345 | Bằng chứng repair local; giữ nguyên log/JSON/XML; không thay CI mới |
| `reports/cf-repair-2026-11-28/capture.log` | 897 | Bằng chứng repair local; giữ nguyên log/JSON/XML; không thay CI mới |
| `reports/cf-repair-2026-11-28/cf-status.json` | 10188 | Bằng chứng repair local; giữ nguyên log/JSON/XML; không thay CI mới |
| `reports/cf-repair-2026-11-28/fixtures.log` | 1500 | Bằng chứng repair local; giữ nguyên log/JSON/XML; không thay CI mới |
| `reports/cf-repair-2026-11-28/input-findings.json` | 91354 | Bằng chứng repair local; giữ nguyên log/JSON/XML; không thay CI mới |
| `reports/cf-repair-2026-11-28/modules.log` | 2171 | Bằng chứng repair local; giữ nguyên log/JSON/XML; không thay CI mới |
| `reports/cf-repair-2026-11-28/modules.xml` | 40205 | Bằng chứng repair local; giữ nguyên log/JSON/XML; không thay CI mới |
| `reports/cf-repair-2026-11-28/postgres.log` | 12733 | Bằng chứng repair local; giữ nguyên log/JSON/XML; không thay CI mới |
| `reports/cf-repair-2026-11-28/verification.json` | 1689 | Bằng chứng repair local; giữ nguyên log/JSON/XML; không thay CI mới |
| `reports/ml-pipeline-2026-11-15.md` | 2735 | Pilot ML offline lịch sử; không dùng kết luận live |
| `reports/ml-pipeline-2026-11-18.md` | 10064 | Pilot ML offline lịch sử; không dùng kết luận live |
| `results/.gitkeep` | 0 | Giữ placeholder; không phải bằng chứng |
| `results/README.md` | 3974 | Lịch sử 24/11, báo FAIL cũ; gắn banner |
| `scripts/capture_contract_fixtures.py` | 10898 | Entry point thực thi hợp đồng/capture; G1 CI pass, không live |
| `scripts/generate_ml_data_fixture.py` | 2671 | Entry point thực thi hợp đồng/capture; G1 CI pass, không live |
| `scripts/run_benchmarks.py` | 4790 | Triển khai có regression; benchmark protocol/live chưa nghiệm thu |
| `scripts/run_tests.py` | 0 | RỖNG; chưa là entry point; đề xuất bỏ hoặc triển khai sau review |
| `scripts/test_postgresql_contracts.py` | 29610 | Entry point thực thi hợp đồng/capture; G1 CI pass, không live |
| `scripts/validate_results.py` | 8457 | Triển khai có regression; benchmark protocol/live chưa nghiệm thu |
| `tests/test_benchmark_tools.py` | 4186 | Triển khai có regression; benchmark protocol/live chưa nghiệm thu |

## Điểm đã hội tụ ở code (chưa thay cho owner approval)

- Topology v1.0, raw topology hash; 48 flow groups; 24 link đo, 16 fabric. UTC origin thực, warm-up 60 s, Δ=1 s; không dùng schedule tương lai trong M1–M3.
- ML frozen RF L30/H1/W5/epsilon0, 1440 history, 48 predictions/tick; H5 240 chỉ regression. G0 mean forecast cuối, không áp epsilon lần hai.
- G0 tham số code TH=.85, N=3, h=.05, T_hold=10, B=4, lambda=.01, C_e=100; không coi là calibration đã khóa. Decision Data đủ 48 trước changed-only SDN apply, version/admission/replay; runtime code đã hội tụ.
- QA G1 evidence PASS, sign-off pending; G2–G6 không PASS. Network/S0 gate riêng.

## Quyết định còn mở — đề xuất agenda, chưa chốt

| ID | Owner | Cần xác nhận / evidence | Gate |
|---|---|---|---|
| D01 | Runner, Network, Data | host/API/key, launcher preflight release+telemetry URL, Data ACK và SQL read-back, drain/retry retention; môi trường thực | G2 |
| D02 | Runner, ML, SDN, G0 | first cutoff L30 và audit skip, H1 latency/timeout/fallback/expiry/reconcile; timeout dev không là SLA | G3 |
| D03 | Network, SDN, G0, QA | OVS port mapping, 24/16 counter scope, k_e, pacing/headroom, deadline packet, coverage, drain/calibration | G2/G4 |
| D04 | Network, ML, QA | seed family độc lập, split/hash, scenario/repeats/order/fault seeds; metric/CI protocol và retry | G5 |
| D05 | tất cả | TC-I01 vòng kín live, F01–F12 pilot có bằng chứng, nghiệm thu từng gate và benchmark unlock | G4–G6 |
| D06 | QA | Bốn negative variants trong baseline vẫn NOT_RUN mới; ghi kết quả riêng, không kế thừa PASS tự động; quyết định bỏ hay triển khai run_tests.py và protocol kiểm soát benchmark | G1/G5 |

Các C01–C16 và mapping CF đã có trong [decision log](../baselines/2026_11_28/decision_log.md); CF-22/23/39/46 và các phần live chưa đóng. Không suy ra 46 mục đều mở: 29 findings trong repair source, các ID không liên tục. Owner xác nhận cần tên, ngày UTC, commit/hash, evidence và approval link.

## Chỉ dẫn tổ chức

Giữ `baselines/2026_11_28` frozen và `fixtures/` runtime (11 bản sao giống nhau là có mục đích). Không xóa file cũ vì links và trace. Gắn nhãn lịch sử ở v2/results/reports; dùng `testing/README.md` làm điểm vào duy nhất. Không thay producer, schema, script kiểm thử hoặc thông số vận hành trong đợt chỉnh tài liệu này. Nếu quyết định tái cấu trúc, xử lý link/reference và kiểm thử lại trước merge.

