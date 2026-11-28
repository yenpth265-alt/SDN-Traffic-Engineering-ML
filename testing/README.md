# Kiểm thử SDN Traffic Engineering

## Baseline 28/11/2026

Bộ tài liệu đồng bộ QA tại [baseline 28/11/2026](baselines/2026_11_28/README.md), đối chiếu checkout `main@29b95149`.

[GitHub Actions](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/actions/runs/46131298984): 289 ca module, 19 ca hợp đồng Data/QA và 3 ca SDN–PostgreSQL đạt; migrations 1–4, SDN không skip. G1 có bằng chứng kiểm thử nhưng chờ owner xác nhận; TC-I01 live và benchmark M0–M4 chưa được nghiệm thu.

| Đầu ra | Đường dẫn |
| --- | --- |
| Kế hoạch kiểm thử | [test_plan.md](baselines/2026_11_28/test_plan.md) |
| Hợp đồng giao diện | [interfaces.md](baselines/2026_11_28/interfaces.md) |
| Tiêu chí nghiệm thu | [acceptance_criteria.md](baselines/2026_11_28/acceptance_criteria.md) |
| Sổ quyết định | [decision_log.md](baselines/2026_11_28/decision_log.md) |
| Baseline manifest | [baseline_manifest.json](baselines/2026_11_28/baseline_manifest.json) |
| Danh mục ca kiểm thử | [test_cases.md](baselines/2026_11_28/test_cases.md) |
| Fixture và provenance | [fixtures/](baselines/2026_11_28/fixtures/README.md) |
| Readiness checklist | [readiness_checklist.md](baselines/2026_11_28/readiness_checklist.md) |

## Đối chiếu sau merge PR #8

`main@71f120a41f2eab9adfdd358a3feb91ef4d4a0656` và `docs/testing-sync-20261128@a4da343` có cùng cây nội dung file. [CI main@71f120a](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/actions/runs/46134738871) đạt 289 module (7 warnings), 19 Data/QA và 3 SDN–PostgreSQL, zero skip SDN. 78/78 source blob pins và 20/20 source fixture pins khớp cây main. Baseline 29b95149 vẫn là mốc frozen; không ghi đè evidence cũ. Xem [bảng rà soát từng file và điểm mở](reports/testing_file_review_2026_11_28.md).

`testing/fixtures/` là fixture thực thi; các bản sao trong baseline là frozen evidence. Không xóa hoặc cập nhật đồng thời bằng thao tác thủ công. `testing/scripts/run_tests.py` đang rỗng, không phải entry point nghiệm thu; dùng workflow CI cho G1.

## Tài liệu và kết quả lịch sử

- [Kế hoạch v2](docs/test_plan_v2.md), [tiêu chí v2](docs/acceptance_criteria_v2.md), [giao diện v2](docs/interfaces_v2.md), [yêu cầu tích hợp](docs/integration_requirements.md).
- [Sổ quyết định API](../docs/architecture/Online_API_Team_Decisions.md), [fixture producer](fixtures/online_contract/README.md), [kết quả kiểm thử ngày 24/11](results/README.md).
- [Ánh xạ ID v2 sang baseline mới](baselines/2026_11_28/reference/legacy_id_crosswalk.md).

Hướng dẫn tái hiện: [workflow PostgreSQL CI](../.github/workflows/postgresql_interface_contracts.yml) và [runbook runtime hiện hành](../integration/orchestration/README.md).

