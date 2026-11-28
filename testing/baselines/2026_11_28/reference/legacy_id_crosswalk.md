# Crosswalk mã ca/tiêu chí lịch sử → baseline 28/11
Các ID TC/AC không có tiền tố phiên bản trong package này thuộc namespace qa_20261128; không dùng trực tiếp để ghép trạng thái lịch sử của v2. TC-I01/I02/I03 giữ nguyên ý nghĩa (H5 regression), C01–C16 giữ nguyên. Các mã dưới đây bảo toàn trace của yêu cầu v2; nếu một tiêu chí chưa có bằng chứng mới thì giữ pending, không đánh dấu đóng.

| TC v2 | TC hiện hành | Nội dung bảo toàn |
|---|---|---|
| N01/N02/N03 | TC-N01/N02/N03 | topology, schedule seed, calibration |
| D01/D02 | TC-D01/D02 | invalid/missing/duplicate |
| D03 | TC-N02, TC-E02 | family split & fingerprints |
| D04 | TC-I02, TC-M02 | causality/window/labels |
| D05 | TC-N03, TC-E01 | counter epoch/reset/missing coverage |
| D06 | TC-M01, TC-I03 | complete READY and strict bins |
| D07 | TC-I04, TC-S02, TC-D06 | apply results/history/event evidence |
| M01 | TC-M01, TC-M02, TC-E01 | feature/vector/mean and provider comparison |
| M02/M03 | TC-M02/M03 | invalid model, fallback/degraded (current policy skip) |
| M04 | TC-M04 | nonzero epsilon test (not deployed parameter) |
| O01 | TC-O02 | trigger strict threshold |
| O02/O03 | TC-O02 | toy MILP/hold-down/R1–R7 |
| O04/O05 | TC-O04/O05 | J hysteresis boundary and timeout/stale |
| S01/S02/S03 | TC-S01/S02/S03 | actual route, verify/rollback, no overlap |
| E01/E02/E03 | TC-E01/E02/E03 | metrics/replay/paired |
| I01/I02/I03 | TC-I01/I02/I03 | no renumbering |

| AC v2 | AC hiện hành |
|---|---|
| N01,N02,N06 | AC-N01, AC-S01 |
| N03 | AC-N02 |
| N04,N05 | AC-N03, AC-N04 (calibration pending) |
| D01,D02 | AC-D01,D02,D03, AC-M02 |
| D03 | AC-D02 |
| D04 | AC-N02, AC-M02 |
| D05 | AC-D03, AC-I02 |
| D06 | AC-N04, AC-E01 |
| D07 | AC-E01, AC-I01 |
| D08 | AC-D04 |
| D09 | AC-D05, AC-S03/S04 |
| M01,M02 | AC-M01/M02, AC-D04 |
| M03,M07 | AC-M01/M02, AC-D04 |
| M04,M05 | AC-M03, AC-R02 |
| M06 | AC-E01/E02 |
| M08 | AC-O02, AC-M01 |
| O01–O05,O08 | AC-O02, AC-O01/O03 |
| O06 | AC-O01/O02 |
| O07 | AC-O04, AC-R02 |
| S01,S03,S06 | AC-S01/S03 |
| S02,S04,S07,S08,S09 | AC-S02/S03/S04, AC-O03/O04 |
| S05 | AC-R02, AC-N04 |
| E01,E02 | AC-E01 |
| E03,E04,E05 | AC-E02 (benchmark and analysis pending) |
| I01 | AC-I01 |
| I02 | G1 owner sign-off + decision_log |
| I03 | AC-I03 |

Source historical references: repository `testing/docs/test_plan_v2.md`, `acceptance_criteria_v2.md`, `interfaces_v2.md`, `integration_requirements.md` at common baseline. Only use current runtime contract for wire and run settings. Preserve detailed tests below when reviewing regressions; do not mix legacy parameters with deployment.

