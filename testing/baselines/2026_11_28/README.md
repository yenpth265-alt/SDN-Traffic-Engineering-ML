# Bộ bàn giao Testing — đồng bộ 28/11/2026
**Baseline:** `main@29b95149e728078fd8f1840ffd021afb37536c7f` — 28/11/2026. **Trạng thái:** bản đồng bộ QA để owner rà soát; chưa sign-off toàn hệ thống. Nguồn phiên bản: [baseline_manifest.json](baseline_manifest.json).

Mục tiêu: thống nhất một nguồn tham chiếu cho QA sau merge SDN/G0 và sửa portability ML; không thay đổi code producer, không thay thế quyết định của owner.

| Đầu ra | Chức năng |
|---|---|
| [test_plan.md](test_plan.md) | Kế hoạch G1–G6, phạm vi và phương pháp thực nghiệm |
| [interfaces.md](interfaces.md) | Hợp đồng wire hiện hành, endpoint, schema, validator và ACK |
| [acceptance_criteria.md](acceptance_criteria.md) | PASS/FAIL, bằng chứng theo module/gate |
| [decision_log.md](decision_log.md) | C01–C16, CF, chủ sở hữu/điểm cần xác nhận |
| [baseline_manifest.json](baseline_manifest.json) | Commit, blob SHA, config, frozen model, bằng chứng và mục chưa khóa |
| [test_cases.md](test_cases.md) | Expected/actual, ca âm, test entry point, phạm vi |
| [fixtures/](fixtures/README.md) | Fixture nguyên bytes của producer + negative QA + manifest SHA |
| [readiness_checklist.md](readiness_checklist.md) | Điều kiện mở gate tích hợp và benchmark |

## Xác nhận sau merge

PR #8 đưa bộ tài liệu vào `main@71f120a`; [CI main@71f120a](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/actions/runs/46134738871) chạy thành công trên đúng SHA (289 module, 19 Data/QA, 3 SDN–PostgreSQL, zero skip SDN). 78/78 pin nguồn và 20/20 pin fixture khớp. Mốc frozen và artifact cũ không thay đổi; owner sign-off/live vẫn pending.

## Kết luận hiện trạng
- CI đúng SHA trên Ubuntu/Python 3.11/PostgreSQL 17.12: **289/289 module**, **19/19 Data/QA**, **3/3 SDN–PostgreSQL**, migrations 1–4, zero skip cho SDN. Không cộng số lịch sử 288 vào số mới.
- H=1 dùng triển khai, **48** dự báo/tick. H=5/240 là ca regression TC-I03, không được thay đổi model để qua kiểm thử. L30 = 1.440 history rows.
- G1 contract evidence đạt trên baseline CI; sign-off G1 vẫn cần owner. G2–G6 live/benchmark chưa đạt điều kiện mở.
- Runner có closed-loop code trên main; gói preflight Network độc lập 18 unit PASS chưa được xác nhận merge/launch thật. Không công bố Data delivery chỉ từ exit 0 của preflight.
- Tài liệu này supersede nội dung runtime cũ trong `testing/docs/*_v2.md`; phương pháp phân tích thống kê vẫn kế thừa, tham số chưa đo phải khóa sau pilot. Không sửa/mất lịch sử của repo; bộ này dùng để review trước khi áp vào nhánh Testing.


[Mã truy vết v2 sang baseline mới](reference/legacy_id_crosswalk.md). Các ID hiện hành thuộc namespace qa_20261128; dùng crosswalk khi đối chiếu lịch sử.

