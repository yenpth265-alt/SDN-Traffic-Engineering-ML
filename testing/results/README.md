> **TÀI LIỆU LỊCH SỬ — không dùng làm đặc tả runtime hiện hành.** Giữ nguyên nội dung và bằng chứng tại mốc ghi bên dưới. Đối chiếu [baseline QA 28/11/2026](../baselines/2026_11_28/README.md), [ánh xạ ID](../baselines/2026_11_28/reference/legacy_id_crosswalk.md) và [runtime contract](../../integration/orchestration/README.md). [CI main@71f120a](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/actions/runs/46134738871) đạt 289 module, 19 Data/QA và 3 SDN–PostgreSQL; không đồng nghĩa sign-off/live.

# Kết quả kiểm thử

Mốc rà soát là `main@db2dacd` ngày 24/11/2026. Lượt CI gần nhất chạy trên Ubuntu, Python 3.12 và PostgreSQL 17.12 với DB riêng `sdn_qa` và đủ ba migration. Lượt chạy thuộc `44acd20`, có **16/17 ca đạt**. `main` có thêm thay đổi telemetry collector sau PR #5 và chưa được kiểm lại tại mốc rà soát. [Log CI](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/actions/runs/45223682285/job/137864381051) · [So sánh thay đổi](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/compare/feaaed0...db2dacd).

| Ca hoặc nhóm ca | Kỳ vọng | Thực tế | Trạng thái |
| --- | --- | --- | --- |
| Runner đăng ký lặp | Cùng ID và nội dung, HTTP 200, `inserted: 0`, giữ một run. | HTTP 409, PostgreSQL giữ một run. | FAIL |
| Telemetry, demand và traffic | Replay thêm 0. Payload sai bị chặn. Khi một dòng traffic vi phạm khóa ngoại, toàn bộ lô rollback. | Telemetry chèn 24 rồi 0. Demand gửi đồng thời trả ACK 0 và 1. Traffic vi phạm khóa ngoại trả 422 và rollback. | PASS trong phạm vi fixture |
| TC-I02 và snapshot thiếu bin | Bin tương lai và bản ghi đến sau `as_of` không vào lịch sử cũ. Snapshot thiếu bin chặn forecast. M1–M3 bất biến khi đổi lịch tương lai. | History cũ 95 dòng, history mới 96 dù DB có 97 observation. Ép VALID trả 422. M1–M3 giữ kết quả với đầu vào ML dựng từ fixture và giá trị history DB. | PASS cho bộ lọc thời gian và hành vi M1–M3 đã kiểm |
| TC-I03, biên thời gian và batch | Traffic 160 = measurement 100. H=5 cho 240 dòng trong một batch, replay thêm 0. | Đúng mốc `10:02:40Z`. DB có 240 dòng ở bin 100–104 khi bin được chuẩn hóa thành số nguyên trong bộ test. Forecast nguyên dạng có bin float nhận 422. | PASS cho quy đổi và batch integer. Lỗi wire ML còn mở. |
| Routing version và deadline | Chặn version cũ, nhảy version và deadline hết trước ghi. | DB lưu version 1 và 2. Version không hợp lệ trả 409. Deadline hết trả 422, còn 0 tick và 0 decision batch. | PASS trong phạm vi fixture |
| Apply result | Routing v2 được ghi trước kết quả và result đủ nhóm trùng decision. Replay thêm 0. | Sai thứ tự trả 409, result một dòng trả 422. Dạng mở rộng ghi 1 APPLIED và 47 KEPT, ACK lặp `updated: 0`. | PASS cho fixture đủ nhóm. Đường publish runtime còn mở. |
| Sự kiện và trạng thái run | Kiểm role, replay và khóa trạng thái cuối. | Role sai trả 401, event giữ một dòng, PATCH lặp cập nhật 0, đổi trạng thái cuối trả 409. | PASS |
| TC-I01 và benchmark M0–M4 | Truy vết end-to-end trên Mininet/OVS và PostgreSQL. Có số liệu đối chứng theo cùng cấu hình và seed. | Có báo cáo SDN chạy riêng trên Mininet/OVS và ML pilot offline. Nhánh tích hợp, adapter SDN Data v1 và Optimization còn phân tán. | NOT_RUN cho vòng tích hợp và benchmark |

Các phản hồi 422 ở ca âm là kết quả kỳ vọng của validator. Hai payload gốc bị từ chối vẫn là lỗi cần sửa để chạy đường producer–consumer. Phạm vi M4 trong TC-I02 và các ca áp dụng runtime sau khi ghi còn chờ xác minh.

Xem [expected, actual và truy vấn đọc lại theo từng ca](../../docs/architecture/Online_API_Team_Decisions.md#kết-quả-chạy-postgresql), [báo cáo SDN live](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/blob/integration/e2e/sdn/DYNAMIC_ROUTING_LIVE_VALIDATION_REPORT.md), [pilot ML offline](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/blob/integration/e2e/testing/reports/ml-pipeline-2026-11-18.md) và [so sánh nhánh tích hợp](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/compare/main...integration/e2e).

