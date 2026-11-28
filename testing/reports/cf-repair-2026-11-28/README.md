# Kết quả sửa các mục CF — 28/11/2026

Nhánh: `fix/integration-conflicts-20261128`. Commit mã sửa: `a64916e`. Bản gộp lấy `main@baa4967`, SDN `d7b9bce`, G0 `b3a85bb`; đã giải hết 8 file conflict. Bản làm việc gốc ở nhánh `feat-ml` được giữ nguyên. Chưa push, chưa thay đổi remote main hoặc tạo PR.

Kiểm chứng: **288 ca module đạt**, **22 ca PostgreSQL đạt, không skip**; fixture SDN v1.4 hiện hành qua kiểm tra. Log và lệnh chính xác nằm trong [verification.json](verification.json), [modules.log](modules.log), [postgres.log](postgres.log). PostgreSQL là database tạm riêng, không dùng dữ liệu triển khai.

| CF | Kết quả trong bản sửa |
| --- | --- |
| 01 | Giải 8 conflict G0/main, giữ hợp đồng model ML và guard G0 |
| 02 | SDN/G0 đã hội tụ trong nhánh local; chờ publish/PR |
| 03 | Gộp SDN qua main, giữ các file từng bị modify/delete |
| 06 | Tách run config hash và optimizer parameter hash; giữ topology hash nguyên gốc |
| 07 | Có mapper manifest thực sang RunCreate và frozen model context |
| 08 | Có Runner lifecycle/tick/recovery/audit/drain; chờ chạy live toàn hệ thống |
| 11 | Reject thiếu run/snapshot ID; bỏ ID mặc định tự tạo |
| 12 | Kiểm UTC span/anchor/H×Δ; fixture G0 dùng Δ=1 |
| 14 | Có adapter READY envelope từ run/snapshot/routing |
| 16 | Audit SQLite/JSONL bền vững và retry giữ payload |
| 18 | Chặn ghi đè journal, lỗi journal thì dừng; recovery trước batch mới; đọc đúng SDN job envelope |
| 19 | Kiểm hash model/config và frozen H; không áp epsilon hai lần |
| 22 | Code chờ đủ 30 bin measurement, audit tick thiếu history; policy còn chờ owner xác nhận |
| 23 | Chờ pilot latency H1, retention/late ACK và ngân sách timeout thực tế |
| 26 | RoutingHistory/VerificationEvidence gửi qua Data events; PostgreSQL lưu/replay đạt |
| 28 | Fixture PostgreSQL dùng bulk store, DB clock và catch-up hữu hạn; giữ ngưỡng stale cũ |
| 30 | Receive overcount thành invalid/unknown loss; validator có dedup/deadline |
| 32 | Network outbox ghi trước HTTP, fsync và retry; corruption không bị bỏ qua |
| 35 | Snapshot wire v1.4 và producer captures hiện hành; SQL byte round-trip/replay |
| 36 | Giữ guard coverage 24 link; empty batch bị từ chối đúng contract |
| 37 | DB clock cho availability; controlled clock cho fixture snapshot lớn |
| 38 | CI main/PR/manual, cùng checkout và environment lock; chưa có lượt CI remote |
| 39 | Chờ TC-I01 và fault pilot với Mininet/OVS thật; không tuyên bố PASS_SYSTEM |
| 40 | Có executor M0–M4/fault, raw validator và paired API; chưa chạy benchmark thật |
| 41 | Sửa ID, warm-up 60 và namespace QA gate/Network S0; supersede đề xuất cũ |
| 43 | Có requirements/runbook theo process; chờ launch controller/Mininet ở môi trường đích |
| 44 | README dùng `/apply`, đóng code fence, phân biệt audit và wire |
| 45 | Bỏ 9 file pyc khỏi tracking; giữ ignore cache |
| 46 | Chờ xác nhận người phụ trách, calibration và gate; không điền sign-off giả |

[cf-status.json](cf-status.json) đối chiếu đủ 29 mục còn mở từ báo cáo đầu vào. Các trạng thái “có code”, “đã kiểm thử contract” và “đã nghiệm thu live” được ghi riêng. Các mục policy/pilot/sign-off ở trên vẫn mở; việc sửa mã chưa thay thế các bằng chứng đó.

Chi tiết vận hành: [runtime contract và runbook](../../../integration/orchestration/README.md). Thời gian timeout trong runtime.yaml là profile phát triển, chưa được chứng nhận đáp ứng H1 trên mạng thật.
