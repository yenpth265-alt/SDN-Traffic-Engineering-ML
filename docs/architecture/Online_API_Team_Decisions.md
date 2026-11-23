# API online Data — các nội dung cần team chốt

Cập nhật: 23/11/2026. Các phương án dưới đây đang được dùng tạm trong code, cần các module xác nhận trước khi tích hợp.

## Luồng thống nhất tạm thời

Runner đăng ký lần chạy → Network gửi dữ liệu → SDN tạo snapshot → AI dự báo → Optimization chọn đường → SDN áp dụng và báo kết quả. Data kiểm tra, lưu và cung cấp dữ liệu giữa các bước.

Snapshot là bản dữ liệu cố định tại một thời điểm. Runner là chương trình điều phối một lần chạy; cần phân công người phụ trách.

## Mỗi module cần xác nhận gì?

| Module | Phương án tạm | Nội dung cần chốt |
|---|---|---|
| **Data** | Một địa chỉ API chung, mỗi vai trò một API key | Máy chạy API/DB, địa chỉ và cổng truy cập, HTTPS, cách cấp key; cập nhật tài liệu sau khi team xác nhận |
| **Network** | Gửi demand từng giây đã hoàn tất; ghép sender/receiver rồi gửi traffic; gửi lại cùng ID khi lỗi | Định nghĩa requested_mbps và thời điểm phát hành; ai ghép số liệu gửi–nhận; xử lý dữ liệu thiếu/đến muộn; Network hay SDN gửi số liệu liên kết |
| **SDN** | Tạo snapshot; công bố routing version; kiểm tra lại version và hạn áp dụng trước khi đổi đường; công bố routing trước khi báo kết quả cuối | Cấu trúc snapshot và mốc đếm giây; quy tắc tăng version; cách báo từ chối/thất bại/khôi phục đường cũ; ý nghĩa FAILED và UNKNOWN |
| **AI/ML** | Đọc snapshot hợp lệ; gửi đủ 48 × H dòng dự báo, gắn đúng snapshot | L (số giây lịch sử), H (số giây dự báo), phiên bản mô hình/cấu hình; thứ tự nhóm luồng; cách xử lý thiếu lịch sử |
| **Optimization** | Gửi đủ mọi nhóm luồng, kể cả giữ nguyên; kèm snapshot, forecast, version và hạn áp dụng | Gửi toàn bộ hay chỉ nhóm đổi đường; hạn áp dụng tính từ đâu và bao lâu; ai tạo tick_id (ID mỗi lượt tối ưu); cách báo không có nghiệm/bỏ qua lượt |
| **Test/Runner** | Đăng ký run qua API; chuyển decision_batch_id cho SDN; kết thúc run bằng ok/failed/discarded | Người viết Runner; cách thông báo ID cho SDN; tiêu chí kết thúc/hủy lần chạy; thời gian chờ dữ liệu muộn; bài kiểm thử tích hợp |

## Quy tắc chung cần tất cả module thống nhất

- **Thời gian:** dùng UTC và đồng bộ đồng hồ. Bắt đầu đo = bắt đầu phát lưu lượng + thời gian khởi động; các mốc cố định khi đăng ký run. Nếu khởi động lệch, cần thống nhất hủy run hay sửa cơ chế đăng ký.
- **Cách đếm giây:** demand/snapshot đếm từ lúc phát lưu lượng; dự báo đếm từ lúc đo chính thức. Ví dụ khởi động 60 giây thì giây 60 của demand tương ứng giây 0 của dự báo.
- **Snapshot:** SDN tạo, Data kiểm tra/lưu/trả; thống nhất một cấu trúc. Không đưa trực tiếp history `format=sdn` vào snapshot hiện tại vì khác trường và mốc đếm giây.
- **Dữ liệu online:** không lấy lịch tương lai từ demand_plan để bù lịch sử thiếu.
- **Gửi lại:** giữ nguyên ID và nội dung; bên gửi lưu bản chưa được xác nhận. Chốt thời gian chờ, số lần thử; 409 là xung đột nội dung, 422 là dữ liệu không hợp lệ, cần kiểm tra trước khi gửi lại.
- **Kết quả SDN:** APPLIED = đường đề xuất; KEPT/ROLLED_BACK = đường cũ; UNKNOWN = chưa xác minh được đường thực tế. Chốt cách tổng hợp trạng thái cả lô và ghi sự kiện lỗi/khôi phục.
- **Giao nhận:** tạm dùng Runner chuyển ID; chưa có API danh sách quyết định chờ hay thông báo tự động. Topology có thể đọc qua API, cần chốt dùng API hay file cấu hình chung.

## API theo module

Các đường dẫn bắt đầu bằng `/v1`, trừ health. `{run_id}` là ID lần chạy.

| Module | API sử dụng |
|---|---|
| Network | POST `/v1/network/demand`, `/v1/network/traffic`; POST `/v1/sdn/links` nếu được giao thu thập liên kết |
| SDN | POST `/v1/sdn/routing`, `/v1/sdn/snapshots`, `/v1/sdn/apply-results`; GET `/v1/runs/{run_id}/demand-history`, `/v1/runs/{run_id}/decisions/{decision_batch_id}` |
| AI/ML | GET `/v1/runs/{run_id}/snapshots/{snapshot_id}` bằng Reader key; POST `/v1/ml/forecasts` bằng ML key |
| Optimization | GET `/v1/runs/{run_id}/forecasts/{forecast_batch_id}`; POST `/v1/optimization/decisions` |
| Test/Runner | POST `/v1/runs`; GET `/v1/runs/{run_id}`; PATCH `/v1/runs/{run_id}/status` |
| Dùng chung | GET `/v1/runs/{run_id}/topology`; POST `/v1/runs/{run_id}/events/{component}` với component là network/sdn/ml/optimization/runner và key tương ứng; GET `/health/live`, `/health/ready` |

Xem trường dữ liệu và thử API tại `/docs` khi dịch vụ chạy. Code nằm trong `data/ingestion/online_api.py`, `online_models.py`, `online_store.py` và `management_api.py`.

## Ưu tiên chốt và kiểm thử

Chốt trước: **người phụ trách Runner → địa chỉ/key → thời gian → L/H và snapshot → cách giao quyết định → hạn áp dụng và kết quả SDN**.

Sau đó chạy thử một vòng đủ 48 nhóm luồng; thử mất kết nối, gửi trùng, dữ liệu thiếu và version thay đổi. Hiện mới kiểm tra cú pháp và viết kiểm thử; chưa xác nhận API chạy đạt với PostgreSQL và mạng thật.
