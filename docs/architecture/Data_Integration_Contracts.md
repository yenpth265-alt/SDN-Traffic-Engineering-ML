# Hợp đồng tích hợp Data với các module

**Ngày cập nhật:** 25/11/2026  
**Trạng thái:** Ba hợp đồng tích hợp dưới đây là phương án nhóm đã thống nhất. Các mục ghi rõ “cần chốt chi tiết” là phần triển khai còn mở; không làm thay đổi các hợp đồng đã thống nhất.

## 1. Network → Data → SDN: DemandRelease

| Nội dung | Hợp đồng đề nghị |
|---|---|
| Network gửi | Demand đã hoàn tất theo từng giây và từng `flow_group_id` qua `POST /v1/network/demand`. Giữ nguyên `event_id`, `run_id`, `bin_index`, thời gian UTC và giá trị khi gửi lại. Nếu `requested_mbps` không có, gửi `null` cùng `reason`; không thay bằng 0. |
| Data lưu | Data là nơi lưu chính. Data tự gán `available_at_utc` theo đồng hồ PostgreSQL lúc nhận; Network không tự đặt thời điểm này. Data không dùng `demand_plan` để điền lịch sử còn thiếu. |
| SDN đọc | SDN đọc lịch sử từ `GET /v1/runs/{run_id}/demand-history`, giới hạn theo `cutoff`, `as_of` và `lookback`, rồi tự tạo Snapshot. Không suy `requested_mbps` từ goodput hoặc counter của cổng mạng. |
| Gửi lại và lỗi | Cùng khóa, cùng nội dung: Data xác nhận replay, không tạo bản ghi mới. Cùng khóa nhưng nội dung khác: `409`. Payload không hợp lệ: `422`. Khi Data không truy cập được, Network giữ bản chưa xác nhận trong outbox và gửi lại nguyên ID/nội dung. Không tạo Snapshot `VALID` từ dữ liệu chưa đọc được từ Data. |

**Cần chốt chi tiết:** số lần/thời gian retry tối đa và việc Runner đánh dấu run `failed` hay `discarded` khi hết thời gian chờ.  
**Vai trò SQLite ledger của SDN:** đề nghị chỉ dùng làm sổ cục bộ cho retry/outbox và chống gửi trùng; PostgreSQL/Data vẫn là nguồn dữ liệu chính. SDN chốt ledger lưu payload hay chỉ lưu ID/trạng thái gửi.

## 2. SDN → Data → ML: Snapshot

| Nội dung | Hợp đồng đề nghị |
|---|---|
| Ai tạo | SDN tạo Snapshot từ demand history đã phát hành và routing state SDN đã xác minh. Data không tự tạo hoặc sửa Snapshot. |
| Payload | Dùng một schema Snapshot đầy đủ, thống nhất với SDN Snapshot v1.4: metadata nguồn gốc/cấu hình/topology, hash, lịch sử nhân quả và routing state đủ 48 luồng. Data giữ nguyên các trường; thiếu giá trị thì giữ `null`, không tự bỏ trường và không điền 0. |
| Lưu và đọc lại | SDN gửi JSON cùng `X-Content-SHA256`. Data kiểm tra hash của đúng bytes nhận được, lưu bytes bất biến và GET trả lại đúng bytes cùng hash. Hash vận chuyển này khác `content_hash` bên trong Snapshot; SDN cần công bố cách tạo `content_hash` trên nội dung chuẩn hóa. |
| Chất lượng | `VALID` chỉ khi lịch sử đủ `48 × L`, giá trị demand có bằng chứng và cả 48 route được xác minh. Thiếu lịch sử dùng `INCOMPLETE`; `STALE`/`INVALID` không được dùng để dự báo hoặc tối ưu. Không thay dữ liệu thiếu bằng 0. |

### Chi tiết còn mở trong hợp đồng Snapshot đã thống nhất

1. **Schema trên đường truyền:** Data nhận body canonical v1.4 dạng `{"sdn": {...}}`, gồm `origin_refs`, `flow_group_ids`, `routing_history`, metadata cấu hình/topology và hash. SDN gửi nguyên payload này; không chuyển thành schema phẳng rút gọn. Data lưu/trả lại nguyên bytes.
2. **Các trường causal history:** chốt danh sách bắt buộc, tối thiểu gồm `flow_group_id`, `bin_index`, cửa sổ UTC, `requested_mbps`, `released_at_utc`, `available_at_utc`, `valid`, `reason` và `release_record_id`. Nếu không có bằng chứng, các giá trị và timestamp liên quan phải là `null`, không được bịa giá trị.
3. **Gốc bin:** `cutoff_offset_s` và history `bin_index` trong Snapshot đếm từ `measurement_started_at_utc`; bin demand lưu trong Data đếm từ `traffic_started_at_utc`, bao gồm warm-up. Data chuyển đổi qua `origin_refs.warmup_s` và kiểm tra các mốc UTC tương ứng. Forecast offsets cũng theo measurement origin.
4. **Hash:** `content_hash` là SHA-256 của JSON UTF-8 canonical trong object `sdn`, bỏ trường `content_hash` trước khi tính, sắp xếp key và dùng dấu phân cách compact `,`/`:`. `X-Content-SHA256` là SHA-256 của nguyên bytes HTTP. Hai hash phục vụ hai mục đích khác nhau.
5. **Version chất lượng:** SDN và ML xác nhận cách đánh dấu `VALID`, `INCOMPLETE`, `STALE`, `INVALID`, đặc biệt khi route chưa xác minh hoặc demand đến muộn.

## 3. Optimization → SDN → Data: DecisionBatch và ApplyResult

| Nội dung | Hợp đồng đề nghị |
|---|---|
| Optimization gửi | `POST /v1/optimization/decisions` với đủ đúng 48 luồng, gồm `old_path_id` và `proposed_path_id` cho từng luồng, kèm `tick_id`, `decision_batch_id`, snapshot/version và deadline. |
| SDN thực thi | SDN chỉ cài lại B luồng có tuyến đề xuất khác tuyến cũ. Với các luồng không đổi, SDN vẫn đưa `KEPT` vào kết quả để tổng kết quả có đủ 48 luồng. |
| Thứ tự | Runner tạo và truyền `tick_id` cùng `decision_batch_id`. Data ghi tick và DecisionBatch. SDN kiểm tra version/deadline, áp dụng và xác minh; sau đó công bố RoutingState/version đủ 48 luồng **trước** khi gửi `POST /v1/sdn/apply-results`. |
| Kết quả lưu | Data lưu kết quả cho đủ 48 luồng theo routing state thực tế đã xác minh. `APPLIED` phải khớp tuyến đề xuất; `KEPT`/`ROLLED_BACK` phải khớp tuyến cũ; `UNKNOWN` không có tuyến hoặc thời điểm xác minh. Batch `APPLIED` có thể gồm cả luồng `APPLIED` lẫn `KEPT`. |

**Lỗi cần xử lý:** version cũ hoặc thứ tự công bố sai trả `409`; sai schema/thiếu luồng trả `422`; hết deadline thì SDN từ chối trước khi đổi tuyến. Mọi lần gửi lại phải giữ nguyên ID và nội dung; kết quả cuối không được thay bằng payload khác.

## 4. Telemetry link Network → Data

- Mỗi batch phải có đúng các link trong topology có `from_port > 0`; các chiều không đo được với `measure_port=0` không gửi vào Data.
- Topology hiện có 24 link đo được. Data đối chiếu danh sách gửi với topology, không chỉ tin `sample_count` do producer khai báo.
- Mỗi link vẫn phải có một dòng khi counter chưa dùng được; gửi các số đo không có bằng chứng là `null` và khai báo `counter_status` phù hợp. Không bỏ link chỉ vì lần đo đó bị lỗi.
- Cùng `batch_id` phải gửi lại đúng cùng danh sách link và cùng nội dung.

## 5. Forecast ML → Data → Optimization

- ML gửi đúng `48 × H` dòng cho Snapshot, mỗi flow/bin đúng một lần.
- `forecast_start` và `forecast_end` trên API Data là số nguyên strict; không gửi số dạng `100.0` dù giá trị toán học là số nguyên.
- Optimization đọc batch READY từ Data và tự tính trung bình H bước theo hợp đồng v1.4; Data không tính trung bình thay Optimization.

## 6. Việc mỗi module cần hoàn tất

| Module | Cần xác nhận |
|---|---|
| Network | Duy trì demand mỗi giây, ID ổn định khi retry; gửi đủ 24 link đo được; thống nhất biểu diễn counter thiếu và retry khi Data unavailable. |
| SDN | Dùng Snapshot v1.4 đã thống nhất; khóa gốc bin, `content_hash`, quality status; chốt chi tiết SQLite ledger và retry/outbox. |
| ML | Đọc Snapshot v1.4 theo gốc bin đã khóa; xử lý `INCOMPLETE`; gửi offset nguyên và đủ `48 × H` dòng. |
| Optimization | Gửi DecisionBatch đủ 48 luồng; giữ đúng quy tắc cho luồng không đổi và deadline. |
| Runner | Sinh/truyền ID; điều phối đúng thứ tự API; chốt timeout/retry và cách kết thúc run nếu Data không khả dụng. |
| Testing | Kiểm tra PostgreSQL migrations, 24 link, Snapshot đủ/thiếu dữ liệu, hash, 48 kết quả và thứ tự RoutingState trước ApplyResult. |

## 7. Trạng thái phía Data

- Đã có API nhận demand, telemetry, routing, snapshot, forecast, decision và kết quả apply; có kiểm tra replay/version/48 luồng theo các endpoint tương ứng.
- Working copy hiện đã được sửa để cho phép history Snapshot không có timestamp/lý do khi không có bằng chứng, kiểm tra batch link theo topology, và chấp nhận kết quả `APPLIED` gồm luồng `APPLIED` cùng `KEPT`.
- **Còn phải đồng bộ:** SDN/ML adapter cần gửi/đọc body `{"sdn": {...}}` thay cho schema phẳng cũ; Testing cần đổi fixture/harness theo wire schema này. Chạy PostgreSQL/TC-I01 sau khi migration cần thiết được áp dụng; sửa đổi working copy chưa qua kiểm thử tích hợp.
