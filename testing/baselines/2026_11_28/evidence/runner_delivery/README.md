# Runner — Network → Data delivery preflight

**Ngày:** 28/11/2026  
**Phạm vi:** R05 (runtime configuration preflight) và R06/R11 (lifecycle, evidence).  
**Baseline đối chiếu:** main @ [baa496769deff49f2d17e05371b8d9977df8483e](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/tree/baa496769deff49f2d17e05371b8d9977df8483e).

## 1. Hợp đồng nhận từ Network

| Producer | Tham số bắt buộc trong integration | Role Data |
| --- | --- | --- |
| Demand sender | --release-url http://<DATA_HOST>:<PORT>/v1/network/demand | NETWORK |
| Telemetry collector | --telemetry-url http://<DATA_HOST>:<PORT>/v1/sdn/links | NETWORK hoặc SDN; dùng NETWORK cho hai Network producer |

API key qua biến môi trường DATA_API_KEY hoặc tham số --api-key; không ghi key vào file cấu hình, manifest, evidence hay log. Ưu tiên biến môi trường để tránh hiện secret trong command line/process list/shell history. Nếu người vận hành chọn tham số --api-key của Runner, gate chuyển key vào môi trường tiến trình Network (không thêm --api-key vào child args).

Nguồn:

- [sender.py](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/blob/baa496769deff49f2d17e05371b8d9977df8483e/network/traffic_generator/sender.py): release URL optional trong producer, api_key lấy từ tham số hoặc DATA_API_KEY; nếu thiếu URL, không phát hành sang Data.
- [telemetry_collector.py](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/blob/baa496769deff49f2d17e05371b8d9977df8483e/network/telemetry/telemetry_collector.py): telemetry URL optional; --dry-run mô phỏng counters nhưng không tự cấm POST nếu vẫn có URL.
- [online_api.py](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/blob/baa496769deff49f2d17e05371b8d9977df8483e/data/ingestion/online_api.py): demand endpoint yêu cầu role NETWORK; links endpoint chấp nhận NETWORK/SDN; header X-API-Key.

## 2. Chính sách tại Runner

| Mode | URL | Secret | Khởi chạy | Evidence |
| --- | --- | --- | --- | --- |
| integration | Bắt buộc cả hai URL; đúng path và cùng origin Data API | Bắt buộc key không rỗng | Gate cấu hình cho phép **chỉ khi tất cả điều kiện đúng**; còn phải qua các gate runtime khác | CONFIG_VALID_NOT_RUN; delivery_verified=false |
| offline | Cấm truyền URL | Không dùng key; loại DATA_API_KEY khỏi môi trường child | Có thể chuẩn bị options cho thử nghiệm local sau các gate phù hợp; không gửi Data | data_delivery_enabled=false |
| dry-run | Cấm truyền URL | Không dùng key trực tiếp | Không launch producer nào; chỉ validate/preview | network_launch_allowed_by_this_gate=false |

Dùng chế độ rõ ràng, không tự suy mode theo URL. Khi thiếu URL, thiếu key, URL sai hoặc mode mâu thuẫn, raise DeliveryConfigError trước khi launch bất kỳ Network producer nào; CLI exit 2. Không fallback sang offline.

Quy tắc bổ sung của bản đề xuất: chấp nhận HTTP(S), URL tuyệt đối đúng path, không userinfo/query/fragment hoặc placeholder; hai URL phải trỏ cùng origin Data theo hợp đồng một DATA_HOST/PORT. Nếu nhóm chủ đích triển khai hai origin riêng, cần chốt lại policy. Dùng HTTPS nếu truyền bearer-like API key qua môi trường không được cô lập.

## 3. Bản triển khai và cách kiểm tra

- runner/network_delivery.py: hàm prepare_network_delivery, lớp PreparedDelivery, CLI chỉ validate/preview.
- testing/runner/test_network_delivery.py: unittest với giá trị tổng hợp; không gọi HTTP hay launch tiến trình Network.
- evidence/unit_tests.txt và evidence/cli_cases.json: kết quả thực tế, không lưu key.

Tại thư mục giải nén:

```sh
python -m unittest -v testing.runner.test_network_delivery
python -m runner.network_delivery --mode offline
python -m runner.network_delivery --mode dry-run
```

CLI integration kiểm tra khi người vận hành đã cung cấp biến DATA_API_KEY và hai URL thật. Không viết giá trị key vào lệnh mẫu. Gate không gửi HTTP; exit 0 chỉ nói cấu hình hợp lệ, không xác nhận endpoint reachable, key được Data chấp nhận, run đã register, producer đã chạy hoặc DB đã có bản ghi.

Tại lifecycle Runner thật: gọi hàm gate ngay trước mọi bước launch Network; nếu báo lỗi thì trả failed preflight và không launch. Khi gate thành công, lấy sender_options/collector_options và môi trường child từ đối tượng trả về; không cho caller ghép thêm URL/key ngoài options đã kiểm, không log môi trường. Dry-run có options=None và không được gọi child_environment. Gate này không tự mở G0/G1/G2 hoặc thay thế readiness và kiểm tra topology/config hash.

## 4. Evidence — 28/11

| Case | Expected | Actual | Status |
| --- | --- | --- | --- |
| Unit suite | 18 ca đạt | 18/18 đạt; 0 lỗi, 0 skip | PASS_UNIT |
| CLI integration đủ hai URL và key tổng hợp | exit 0, delivery_verified=false | exit 0 | PASS_OFFLINE |
| Integration thiếu telemetry URL | exit 2; không launch | exit 2 | PASS_OFFLINE |
| Offline | exit 0, delivery disabled | exit 0 | PASS_OFFLINE |
| Dry-run | exit 0, launch prohibited | exit 0 | PASS_OFFLINE |
| Dry-run kèm telemetry URL | exit 2 | exit 2 | PASS_OFFLINE |

Unit suite kiểm bổ sung thiếu release URL, thiếu cả hai URL, thiếu key, key chứa newline, endpoint đảo, URL sai, origin khác, mode không hợp lệ, secret không xuất hiện trong evidence/args và không sửa môi trường nguồn.

## 5. Điều kiện nghiệm thu runtime sau khi group phê duyệt

1. Xác nhận Data base URL, key role NETWORK, Data health/readiness và canonical topology/run manifest; không lưu key.
2. Đi qua đúng gate trong entry point Runner thực tế; kiểm tra thiếu URL làm fail trước launch, kể cả với hai tiến trình Network dự kiến.
3. Sau khi cho phép chạy tích hợp, kiểm bằng ACK/outbox và truy vấn chỉ đọc DB kiểm thử riêng: DemandRelease và LinkSampleBatch thuộc đúng run, đúng bin, đúng topology. Không chỉ dựa trên stdout hoặc exit 0.
4. Ghi expected/actual/evidence riêng cho producer delivery, duplicate replay, timeout/mất kết nối và terminal status theo contract nhóm.

Chưa thực hiện bước 1–4 trong bản chuẩn bị offline. Không thay đổi sender/collector hoặc Data code. Chưa commit/push/merge. Main tại baseline không có implementation Runner hoàn chỉnh; khi ghép patch vào repo, giữ package init hiện có nếu có, chỉ thêm module và test phù hợp.
