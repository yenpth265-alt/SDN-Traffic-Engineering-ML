# Runtime contract và cách chạy

Baseline sửa CF ngày 28/11/2026 gộp `main@baa4967`, SDN `d7b9bce` và G0 `b3a85bb` trên nhánh riêng. Giữ model L30/H1/W5/epsilon0; không huấn luyện lại hoặc đổi H để che lỗi timing. Kiểm thử module, PostgreSQL và nghiệm thu Mininet/OVS có phạm vi riêng.

## Hợp đồng hiện hành

| Nội dung | Quy tắc |
| --- | --- |
| ID | `flow_group_id=0x103`, `path_id=0x103_P1`, `link_id=e1>c1` theo catalog Network |
| Topology | `v1.0`; `topology_hash` là 64 ký tự hex nguyên gốc trong bundle, không thêm tiền tố khi gửi SDN |
| Config | `config_version` từ Network; `config_hash` của run được truyền nguyên vẹn cho Data/ML/SDN/G0. `optimizer_config_hash` chỉ định danh tham số solver |
| Origin | Run và traffic origin phải là thời điểm UTC thực do launcher cấp; measurement origin = traffic origin + warm-up 60 s |
| Tick đầu | Không dùng bin âm hoặc padding. Audit các tick thiếu lịch sử; chỉ tạo snapshot khi cutoff ≥ L30 |
| Forecast | PredictionRow giữ đúng 7 trường và integer measurement bins; Data chỉ trả lô READY đủ 48×H. Runner thêm metadata vào envelope từ run/snapshot |
| Giá trị dự báo | ML đã áp epsilon; G0 dùng mean của giá trị cuối cùng, không áp lần hai |
| Thời hạn | UTC window và integer bins phải cùng origin, span H×Δ. `apply_deadline_utc` không vượt forecast end; hết hạn thì skip/reconcile |
| Decision | Ghi đủ 48 dòng vào Data trước `POST /apply`; polling dùng `/apply-status` và giữ cả run/batch ID |
| Recovery | Journal lưu trước HTTP; batch chưa resolve chặn batch tiếp theo. Runner lock ngăn hai worker cùng dùng một journal |
| Audit | SQLite/JSONL bền vững trước HTTP, giữ nguyên payload khi mất ACK. RoutingHistory và VerificationEvidence đi qua `/v1/runs/{run_id}/events/sdn`, giữ đầy đủ dữ liệu trong `detail` |
| Chất lượng metric | Thiếu log/link thì NULL hoặc INCOMPLETE; receive count vượt sent là INVALID, không tạo loss âm hoặc tự clamp về 0 |
| Gate | G1–G6 là gate nghiệm thu QA. Gate Network/S0 là gate module; tên trùng không đồng nghĩa cùng tiêu chí |

## Môi trường theo process

Data API dùng `requirements-online.txt`. Controller Ubuntu dùng `sdn/requirements-osken.txt` và cấu hình trong `sdn/SDN_INTEGRATION_PROFILE_V14.md`. Network/OVS/Mininet chạy trong môi trường hệ thống Ubuntu. ML và Runner dùng Python 3.11 với artifact lock; không ép controller và model vào chung một venv.

```bash
python3.11 -m venv .venv-runner
. .venv-runner/bin/activate
python -m pip install -r integration/requirements-runner.txt
```

Runner cần các biến môi trường `SDN_RUNNER_KEY`, `SDN_READER_KEY`, `SDN_ML_KEY`, `SDN_OPTIMIZATION_KEY`, `SDN_NORTHBOUND_TOKEN`. Data/SDN dùng role key tương ứng theo profile; không ghi secret vào manifest hay báo cáo. Controller phải dùng cùng run ID, config version/hash và topology; bật Data v1 demand provider, publisher và durable idempotency theo profile SDN.

## Launcher và Runner

Launcher chịu trách nhiệm khởi động Data, controller, Mininet, 8 receiver, 8 sender và collector; cấp `run_started_at_utc` và `traffic_started_at_utc` thực vào bản manifest riêng. File mẫu `configs/experiments/runner_manifest.json` không phải một lần chạy mới. Runner đăng ký run trước traffic; endpoint Data `GET /v1/runs/{run_id}/topology` phải đọc được catalog đã nạp. Controller readiness phải xác nhận 6 switch, 48 flow VERIFIED và Data sync gate.

Sửa địa chỉ và đường dẫn log theo môi trường trong `configs/experiments/runtime.yaml`, rồi kiểm tra mapping trước:

```bash
python -m integration.orchestration.runner actual-manifest.json
python -m integration.orchestration.runner actual-manifest.json --execute
```

M0 giữ routing tĩnh. M1–M3 dùng OnlineForecastRunner và không đọc schedule tương lai. M4 phải khai báo `mode=oracle`, `schedule_file`, `schedule_id` và `schedule_sha256`; chỉ OracleProvider được đọc phần tương lai. Các giá trị timeout trong runtime.yaml là cấu hình phát triển, chưa phải ngân sách latency được nghiệm thu cho H1.

Runner không xếp hàng tick quá hạn. Nó audit từng tick bị lỡ, chặn khi routing/apply chưa đồng bộ, chờ hết measurement/drain và kiểm tra các outbox trước khi kết thúc. Các producer phải ghi log vào `network_outbox_dir`; chạy correlate sau khi receiver kết thúc và trong thời gian drain để tạo `traffic_outbox.jsonl`. Nếu thiếu journal hoặc còn pending, run kết thúc failed và giữ bằng chứng để replay. Không xóa `logs/runtime` khi còn batch chưa resolve.

## Benchmark và kiểm tra raw

Protocol JSON cần `schedules` (schedule_id/scenario_id/seed/split=test/schedule_sha256), `order_seed`; có thể thêm `faults` với fault_id F01–F12, schedule_id và seeds. Khi execute, `launcher` là mảng argv, hỗ trợ `{run_id}`, `{run_dir}`, `{method_id}`, `{schedule_id}`, `{fault_id}`, `{fault_seed}`; `run_timeout_s` là giới hạn mỗi run. Launcher phải thực thi/reset hook thật và lưu fault trace. Không sửa raw log để giả lập fault.

```bash
python -m testing.scripts.run_benchmarks protocol.json run-order.json
python -m testing.scripts.run_benchmarks protocol.json results/benchmark --execute
python -m testing.scripts.validate_results results/benchmark/RUN_ID --packet-deadline-s 0.5
```

Deadline `0.5` ở lệnh cuối chỉ là ví dụ tham số; phải thay bằng giá trị cohort deadline đã khóa trong protocol. Validator đọc `manifest.json` và `raw/` gồm sent_h*.csv, recvpkts_h*.csv và link_samples.jsonl; tính lại unique receive/loss/goodput, MLU/coverage và SHA raw. Packet log mới ghi cả `received_bytes`; log cũ thiếu byte evidence không được suy diễn goodput. `paired_differences` trong validate_results.py ghép theo schedule, báo n/missing và bootstrap CI; không coi các giây trong cùng schedule là mẫu độc lập.

Mỗi launcher return code và run lỗi được giữ trong execution.json. `COMPLETED` chỉ có nghĩa process đã thoát 0, chưa đồng nghĩa VALID metric hoặc PASS_SYSTEM. Chỉ đóng CF-23/CF-39/CF-46 sau pilot live, fault trace và xác nhận người phụ trách; không dùng kết quả unit test để tự ký các gate này.
