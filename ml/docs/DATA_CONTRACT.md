# File adapter ML v1 — đầu vào offline và snapshot inference

> **Cập nhật 26/11/2026:** Tài liệu hợp đồng API online v1.4 đầy đủ (endpoint HTTP, `X-API-Key`, envelope `{"sdn": ...}`, `ForecastBatch` với offset số nguyên và cách chạy `python -m ml online`) nằm tại [ONLINE_INTEGRATION_CONTRACT.md](ONLINE_INTEGRATION_CONTRACT.md). Tài liệu dưới đây giữ đặc tả chi tiết cho file adapter offline và `Predictor` lõi.

**Làm rõ phạm vi ngày 28/11/2026:** `history.csv` và `runs.json` trong mục A chỉ phục vụ tạo TrainingBundle cho bộ dữ liệu offline mới, huấn luyện lại hoặc đánh giá offline trên dữ liệu mới. Hai file không phải đầu vào của `python -m ml online`/`Predictor` và không phải đầu ra Data bắt buộc bổ sung cho model hiện tại. Inference dùng artifact đã khóa, run metadata và snapshot có history do SDN tạo/Data lưu và trả qua API. Nếu dùng lệnh `predict` bằng file, đầu vào là snapshot/context JSON, không phải cặp file huấn luyện này.

Semantics theo Architecture v1.4 và Testing IF04–IF06. ML không truy cập DB, tạo snapshot sản xuất hoặc tự sửa nguồn dữ liệu.

**Nguồn chuẩn split:** thống nhất với Network và Architecture v1.4 ngày 22/11/2026: 10/5/5 (50/25/25). Với 20 seed trong `configs/experiments/runner_manifest.json`: 1–10 train, 11–15 validation, 16–20 test. ML giữ nguyên nhãn nguồn và kiểm tra split trước khi tạo window.

**Khóa model offline (26/11/2026):** Chọn L/H bằng validation pooled vector MAE; test không chọn tham số. Online đọc `configs/ml/selected.yaml` và model được pin trong đó. Runner đăng ký L/H tương ứng; không tự thay H theo từng tick. `ml_config_hash` khi đăng ký run lấy từ `ml_config_sha256` trong YAML.

## A. Export ngoại tuyến

History CSV:

~~~csv
run_id,flow_group_id,time_offset_s,requested_mbps
run_seed01,0x103,0,0.75
run_seed01,0x104,0,0.83
~~~

Ví dụ chỉ có hai dòng để minh họa. Bản thật đủ 48 nhóm mỗi giây, kể cả nhu cầu 0, cho mọi run khai báo. Offset từ traffic start, bắt đầu 0, không có bin drain. Cột phụ schedule_id/scenario_id/split nếu có phải khớp runs.json. Các cột khác không vào feature.

Demand không âm, hữu hạn, đơn vị Mbps. Không điền missing thành 0; không đổi tên goodput/counter thành requested_mbps.

Run manifest:

~~~json
{
  "delta_s": 1,
  "runs": [
    {
      "run_id": "run_seed01",
      "schedule_id": "S1_seed01",
      "parent_family_id": "family_seed01",
      "scenario_id": "S1",
      "seed": 1,
      "split": "train",
      "run_started_at_utc": "2026-11-12T08:59:50Z",
      "traffic_started_at_utc": "2026-11-12T09:00:00Z",
      "measurement_started_at_utc": "2026-11-12T09:01:00Z",
      "measurement_end_utc": "2026-11-12T09:07:00Z"
    }
  ]
}
~~~

Ví dụ chỉ minh họa một run. Cần ít nhất 20 seed với split 10/5/5, hoặc bội số 20 theo 50/25/25. Cùng seed/schedule/family giữ cùng split; replay khác method không đổi family.

Các CSV trong `ml/data/` dùng offset measurement -60–359 và chưa có đủ schema của cặp history/runs ở trên. Để chạy bộ offline hiện tại, dùng `from-schedule` với `network/traffic_schedules/demand_plan.zip` (offset traffic 0–419), warm-up 60 giây và measurement 360 giây như hướng dẫn trong README.

Với mốc ví dụ, CSV cần bin 0–419. Cutoff measurement t=100 ứng với traffic offset 160. L=30 lấy X ở offset traffic 130–159, H=5 lấy y ở 160–164. Các khoảng phải đúng giây theo origin.

Chạy demo để lấy ví dụ đầy đủ: python -m ml demo --out ml/artifacts/demo-contract.

### A.1. Đầu vào từ schedule Network, chỉ dùng offline

`python -m ml from-schedule` nhận CSV hoặc ZIP có đúng một CSV, gồm `schedule_id,scenario_id,seed,split,time_offset_s,flow_group_id,requested_mbps`. Nhãn seed/split phải khớp `seed_allocations` của manifest, mỗi scenario phủ đủ tập seed, mỗi schedule đủ `(warmup_seconds + measurement_seconds) × 48` dòng không trùng. Thời lượng phải khai báo rõ qua CLI, không suy từ manifest đang có hai cách đọc khác nhau.

Adapter chuyển thành `history.csv` / `runs.json` theo phần A, với:

- `run_id=offline_<schedule_id>`; `parent_family_id` lấy từ CSV nếu có, nếu không gộp bảo thủ theo seed. Điều này không thay chứng minh độc lập family của Network.
- UTC origin cố định `2000-01-01T00:00:00Z` chỉ phục vụ phép đổi offset offline, không giả làm thời gian thu thập thật.
- `provenance.kind=offline_network_schedule`, `online_eligible=false`, hash schedule/seed manifest và giả định thời gian. Provenance được giữ qua bundle, model metadata và evaluation.
- Không tạo `released_at`, `available_at`, routing state hoặc snapshot. Online vẫn bắt buộc nhận snapshot có bằng chứng từ producer.

`input/import_report.json` ghi số dòng/lịch và hash. Split, missing bins, NaN hoặc metadata sai làm import thất bại; không tự sửa dữ liệu nguồn.

## B. TrainingBundle nếu Data cung cấp X/y trực tiếp

~~~text
bundle.json
train.npz          train.csv
validation.npz     validation.csv
test.npz           test.csv
~~~

- bundle.json có format=training_bundle_history_v1, target=requested_mbps, delta_s=1, X_schema=oldest_to_newest_demand_only, max_lookback, max_horizon, stride, flow_group_ids, runs, counts, files, source_hashes; `data_provenance` ghi loại/nguồn dữ liệu nếu có.
- Files ánh xạ tên 6 file CSV/NPZ sang SHA-256 bytes file. Runs giữ mô tả phần A, thêm series_fingerprint: SHA-256 ma trận demand time×48, flow tăng theo hex, float64 little-endian, C-order. Không tự bịa fingerprint.
- Mỗi NPZ có X float64 N×Lmax, y_vector float64 N×Hmax, sample_ids Unicode N. Không dùng object/pickle.
- X là lịch sử demand cũ → mới, chưa có one-hot/stats. ML tạo feature từ L đã chọn; lag_1 là cột cuối lịch sử.
- CSV có sample_id,run_id,schedule_id,parent_family_id,scenario_id,seed,split,flow_group_id,cutoff,measurement_started_at_utc.
- Join một–một theo sample ID; CSV có thể khác thứ tự. Mỗi cutoff/run đủ 48 flow. Cutoff tính từ measurement origin.
- Chia trước window; mỗi X/y nằm trong một run/flow, không nối run. Common samples dùng Lmax/Hmax.

Data có thể gọi ml.preprocessing.bundle.prepare_bundle(...) để thống nhất serializer/validator. Feature schema khác cần adapter/version riêng; loader không đoán thứ tự.

## C. Snapshot online

Snapshot **SDN tạo, Data lưu và trả nguyên bản** dưới envelope `{"sdn": SDNSnapshotV14}` (công cụ `Predictor` hỗ trợ cả envelope `{"sdn": ...}` lẫn object phẳng bên trong; xem chi tiết trường v1.4 ở [ONLINE_INTEGRATION_CONTRACT.md](ONLINE_INTEGRATION_CONTRACT.md)):

| Trường | Yêu cầu |
| --- | --- |
| snapshot_id, run_id | Khớp context của tick |
| history_cutoff_utc, snapshot_as_of_utc | UTC; cutoff ≤ as_of ≤ thời điểm xử lý |
| config_hash, topology_hash | Khớp Runner |
| config_version, topology_version | Metadata của producer |
| flow_group_ids | Đủ 48 mã canonical, tăng theo giá trị hex |
| state_version | Số nguyên không âm |
| quality_status | VALID |
| routing_state_complete | true |
| missing_groups, unknown_flow_group_ids | Mảng rỗng |
| routing_state | 48 dòng flow_group_id,path_id,verification_status,state_version; VERIFIED và cùng version |
| history | Đúng 48×L* dòng |
| content_hash | Hash canonical snapshot, loại duy nhất trường content_hash |

Một history row:

~~~json
{
  "flow_group_id": "0x103",
  "window_start_utc": "2026-11-12T09:02:39Z",
  "window_end_utc": "2026-11-12T09:02:40Z",
  "requested_mbps": 4.0,
  "released_at_utc": "2026-11-12T09:02:40.010Z",
  "available_at_utc": "2026-11-12T09:02:40.030Z",
  "valid": true,
  "reason": null,
  "release_record_id": "network:run001:0x103:159"
}
~~~

History phủ liên tục [cutoff-L*,cutoff), bin dài 1 giây. End ≤ released ≤ available ≤ as_of. Không nhận future/missing/duplicate. Nếu row có run_id thì phải khớp snapshot.

Canonical hash: SHA-256 của JSON UTF-8 tạo bằng json.dumps với sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False, loại content_hash trước khi serialize. Dùng ml.common.digest để thống nhất. Số 4 và 4.0 có bytes khác nhau theo serializer này. Đây là quy ước adapter cần freeze, không tự thay hash của snapshot đã lưu theo schema khác.

## D. Context do Runner cung cấp riêng

~~~json
{
  "run_id": "run001",
  "snapshot_id": "snap_run001_100",
  "snapshot_content_hash": "<hash snapshot SDN do Data cung cấp>",
  "config_hash": "<hash cấu hình hệ thống>",
  "topology_hash": "<hash topology>",
  "ml_config_sha256": "<hash bytes ml_config.json>",
  "model_sha256": "<hash bytes model.joblib>",
  "measurement_started_at_utc": "2026-11-12T09:01:00Z",
  "measurement_end_utc": "2026-11-12T09:07:00Z",
  "max_snapshot_age_s": 2.0,
  "forecast_timeout_ms": 500,
  "fallback_policy": "skip",
  "fallback_epsilon": 0.0
}
~~~

Thay dấu <...> bằng hash thật. Hai hash model/config lấy từ model/metadata.json. Các ngưỡng minh họa cần được khóa sau pilot, chưa là tiêu chí đã nghiệm thu. Policy chỉ có skip/persistence. Persistence fallback toàn batch khi history hợp lệ và RF lỗi; artifact sai hash không được fallback.

## E. Payload trả Data

`Predictor.predict_batch()` trả list `48 × H*` object có đúng 7 trường `run_id,flow_group_id,issued_at,forecast_start,forecast_end,predicted_mbps,source`, với `forecast_start` và `forecast_end` là **số nguyên (`int` strict)**, `end = start + 1`, chung `issued_at`/`source`, đủ flow/step. Raw, epsilon, model/config ref, lý do lỗi và latency chỉ ở artifact/audit riêng.

Khi gửi tới Data API (`POST /v1/ml/forecasts`), client online (`ml/inference/online.py` hoặc Runner) bọc `rows` thành `ForecastBatch` (`run_id`, `forecast_batch_id`, `snapshot_id`, `rows`). Data tự kiểm READY và ghi DB. Optimization tự tính mean H từ các hàng final, không áp lại epsilon.
