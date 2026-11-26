# Hợp đồng Tích hợp Online v1.4 — Luồng `SDN → Data → ML → Data → Optimization`

**Ngày cập nhật:** 26/11/2026
**Phạm vi:** Thống nhất giao diện online giữa **ML**, **Data**, **SDN**, **Optimization**, **Network**, **Runner** và **Testing/QA** theo `Data_Integration_Contracts.md` (25/11), `data/ingestion/online_api.py` / `online_models.py` / `management_api.py`, và gói đề xuất G0 của ML (26/11).

---

## 1. Các quy tắc chung đã thống nhất

| Hạng mục | Quy ước chuẩn | Căn cứ triển khai |
| --- | --- | --- |
| **ID canonical (`A01`)** | 48 `flow_group_id` dạng hex (`0x103`…`0x806`), `path_id` dạng `<flow_group_id>_P1` / `_P2`, kiểm theo `topology_bundle.json`. Không quy đổi ngầm sang `h1_h3`. | `configs/network/flow_groups.csv`, `topology_bundle.json` |
| **Thời gian & Origin (`A02`)** | Chuẩn UTC RFC3339 (`Z` hoặc `+00:00`, tối đa 6 chữ số phần thập phân). `demand` lưu theo **traffic origin** (`traffic_started_at_utc`); `snapshot` (`cutoff_offset_s`, `bin_index`) và `forecast` (`forecast_start`, `forecast_end`) tính theo **measurement origin** (`measurement_started_at_utc = traffic_started_at_utc + warmup_s`). | `online_store.py`, `ml/inference/predict.py` |
| **Split & Phase (`A12`)** | Split **10/5/5** (`train` seeds 1–10, `validation` 11–15, `test` 16–20); phase **60s warmup / 360s measurement / 10s drain**. | `configs/experiments/runner_manifest.json` |
| **Owner `epsilon` & `mean H` (`A07`)** | ML áp `max(0, (1 + epsilon) * raw)` **đúng một lần**. Data lưu nguyên `48 × H` dòng. Optimization đọc từ Data và tự tính trung bình theo `H` bước, không áp lại `epsilon`. | `ml/inference/predict.py`, `online_store.py` |

---

## 2. API & Xác thực (`Data API v1`)

Mọi request tới Data API (trừ `/health/*`) bắt buộc có header `X-API-Key` tương ứng với vai trò:

| Bước | Endpoint | Method | Role Key (`env`) | Đầu vào / Đầu ra chính |
| --- | --- | --- | --- | --- |
| 0 | `/health/ready` | `GET` | Không cần | Kiểm tra DB đã áp đủ migration `1..4` (`200` / `503`). |
| 1 | `/v1/runs/{run_id}` | `GET` | `RUNNER`, `READER`, `SDN`, `ML`, `OPTIMIZATION` | Trả thông tin run đã đăng ký (`method_id`, `measurement_started_at_utc`, `measurement_s`, `warmup_s`, `lookback_bins`, `horizon_bins`, `config_hash`, `ml_config_hash`, `topology_id`). |
| 2 | `/v1/sdn/snapshots` | `POST` | `SDN` (`SDN_SDN_KEY`) | SDN gửi canonical `{"sdn": SDNSnapshotV14}` kèm `X-Content-SHA256` (SHA-256 của raw HTTP bytes). |
| 3 | `/v1/runs/{run_id}/snapshots/{snapshot_id}` | `GET` | `SDN`, `READER` (`SDN_READER_KEY`) | Data trả nguyên bytes đã lưu kèm header `X-Content-SHA256`. ML kiểm `sha256(raw_bytes) == X-Content-SHA256` trước khi parse JSON. |
| 4 | `/v1/ml/forecasts` | `POST` | `ML` (`SDN_ML_KEY`) | ML/Runner gửi `ForecastBatch` gồm `48 × H` dòng (`forecast_start`, `forecast_end` là **integer strict**). |
| 5 | `/v1/runs/{run_id}/forecasts/{forecast_batch_id}` | `GET` | `OPTIMIZATION` (`SDN_OPTIMIZATION_KEY`) | Optimization đọc batch đã READY (`404` nếu chưa sẵn sàng/chưa đủ). |
| 6 | `/v1/runs/{run_id}/events/ml` | `POST` | `ML` (`SDN_ML_KEY`) | ML gửi sự kiện audit (`ML_FORECAST_PUBLISHED`, `ML_FORECAST_FALLBACK`, `ML_FORECAST_SKIPPED`) theo `event_id` idempotent. |

### Quy tắc Retry & Mã lỗi HTTP
- **`200` (`{"inserted": N}` hoặc `{"inserted": 0}`):** `N > 0` là ghi mới; `0` là replay idempotent cùng ID và cùng nội dung.
- **`409 Conflict`:** Trùng ID (`forecast_batch_id` hoặc `event_id`) nhưng khác nội dung, hoặc snapshot không `VALID` / chưa sẵn sàng. **Tuyệt đối không retry.**
- **`422 Unprocessable Entity`:** Sai schema/kiểu dữ liệu (ví dụ gửi `100.0` thay vì `100`, thiếu dòng, sai horizon). **Tuyệt đối không retry.**
- **`500/502/503/504` hoặc lỗi mạng:** Có thể retry hữu hạn với **nguyên cùng `forecast_batch_id` và payload JSON**.

---

## 3. Hợp đồng Snapshot v1.4 (`SDN → Data → ML`)

### 3.1. Envelope & Hai loại Hash
Data API gửi/trả JSON dạng:

```json
{
  "sdn": {
    "snapshot_id": "snap:run001:t100:v1",
    "run_id": "run001",
    "snapshot_as_of_utc": "2026-11-12T09:02:40.200000Z",
    "history_cutoff_utc": "2026-11-12T09:02:40Z",
    "cutoff_offset_s": 100,
    "lookback_L": 30,
    "delta_s": 1,
    "state_version": 1,
    "config_version": "1.0",
    "config_hash": "...",
    "topology_version": "1.0",
    "topology_hash": "<64-hex>",
    "content_hash": "<64-hex>",
    "origin_refs": {
      "run_started_at_utc": "2026-11-12T09:00:00Z",
      "traffic_started_at_utc": "2026-11-12T09:00:00Z",
      "measurement_started_at_utc": "2026-11-12T09:01:00Z",
      "warmup_s": 60,
      "delta_s": 1
    },
    "flow_group_ids": ["0x103", "... (48 flows)"],
    "history": ["... (48 * L rows)"],
    "routing_state": ["... (48 verified routes)"],
    "routing_history": ["..."],
    "link_sample_refs": [],
    "quality_status": "VALID",
    "missing_groups": [],
    "routing_state_complete": true,
    "unknown_flow_group_ids": [],
    "provenance": { "..." : "..." },
    "reason": null,
    "expected_bin_count": 1440,
    "valid_bin_count": 1440
  }
}
```

1. **`X-Content-SHA256` (HTTP header):** `SHA-256` trên **nguyên raw HTTP bytes** của body `{"sdn": ...}`.
2. **`sdn.content_hash` (bên trong payload):** `SHA-256` của `json.dumps(sdn_without_content_hash, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")`.

### 3.2. Quy tắc nhân quả & Chất lượng
- **Causal history:** `origin <= cutoff <= snapshot_as_of <= now`. Với mỗi dòng thuộc `[cutoff - L, cutoff)`, yêu cầu `window_end - window_start == 1s`, `window_end <= released_at <= available_at <= snapshot_as_of`, và `bin_index == cutoff_offset_s - L + col` (measurement origin).
- **Thiếu dữ liệu (`INCOMPLETE` / `STALE` / `INVALID`):** Không được điền `0` giả vào `requested_mbps`; giữ `requested_mbps = null`, `released_at_utc = null`, `available_at_utc = null`, `release_record_id = null`, `valid = false`, `reason != null`.
- **Hành vi ML (`ml/inference/predict.py`):** Chấp nhận cả envelope `{"sdn": ...}` lẫn dict phẳng; chỉ dự báo khi `quality_status == "VALID"` và mọi kiểm tra hash/origin/routing/causal đạt. Mọi trạng thái khác đều skip sạch sẽ (không phát hành dự báo, ghi audit).

---

## 4. Hợp đồng Forecast (`ML → Data → Optimization`)

### 4.1. Envelope `ForecastBatch` gửi `POST /v1/ml/forecasts`

```json
{
  "run_id": "run001",
  "forecast_batch_id": "run001:snap:run001:t100:v1:rf",
  "snapshot_id": "snap:run001:t100:v1",
  "rows": [
    {
      "run_id": "run001",
      "flow_group_id": "0x103",
      "issued_at": "2026-11-12T09:02:40.250000Z",
      "forecast_start": 100,
      "forecast_end": 101,
      "predicted_mbps": 4.4,
      "source": "rf_v1"
    }
  ]
}
```

### 4.2. Ràng buộc kiểm tra phía Data (`online_models.py`, `online_store.py`)
- **`rows`:** Đúng `48 × H` phần tử (`H = run.horizon_bins`), mỗi phần tử có **đúng 7 trường**: `run_id`, `flow_group_id`, `issued_at`, `forecast_start`, `forecast_end`, `predicted_mbps`, `source`.
- **`forecast_start`, `forecast_end`:** Bắt buộc kiểu **`int` strict trong JSON** (ví dụ `100`, `101`; không được xuất `100.0`, `101.0`), thỏa `anchor <= forecast_start < anchor + H` và `forecast_end == forecast_start + 1`, trong đó `anchor = cutoff_offset_s` (measurement origin) và `anchor + H <= measurement_s`.
- **`issued_at`:** UTC RFC3339 tối đa 6 chữ số microsecond, giống nhau trên cả `48 × H` dòng, và phải thỏa `issued_at >= snapshot.available_at_utc`.
- **`predicted_mbps`:** Số thực hữu hạn `>= 0.0` (đã áp `epsilon` một lần ở ML).
- **`source`:** Giống nhau trên cả batch (`rf_v1`, `persistence`, `moving_avg`, hoặc `fallback_persistence`).
- **`method_id` ánh xạ từ Run:** `M1 → persistence`, `M2 → moving_average`, `M3 → rf`.

---

## 5. Vận hành luồng Online của ML (`ml/inference/online.py`)

### 5.1. Chạy một tick qua CLI
```powershell
$env:SDN_ML_KEY = "<ml-key>"
$env:SDN_READER_KEY = "<reader-key>"
python -m ml online `
  --deployment-config configs/ml/selected.yaml `
  --base-url http://127.0.0.1:8000 `
  --run-id run001 `
  --snapshot-id snap:run001:t100:v1 `
  --emit-event
```

### 5.2. Chạy persistent worker cho Runner (`--watch-stdin`)
Giữ model nạp một lần trong RAM, nhận mỗi dòng từ `stdin` là `snapshot_id` hoặc JSON `{"snapshot_id": "...", "forecast_batch_id": "..."}` và trả về 1 dòng JSON kết quả trên `stdout`:

```powershell
python -m ml online `
  --deployment-config configs/ml/selected.yaml `
  --base-url http://127.0.0.1:8000 `
  --run-id run001 `
  --watch-stdin `
  --emit-event
```

### 5.3. Gọi trực tiếp bằng Python (`Runner`)
```python
from ml.inference.online import DataApiClient, OnlineForecastRunner

client = DataApiClient("http://127.0.0.1:8000", ml_api_key="...", reader_api_key="...")
runner = OnlineForecastRunner("configs/ml/selected.yaml", client)
result = runner.process_snapshot("run001", "snap:run001:t100:v1", emit_event=True)
```

---

## 6. L/H khóa bằng offline và các tham số vận hành còn lại
- **L/H cho M1–M3:** Theo quyết định 26/11/2026, chọn bằng validation pooled vector MAE trên split 10/5/5; không áp điều kiện H tối thiểu từ vòng điều khiển vào lượt chọn offline. Kết quả xuất thành `configs/ml/selected.yaml`, gồm L/H/W/epsilon cùng đường dẫn và hash model/config; online nạp YAML và không tuning lại. File search (`training.yaml`) và file đã chọn (`selected.yaml`) có vai trò riêng.
- **Đăng ký run:** Runner/SDN dùng `lookback_L` và `horizon_H` từ YAML cho `lookback_bins`/`horizon_bins`; `ml_config_hash` là `ml_config_sha256` đã pin trong YAML. M1–M3 dùng cùng L/H; W của MA và epsilon được đóng băng cùng model. Sửa L/H mà không huấn luyện lại model tương ứng bị loader từ chối.
- **Thời hạn thực thi:** Forecast quá hạn vẫn bị bỏ theo cơ chế hiện có; online không tự tăng H hoặc lấy lịch tương lai để bù. Lợi ích điều khiển mạng được kiểm chứng riêng bằng runtime/benchmark.
- **Ngưỡng `context` online mặc định:** `max_snapshot_age_s`, `forecast_timeout_ms`, `fallback_policy` (`skip` hay `persistence`), và `fallback_epsilon`.
- **Kênh dữ liệu offline chuẩn (`I03`):** Chốt dùng trực tiếp `network/traffic_schedules/demand_plan.zip` qua `python -m ml from-schedule` hay cập nhật `data/export/export_ml_offline.py` xuất cặp `history.csv` + `runs.json`.

## 7. C?u h?nh b?n giao ?? kh?a

Theo quy?t ??nh ng??i ph? tr?ch ng?y 26/11/2026: **L=30, H=1, epsilon=0**. YAML d?ng chung l? `configs/ml/selected.yaml`; Optimization tham chi?u qua `ml_deployment_config` trong `configs/optimization/optimization.yaml`. Optimization ki?m H=1, nh?n c?c gi? tr? final v? t?nh mean H, kh?ng ?p l?i epsilon. Khi truy?n Data ForecastBatch d?ng rows, Runner b? sung `measurement_started_at_utc` t? run v? deadline th?c t?; adapter kh?ng t? b?a origin/deadline.

L??t t?m ki?m to?n l??i tr??c ?? b? ng?t; model b?n giao ???c hu?n luy?n l?i v?i c?p L/H ?? ch?n, 50.000 m?u train v? RF 150 c?y, depth 24, min_samples_leaf 4. Kh?ng coi ??y l? k?t qu? ho?n t?t 24 c?u h?nh.
