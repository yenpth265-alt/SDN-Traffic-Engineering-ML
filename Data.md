# Schema dữ liệu v1: Tối ưu điều phối lưu lượng SDN bằng dự báo học máy

Phiên bản: `schema_v1` (bản nháp để cả nhóm góp ý)

Phụ trách: mảng Data

---

## 1. Nguyên tắc chung

### 1.1. Năm loại dữ liệu cần ghi

| Loại | Câu hỏi nó trả lời | Bảng |
| --- | --- | --- |
| Danh bạ | Mạng có những gì? | `flow_groups`, `links` |
| Kế hoạch | Đáng lẽ phải gửi bao nhiêu? | `schedules`, `demand_plan` |
| Lần chạy | Đây là lần thi nào, phương pháp nào? | `runs` |
| Đo đạc | Thực tế mỗi giây đã xảy ra gì? | `traffic_actual`, `link_samples` |
| Quyết định | AI đoán gì, hệ thống đổi đường gì? | `predictions`, `decisions`, `events` |

### 1.2. Quy tắc thiết kế

1. Bảng đo đạc luôn có `run_id` và mốc thời gian.
2. Tên cột có đơn vị: `requested_mbps`, `capacity_mbps`, `sent_bytes`.
3. Thiếu dữ liệu thì để `NULL` và ghi lý do. Không điền 0. Số 0 nghĩa là "đo được và bằng 0", `NULL` nghĩa là "không biết".
4. Giữ dữ liệu thô (log gốc) tách khỏi dữ liệu đã tính (goodput, loss) để tính lại được khi sai.

### 1.3. Quy ước đơn vị và thời gian

- 1 Mbps = 1.000.000 bit/giây. Số byte và số gói luôn ghi riêng.
- Ba loại tốc độ **không được dùng thay nhau**:

| Tên | Nghĩa | Nguồn |
| --- | --- | --- |
| `requested_mbps` | Tốc độ được yêu cầu phát theo lịch | File lịch |
| sent (`sent_bytes`) | Dữ liệu thực gửi đi thành công | Log máy gửi (sender) |
| received (`received_bytes`) | Dữ liệu thực nhận được, không tính trùng | Log máy nhận (receiver) |

- Mỗi cửa sổ đo là `[window_start, window_end)`: gồm đầu trái, không gồm đầu phải. Ví dụ `[2,3)` là từ giây 2 đến trước giây 3.
- Mọi bản ghi đo đạc có `available_at`: thời điểm bản ghi thực sự sẵn sàng để dùng. Cột này dùng để đảm bảo AI không "nhìn trộm tương lai".
- Giờ ghi log theo UTC.

### 1.4. Định danh

| ID | Ý nghĩa |
| --- | --- |
| `flow_group_id` | Một cặp máy gửi → máy nhận **có hướng**. h1→h3 và h3→h1 là hai nhóm khác nhau |
| `schedule_id` | Một kịch bản tải (một file lịch) |
| `run_id` | Một lần chạy thật của một kịch bản bằng một phương pháp |
| `link_id` | Một đường nối **có hướng**. Mỗi chiều là một link riêng |

Cùng một `schedule_id` có thể có nhiều `run_id` (chạy bằng M1, M2, M3...). Các run này luôn nằm cùng một tập (train, validation hoặc test).

---

## 2. Sơ đồ quan hệ

```
schedules ──< demand_plan
    │
    └──< runs ──< traffic_actual
           │ ──< link_samples
           │ ──< predictions
           │ ──< decisions
           └──< events

flow_groups, links: bảng danh bạ, các bảng khác tham chiếu đến
```

`runs` là trung tâm. Muốn xem mọi thứ của một lần thi, lọc theo `run_id`.

---

## 3. Nhóm Danh bạ (tạo một lần từ cấu hình)

### 3.1. `flow_groups`: danh sách nhóm gửi–nhận (tối đa 48 dòng)

Khóa chính: `flow_group_id`

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `flow_group_id` | TEXT | Mã nhóm, ví dụ `h1_h3` |
| `src_host` | TEXT | Máy gửi, ví dụ `h1` |
| `dst_host` | TEXT | Máy nhận, ví dụ `h3` |

### 3.2. `links`: danh sách đường nối (khoảng 32 dòng)

Khóa chính: `link_id`

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `link_id` | TEXT | Mã đường nối, ví dụ `e1_c1` |
| `from_node` | TEXT | Nối từ thiết bị nào |
| `to_node` | TEXT | Nối đến thiết bị nào (chiều ngược lại là một link khác) |
| `capacity_mbps` | REAL | Sức chứa, hiện là 10 |

---

## 4. Nhóm Kế hoạch

### 4.1. `schedules`: mỗi dòng là một kịch bản

Khóa chính: `schedule_id`

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `schedule_id` | TEXT | Mã kịch bản |
| `scenario` | TEXT | Loại: ổn định, chu kỳ, hotspot, burst... |
| `seed` | INTEGER | Số ngẫu nhiên đã dùng để sinh lịch (để tái tạo) |
| `split` | TEXT | `train`, `validation` hoặc `test` |
| `duration_s` | INTEGER | Độ dài kịch bản, giây |
| `file_hash` | TEXT | "Dấu vân tay" nội dung file lịch, để phát hiện hai lịch trùng nhau |

### 4.2. `demand_plan`: nội dung chi tiết từng kịch bản

Khóa chính: (`schedule_id`, `flow_group_id`, `time_offset_s`)

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `schedule_id` | TEXT | Thuộc kịch bản nào |
| `flow_group_id` | TEXT | Nhóm nào |
| `time_offset_s` | INTEGER | Giây thứ mấy tính từ đầu kịch bản |
| `requested_mbps` | REAL | Yêu cầu gửi bao nhiêu Mbps |

Bảng này là nội dung file CSV do Network sinh ra, Data chỉ nạp vào.

---

## 5. Nhóm Lần chạy

### 5.1. `runs`: mỗi dòng là một lần thi

Khóa chính: `run_id`

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `run_id` | TEXT | Mã lần chạy |
| `schedule_id` | TEXT | Chạy kịch bản nào |
| `method` | TEXT | Phương pháp: `M0` đến `M4` |
| `started_at` | TIMESTAMP | Giờ bắt đầu (UTC) |
| `status` | TEXT | `ok`, `failed`, `discarded` |
| `note` | TEXT | Ghi chú, ví dụ lý do lỗi hoặc lý do loại run |

Không xóa run bị lỗi. Chỉ đánh dấu `status` và ghi `note`.

---

## 6. Nhóm Đo đạc (mỗi dòng thuộc một lần chạy và một mốc thời gian)

### 6.1. `traffic_actual`: thực tế gửi và nhận, theo từng giây, từng nhóm

Khóa chính: (`run_id`, `flow_group_id`, `window_start`)

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `run_id` | TEXT | Của lần chạy nào |
| `flow_group_id` | TEXT | Nhóm nào |
| `window_start` | REAL | Đầu cửa sổ đo (giây, tính từ lúc bắt đầu run) |
| `window_end` | REAL | Cuối cửa sổ đo |
| `sent_bytes` | INTEGER | Số byte gửi đi thành công (từ máy gửi) |
| `received_bytes` | INTEGER | Số byte nhận duy nhất (từ máy nhận) |
| `available_at` | TIMESTAMP | Lúc bản ghi này sẵn sàng để dùng |

Goodput (Mbps) = `received_bytes × 8 / 1.000.000 / (window_end − window_start)`.

### 6.2. `link_samples`: số đếm của từng đường nối

Khóa chính: (`run_id`, `link_id`, `sample_at`)

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `run_id` | TEXT | Của lần chạy nào |
| `link_id` | TEXT | Đường nối nào |
| `sample_at` | TIMESTAMP | Thời điểm lấy mẫu |
| `counter_bytes` | INTEGER | Số byte switch đã đếm (số gốc, cộng dồn) |
| `throughput_mbps` | REAL | Tốc độ tính từ hai lần đếm liên tiếp |
| `valid` | BOOLEAN | `false` nếu counter bị reset hoặc mẫu không đáng tin |

Công thức:

```
throughput_mbps = 8 × (counter_mới − counter_cũ) / ((giờ_mới − giờ_cũ) × 1.000.000)
utilization = throughput_mbps / capacity_mbps
```

Lưu giá trị gốc, không tự cắt tỷ lệ về tối đa 1.

---

## 7. Nhóm Quyết định

### 7.1. `predictions`: AI đoán gì

Khóa chính: (`run_id`, `flow_group_id`, `issued_at`)

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `run_id` | TEXT | Của lần chạy nào |
| `flow_group_id` | TEXT | Dự báo cho nhóm nào |
| `issued_at` | TIMESTAMP | Lúc đưa ra dự báo |
| `forecast_start` | REAL | Đầu khoảng được dự báo (giây) |
| `forecast_end` | REAL | Cuối khoảng được dự báo (giây) |
| `predicted_mbps` | REAL | Giá trị dự đoán (nhu cầu trung bình trong khoảng) |
| `source` | TEXT | Ai đoán: `RF`, `moving_avg`, `persistence`, `fallback`, `oracle` |

Dự báo ghi ở bảng riêng, không ghi đè lên số liệu nhu cầu.

### 7.2. `decisions`: hệ thống đổi đường gì

Khóa chính: (`run_id`, `flow_group_id`, `decided_at`)

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `run_id` | TEXT | Của lần chạy nào |
| `flow_group_id` | TEXT | Nhóm nào |
| `decided_at` | TIMESTAMP | Thời điểm quyết định |
| `old_path` | TEXT | Đường cũ: `c1` hoặc `c2` |
| `new_path` | TEXT | Đường mới: `c1` hoặc `c2` |
| `status` | TEXT | `proposed` (đề xuất), `applied` (đã áp dụng), `failed`, `kept` (giữ nguyên) |
| `reason` | TEXT | Lý do (đổi vì quá tải, giữ vì chưa đủ lợi...) |

Phân biệt rõ "đề xuất" với "đã áp dụng thật".

### 7.3. `events`: nhật ký chung

Khóa chính: `event_id`

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `event_id` | INTEGER | Số thứ tự tự tăng |
| `run_id` | TEXT | Của lần chạy nào |
| `event_at` | TIMESTAMP | Thời điểm xảy ra |
| `component` | TEXT | Ai ghi: `network`, `ai`, `optimizer`, `controller`... |
| `event_type` | TEXT | Loại sự kiện |
| `detail` | TEXT | Chi tiết (có thể là chuỗi JSON) |

Dữ liệu chưa biết xếp vào bảng nào thì ghi tạm vào bảng này.

---

## 8. Quy tắc kiểm tra chất lượng

| Tình huống | Cách xử lý |
| --- | --- |
| Thiếu dữ liệu | Để `NULL`, ghi lý do. Không điền 0 |
| Nhóm có lịch quy định nhu cầu 0 | Đây là 0 hợp lệ, khác với thiếu dữ liệu |
| Counter giảm (switch reset) | Đặt `valid = false`, bỏ khoảng đó, lấy mốc mới |
| Burst (tăng vọt) đúng theo lịch | Không xóa, vì không phải ngoại lai |
| Dự báo lớn hơn dung lượng link | Có thể hợp lệ, không coi là lỗi |
| Hai bản ghi mâu thuẫn | Không tùy tiện giữ một giá trị, phải điều tra |
| Run bị lỗi | Giữ lại, đánh dấu `status` và `note`. Không xóa run bất lợi |

Kiểm tra thêm: ID hợp lệ, đơn vị đúng, thời lượng dương, không trùng khóa, thứ tự thời gian đúng.

Với lỗi hạ tầng (mạng không dựng được, cấu hình sai, log hỏng) thì được loại run theo quy tắc khóa từ trước. Với hạn chế của phương pháp (RF chậm, solver quá hạn, phải dùng fallback) thì phải giữ trong kết quả.

---

## 9. Quy tắc chia tập train / validation / test

- Chia theo lịch (`schedule_id`), trước khi cắt thành mẫu nhỏ. Không xáo trộn từng dòng.
- Seed của ba tập không giao nhau.
- Mọi lần chạy lại của cùng một lịch nằm cùng một tập.
- Tỷ lệ gợi ý 70/15/15, đủ các họ kịch bản.
- Kiểm tra `file_hash` để không có lịch trùng giữa các tập.

---

# Lấy dữ liệu cho từng bảng ở đâu

## Bảng tổng hợp

| Bảng | Nguồn chính | Ai cung cấp | Khi nào có |
| --- | --- | --- | --- |
| `flow_groups` | File cấu hình mạng | Network | Một lần, trước khi chạy |
| `links` | File cấu hình mạng | Network | Một lần, trước khi chạy |
| `schedules` | Manifest của file lịch, cộng phần bạn tự thêm | Network + bạn | Khi sinh lịch |
| `demand_plan` | File lịch CSV | Network | Khi sinh lịch |
| `runs` | Do bạn tạo khi bắt đầu mỗi lần chạy | Bạn | Mỗi lần chạy |
| `traffic_actual` | Log máy gửi và log máy nhận | Network | Trong và sau mỗi lần chạy |
| `link_samples` | Số đếm Port Stats của switch | SDN (Controller) | Trong mỗi lần chạy, theo chu kỳ |
| `predictions` | Kết quả hàm dự báo | AI | Trong mỗi lần chạy |
| `decisions` | Kết quả bộ tối ưu và kết quả thực thi | Optimization + SDN | Trong mỗi lần chạy |
| `events` | Mọi thành phần ghi vào | Tất cả | Trong mỗi lần chạy |

## Chi tiết từng bảng

### 1. `flow_groups` và `links` (danh bạ)

- **Lấy từ:** `config.yaml` và thông tin topology mà Network dùng để dựng mạng (file dự kiến là `leafspine.py`, `config.yaml`).
- **Cách làm:**
  - `flow_groups`: sinh từ danh sách 8 host. Mỗi cặp máy gửi → máy nhận thuộc hai switch biên khác nhau là một nhóm, tối đa 8 × 6 = 48 dòng.
  - `links`: 16 link fabric (biên–trung gian, mỗi chiều một dòng) cộng 16 link host. `capacity_mbps` lấy từ cấu hình (hiện là 10).
- **Lưu ý:** hỏi Network tên thiết bị và tên link chính xác để các bảng khác ghép được.

### 2. `schedules` và `demand_plan` (kế hoạch)

- **Lấy từ:** file lịch CSV do Network sinh ra (`generate_schedule.py`). Mỗi dòng trong file có dạng `schedule_id, time_offset_s, flow_group_id, requested_mbps`.
- **Cách làm:**
  - `demand_plan`: nạp thẳng nội dung file CSV.
  - `schedules`: lấy `scenario`, `seed`, `duration_s` từ manifest đi kèm file lịch. Hai cột sau **do bạn tự tạo**: `file_hash` (tính từ nội dung file) và `split` (kết quả chia train/validation/test của bạn).

### 3. `runs` (lần chạy)

- **Lấy từ:** chính bạn tạo. Không bên nào gửi bảng này cho bạn.
- **Cách làm:** mỗi khi bắt đầu một lượt chạy thì thêm một dòng, ghi `schedule_id` và phương pháp đang chạy. Khi chạy xong thì cập nhật `status`.
- **Cần phối hợp:** thống nhất với Network cách nhận `run_id`, vì tên thư mục log nên chứa `run_id` để dễ ghép.

### 4. `traffic_actual` (thực tế gửi và nhận)

- **Lấy từ:** hai nguồn log do Network cung cấp (`udp_sender.py` và `udp_receiver.py`).
  - `sent_bytes`: từ log máy gửi.
  - `received_bytes`: từ log máy nhận (dữ liệu nhận duy nhất, không tính gói trùng).
- **Cách làm:** hai bên ghi theo từng gói, mỗi gói có số thứ tự (sequence) và thời điểm. Bạn gom các gói vào từng cửa sổ 1 giây theo nhóm, rồi cộng số byte.
- **Hỏi Network:** log gói thô có số thứ tự gói và thời điểm gửi/nhận không, định dạng ra sao.

### 5. `link_samples` (số đếm đường nối)

- **Lấy từ:** Controller (phần SDN) định kỳ gửi yêu cầu Port Stats cho switch và lưu số byte đã đếm.
- **Cách làm:** SDN lưu số gốc (`counter_bytes`) và thời điểm lấy mẫu. Bạn tính `throughput_mbps` từ hai lần đếm liên tiếp theo công thức trong file schema.
- **Hỏi SDN:**
  - Tên cổng và tên link của họ có khớp `link_id` của bạn không (họ có bảng ánh xạ switch–cổng).
  - Bao lâu lấy một lần.
  - Counter đếm ở điểm nào (trước hay sau chỗ giới hạn tốc độ), vì ảnh hưởng đến cách giải thích số liệu.

### 6. `predictions` (dự báo của AI)

- **Lấy từ:** hàm `predict_batch` của AI trả về. Mỗi lần dự báo gồm giá trị đoán cho từng nhóm, khoảng được dự báo và nguồn (RF hay phương án dự phòng).
- **Cách làm:** thống nhất với AI rằng kết quả trả về được ghi thẳng vào bảng này, hoặc AI ghi ra file để bạn nạp.
- **Lưu ý:** phương pháp M1, M2 cũng có "dự báo" (giá trị gần nhất, trung bình trượt). Cột `source` dùng để phân biệt.

### 7. `decisions` (quyết định đổi đường)

- **Lấy từ:** hai nơi nối lại với nhau.
  - Bộ tối ưu (Optimization) cho biết đề xuất đổi nhóm nào, từ đường nào sang đường nào, lý do.
  - Controller (SDN) cho biết đã áp dụng thật chưa, thành công hay thất bại.
- **Cách làm:** ghi trạng thái từ `proposed` đến `applied` hoặc `failed`. Cả trường hợp **giữ nguyên đường** cũng ghi (`kept`) kèm lý do, vì đề cương yêu cầu ghi lại mọi lần không đổi.

### 8. `events` (nhật ký chung)

- **Lấy từ:** mọi thành phần tự ghi (Network, AI, Optimization, Controller), hoặc từ log chương trình của họ.
- **Cách làm:** thống nhất một định dạng dòng log chung (thời điểm, `run_id`, ai ghi, loại sự kiện, chi tiết). Nên dùng file JSONL, mỗi dòng một sự kiện, để bạn nạp dễ.

---

# Thiết kế data pipeline

Chia pipeline thành **2 luồng**:

- **Luồng 1, ngoại tuyến:** chạy trước, từ file lịch ra bảng học cho AI. Không cần chạy mạng.
- **Luồng 2, theo từng lần chạy:** thu log thô, làm sạch, tính chỉ số và bàn giao cho Evaluation. Trong lúc chạy còn có một nhánh **snapshot mỗi giây** cho Optimizer.

Công cụ: Python, pandas, DuckDB (hoặc SQLite), lưu file thô dạng JSONL/CSV và file kết quả dạng Parquet/CSV.

---

## 1. Sơ đồ tổng thể

```
LUỒNG 1: NGOẠI TUYẾN (chạy 1 lần cho mỗi bộ lịch)
─────────────────────────────────────────────────
 Network sinh lịch CSV + manifest
        │
        ▼
 [1.1 Nạp lịch] ──► schedules, demand_plan
        │
        ▼
 [1.2 Kiểm tra lịch] (hash trùng? đơn vị đúng?)
        │
        ▼
 [1.3 Chia tập] ──► split_manifest (train/val/test)
        │
        ▼
 [1.4 Tạo mẫu X, y] ──► X.parquet, y.parquet, sample_metadata.parquet ──► giao AI


LUỒNG 2: THEO TỪNG LẦN CHẠY
───────────────────────────
                   ┌─ Network: log sender / receiver ─┐
 Nguồn thô ────────┼─ SDN: counter switch             ├──► [2.1 Thu thập] ──► data/raw/<run_id>/
                   ├─ AI: dự báo                      │
                   └─ Optimizer/Controller: quyết định┘
                                                          │
                                                          ▼
                                                   [2.2 Làm sạch + kiểm tra]
                                                          │
                                                          ▼
                                                   [2.3 Nạp vào DB]
                                                          │
                                                          ▼
                                                   [2.4 Tính chỉ số]
                                                          │
                                                          ▼
                                                   [2.5 Xuất bàn giao] ──► Evaluation


NHÁNH ONLINE (trong lúc chạy, mỗi 1 giây)
──────────────────────────────────────────
 Bộ đệm trong RAM ──► [Snapshot Provider] ──► Optimizer / AI
```

---

## 2. Bốn tầng lưu trữ

| Tầng | Nội dung | Quy tắc |
| --- | --- | --- |
| **raw** | Log gốc nguyên bản từ các bên | **Chỉ ghi thêm, không sửa, không xóa.** Sai thì còn tính lại được |
| **clean** | Dữ liệu đã chuẩn hóa tên cột, đơn vị, kiểu thời gian | Tạo lại được từ raw bất cứ lúc nào |
| **db** | Các bảng trong `schema_v1` (DuckDB/SQLite) | Là nơi truy vấn chính |
| **export** | File bàn giao: X/y, metrics, bảng so sánh | Mỗi file có phiên bản và hash |

Cấu trúc thư mục gợi ý:

```
project/
├── config.yaml                  # cấu hình chung (cả nhóm dùng)
├── data/
│   ├── schedules/               # file lịch CSV + manifest từ Network
│   ├── raw/<run_id>/            # log thô của từng lần chạy
│   │   ├── sender.jsonl
│   │   ├── receiver.jsonl
│   │   ├── link_counters.jsonl
│   │   ├── predictions.jsonl
│   │   ├── decisions.jsonl
│   │   └── events.jsonl
│   ├── clean/<run_id>/
│   ├── db/experiment.duckdb
│   └── export/
│       ├── train_val_test/      # X, y, sample_metadata
│       └── metrics/<run_id>.csv 
├── pipeline/
│   ├── ingest_schedule.py
│   ├── split_data.py
│   ├── build_samples.py
│   ├── ingest_run.py
│   ├── clean_validate.py
│   ├── compute_metrics.py
│   ├── export.py
│   └── snapshot_provider.py
└── tests/
```

---

## 3. Mô tả từng file

### File cấu hình và dữ liệu

| File | Ai tạo | Nội dung và mục đích |
| --- | --- | --- |
| `config.yaml` | Cả nhóm thống nhất | Cấu hình chung, ví dụ bước lấy mẫu, tham số cửa sổ và quy ước đo. |
| CSV trong `data/schedules/` | Network | Lịch phát theo nhóm và thời gian, gồm `schedule_id`, `flow_group_id`, `time_offset_s` và `requested_mbps`. Nạp vào `demand_plan`. |
| Manifest trong `data/schedules/` | Network | Mô tả lịch, gồm mã lịch, kịch bản, seed và thời lượng. Nạp vào `schedules`, sau đó Data bổ sung hash và split. |
| Manifest lượt chạy | Runner | Ghi `run_id`, lịch được phát, phương pháp, cấu hình và các mốc đo. Data dùng để nạp `runs` và đối chiếu log cùng lượt. |
| `sender.jsonl` | Network | Log gửi UDP, gồm ID lượt, nhóm, số thứ tự gói, thời điểm gửi, số byte payload và kết quả gửi. |
| `receiver.jsonl` | Network | Log nhận UDP, gồm ID lượt, nhóm, số thứ tự gói, thời điểm nhận và số byte payload. Ghép với log gửi để tính loss. |
| `link_counters.jsonl` | SDN | Counter byte gốc và thời điểm lấy mẫu theo chiều link. Nếu log ghi switch/cổng, Data tra ánh xạ để lấy `link_id`. |
| `predictions.jsonl` | AI hoặc bộ cung cấp nhu cầu | Giá trị dự báo theo nhóm, thời điểm phát hành, khoảng dự báo và nguồn dự báo. |
| `decisions.jsonl` | Optimization và Controller | Phương án giữ hoặc đổi đường, lý do, kết quả thực thi và xác minh. |
| `events.jsonl` | Các module | Sự kiện như lỗi dữ liệu, fallback, quá hạn hoặc lỗi thực thi. |
| Các file trong `data/clean/<run_id>/` | Data | Bản log đã chuẩn hóa tên cột, kiểu, đơn vị và thời gian. Có thể tạo lại từ log gốc. |
| `data/db/experiment.duckdb` | Data | Chứa các bảng schema v1 để truy vấn theo lịch hoặc lượt chạy. |
| `split_manifest` | Data | Ghi lịch nào thuộc train, validation hoặc test để dùng lại khi tạo mẫu học. |
| `X.parquet` | Data | Đặc trưng tính từ lịch sử nhu cầu của từng mẫu. |
| `y.parquet` | Data | Nhu cầu trung bình thật trong khoảng dự báo. Ghép với X theo định danh mẫu. |
| `sample_metadata.parquet` | Data | Mã lịch, nhóm, mốc dự báo, khoảng dự báo và split để truy vết mẫu. |
| `data/export/metrics/<run_id>.csv` | Data | Chỉ số của từng lượt để Evaluation so sánh các phương pháp trên cùng lịch. |

### File xử lý của Data

| File | Đọc gì? | Làm gì và tạo ra gì? |
| --- | --- | --- |
| `ingest_schedule.py` | CSV lịch và manifest | Kiểm tra cột, nhóm và giá trị nhu cầu, tính hash rồi nạp `schedules` và `demand_plan`. |
| `split_data.py` | Danh mục lịch và thông tin họ lịch | Chia train, validation và test theo lịch trước khi tạo mẫu. Ghi `split` và xuất `split_manifest`. |
| `build_samples.py` | Lịch đã chia tập và cấu hình cửa sổ | Tạo X từ lịch sử nhu cầu và y từ khoảng cần dự báo, rồi xuất cùng metadata. |
| `ingest_run.py` | Manifest và log của lượt | Xác định đúng bộ file, đọc log và điều phối việc kiểm tra, chuẩn hóa, nạp bảng theo `run_id`. |
| `clean_validate.py` | Dữ liệu thô của lượt | Chuẩn hóa cột và thời gian, kiểm tra khóa, đánh dấu dữ liệu thiếu, bản ghi mâu thuẫn hoặc counter reset. |
| `compute_metrics.py` | Log đã chuẩn hóa, lịch và kết quả thực thi | Tính goodput, loss theo gói và deadline, tốc độ link, MLU và số lần đổi đường đã xác minh. |
| `export.py` | Dữ liệu học và chỉ số đã xử lý | Xuất file bàn giao cho AI và Evaluation, kèm thông tin phiên bản để truy vết. |
| `snapshot_provider.py` | Bộ đệm RAM và trạng thái định tuyến | Chốt dữ liệu đã sẵn dùng theo từng chu kỳ. Cấp lịch sử nhu cầu cho AI, cấp trạng thái mạng cho Optimizer. Khi dùng AI, Optimizer nhận thêm kết quả dự báo để tính phương án. |

Thư mục `tests/` chứa các bài kiểm tra như phát hiện lịch trùng giữa hai tập, kiểm tra snapshot không dùng dữ liệu đến muộn, xử lý counter reset và nạp lại cùng một lượt mà không tạo dòng trùng.
