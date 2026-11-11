# SDN Traffic Engineering — Đặc tả triển khai thống nhất cho 6 role (Mục 1–13)

**Phiên bản:** 1.4-network-id-trimmed  
**Ngày:** 12/11/2026  
**Trạng thái:** Tài liệu triển khai chung (bản hiệu chỉnh theo yêu cầu nhóm); các thông số pilot/giao thức vận chuyển còn cần xác nhận trước khi chạy benchmark  
**Mục đích:** Một tài liệu làm việc chung để sáu role triển khai đúng Input/Output, ID, đơn vị, timestamp và interface; tránh việc mỗi role tự định nghĩa dữ liệu riêng.

> Đây là đặc tả tích hợp đề xuất, chưa phải xác nhận code đã chạy end-to-end. Bản này tổng hợp từ thiết kế Network, Data, `interfaces_v2.md`, báo cáo audit conflict **và đặc tả ML do thành viên gửi ngày 11/11/2026**. Thiết kế chi tiết Optimization, SDN và Testing vẫn cần owner kiểm tra trước khi contract freeze. **Hiệu đính v1.2 theo xác nhận của nhóm:** (i) **SDN tạo snapshot, Data chỉ nhận/lưu và cấp lại**, (ii) **ML tự chọn L/H/epsilon offline, lưu cấu hình riêng và khi online chỉ gửi 7 trường dự báo cho Data**, (iii) **Optimization tự tính mean H trên dự báo do Data trả lại**.

> **Quyết định từ nhóm:** `flow_group_id` **theo Network**, mã hex `src_host_number × 256 + dst_host_number`, ví dụ `h1→h3 = 0x103`, `h1→h5 = 0x105`. Mã số này đồng thời được Network dùng làm OpenFlow cookie (hai field có chức năng khác nhau). `path_id` dùng quy ước theo Data: `<flow_group_id>_P1` / `<flow_group_id>_P2`; `link_id` lấy nguyên catalog Network, ví dụ `e1>c1`. Các tài liệu/giao diện còn dùng ID kiểu `h1_h3` phải sửa theo mã Network, không chấp nhận hai namespace cùng lúc.

---

### Các quyết định triển khai đã xác nhận / hiệu chỉnh ở v1.4

- **SDN tạo snapshot → Data chỉ lưu và cung cấp lại**; không còn `Data Snapshot Builder` trong kiến trúc.
- **ML tự chốt L*/H*/epsilon*** sau validation và lưu `ml_config.yaml`/model metadata; **ML → Data chỉ gửi 7 trường `predictions`**, không kèm các tham số cấu hình.
- **Data đọc H* từ frozen config/RunManifest để kiểm đủ 48×H* rows**, không tính mean; **Optimization tự tính mean**.
- **Flow ID theo Network:** `flow_group_id` = mã hex `src × 256 + dst` (ví dụ `0x103`); dùng thống nhất trong Data, ML, Optimization, SDN và Testing. Network xuất danh mục ánh xạ chính thức.
- **Còn cần thống nhất:** đường cấp `requested_mbps` lịch sử hợp lệ cho SDN tạo snapshot; không được suy từ goodput/port counters.

---

## Đọc trước trong 10 phút: dự án này vận hành như thế nào?

> **Vị trí của tài liệu:** Đây là một **Integration Handbook** để nhóm hiện thực và nghiệm thu, không phải chứng nhận phần mềm đã chạy. Các cấu trúc dưới đây chỉ trở thành chuẩn bắt buộc sau khi owner ký duyệt hợp đồng. Các giá trị pilot còn cần xác nhận được gắn nhãn [PROPOSED] hoặc [MUST-LOCK].
>
> **Phân loại phát biểu:** **[SOURCE]** = kế thừa từ tài liệu hiện có; **[PROPOSED]** = cấu trúc/giá trị đề xuất cho bản pilot; **[MUST-LOCK]** = còn cần quyết định được ghi nhận; **[TEST]** = điều kiện cần chứng minh bằng bài test; **[IMPLEMENTED]** chỉ được sử dụng sau khi có commit/test logs xác nhận.

**Mục tiêu trong một câu:** Dự báo *nhu cầu payload* giữa các cặp host, dùng MILP phân bổ các flow sang một trong hai tuyến SDN để giảm tải link fabric cao nhất và tránh đổi đường quá nhiều; sau đó đo hiệu quả thực tế trên Mininet/OVS/Ryu.

**Đọc theo role:**

| Thành viên | Cần đọc bắt buộc | Bắt đầu triển khai tại | Bàn giao trước tiên |
|---|---|---|---|
| Network | Mục 2–4, 5.1, 13 | Topology + bảng mapping + S0 | `TopologyBundle`, 48 flows, 96 paths, lịch và telemetry |
| Data | Mục 3–4, 5.2, 13 | Schema + ingestion + **lưu snapshot SDN** | `TrainingBundle`, snapshot đã lưu, prediction registry |
| ML | Mục 3–4, 5.3, 13 | Persistence/MA/RF, chọn L/H/epsilon trên validation | **Chỉ 7 trường `predictions` cho Data**; model/config lưu riêng |
| Optimization | Mục 3–4, 5.4, 13 | Toy fixture MILP + validator | `OptimizationResult` + `ApplyRequest` adapter |
| SDN | Mục 3–4, 5.5, 13 | Static P1↔P2 + verified state | `ApplyResponse`, event trace và rule evidence |
| Testing | Toàn bộ, đặc biệt 5.6, 10, 13 | Golden fixtures + contract tests | Gates, manifest, benchmark M0–M4 |

**3 điều không được hiểu sai:**

1. `requested_mbps` là yêu cầu phát lưu lượng, **không phải** tốc độ nhận thành công. ML học demand, Testing đo delivery và utilization.
2. ML sinh **H giá trị đã hiệu chỉnh mỗi flow**, gửi sang Data thành **H hàng dự báo**, không kèm L/H/epsilon/raw/mean; Optimization tự tính mean từ các hàng đó.
3. `APPLIED` nghĩa là **đường mới đã được xác minh**, không phải đã gửi FlowMod hoặc Barrier. Nếu chưa xác định được đường thực tế thì phải giữ `UNKNOWN/INCONSISTENT`.

**Định nghĩa hoàn thành:** Một lần chạy thực nghiệm đi tự động từ topology → traffic → dữ liệu → dự báo → MILP → cài/verify route → log → metric, không sửa file tay giữa các role, có đủ `run_id`, version, hash, evidence và test tái hiện.

---

# 1. Cách sử dụng và nguyên tắc quản lý

1.  Cả nhóm đọc mục 1–5 trước khi code.
2.  Mỗi thành viên đọc phần role mình phụ trách và interface với role liền kề.
3.  Producer phải bàn giao đúng Output trước khi Consumer tích hợp.
4.  Không tự đổi tên field, ID, đơn vị, timestamp hoặc status.
5.  Nếu cần đổi contract, phải cập nhật `contracts/`, tài liệu, config và contract tests trước khi sửa code.
6.  Nếu code khác contract thì sửa code; không để các role tự viết adapter riêng theo cách không thống nhất.

**Thứ tự ưu tiên khi tài liệu cũ mâu thuẫn:**

1.  Contract chung đã được nhóm duyệt.
2.  Schema trong `src/sdn_te/contracts/`.
3.  Cấu hình chung trong `configs/`.
4.  Tài liệu chi tiết từng role.
5.  Code triển khai.

# 2. Kiến trúc tổng thể

## 2.1. Mục tiêu

Hệ thống thực hiện chu trình:

1.  Network dựng topology, phát traffic và đo trạng thái mạng.
2.  Network phát lưu lượng/demand quá khứ; **SDN tạo snapshot** từ các nguồn lịch sử hợp lệ và routing state, sau đó gửi **Data chỉ để lưu/cấp lại**. Nguồn cung demand lịch sử cho SDN còn phải chốt ở interface.
3.  ML dự báo demand payload cho H bước tiếp theo.
4.  Data kiểm tra và lưu **các hàng dự báo** theo schema 7 trường đã có; Data tự gắn batch nội bộ nếu cần.
5.  Optimization đọc H hàng dự báo cho mỗi flow từ Data, **tự tính trung bình** rồi tính tải, trigger và giải MILP.
6.  SDN Controller kiểm tra, cài rule, xác minh route thực tế.
7.  Data lưu kết quả thực thi và routing state đã xác minh.
8.  Testing đánh giá M0–M4 bằng dữ liệu đo.

Kiến trúc là **ML forecasting + MILP optimization + SDN execution**, không phải Reinforcement Learning.

## 2.2. Luồng tích hợp

```text
NETWORK: topology / traffic generator / released demand / telemetry
                |                    |
                v                    v
      DATA: lưu sự kiện       SDN: tạo snapshot hợp lệ
      demand/telemetry         (lịch sử + verified route)
                |                    |
                |                    v
                |         DATA: nhận và lưu snapshot của SDN
                |                    |
                |                    v
                |         ML: đọc snapshot đã lưu
                |         offline chọn L/H/epsilon; online forecast
                |                    |
                |                    v
                |         DATA: nhận 7 trường `predictions`, lưu H rows/flow
                |                    |
                |                    v
                |         OPTIMIZATION: đọc rows, tự tính mean H + MILP
                |                    |
                |                    v
                +---------- SDN: apply -> verify actual path
                                     |
                                     v
                        DATA: lưu events / confirmed state
                                     |
                                     v
                        TESTING: contract / E2E / benchmark
```

**Quyền sở hữu:**

- **SDN sở hữu việc tạo snapshot và trạng thái định tuyến thực đã xác minh.**
- **Data sở hữu database, tiếp nhận, lưu và truy vấn snapshot do SDN gửi; không tự tạo hoặc sửa nội dung snapshot.** Data có thể từ chối bản sai schema/cutoff khi ingest.
- Network sở hữu topology, demand release và telemetry; **cách chuyển lịch sử requested_mbps hợp lệ tới SDN để tạo snapshot là interface còn OPEN**. SDN không được tự suy requested demand từ goodput/port counter.
- ML sở hữu việc huấn luyện, chọn L/H/epsilon, áp dụng epsilon, xuất **chỉ dự báo cuối cùng**; config/model metadata lưu riêng, được đóng băng và tham chiếu trong RunManifest.
- Optimization sở hữu mean-H adapter, MILP, trigger và proposal; Data không tính mean thay Optimization.
- Testing giám sát hợp đồng, kiểm tra snapshot/forecast và không sửa dữ liệu gốc.

## 2.3. Trách nhiệm sáu role

| Role         | Chịu trách nhiệm                                         | Không làm                                       |
|:-------------|:---------------------------------------------------------|:------------------------------------------------|
| Network      | Topology, traffic, demand release, counters              | Không tự quyết định route tối ưu                |
| Data         | Schema, ingestion, **lưu/truy xuất snapshot SDN**, validation, API | Không tạo snapshot, không tính mean hoặc forecast |
| ML           | Baseline, RF, feature engineering, chọn L/H/epsilon, forecast | Không gửi L/H/epsilon trong dự báo, không đổi route |
| Optimization | Trigger, MILP, route proposal, solution validation       | Không gửi FlowMod trực tiếp                     |
| SDN          | **Tạo snapshot**, Ryu, FlowMod, verify route, state version | Không chạy ML/MILP trong Ryu event loop |
| Testing      | Unit/contract/integration/E2E, benchmark, metrics        | Không tự sửa logic role khác                    |

---

# 3. Contract chung — mọi role phải tuân thủ

## 3.1. Topology chuẩn

- 8 host: `h1`–`h8`.
- 4 edge switch: `e1`–`e4`.
- 2 core switch: `c1`, `c2`.
- 1 Ryu Controller, OpenFlow 1.3.

| Edge | Host   |
|:-----|:-------|
| e1   | h1, h2 |
| e2   | h3, h4 |
| e3   | h5, h6 |
| e4   | h7, h8 |

DPID: e1=1, e2=2, e3=3, e4=4, c1=5, c2=6.

Port mapping:

- Edge switch: port 1–2 nối host; port 3 nối c1; port 4 nối c2.
- Core switch: port 1–4 lần lượt nối e1–e4.
- Phải xác minh bằng `ovs-ofctl -O OpenFlow13 show <switch>`, không chỉ tin vào thứ tự khai báo Mininet.

Link:

- 8 host-edge links hai chiều, mỗi hướng 10 Mbps, delay 1 ms.
- 8 edge-core links hai chiều, mỗi hướng 10 Mbps, delay 5 ms.
- Tổng 16 physical bidirectional links = 32 directed links.
- Trong đó có 16 directed fabric links: 8 edge→core và 8 core→edge. Đây là tập link được MILP tối ưu.
- 16 directed host-edge links là đoạn cố định theo host; phải theo dõi và giới hạn tải nhưng không phải lựa chọn P1/P2.
- Queue: 50 packets.
- Mỗi đường end-to-end có 4 chặng, độ trễ danh nghĩa một chiều 12 ms, RTT danh nghĩa 24 ms trước queueing/processing delay.

## 3.2. Flow groups và path IDs

Có 48 directed flow groups giữa host thuộc hai edge khác nhau: mỗi host có 6 đích ở edge khác, tổng 8 × 6 = 48. Cặp cùng edge đi trực tiếp, không thuộc 48 flow tối ưu. Mỗi flow có hai path; 96 path × 2 fabric directed links/path = 192 path–fabric-link incidences; đây là một invariant bổ sung cho kiểm thử topology.

Mỗi flow có 2 candidate paths:

- P1 qua c1.
- P2 qua c2.

Ví dụ h1→h3:

| Field | Giá trị |
|:--|:--|
| `flow_group_id` | `0x103` (Network: `1×256+3`) |
| `flow_cookie` | `0x103` (cùng trị số, dùng trong OpenFlow) |
| `path_id` P1 | `0x103_P1` |
| `path_id` P2 | `0x103_P2` |
| P1 | h1-e1-c1-e2-h3 |
| P2 | h1-e1-c2-e2-h3 |

**Quy ước định danh bắt buộc theo Network:**

- **Flow:** `flow_group_id = 0x{src×256+dst:X}`, ví dụ `h1→h3 = 0x103`, `h1→h5 = 0x105`. Số host là phần số trong `h1`–`h8`; ID là chuỗi hex chuẩn hóa nhất quán (không dùng `h1_h3` trong interface).
- **OpenFlow cookie:** `flow_cookie` dùng cùng trị số mã nhóm do Network quy định, nhưng là field kỹ thuật trong switch, không thay cho đối tượng flow hoặc phần match rule.
- **Path:** `path_label ∈ {P1,P2}` với P1 qua c1, P2 qua c2. `path_id` **quy ước trong Data schema** là `<flow_group_id>_P1`/`_P2`, ví dụ `0x103_P1`, `0x103_P2`; Network xuất danh mục path chính thức để cả nhóm dùng chung.
- **Link:** dùng `link_id` **đúng từ danh mục Network** dạng `from_node>to_node` (có hướng), ví dụ `e1>c1`, `c1>e2`, `h1>e1`; chiều ngược là link khác.
- **Order:** nếu cần vector 48 nhóm thì sắp tăng dần theo **giá trị số của mã hex**, nhưng mọi join Data/Optimization vẫn theo `flow_group_id`, không dựa vào index vector.

Không dùng đồng thời `h1_h3`, `h1-h3`, `f13`, `0x103` làm các mã nhóm khác nhau; **chỉ `0x103` là `flow_group_id`** của cặp h1→h3. Không dùng `P1` đơn lẻ làm `path_id` và không tự thay `e1>c1` thành `e1_to_c1`.

**Việc các role phải sửa ngay khi tích hợp:** `interfaces_v2.md` và fixture cũ dùng `h1_h3`, `h1_h3_c1` hoặc `e1_to_c1` là **quy ước cũ không còn áp dụng**. Chuyển sang mapping chính thức Network (`0x103`, `0x103_P1`, `e1>c1`) ở **toàn bộ** schema/fixtures/API/serializer/test. Không đổi dữ liệu gốc của Network để chạy theo interface cũ.

## 3.3. Đơn vị và semantics

| Khái niệm   | Ý nghĩa                                                    | Field            |
|:------------|:-----------------------------------------------------------|:-----------------|
| Demand      | Nhu cầu payload do generator phát/cấu hình                 | `requested_mbps` |
| Goodput     | Payload nhận thành công ở đích                             | `goodput_mbps`   |
| Port rate   | Tốc độ tính từ TX counter, gồm overhead theo packet format | `port_tx_mbps`   |
| Capacity    | Dung lượng link                                            | `capacity_mbps`  |
| Utilization | Tỷ lệ 0–1                                                  | `utilization`    |
| RTT         | ms                                                         | `rtt_ms`         |
| Timestamp   | UTC                                                        | `*_utc`          |
| Offset      | giây                                                       | `*_offset_s`     |
| Duration    | giây                                                       | `*_s`            |

Không dùng chung field `throughput` cho demand, goodput và port rate.

ML dự báo `requested_mbps` (payload demand), không dự báo goodput vì goodput đã bị ảnh hưởng bởi congestion/loss.

## 3.4. Timestamp và bin

Mỗi run có:

- `run_started_at_utc`.
- `traffic_started_at_utc`.
- `measurement_started_at_utc`: mốc gốc t=0.

Warm-up đề xuất: 60 giây vì `L_max=60` (đủ 60 bin hoàn chỉnh). Measurement: 360 giây. Ba origin UTC không được đánh đồng; `traffic_started_at_utc` bắt đầu cả warm-up, `measurement_started_at_utc` là mốc t=0 của dự báo/benchmark.

Mỗi bin là khoảng nửa kín `[k,k+1)`. Với cutoff `t=100`, `L=30`, `H=5`:

- X = bin 70–99 = `[70,100)`.
- Y = bin 100–104 = `[100,105)`.
- lag_1 là bin 99; lag_L là bin 70.

Tổng quát:

$$
X = [t-L, t), \qquad Y = [t, t+H)
$$

Các timestamp không được đánh đồng:

| Field                                    | Ý nghĩa                         |
|:-----------------------------------------|:--------------------------------|
| `released_at_utc`                        | Network công bố bin             |
| `available_at_utc`                       | Data xử lý xong, có thể đọc     |
| `snapshot_as_of_utc`                     | Thời điểm **SDN** đóng snapshot |
| `history_cutoff_utc`                     | Mốc cuối lịch sử được phép dùng |
| `issued_at_utc`                          | ML phát hành forecast           |
| `forecast_start_utc`, `forecast_end_utc` | Khoảng được dự báo              |
| `apply_deadline_utc`                     | Hạn cuối bắt đầu apply          |

Observation chỉ được vào snapshot nếu:

``` text
window_end <= history_cutoff
released_at <= snapshot_as_of
available_at <= snapshot_as_of
```

Snapshot do **SDN phát hành** phải immutable. Data lưu nguyên bản (cùng snapshot ID/version/hash), không tái tạo bằng query. Dữ liệu đến muộn không được chèn ngược vào snapshot cũ. SDN chỉ được đưa demand bin đã release/available vào snapshot; cần chốt rõ nguồn cấp demand quá khứ cho SDN.

**Forecast cuối run:** Chỉ phát hành forecast nếu `forecast_end <= measurement_end`. Ví dụ H=10 thì không phát hành forecast mới sau cutoff 350. M1–M3 dùng cùng chính sách. Nếu nhóm muốn thêm label tail, phải duyệt riêng và không trộn tail vào metrics chính 360 giây.

## 3.5. Data split và leakage

Kế hoạch benchmark **đề xuất**: mỗi scenario S1–S5 có 20 seeds:

- 10 train.
- 5 validation.
- 5 test.

Chia theo seed/run family **trước khi tạo các cửa sổ chồng lấn**. Không random split các window của cùng schedule sang train và test.

- Train: fit.
- Validation: chọn L/H, model parameters, W của Moving Average, epsilon.
- Test: đánh giá cuối, không điều chỉnh.
- Có thể refit train+validation sau khi chọn tham số; phải ghi rõ, tuyệt đối không dùng test để tune.

`demand_plan` chỉ dùng để traffic generator phát lịch. M1–M3 online không được đọc future rows. M4 Oracle được dùng future demand trong evaluation offline và không deploy.

## 3.6. Config dùng chung

Mọi tham số dùng chung nằm trong `configs/`, không hard-code khác nhau ở từng role. **Riêng các giá trị L*/H*/epsilon* do ML tự chọn sau validation được lưu trong `configs/ml_config.yaml` hoặc metadata model, không truyền trong mỗi `PredictionRows` hoặc ghi thành cột của `predictions`.** Runner freeze version/hash của config ngay đầu run; Data tra `H*` từ RunManifest/config đã khóa để kiểm tra `48×H*` rows.

``` yaml
system:
  schema_version: "1.0"
  delta_s: 1
  warmup_s: 60
  measurement_s: 360
  max_horizon_s: 10

topology:
  host_edge_capacity_mbps: 10
  fabric_capacity_mbps: 10
  host_edge_delay_ms: 1
  edge_core_delay_ms: 5
  queue_packets: 50
  openflow_version: "1.3"

forecast:
  L_candidates: [10, 30, 60]
  H_candidates: [1, 3, 5, 10]
  epsilon_candidates: [0.0, 0.05, 0.1, 0.2]

optimization:
  control_interval_s: 1
  trigger_threshold: 0.85
  trigger_consecutive_ticks: 3
  min_improvement_h: 0.05
  hold_down_s: 10
  max_changed_flows_B: 4
  lambda: 0.1

evaluation:
  seeds_train: 10
  seeds_validation: 5
  seeds_test: 5
```

Đây là **danh sách ứng viên**/pilot defaults để nhóm duyệt, không phải `L/H/epsilon` mà ML phải gửi cho Data.

**Ví dụ artifact riêng của ML (chỉ minh họa, không phải kết quả validation thực tế):**

```yaml
# configs/ml_config.yaml — ML sở hữu, Runner pin hash/version
model_version: "rf_v1"
source: "rf_v1"
lookback_L: 30
horizon_H: 5
epsilon: 0.0
feature_schema_version: "ml_features_v1"
```

- ML có thể lưu thêm model params, feature order, metrics validation vào metadata/model card.
- `H=5` trong config minh họa khiến Data **kỳ vọng** 240 hàng cho 48 flow; Data **không yêu cầu** có field `horizon_steps` trong từng dự báo.
- Epsilon đã được ML áp dụng trước khi xuất `predicted_mbps`; Data/Optimization không áp dụng lại.
- Giá trị thực tế phải lấy từ validation/calibration/benchmark; `k_e`, deadline và timeout vẫn cần khóa riêng.

---

# 4. Interface giữa các role

## 4.1. Object chuẩn

| Object | Producer → Consumer | Contract/ý nghĩa |
|---|---|---|
| `TopologyBundle` | Network → Data/SDN/Optimization/Testing | hosts, switches, links, 48 flows, 96 paths, version |
| `DemandRelease` | Network → Data **và nguồn lịch sử SDN được chốt** | `requested_mbps`, bin, release/available time; không lộ tương lai |
| `LinkSampleBatch` | Network/SDN → Data | counters có hướng, time window, điểm đo |
| **`Snapshot`** | **SDN → Data (lưu) → ML (đọc)** | lịch sử hợp lệ, cutoff, snapshot ID/hash, trạng thái route/version; **SDN tạo, Data không tạo** |
| **`PredictionRows`** | **ML → Data** | **ĐÚNG 7 trường** `run_id`, `flow_group_id`, `issued_at`, `forecast_start`, `forecast_end`, `predicted_mbps`, `source` |
| `ForecastBatchRef` | Data → Optimization/Runner | Data tự gắn ID/status khi đã ghi đủ `48×H*` rows, H* lấy từ frozen config/RunManifest |
| `OptimizationInput` | Data → Optimization | H* hàng dự báo theo flow/batch, candidate paths/links, current verified routes/k_e |
| `OptimizationResult` | Optimization → Data/orchestrator | U/J, solver status, proposal, versions |
| `ApplyRequest` | Orchestrator → SDN | route changes, state version, deadline |
| `ApplyResponse` | SDN → Data | per-flow actual route/status, new state version, event ID |

### Snapshot

**SDN dựng và phát hành Snapshot; Data chỉ ingest/store/query.** SDN phải bảo đảm cutoff/causality khi tạo; Data validate và lưu immutable, không tái tính snapshot từ DB. ML nhận bản snapshot SDN đã được Data lưu/cấp. Nguồn chuyển `requested_mbps` lịch sử đã release đến SDN là [MUST-LOCK], vì port counter/goodput không thay được demand.

### PredictionRows — hợp đồng ML → Data đóng theo schema Data_v2

Mỗi lần suy luận ML tạo H* giá trị mỗi flow, chuyển trực tiếp thành H* hàng 7 trường; **không có** `L`, `H`, `epsilon`, `raw_mbps`, `final_mbps`, `epsilon_owner`, `mean_mbps` hoặc `horizon_steps` trong payload gửi Data. `predicted_mbps` **đã là final** sau khi ML tự hiệu chỉnh. `source` mô tả mô hình/baseline, ví dụ `rf_v1`.

Data có thể tự gom các hàng thành batch để lưu/kiểm tính đầy đủ, dựa trên `run_id`, `issued_at`, `source`, các mốc dự báo và bản config/RunManifest đã freeze. `ForecastBatchRef` là **đối tượng Data tạo sau ingest**, không phải yêu cầu ML gửi thêm metadata dự báo.

### OptimizationInput

Data chỉ truy vấn/trả **các hàng dự báo** của cùng batch và state đã xác minh; **Optimization tự tính** `b_f = mean(H* predicted_mbps)` mỗi flow trước khi dùng MILP. Nếu route UNKNOWN/INCONSISTENT thì skip tick, không giả định route cũ.

### ApplyRequest / ApplyResponse

ApplyRequest chứa lệnh đường đi, expected state/version và deadline; solver metrics ở audit record. Không báo `APPLIED` chỉ nhờ FlowMod/Barrier; cần SDN verify actual path.

## 4.2. API/function flow tham chiếu

``` python
# Network
export_topology() -> TopologyBundle
publish_demand_bin(run_id, bin_index) -> DemandRelease
publish_link_samples(run_id, interval) -> LinkSampleBatch

# Data
ingest_demand(release) -> IngestAck
ingest_link_samples(samples) -> IngestAck
ingest_snapshot(sdn_snapshot) -> SnapshotStoreAck  # Data KHÔNG tạo snapshot
get_stored_snapshot(run_id, cutoff) -> Snapshot  # chỉ đọc bản SDN đã lưu
commit_prediction_rows(rows_7_fields) -> ForecastBatchRef  # H* từ frozen config
get_optimization_input(run_id, forecast_batch_id) -> OptimizationInput
commit_apply_result(response) -> CommitAck

# ML
predict_batch(snapshot, frozen_ml_config) -> list[PredictionRow]  # 7 fields/row

# Optimization
solve(optimization_input, config) -> OptimizationResult

# SDN
create_snapshot(run_id, cutoff, lookback_L) -> Snapshot  # nguồn demand lịch sử: MUST-LOCK
get_routing_state(run_id) -> RoutingStateSnapshot
apply(request) -> ApplyResponse
```

Dùng `dataclasses` cho contract nội bộ hoặc Pydantic ở boundary. Nếu chạy nhiều process, serialize JSON qua REST/message adapter. Không để Ryu import solver/model.

---

# 5. Đặc tả chi tiết theo từng role

## 5.1. NETWORK

### Cấu trúc thư mục

``` text
network/
├── topology.py
├── port_mapping.py
├── flow_catalog.py
├── path_catalog.py
├── scenario_builder.py
├── traffic_generator.py
├── demand_releaser.py
├── telemetry_collector.py
├── counter_parser.py
└── exporter.py

artifacts/network/
├── flow_groups.csv
├── candidate_paths.csv
├── links.csv
├── initial_paths.csv
├── demand_plan/
└── calibration/
```

### Input

`system.yaml`, `topology.yaml`, `scenarios.yaml`, seed, scenario ID, traffic profile và canonical ID contract.

### Các bước thực hiện

**N1 — Dựng topology**

1. Tạo host/switch và đặt DPID/port tường minh.
2. Cấu hình OpenFlow 1.3, bandwidth, delay, queue.
3. Kiểm tra port bằng `ovs-ofctl`.
4. Xuất `TopologyBundle`.

**N2 — Sinh flow/path**

1. Sinh 48 directed flow groups khác edge.
2. Sinh 2 path/flow: P1 qua c1, P2 qua c2.
3. Mỗi path có ordered directed link IDs.
4. Xuất `flow_groups.csv`, `candidate_paths.csv`, `links.csv`, `initial_paths.csv`.

**N3 — Sinh scenarios**

| Scenario | Đặc tả                                                      |
|:---------|:------------------------------------------------------------|
| S0       | Ping, iperf3 UDP, kiểm tra P1/P2, counters, đổi route       |
| S1       | Ma trận demand ổn định; rho=0.4/0.6/0.8/1.0                 |
| S2a      | Tăng tuyến tính rho=0.3 lên 0.9 trong 360s                  |
| S2b      | Nền rho=0.6, sin dao động 0.3–0.9, chu kỳ 2–5 phút          |
| S3       | Hotspot: host đích 9 Mbps; chuyển hotspot mỗi 2 phút        |
| S4       | Nền rho=0.5; mỗi 30–60s chọn 4 flow tăng 3 lần trong 10–30s |
| S5       | Kết hợp chu kỳ, hotspot và burst; giới hạn host 10 Mbps     |

Mỗi host gửi tổng không quá 10 Mbps và nhận tổng không quá 10 Mbps. Nếu clipping/co giãn, lưu cả demand trước và sau clipping; tính lại rho thực tế.

**N4 — Tách ba phép đo**

- `requested_mbps`: demand payload từ generator.
- `goodput_mbps`: payload nhận ở đích.
- `port_tx_mbps`: tốc độ từ port TX counter.

$$
port\_tx\_mbps = \frac{8\,\Delta bytes}{\Delta t_{actual}\,10^6}
$$

Phải xử lý lần đọc đầu, counter reset/wrap và polling interval thực tế. Với directed link, lấy TX ở port thiết bị gửi; không cộng TX và RX cùng link.

**N5 — Online release**

- Schedule có thể sinh trước để tái lập seed.
- Chỉ công bố bin hiện tại qua `DemandRelease`.
- Gửi `released_at_utc` và `event_id`.
- Data ghi `available_at_utc`.
- ML chỉ được nhận snapshot, không đọc schedule tương lai.

**N6 — S0 và calibration**

- Ping giữa host khác edge.
- h1→h5 chạy P1/P2.
- Kiểm tra tải tăng đúng core.
- Đổi P1↔P2 khi đang gửi, ghi packet loss.
- RTT lý thuyết 24 ms (mục tiêu pilot kiểm sai số ±5% khi môi trường cho phép), throughput lệch dưới 5% **sau khi định nghĩa đúng overhead/cohort**, CPU dưới 80%; đây là ngưỡng nghiệm thu đề xuất, không phải số đo đã đạt.
- Bổ sung các mức tải thấp không nghẽn (ví dụ 2/4/6 Mbps) để hiệu chuẩn k_e; test 10 Mbps đơn lẻ không đủ để kết luận overhead ratio.

### Output và điều kiện bàn giao

| Output              | Consumer                  | Điều kiện                   |
|:--------------------|:--------------------------|:----------------------------|
| TopologyBundle      | Data/SDN/Optimization     | DPID/port verified          |
| flow_groups.csv     | Cả nhóm                   | Đủ 48 flow                  |
| candidate_paths.csv | Data/Optimization/SDN     | Đủ 96 paths                 |
| links.csv           | Data/Optimization/Testing | Đủ 32 directed links        |
| initial_paths.csv   | Data/SDN/Testing          | Đủ 48 routes                |
| DemandRelease       | Data                      | Đúng bin, ID, release time  |
| LinkSampleBatch     | Data                      | Đúng port/direction         |
| S0 report           | Cả nhóm                   | Pass hoặc lỗi có bằng chứng |

**Lưu ý sửa:** không để consumer tự suy ra port mapping; không dùng goodput làm demand; không cho online ML đọc future `demand_plan`.

---

## 5.2. DATA

### Cấu trúc thư mục

``` text
data/
├── schema/schema.sql
├── schema/migrations/
├── repositories/
├── ingestion/
├── snapshot_store/   # ingest/query snapshot do SDN tạo, không build
├── batch/
└── api/data_service.py
```

### Input

TopologyBundle, DemandRelease, LinkSampleBatch, **Snapshot do SDN tạo**, PredictionRows (7 trường), OptimizationResult, ApplyResponse.

### Các bước thực hiện

**D1 — Schema**

- Giữ các bảng hiện có, không xóa/đổi nghĩa cột âm thầm.
- Các nhóm dữ liệu: topology, traffic, telemetry, forecasts, decisions, routing state/history, events, run/config metadata.
- Nếu tên bảng hiện có khác thì mapping tại repository, không ép các role khác tự đổi schema.

**D2 — Schedule và online observations**

- `demand_plan`: lịch generator dùng để phát traffic.
- `demand_observations`/`traffic_actual`: các bin đã công bố và Data nhận.
- M1–M3 online chỉ đọc observations.
- M4 future data chỉ qua evaluation path.

Observation tối thiểu: run, flow, bin start/end, requested Mbps, released time, available time, source, event ID.

**D3 — Snapshot storage (SDN là producer)**

- Nhận `Snapshot` SDN đã đóng với `snapshot_id`, `snapshot_as_of_utc`, `history_cutoff_utc`, `state_version`, data/history và `config_version` tương ứng.
- Validate schema, ID/version/hash, `history_cutoff`, `released_at`/`available_at` và nguồn dữ liệu hợp lệ; **không tạo lại Snapshot** hay tự gán `snapshot_id`.
- Lưu nguyên bản immutable; cung cấp API đọc snapshot đã lưu cho ML.
- **Trách nhiệm causality chính ở SDN khi tạo**, Data thêm validation tại boundary. Nếu thiếu dữ liệu, đánh dấu lỗi và trả lý do; không tự lấp bằng tương lai.
- **[MUST-LOCK]** Network/SDN/Data xác nhận đường truyền `requested_mbps` đã release tới SDN: SDN không thể dùng counter nhận/throughput để suy demand.

**D4 — Lưu dự báo 7 trường, không ghi config ML vào prediction rows**

Giữ **đúng** schema `predictions` của Data_v2:

```text
run_id, flow_group_id, issued_at, forecast_start,
forecast_end, predicted_mbps, source
```

Khóa hàng:

```text
(run_id, flow_group_id, issued_at, forecast_start)
```

- ML chỉ gửi các hàng trên. `predicted_mbps` là giá trị đã hiệu chỉnh cuối cùng.
- **Không yêu cầu** ML truyền `H`, `L`, `epsilon`, `raw_mbps`, `epsilon_owner`, `model_params` hay `mean_mbps` trong result.
- Data có thể có **batch registry nội bộ** (`forecast_batch_id`, `status`, `expected_rows`, `actual_rows`) để điều phối/kiểm tra. Registry do Data tự tạo, **không phải trường của các hàng ML gửi**.
- `expected_rows = 48×H*`, trong đó `H*` do Data đọc từ **frozen RunManifest/ML config version** lúc khởi động, không lấy từ metadata thêm vào từng forecast. Với H*=5 kỳ vọng 240 hàng; kiểm liên tục/không trùng mỗi flow.
- Chỉ đánh dấu READY sau khi toàn bộ 7-field rows hợp lệ, không trộn các `issued_at` hoặc `source`.
- Nếu ML cần lưu raw forecast phục vụ phân tích offline, lưu trong **artifact ML riêng**. Không ép mở rộng schema predictions.

**D5 — Decision batch/control tick**

- `decision_batches`: lưu OptimizationResult và execution status cấp batch.
- `control_ticks`: lưu mọi tick kể cả không đổi route.
- Reason ví dụ: `NO_TRIGGER`, `DATA_MISSING`, `FORECAST_INVALID`, `NO_IMPROVEMENT`, `HOLDDOWN`, `APPLY_REQUESTED`, `APPLIED`, `PARTIAL`, `RECONCILIATION_REQUIRED`.

**D6 — Routing state**

- SDN là nguồn sự thật của route actual.
- Data lưu mirror có `state_version`.
- Chỉ commit route đã xác minh.
- Event có `event_id`, ingest idempotent.
- Controller retry event nếu Data chưa ack; khi restart phải reconcile actual route.
- `state_version` là version của toàn bộ routing snapshot, không phải mỗi role tự tăng riêng.

**D7 — API** `ingest_demand`, `ingest_link_samples`, **`ingest_snapshot`, `get_stored_snapshot`**, `commit_prediction_rows`, `get_optimization_input`, `commit_optimization_result`, `commit_apply_result`, `get_routing_state`.

### Output và điều kiện bàn giao

- Schema/migration có version.
- Bản snapshot **do SDN tạo** được Data lưu đúng hash/cutoff, không leakage.
- ForecastBatchRef (Data nội bộ) chỉ READY khi đủ 48×H* rows theo frozen config.
- OptimizationInput có 48 flows, 16 fabric links, current route/version.
- Routing state mirror có version.
- ApplyResponse lưu được per-flow actual path.
- Data quality report thống kê missing/late/duplicate.

**Lưu ý sửa:** Data là DB writer duy nhất; **không tạo snapshot**, không tính mean thay Optimization, không nhận L/H/epsilon trong ML result. Không dùng `demand_plan` chưa release làm online snapshot.

---

## 5.3. ML

### Cấu trúc thư mục

``` text
ml/
├── data_loader.py
├── feature_builder.py
├── features.py
├── baselines/persistence.py
├── baselines/moving_average.py
├── models/random_forest.py
├── calibration/epsilon_calibrator.py
├── train.py
├── validate.py
├── predict.py
├── evaluate.py
└── model_registry.py
```

### Input

Bản **snapshot do SDN tạo, Data lưu/cấp lại**; **frozen config ML** (L*/H*/epsilon*/features) ML tự chọn offline; train/validation data và canonical flow IDs.

### Các bước thực hiện

**M1 — Target:** `requested_mbps` (payload demand), không phải `goodput_mbps` hay port counter.

**M2 — Baselines**

- Persistence: lặp giá trị mới nhất đã hoàn tất cho H bước.
- Moving Average: trung bình W bin gần nhất, lặp cho H bước. W là tham số riêng, tune bằng validation, không mặc định bằng L.

$$
\widehat d_{t+h} = d_{t-1} \quad (h=0,\dots,H-1)\;\text{Persistence}
$$

$$
\widehat d_{t+h} = \frac{1}{W}\sum_{i=1}^{W} d_{t-i} \quad (h=0,\dots,H-1)\;\text{MA}
$$

**M3 — Random Forest multi-output**

- Một mô hình chung cho 48 flow groups.
- Một sample là một flow group tại một cutoff.
- Input gồm L bin lịch sử và feature thời gian/flow đã biết tại cutoff.
- Output vector H bước.
- Không dùng future schedule, future label hoặc feature chỉ biết sau cutoff.
- Feature có thể gồm lag, rolling stats/trend và flow identity; mọi rolling feature chỉ dùng quá khứ.

**M4 — Tuning**

- L candidates: 10, 30, 60.
- H candidates: 1, 3, 5, 10.
- Chọn bằng validation; test chỉ đánh giá cuối.
- Không random split các window chồng lấn sau khi đã tạo.

**M5 — Epsilon: ML sở hữu và áp dụng đúng một lần**

$$
\widehat d^{final}_{f,h} = \max\big(0,(1+\varepsilon)\widehat d^{raw}_{f,h}\big)
$$

Candidate epsilon: 0, 0.05, 0.1, 0.2. Chọn trên validation; nếu không cải thiện so với epsilon=0 thì chọn 0; tie thì ưu tiên 0.

ML **chỉ gửi dự báo final** qua `predicted_mbps` của 7 trường `predictions`. Raw forecast có thể giữ **nội bộ ML** để đánh giá, không gửi Data và **không mở rộng schema**. `epsilon*` được ML chọn offline, lưu trong `ml_config.yaml`/model metadata và áp dụng **đúng một lần** trước khi xuất. Data không cần biết epsilon từng dòng; Optimization không nhân epsilon lần nữa. Cấu hình baseline/Oracle phải chốt độc lập trong RunManifest để so sánh công bằng.

**M6 — Đầu ra online duy nhất: PredictionRows theo 7 trường**

- Sau một lần suy luận, ML tạo 48 vector × H* giá trị, nhưng **payload bàn giao Data chỉ là 48×H* hàng** thuộc schema `predictions`.
- Mỗi hàng: `run_id`, `flow_group_id`, `issued_at`, `forecast_start`, `forecast_end`, `predicted_mbps`, `source`.
- `predicted_mbps` đã áp dụng epsilon nếu epsilon* > 0; không kèm trường raw/epsilon/H/L.
- `H*` do ML **tự xác định offline** trong config; Data kiểm tra row count theo frozen config, không bắt ML gửi lại.
- Optimization đọc các hàng thông qua Data rồi **tự tính mean H***. ML không xuất scalar mean cho Optimization.

**Ví dụ minh họa** config H*=5, epsilon*=0; một flow có 5 hàng `predicted_mbps = 2.0, 2.5, 3.0, 3.5, 3.0`; Optimization tính `b_f=2.8 Mbps`. Nếu epsilon*>0, `predicted_mbps` đã là các số cuối cùng được hiệu chỉnh.

**M7 — Fallback** Nếu RF lỗi nhưng history hợp lệ, có thể dùng Persistence fallback với `source=fallback_persistence`; không ghi nhầm thành RF. Nếu history không hợp lệ hoặc batch thiếu, đánh dấu invalid và skip tick. Báo cáo fallback frequency.

### Output

Model artifact và **config L*/H*/epsilon* riêng**, **chỉ các hàng dự báo 7 trường gửi Data**, MAE/RMSE theo bước/scenario, inference latency p50/p95, model card (target/features/split/limitations).

**Lưu ý sửa:** ML không truy cập DB trực tiếp; không dự báo goodput; **không gửi L/H/epsilon/raw/mean cho Data**, không tune bằng test; epsilon chỉ áp dụng một lần. Dữ liệu ML đầu vào là snapshot **SDN tạo, Data lưu/cấp lại**.

---

## 5.4. OPTIMIZATION

### Cấu trúc thư mục

``` text
optimization/
├── input_validator.py
├── load_estimator.py
├── trigger.py
├── model.py
├── solver.py
├── solution_validator.py
├── objective.py
├── planner.py
└── result_serializer.py
```

### Input

OptimizationInput từ Data: forecast READY, 48 flow demands, 16 fabric directed links, capacity, k_e, current routes, last successful change time, state/topology/config version.

### Các bước thực hiện

**O1 — Demand scalar**

Với flow f:

$$
b_f = \frac{1}{H}\sum_{h=1}^{H}\widehat d^{final}_{f,h}
$$

MILP phiên bản hiện tại dùng mean H bước. Giữ vector để đánh giá peak/burst. Đây là giới hạn: mean có thể che khuất peak; không tuyên bố hệ thống tối ưu theo từng giây. Time-expanded MILP là hướng mở rộng riêng.

**O2 — Calibration (k_e)**

(k_e) chuyển payload demand sang wire-level load dự kiến trên directed link e:

$$
L_e = k_e\sum_{f\in F}\sum_{p\in P_f}b_f\,a_{fpe}\,x_{fp}
$$

Calibration:

$$
k_e = \operatorname{median}\left(\frac{\text{observed port TX Mbps}_e}{\text{offered payload Mbps}_e}\right)
$$

Chỉ dùng mẫu không nghẽn, đúng hướng/port. Mẫu số là payload thực sự phát từ sender (hoặc sender counter tương đương), không dùng receiver goodput. Nếu chưa đủ calibration: `k_e=1.0`, status `UNCALIBRATED`. Không tự chọn k_e để cải thiện kết quả.

**O3 — MILP**

Tập:

- F: 48 flows.
- (P_f={P1,P2}).
- E: 16 fabric directed links.

Biến:

- (x\_{fp}): flow f chọn path p. - (y_f): flow f đổi path.
- U: max predicted utilization.

Objective:

$$
\min\; J=U+\frac{\lambda}{|F|}\sum_{f\in F}y_f
$$

R1:

$$
\sum_{p\in P_f}x_{fp}=1 \quad \forall f\in F
$$

R2/R3:

$$
L_e=k_e\sum_{f,p}b_f a_{fpe}x_{fp},\qquad L_e\le U C_e \quad\forall e\in E
$$

Không đặt hard cap U≤0.9 hoặc U≤1; nếu quá tải, U được phép \>1.

R4 — exact route change:

$$
y_f=1-x_{f,p^{current}_f} \quad \forall f\in F
$$

R5:

$$
\sum_{f\in F}y_f\le B
$$

R6 — nếu `t - last_successful_change_at < T_hold` thì bắt buộc giữ current path.

Host-edge links là đoạn cố định, theo dõi riêng và đảm bảo traffic không vượt giới hạn; không coi chúng là path decision variables.

**O4 — Trigger/acceptance**

Pilot defaults:

| Parameter |                     Value |
|:----------|--------------------------:|
| `T_ctl`   |                       1 s |
| `TH`      |                      0.85 |
| `N`       | 3 valid consecutive ticks |
| `h`       |                      0.05 |
| `T_hold`  |                      10 s |
| `B`       |                   4 flows |
| `lambda`  |                       0.1 |

`U_current` phải tính bằng forecast hiện tại, cùng k_e/capacity và confirmed current routes. Trigger nếu `U_current > TH` trong N tick hợp lệ liên tiếp. Tick invalid reset streak.

$$
J_{new}=U_{new}+\frac{\lambda}{48}\sum_f y_f
$$

$$
J_{old}=U_{current}
$$

Chấp nhận nếu:

$$
J_{new}\le(1-h)J_{old}
$$

và có route change, nghiệm qua validator, state version còn đúng, forecast còn hạn, deadline còn đủ, không vi phạm B/hold-down.

Ví dụ U_new=0.58, lambda=0.1, 4 changes:

$$
J_{new}=0.58+0.1\times\frac{4}{48}=0.588333\ldots
$$

Không ghi 0.61 với cùng bộ tham số.

**O5 — Solver status** Phân biệt OPTIMAL, FEASIBLE, INFEASIBLE, TIME_LIMIT, ERROR. Pilot ưu tiên OPTIMAL. Nếu cho phép FEASIBLE phải có `max_gap` được config và validator độc lập; không tự động apply mọi nghiệm feasible.

**O6 — Output** OptimizationResult chứa U/J, solver status/time/gap, route proposal, changed flows, snapshot/forecast/state version và reason. Chỉ orchestration tạo ApplyRequest. Optimization không gọi Ryu trực tiếp, không tự ghi routing state.

### Output

Load estimate, trigger result, OptimizationResult, solver logs, solution validation report.

**Lưu ý sửa:** không nhân epsilon lần hai; không dùng actual MLU thay cho predicted U trong trigger; R4 phải là đẳng thức; không hard cap U; không apply khi current route UNKNOWN.

---

## 5.5. SDN

### Cấu trúc thư mục

``` text
controller/
├── ryu_app.py
├── topology_registry.py
├── flow_matcher.py
├── route_compiler.py
├── rule_installer.py
├── apply_manager.py
├── state_manager.py
├── verifier.py
├── rollback_manager.py
├── event_outbox.py
└── api/controller_api.py
```

### Input

TopologyBundle, ApplyRequest, live routing state, DPID/port mapping và controller config.

### Các bước thực hiện

**S1 — Static routing trước**

1. Cài h1→h3 P1.
2. Ping/iperf.
3. Chuyển sang P2.
4. Xác minh traffic qua c2.
5. Chuyển lại P1.
6. Sau đó mới mở rộng 48 flows.

**S2 — Compile path** Nhận `path_id`, tra candidate path để lấy DPID/port/order. Không để Optimization tự gửi port number. Rule phải match đúng flow; cookie dùng quản lý rule, không thay thế match fields.

**S3 — Apply an toàn**

1. Validate request và state version.
2. Cài rule ở downstream switches của path mới trước.
3. Chờ Barrier/ack.
4. Cập nhật rule ở source edge sau cùng.
5. Verify actual route bằng FlowStats/counter/probe phù hợp.
6. Chỉ sau verify mới dọn rule cũ.
7. Cập nhật state version và trả ApplyResponse.

Barrier Reply chỉ chứng minh switch xử lý message, không chứng minh end-to-end packet đã đi đúng path.

**S4 — Reject trước FlowMod nếu**

- Stale state version.
- Old path không khớp live state.
- New path không phải candidate path.
- Config/topology version mismatch.
- Số flow vượt B, hold-down chưa hết.
- Deadline hết.
- Đang có batch khác apply.
- Batch ID trùng nhưng payload hash khác.

**S5 — Idempotency/timeout**

- Cùng batch ID và payload hash: trả kết quả cũ, không apply lần hai.
- Cùng ID nhưng payload khác: `BATCH_ID_CONFLICT`.
- Timeout sau FlowMod: reconcile Flow Table trước retry.
- Event gửi Data có event ID duy nhất; retry không tạo duplicate.

**S6 — Status** Batch: `APPLIED`, `REJECTED`, `FAILED`, `ROLLED_BACK`, `PARTIAL`, `INCONSISTENT`. Per-flow: `APPLIED`, `KEPT`, `FAILED`, `ROLLED_BACK`, `UNKNOWN`.

Không báo FAILED nếu chưa biết actual path. Nếu có UNKNOWN thì batch INCONSISTENT và phải reconcile trước tick tiếp theo.

**S7 — State version** Controller giữ live version; batch thay đổi được verify thì tăng version theo quy tắc chung. Data lưu mirror. Khi Ryu restart phải reconcile actual routes trước khi nhận request mới.

### Output

ApplyResponse có per-flow actual path/status, routing state event, FlowStats/telemetry và apply/rollback logs.

**Lưu ý sửa:** không chạy solver trong Ryu; downstream trước/source sau; không coi Barrier là verification; timeout phải reconcile; Data là DB writer duy nhất.

---

## 5.6. TESTING

### Cấu trúc thư mục

``` text
tests/
├── unit/
├── contract/
├── integration/
├── e2e/
└── regression/

evaluation/
├── runner.py
├── seed_manager.py
├── metrics_ml.py
├── metrics_network.py
├── metrics_optimization.py
├── comparison.py
└── report.py
```

### Công việc

**T1 — Unit tests**

- Network: topology/path generation, traffic caps, counters.
- Data: duplicate event, snapshot cutoff, batch completeness.
- ML: feature cutoff, X/Y alignment, baselines, epsilon.
- Optimization: objective/constraints/route change.
- SDN: path compiler, stale version, idempotency, rollback.

**T2 — Contract tests bắt buộc**

1. 8 host, 6 switch.
2. 48 flows, 96 candidate paths.
3. 32 directed links, 16 fabric directed links.
4. h1→h3 giữ cùng ID ở mọi role.
5. t=100, L=30, H=5 → X=70–99, Y=100–104.
6. Snapshot không có future demand.
7. H=5 có 240 prediction rows.
8. raw=4, epsilon=0.1 → final=4.4, không phải 4.84.
9. vector `[2,2.5,3,3.5,3]` mean=2.8.
10. R4/J đúng với 0/1/4 route changes.
11. Reject stale state version.
12. Duplicate ApplyRequest không apply hai lần.
13. Timeout phải reconcile.
14. Counter direction/reset xử lý đúng.

**T3 — Integration gates**

| Gate | Kiểm thử                | Điều kiện qua                          |
|:-----|:------------------------|:---------------------------------------|
| G0   | Contract/config/schema  | Cùng schema                            |
| G1   | Network S0              | Topology, ping, P1/P2, counters pass   |
| G2   | Network→Data            | Demand/telemetry ingest đúng           |
| G3   | SDN→Data→ML             | SDN tạo snapshot, Data lưu, không leakage |
| G4   | ML→Data→Optimization    | Chỉ 7 fields/row; Data READY đủ 48×H*; Optimization tự mean |
| G5   | Optimization standalone | Fixture, objective và constraints đúng |
| G6   | Optimization→SDN        | h1→h3 P1↔P2 được verify                |
| G7   | Full 48 flows           | Batch/state/version/rollback đúng      |
| G8   | Benchmark               | M0–M4 cùng điều kiện, đủ metrics       |

Không nhảy G7 nếu G1–G6 chưa qua.

**T4 — Methods**

- M0: static routing, không forecast/MILP.
- M1: Persistence + cùng MILP.
- M2: Moving Average + cùng MILP.
- M3: Random Forest multi-output + cùng MILP.
- M4: Oracle future demand + cùng MILP, upper bound offline, không deploy.

M1–M3 dùng cùng trigger, solver, capacity, k_e, hold-down và apply logic.

**T5 — Metrics**

ML:

- MAE/RMSE theo horizon, scenario, flow.
- Bias/underprediction.
- Inference p50/p95.
- Fallback frequency.
- Không dùng MAPE làm metric chính khi demand có thể bằng 0.

Network:

- Actual MLU trên 16 fabric directed links.
- Host-edge utilization riêng.
- Goodput, packet loss, RTT/latency.
- Thời gian vượt ngưỡng.
- Demand satisfaction, transition loss.

Optimization/SDN:

- Solver status/time/gap.
- Route changes đã verify.
- Apply/verification latency.
- Deadline misses, rollback, PARTIAL/UNKNOWN.
- Flapping, CPU/RAM nếu cần.

Actual MLU:

$$
MLU_{actual}=\max_{e\in E}\frac{\text{port TX Mbps}_e}{C_e}
$$

Không dùng predicted U thay actual MLU. Không cộng TX và RX cùng link. So sánh paired theo seed/run, không coi từng giây trong cùng run là sample độc lập.

### Output

Contract report, integration report, S0 calibration report, benchmark raw results, metrics theo method/scenario/seed, bảng M0–M4, issue log và final report có limitations.

---

# 6. Conflict log — các quy tắc không được làm khác

| Dễ conflict              | Quyết định thống nhất                           |
|:-------------------------|:------------------------------------------------|
| Flow ID                  | **Theo Network:** `0x103` cho h1→h3 (`src×256+dst`) |
| Path ID                  | `0x103_P1`, `0x103_P2` (Network P1/P2 + Data catalog) |
| Link ID                  | Theo catalog Network: `e1>c1` (có hướng)       |
| Demand/goodput/port rate | Ba field riêng                                  |
| Vector H/scalar          | Giữ vector; Optimization dùng mean              |
| Epsilon                  | ML áp dụng một lần; Optimization không nhân     |
| Future demand            | M1–M3 không đọc `demand_plan` tương lai         |
| Timestamp                | Bin `[start,end)`, t là cutoff                  |
| Predictions PK           | Giữ khóa có `forecast_start`                    |
| Batch thiếu rows         | Không READY                                     |
| Current route            | SDN confirmed state + version                   |
| DB writer                | Data duy nhất                                   |
| Apply success            | Chỉ sau verification                            |
| Timeout                  | Reconcile trước retry                           |
| k_e                      | Calibration theo directed link                  |
| Predicted U/actual MLU   | Hai metric riêng                                |
| Warm-up                  | 60s                                             |
| Seed split               | 10/5/5 trước windowing                          |
| M4                       | Oracle offline                                  |
| Human approval           | Không đưa vào realtime loop; validation tự động |

## 6.1. Lưu ý riêng về k_e

(k_e) là hệ số chuyển payload demand sang wire-level load ước lượng, không phải biến để MILP tự chọn và không phải safety margin tùy ý:

$$
k_e = \operatorname{median}\left(\frac{\text{observed port TX Mbps}_e}{\text{offered payload Mbps}_e}\right)
$$

Dùng mẫu không nghẽn, đúng directed link/port. Mẫu số là payload thực sự phát ở sender, không phải receiver goodput. Lưu calibration version, số mẫu và độ phân tán. Chưa đủ dữ liệu thì k_e=1 và đánh dấu `UNCALIBRATED`. Không áp dụng k_e vào actual MLU đã tính từ port counters.

## 6.2. Giới hạn scalar mean

MILP hiện tại dùng mean H bước. Cách này đơn giản nhưng có thể che peak/burst. ML vẫn lưu vector, Testing đánh giá theo horizon và báo cáo phải ghi rõ giới hạn. Time-expanded MILP là phiên bản mở rộng riêng, không tự đổi trong code hiện tại.

---

# 7. Repository và quy tắc code

``` text
sdn-traffic-engineering/
├── README.md
├── requirements.txt
├── configs/
│   ├── system.yaml
│   ├── topology.yaml
│   ├── scenarios.yaml
│   ├── forecast.yaml
│   ├── optimization.yaml
│   └── evaluation.yaml
├── docs/
│   ├── architecture.md
│   ├── integration_contract.md
│   ├── data_dictionary.md
│   ├── runbook.md
│   └── roles/
├── src/sdn_te/
│   ├── contracts/
│   ├── network/
│   ├── data/
│   ├── ml/
│   ├── optimization/
│   ├── controller/
│   └── evaluation/
├── tests/
│   ├── unit/
│   ├── contract/
│   ├── integration/
│   ├── e2e/
│   └── regression/
├── scripts/
└── artifacts/
```

Quy tắc:

- `contracts/` không import logic role.
- Role chỉ import contracts, không import nội bộ của role khác.
- Ryu không import model/solver.
- Python naming: `snake_case`; class `PascalCase`.
- Field có suffix đơn vị.
- ID là string ổn định; không dùng array index làm ID.
- Mọi API có `schema_version`; mọi event có `event_id`; mọi batch có batch ID.
- Không commit DB runtime/raw logs/model lớn nếu không cần; lưu metadata/hash/version trong repo.

---

# 8. Kế hoạch phối hợp theo phase

## Phase 0 — Freeze contract

Cả nhóm duyệt ID, unit, timestamp, schema, status, method ID; chốt `contracts/` và `system.yaml`.

**Output:** Contract v1.0, config v1.0, test fixtures.

## Phase 1 — Network + Data nền tảng

Network bàn giao TopologyBundle, 48 flows, 96 paths, 32 directed links, initial routes, S0 report. Data bàn giao schema/migration, ingestion, **snapshot storage/query do SDN tạo**.

**Gate:** S0 pass; Data ingest đúng ID/unit/time.

## Phase 2 — ML + SDN độc lập

ML bàn giao Persistence, MA, RF, **config được chọn offline** và **PredictionRows 7 trường** (48×H*); SDN bàn giao **snapshot creator**, static P1/P2, path compiler, apply một flow, verification/state version.

**Gate:** Forecast đúng shape; SDN chuyển P1↔P2 và verify.

## Phase 3 — Optimization standalone

Dùng fixture 48 flows, 2 paths/flow, 16 fabric links, current routes và forecast. Kiểm tra R1–R6, objective J, validator.

**Gate:** OptimizationResult đúng, serialize được ApplyRequest.

## Phase 4 — Tích hợp một flow

Chạy h1→h3: Network released demand → **SDN tạo snapshot → Data lưu/cấp** → ML 7-field predictions → Data READY → Optimization tự tính mean → SDN P1→P2 → verify → Data commit. Sau đó chạy P2→P1.

**Gate:** Không sai ID/bin/version, không duplicate apply.

## Phase 5 — Tích hợp 48 flows

Test nhiều tick, trigger N=3, B=4, hold-down, timeout, partial, rollback, restart/reconcile.

**Gate:** Không còn state UNKNOWN chưa xử lý.

## Phase 6 — Benchmark

Reset cùng initial route cho từng method/run; chạy M0–M4 trên cùng schedule/seed/config; S0 calibration, S1–S5 benchmark.

## 8.1. Bảng bàn giao

| Producer     | Consumer     | Producer phải giao                       | Điều kiện nhận               |
|:-------------|:-------------|:-----------------------------------------|:-----------------------------|
| Network      | Data         | Topology, DemandRelease, LinkSampleBatch | Đúng ID/unit/time            |
| Network      | SDN          | TopologyBundle, candidate paths          | P1/P2 verified               |
| SDN → Data   | ML           | **Snapshot SDN tạo; Data chỉ lưu/cấp**    | Không leakage, hash trùng    |
| ML           | Data         | **PredictionRows 7 trường**              | Đủ 48×H* theo config        |
| Data         | Optimization | OptimizationInput                        | Batch READY, đúng state      |
| Optimization | Orchestrator | OptimizationResult                       | Validator pass               |
| Orchestrator | SDN          | ApplyRequest                             | Version/deadline hợp lệ      |
| SDN          | Data         | ApplyResponse/state event                | Actual path verified         |
| Data         | Testing      | Query/export dataset                     | Có run/config/method/version |

## 8.2. Quy tắc xử lý thay đổi

Mỗi bàn giao phải trả lời: 1. Input lấy từ đâu, version nào? 2. Output có field nào, unit gì? 3. ID/timestamp có giữ nguyên không? 4. Test nào chứng minh đúng? 5. Lỗi/thiếu dữ liệu trả status gì?

Khi conflict: 1. Ghi issue. 2. Chỉ rõ field/interface ảnh hưởng. 3. Xác định producer/consumer. 4. Đề xuất một canonical solution. 5. Sửa contract và test. 6. Sau khi duyệt mới sửa các role liên quan.

---

# 9. Git workflow và versioning

- `main`: bản ổn định.
- `develop`: tích hợp.
- Branch: `feature/network-*`, `feature/data-*`, `feature/ml-*`, `feature/optimization-*`, `feature/sdn-*`, `feature/testing-*`.
- Mỗi PR ghi mục tiêu, Input/Output, file thay đổi, contract impact, tests, config/version, role bị ảnh hưởng.
- Không merge nếu contract tests liên quan fail.

Mỗi run lưu (ở RunManifest, không yêu cầu mọi bảng phải lặp tất cả fields):

``` text
run_id
method_id
seed
scenario_id
config_version
topology_version
schema_version
model_version
calibration_version
git_commit
```

Thiếu version metadata thì kết quả benchmark không đủ khả năng tái lập/truy vết.

---

# 10. Acceptance checklist

## Network

- [ ] DPID/port mapping verified.
- [ ] 8 host, 6 switch, 48 flow, 96 paths.
- [ ] 32 directed links, 16 fabric directed links.
- [ ] P1/P2 hoạt động đúng.
- [ ] Host send/receive không vượt 10 Mbps theo schedule.
- [ ] Online release không lộ future.
- [ ] Counter đúng direction/measurement point.
- [ ] S0 pass hoặc có report lỗi.

## Data

- [ ] Schema/migration có version.
- [ ] Ingestion idempotent.
- [ ] **Chỉ nhận/lưu** snapshot SDN đúng cutoff và immutable; không tạo mới.
- [ ] Forecast batch READY chỉ khi đủ rows.
- [ ] Predictions lưu đúng horizon.
- [ ] Routing state mirror có version.
- [ ] ApplyResponse lưu per-flow actual path.

## ML

- [ ] Target là payload demand.
- [ ] Seed split đúng.
- [ ] X/Y không lệch bin.
- [ ] Persistence/MA/RF hoạt động.
- [ ] RF trả vector H.
- [ ] Epsilon áp dụng đúng một lần.
- [ ] Có raw/final **trong đánh giá nội bộ ML nếu cần**; **chỉ final 7 trường** gửi Data.
- [ ] Không future leakage.
- [ ] Có MAE/RMSE theo horizon.

## Optimization

- [ ] Input đủ 48 flow/16 fabric links.
- [ ] Current route có version.
- [ ] k_e calibrated hoặc marked uncalibrated.
- [ ] R1–R6 và J được validate.
- [ ] Trigger dùng predicted U nhất quán.
- [ ] B/hold-down được enforce.
- [ ] Không tự apply.

## SDN

- [ ] Static P1/P2 pass.
- [ ] Reject stale version.
- [ ] Downstream install trước/source rule sau.
- [ ] Verify actual route.
- [ ] Rollback/reconcile được test.
- [ ] Idempotency hoạt động.
- [ ] Không báo APPLIED chỉ từ Barrier.
- [ ] State version tăng đúng.

## Testing

- [ ] Contract tests pass.
- [ ] G0–G8 pass.
- [ ] M0–M4 cùng schedule/seed/initial route.
- [ ] Predicted metrics tách actual metrics.
- [ ] Paired comparison theo run/seed.
- [ ] Có raw results, config, git commit và limitations.

---

# 11. Các quyết định còn phải xác nhận trước khi freeze

Không tự đoán các giá trị sau:

1.  (k_e) thực tế cho từng fabric directed link.
2.  L/H cuối cùng sau validation.
3.  Epsilon cuối và bằng chứng cải thiện validation.
4.  `max_gap` nếu cho phép nghiệm FEASIBLE.
5.  p95 latency của snapshot, inference, MILP, FlowMod và verification.
6.  Apply deadline guard, timeout/retry theo số đo.
7.  Cách khôi phục routing state sau Ryu restart.
8.  Chọn REST hay message queue cho giao tiếp giữa process.
9.  Dừng forecast trước measurement end hay bổ sung label tail.
10. Nơi lưu artifact lớn và cách quản lý version.
11. API/kênh cấp `requested_mbps` đã release/available cho SDN tạo snapshot, bao gồm cách kiểm tra causality.

TH=0.85, N=3, h=0.05, T_hold=10s, B=4, lambda=0.1 là pilot defaults cần kiểm chứng, không phải kết quả tối ưu đã được chứng minh.

---

# 12. Definition of Done

Dự án chỉ được coi là end-to-end khi có thể chạy tự động, không sửa tay dữ liệu giữa các role:

1.  Network dựng đúng topology và phát demand.
2.  **SDN tạo snapshot**, Data nhận/lưu và cấp lại; cùng kiểm tra cutoff và lịch sử đã release.
3.  ML dùng config L*/H*/epsilon* đã khóa, chỉ xuất 48×H* hàng dự báo 7 trường.
4.  Data validate 7-field rows và đánh dấu forecast batch nội bộ READY dựa vào H* trong config.
5.  Optimization đọc H* rows/flow, **tự tính mean**, tính trigger, giải MILP và validate.
6.  ApplyRequest có đúng route, state version và deadline.
7.  SDN validate, cài rule và verify actual path.
8.  Data commit ApplyResponse và routing state mirror.
9.  Testing truy xuất toàn bộ trace theo run/batch ID.
10. Chạy lại cùng seed/config có thể tái lập kết quả trong sai số cho phép.

**Tiêu chí cuối:** Không role nào phải tự đoán ID, đơn vị, timestamp, field hoặc status của role khác. Mọi Input có nguồn, mọi Output có schema, mọi quyết định có version và mọi lần thực thi truy vết được.


# 13. Hướng dẫn tích hợp từng bước: một vòng dự báo–tối ưu–điều khiển

Phần này là **walkthrough để cả 6 thành viên hình dung cùng một sự kiện**, không thay thế schema/JSON Schema chính thức. Mọi giá trị thời gian/số đo dưới đây là **fixture minh họa**, không phải kết quả thực nghiệm.

## 13.1. Một đối tượng đi xuyên suốt như thế nào?

Ví dụ nhóm `0x103`: h1 thuộc e1, h3 thuộc e2. Các đối tượng liên quan:

| Đối tượng | Ví dụ | Ai tạo? | Ai được dùng? |
|---|---|---|---|
| Flow group | `0x103` | Network | Toàn bộ hệ thống |
| OpenFlow cookie | `0x103` | Network/SDN theo quy tắc chung | SDN; cùng mã số với flow ID nhưng field khác |
| Path 1 | `0x103_P1` | Network | Optimization + SDN |
| Path 2 | `0x103_P2` | Network | Optimization + SDN |
| Fabric link | `e1>c1` | Network | Data + Optimization + Testing |
| Demand bin | `[99,100)`, `requested_mbps=2.5` | Network release | Data → ML |
| Snapshot | `snap_run001_100` | **SDN tạo; Data lưu** | ML, Optimization |
| Forecast batch | `fc_run001_100_M3` | **Data tự tạo mã batch** sau khi nhận đủ hàng dự báo từ ML | Optimization, Testing |
| Decision batch | `batch_run001_100` | Optimization/Runner | SDN, Data |
| Verified route | `0x103_P2`, `state_version=16` | SDN xác minh | Data mirror → Optimization |

**Không được suy sai:** cùng giá trị `0x103` không biến `flow_cookie` thành khóa DB thứ hai; P1/P2 **không** là ID toàn cục nếu thiếu mã nhóm; port number phụ thuộc topology; `forecast_batch_id` **không** phải `decision_batch_id`.

## 13.2. Mốc thời gian mẫu, không lệch 60 giây warm-up

Giả sử `traffic_started_at_utc=2026-11-12T09:00:00Z`, warm-up 60s và `measurement_started_at_utc=2026-11-12T09:01:00Z`.

Tại cutoff `t=100` (theo **measurement origin**):

- Lịch traffic offset tương ứng là **160 giây**, không phải 100 giây.
- `history_cutoff_utc=2026-11-12T09:02:40Z`.
- Với `L=30`, X gồm **bin 70 đến 99**, tương đương UTC `[09:02:10Z,09:02:40Z)`.
- Với `H=5`, forecast targets là **bin 100 đến 104**, UTC `[09:02:40Z,09:02:45Z)`.
- Ví dụ bin 99 được Network công bố lúc `09:02:40.010Z`, bản lịch sử hợp lệ có mặt lúc `09:02:40.035Z`, **SDN đóng snapshot** lúc `09:02:40.050Z` (ví dụ), Data lưu và ML dự báo sau đó. Các mốc chỉ minh họa; cần đo thời gian SDN→Data→ML thực tế.
- Vì suy luận có latency, `issued_at` **có thể muộn hơn `forecast_start`**, hoàn toàn bình thường. Optimization/SDN vẫn phải bảo đảm forecast chưa hết hạn, có thời gian guard để thực thi. Không tự dịch target từ `[100,105)` sang `[101,106)` để che latency; muốn đổi anchor phải tạo hợp đồng mới.

`released_at <= snapshot_as_of` và `available_at <= snapshot_as_of` đều bắt buộc; Data không được dùng row lịch tại t=100 chỉ vì file CSV đã được sinh trước toàn bộ.

## 13.3. Dữ liệu mẫu ở mỗi biên interface

Các JSON dưới đây minh họa **field nghiệp vụ**, chưa phải JSON Schema đóng băng. Metadata vận chuyển như `api_version`, `schema_version`, `producer`, `produced_at`, `config_version`, `config_hash`, `topology_version` có thể nằm trong **message envelope do Runner/Data adapter tạo** và phải nhất quán với RunManifest; **không được tự thêm chúng vào 7 trường của từng hàng `predictions` do ML gửi**.

**Bước A — Network công bố observation đã kết thúc:**

```json
{
  "event_id": "network:run001:demand:0x103:99",
  "run_id": "run001", "flow_group_id": "0x103", "bin_index": 99,
  "window_start_utc": "2026-11-12T09:02:39Z",
  "window_end_utc": "2026-11-12T09:02:40Z",
  "requested_mbps": 2.5,
  "released_at_utc": "2026-11-12T09:02:40.010Z"
}
```

Data ghi riêng `available_at_utc` là lúc row dùng được; không sửa lại timestamp phát hành Network. `requested_mbps=0` hợp lệ, còn dữ liệu thiếu phải là null + `reason`, không coi là 0.

**Bước B — SDN tạo snapshot lịch sử; Data nhận và lưu nguyên bản:**

```json
{
  "snapshot_id": "snap_run001_100",
  "run_id": "run001",
  "history_cutoff_utc": "2026-11-12T09:02:40Z",
  "snapshot_as_of_utc": "2026-11-12T09:02:40.050Z",
  "state_version": 15,
  "quality_status": "VALID",
  "history": [
    {"flow_group_id":"0x103","bin_index":99,"requested_mbps":2.5,
     "available_at_utc":"2026-11-12T09:02:40.035Z"}
  ]
}
```

Ở đây `history` bị **rút gọn** còn một row. **SDN** phải tạo nội dung đúng L* bins × 48 groups (hoặc báo thiếu minh bạch), chốt cutoff/route state/version và hash; **Data chỉ kiểm và lưu nguyên bản**. Nếu `quality_status=VALID` mà thiếu một nhóm/bin, contract test phải fail. Nguồn lịch sử requested_mbps do Network phát tới SDN cần chốt và kiểm chứng.

**Bước C — ML chỉ gửi các hàng dự báo (7 trường) cho Data:**

Giả sử ML đã **chọn L*=30, H*=5, epsilon*=0 từ validation** và lưu trong file cấu hình ML riêng; đây là thông số ví dụ, **không có trong payload dự báo**. Với một flow `0x103`, ML gửi 5 hàng sau (48 flow thì 240 hàng):

```json
[
  {"run_id":"run001", "flow_group_id":"0x103", "issued_at":"2026-11-12T09:02:40.150Z", "forecast_start":100, "forecast_end":101, "predicted_mbps":2.0, "source":"rf_v1"},
  {"run_id":"run001", "flow_group_id":"0x103", "issued_at":"2026-11-12T09:02:40.150Z", "forecast_start":101, "forecast_end":102, "predicted_mbps":2.5, "source":"rf_v1"},
  {"run_id":"run001", "flow_group_id":"0x103", "issued_at":"2026-11-12T09:02:40.150Z", "forecast_start":102, "forecast_end":103, "predicted_mbps":3.0, "source":"rf_v1"},
  {"run_id":"run001", "flow_group_id":"0x103", "issued_at":"2026-11-12T09:02:40.150Z", "forecast_start":103, "forecast_end":104, "predicted_mbps":3.5, "source":"rf_v1"},
  {"run_id":"run001", "flow_group_id":"0x103", "issued_at":"2026-11-12T09:02:40.150Z", "forecast_start":104, "forecast_end":105, "predicted_mbps":3.0, "source":"rf_v1"}
]
```

Đầu ra **không chứa** `horizon_steps`, `epsilon_ratio`, `epsilon_owner`, `raw_mbps`, `final_mbps` hay `mean_mbps`. Data đọc `H*=5` từ config/RunManifest đã freeze để kiểm đủ 240 hàng và tự gắn `forecast_batch_id` nội bộ; ML không cần xuất batch metadata này.

**Bước D — Data trả H hàng; Optimization tự tính mean rồi kết hợp current verified state:**

```json
{
  "run_id": "run001", "forecast_batch_id": "fc_run001_100_M3",
  "snapshot_id": "snap_run001_100", "expected_state_version": 15,
  "horizon_steps_from_frozen_config": 5,
  "prediction_rows_example": [
    {"flow_group_id":"0x103","forecast_start":100,"forecast_end":101,"predicted_mbps":2.0},
    {"flow_group_id":"0x103","forecast_start":101,"forecast_end":102,"predicted_mbps":2.5},
    {"flow_group_id":"0x103","forecast_start":102,"forecast_end":103,"predicted_mbps":3.0},
    {"flow_group_id":"0x103","forecast_start":103,"forecast_end":104,"predicted_mbps":3.5},
    {"flow_group_id":"0x103","forecast_start":104,"forecast_end":105,"predicted_mbps":3.0}
  ],
  "fabric_link_count": 16, "flow_count": 48,
  "route_state_source": "SDN_VERIFIED",
  "calibration_id": "calibration_example"
}
```

`prediction_rows_example` là phần dữ liệu Data trả về được **rút gọn chỉ còn một flow và bốn cột đã có trong database**; đây không phải schema ML→Data mới. Trong thực tế phải trả đủ 48 flow × H* hàng, chưa có `predicted_mean_mbps`. **Optimization tự tính `b_0x103 = (2+2.5+3+3.5+3)/5 = 2.8 Mbps`** rồi tạo 48 nhu cầu scalar cho solver, cùng 16 directed fabric links (capacity/k_e), 96 candidate paths và 48 current routes.

**Bước E — Chỉ khi trigger và nghiệm hợp lệ, Runner gửi lệnh apply:**

```json
{
  "run_id":"run001", "decision_batch_id":"batch_run001_100",
  "snapshot_id":"snap_run001_100", "expected_state_version":15,
  "config_version":"v1",
  "forecast_start":"2026-11-12T09:02:40Z",
  "forecast_end":"2026-11-12T09:02:45Z",
  "apply_deadline_utc":"2026-11-12T09:02:44Z",
  "changes":[
    {"flow_group_id":"0x103","old_path_id":"0x103_P1",
     "new_path_id":"0x103_P2"}
  ]
}
```

Tối thiểu phải kèm topology/config hashes trong envelope và `payload_hash` cho idempotency. Request ở ví dụ chỉ thể hiện đổi một flow; có thể còn tối đa B flow nếu được nghiệm MILP chọn. SDN không được tự đoán `old_path_id` hoặc port dựa vào `path_label` trống context.

**Bước F — SDN verify và trả kết quả:**

```json
{
  "run_id":"run001", "decision_batch_id":"batch_run001_100",
  "status":"APPLIED", "state_version":16,
  "completed_at":"2026-11-12T09:02:40.850Z",
  "results":[
    {"flow_group_id":"0x103","status":"APPLIED",
     "actual_path_id":"0x103_P2","rule_count":3,
     "verification_status":"VERIFIED", "reason":null}
  ]
}
```

`rule_count` minh họa; giá trị thực phải được controller đo/đếm, không hard-code. Nếu verify không xong, actual có thể null + `UNKNOWN`, batch `INCONSISTENT`, không trả APPLIED. Data lưu quyết định, response, event, routing state mirror bằng các write idempotent.

## 13.4. Bảy trạng thái của một control tick và quyền làm tiếp

| Điều kiện tại tick | Xử lý | Output cần lưu | Có gọi SDN? |
|---|---|---|---|
| Không đủ lịch sử hợp lệ | Skip dự báo, reason `DATA_MISSING`/`DATA_STALE` | `control_ticks` + quality | Không |
| ML timeout/forecast batch INVALID | Skip hoặc fallback đúng policy đã freeze | Source/fallback reason | Không nếu không có READY |
| Dự báo READY nhưng chưa đủ N tick vượt TH | `NO_TRIGGER` | U_current, trigger counter | Không |
| Hold-down/B hoặc solver không có nghiệm đạt | `KEPT` | Solver status, J_old/J_new và reason | Không |
| Nghiệm đạt, route/state/deadline hợp lệ | Tạo ApplyRequest | batch ID, expected version | Có |
| Controller đang APPLYING/RECONCILING | Không submit batch mới | `BUSY`/`SKIPPED_IN_FLIGHT` | Không |
| Controller trả PARTIAL/INCONSISTENT | Block tick tiếp, reconcile | Actual per-flow hoặc UNKNOWN | Chỉ thao tác khôi phục |

**Không trộn `NO_TRIGGER`, `KEPT` với status cuối của ApplyResponse.** Một tick không đổi đường là event Optimization/Runner, không nhất thiết là execution batch gửi SDN.

