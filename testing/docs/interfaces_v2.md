# Giao diện tích hợp tổng hợp cho kiểm thử SDN

- Phiên bản `testing_v2 2026-11-14`, đồng bộ Integration Guide `1.4-network-id-trimmed`.
- API mục tiêu `sdn_qa_api_v2`; schema/API version triển khai phải được khóa cùng contract, không coi nhãn này là API đã hiện thực.
- Phụ trách Testing/QA/QC.
- Trạng thái: hoàn thiện đặc tả để owner xác nhận, chưa nghiệm thu triển khai.

Nguồn đối chiếu là [Integration Guide v1.4](../../docs/architecture/SDN_Traffic_Engineering_Role_Integration_Guide_v1.4.md), đặc biệt mục 3–5 và 13. Các quy tắc ID, snapshot, 7-field predictions và mean/epsilon dưới đây thay thế quy ước cũ của Testing; chưa phải xác nhận API đã hiện thực. Theo thứ tự ưu tiên ở Guide mục 1, contract đã được nhóm duyệt → schema chung → config chung → tài liệu role → code; khác biệt phải được ghi nhận và sửa đồng bộ, không tự viết adapter riêng để giữ hai chuẩn. Owner khóa các mục OPEN/MUST_LOCK trong [test_plan_v2.md](test_plan_v2.md) trước freeze.

Ranh giới: Network tạo tải/demand release; **SDN tạo snapshot và xác minh route**; **Data là DB writer duy nhất, lưu/cấp snapshot nguyên bản**; ML chọn L*/H*/epsilon* offline rồi gửi đúng 7 trường dự báo final; Optimization tự tính mean và quyết định; Runner gửi ApplyRequest; Testing đối chiếu độc lập. Các chi tiết vận chuyển, schema mở rộng và ngưỡng pilot dưới đây là đề xuất cần freeze; không mở lại các quyết định ownership đã xác nhận ở v1.4.

## 1. Danh mục và quyền sở hữu

| ID | Producer → Consumer | Nội dung | Owner xác nhận |
| --- | --- | --- | --- |
| IF01 | Network → SDN/Data/Optimization | TopologyBundle: host/switch/link/flow/path/initial map | Network; SDN đối soát port |
| IF02 | Network → Data/Runner | ScheduleBundle: CSV + manifest/hash | Network+Data |
| IF03 | Network/SDN → Data/Testing | PacketLog, ProbeLog, CounterLog | Producer; Data kiểm nguồn đo |
| IF04 | Data → ML | TrainingBundle: X/y_vector/metadata/split; ML/Testing tính mean nhãn khi đánh giá offline | Data+ML |
| IF05 | SDN → Data (lưu) → ML/Optimization (đọc) | Snapshot lịch sử causal do SDN tạo, ID/hash/cutoff và confirmed state | SDN tạo; Data validate/lưu/cấp |
| IF06 | ML/baseline → Data → Optimization | Đúng 7 trường/hàng; Data tạo ForecastBatchRef READY đủ 48×H*; Optimization tự mean | ML+Data+Optimization |
| IF07 | Data → Optimization → Runner/Data | H* rows/flow+state → Optimization tính mean, proposal/audit | Optimization |
| IF08 | Runner/Orchestrator → SDN → Data | ApplyRequest từ proposal đã validate; ApplyResponse, verified state/history | Optimization+SDN+Data |
| IF09 | Tất cả → Data/QA | Events, control ticks, metrics và provenance | Data+QA |
| IF10 | Runner/QA → tất cả | RunManifest/frozen config/seed/order | Runner+QA, các owner echo version |

Coordinator là vai trò điều phối logic, có thể nằm trong runner; không bắt buộc thêm service. Transport thực tế (Python IPC/REST/queue), encoding, timeout/retry/ack cần owner khóa và chạy contract tests; hiện MUST_LOCK. Không coi chữ ký logic trong tài liệu là API đang tồn tại.

## 2. Quy ước định danh và thời gian

### 2.1. Namespace chuẩn theo Network

| Khái niệm | Canonical | Nguồn cũ và chuyển đổi |
| --- | --- | --- |
| Nhóm có hướng | `flow_group_id=0x103` | Chuỗi hex Network `0x{src×256+dst:X}`; h1→h3 = `0x103` |
| OpenFlow cookie | `flow_cookie=0x103` | Cùng trị số mã nhóm nhưng là field kỹ thuật OpenFlow; không thay flow object hoặc match rule |
| Đường | `path_id=0x103_P1` / `0x103_P2` | Quy ước Data `<flow_group_id>_P1/_P2`; Network xuất catalog chính thức |
| Nhãn đường | `path_label=P1` / `P2` | P1 qua c1, P2 qua c2 |
| Link có hướng | `link_id=e1>c1` | Lấy nguyên catalog Network; `c1>e1` là link khác |
| Batch forecast | `forecast_batch_id=fc_001` | Khác batch điều khiển; nhóm nhiều prediction rows |
| Batch thực thi | `decision_batch_id=batch_001` | Optimization.batch_id → decision_batch_id |
| Trạng thái | `state_version` integer toàn run | Không dùng bộ đếm riêng từng flow |
| Phiên bản | config_version/topology_version/schema_version/api_version | Version có hash nội dung kèm; không dùng chuỗi v1 thay checksum |

Network xuất mapping máy đọc được; schema/fixtures/API/serializer/test phải dùng cùng catalog. `h1_h3`, `h1_h3_c1`, `e1_to_c1` chỉ là ví dụ **ID cũ bị loại**, không phải alias được chấp nhận online. Nếu chuyển dữ liệu cũ, migration có version/hash và kiểm tra ánh xạ trước ingest; không đổi dữ liệu Network theo chuẩn Testing cũ. Thứ tự vector 48 nhóm theo giá trị số của flow ID hex tăng dần, song consumer phải join bằng `flow_group_id` và kiểm tập ID, không chỉ zip theo vị trí.

### 2.2. Origin và khoảng thời gian

Mỗi run có ba mốc rõ: run_started_at_utc (khởi động tiến trình), traffic_started_at_utc (bắt đầu replay, gồm warm-up), measurement_started_at_utc (bắt đầu360s đo). Không giả chúng trùng nhau. Dùng timestamp UTC RFC3339 trong API, offset số thực trong bảng vật lý theo origin khai báo. Duration dùng clock monotonic; không trừ hai monotonic khác miền clock.

Baseline đề xuất:

- `demand_plan.time_offset_s`: từ traffic_started_at_utc.
- `traffic_actual.window_start/end`: từ traffic_started_at_utc, giữ semantics Data §6.1.
- `predictions.forecast_start/end`: từ measurement_started_at_utc, giữ semantics ML §6.3.
- Snapshot/ApplyRequest UTC; mọi interval `[start,end)`. ML d[k] là giá trị trong [k,k+1) theo measurement origin; các bin warm-up âm có thể là lịch sử hợp lệ.
- Quy đổi `utc=declared_origin+offset_s`; pilot warm-up 60s, measurement=traffic origin+60s và forecast bin100 thì traffic offset160. Không cộng warm-up hai lần; luôn dùng origin thực trong manifest.

Ví dụ TC-I03 theo Guide mục 13.2: traffic `2026-11-12T09:00:00Z`; measurement `09:01:00Z`; forecast offsets100..105 → UTC `09:02:40Z`..`09:02:45Z`. Ví dụ run origin trong báo cáo chỉ áp khi field đó thực sự khai báo run origin.

Ở cutoff t, X lấy [t−L,t), Y lấy [t,t+H); mẫu online chỉ dùng khi `window_end_utc≤history_cutoff_utc`, `released_at_utc≤snapshot_as_of_utc` và `available_at_utc≤snapshot_as_of_utc≤decision time`. `issued_at` là lúc forecast phát hành, có thể muộn hơn forecast_start và không bị backdate để khớp bin. `forecast_start` là target anchor t. Late history không được nhập lại snapshot SDN đã đóng.

### 2.3. Đơn vị và envelope

Mbps=1.000.000 bit/s; payload bao gồm metadata ứng dụng. requested_mbps, sent_bytes, received_unique_bytes và Port TX bytes có semantics khác nhau. Dữ liệu thiếu=null+reason; 0 là giá trị thật. Demand hữu hạn≥0,capacity>0,k_e>0; dự báo lớn hơn capacity có thể hợp lệ. NaN/Infinity không hợp lệ JSON; ghi null+reason trong lỗi. Không clip utilization về1.

Envelope vận chuyển do Runner/Data adapter tạo gồm api_version,schema_version,run_id,producer,produced_at,config_version,config_hash,topology_version,topology_hash; payload_hash dùng cho idempotency. **Payload ML→Data chỉ là PredictionRows đúng 7 trường**, không yêu cầu ML xuất envelope, batch ID hoặc config metadata bổ sung. Envelope/audit liên kết registry/RunManifest ở ngoài rows. Ingest có khóa/record ID, provenance raw file+line/hash; conflict không overwrite im lặng.

## 3. IF01 TopologyBundle

| Danh mục | Trường tối thiểu |
| --- | --- |
| switches | switch_id,dpid (hex string hoặc integer chuẩn hóa),ports |
| hosts | host_id,ip,mac,edge_switch_id,edge_port |
| links | link_id,src_node_id,src_port,dst_node_id,dst_port,capacity_mbps,delay_ms,queue_packets,link_type |
| flow_groups | flow_group_id,flow_cookie,src_host_id,dst_host_id,src_edge_id,dst_edge_id |
| candidate_paths | path_id,flow_group_id,path_label,via_spine,ordered_link_ids |
| initial_paths | flow_group_id,path_id; hash chung mọi method |
| control | OF1.3,controller endpoint,fail-mode,ARP/miss/same-edge policy |

DPID e1..e4=1..4,c1=5,c2=6; edge port1/2 host,3 c1,4 c2; spine port1..4 e1..e4. 8 host/6 switch/48 directed groups/96 candidates/16 fabric directions/32 all directions. Mỗi path4 link có hướng, đúng2 fabric links, không vòng. Cặp cùng edge phục vụ chuyển tiếp riêng, ngoài F tối ưu. Mỗi fabric direction xuất hiện trong12 candidates; tổng incidence192.

Ví dụ `0x103_P1`: `[h1>e1,e1>c1,c1>e2,e2>h3]`. Match phải lấy IP/MAC từ config; không tự suy MAC. Tham số baseline10Mbps mỗi chiều,1ms host/5ms fabric,queue50 là cấu hình thiết kế; k_e/measurement point có calibration_id, chưa đo không ghi đã hiệu chuẩn.

## 4. IF02 ScheduleBundle và IF10 RunManifest

CSV khóa `(schedule_id,flow_group_id,time_offset_s)`; cột requested_mbps; core dense48 dòng/bin, zero explicit. Manifest ghi scenario/rho/seed/parent_family_id/split, origin,Δ=1s,duration_s,generator version/params,packet payload,content hash,series fingerprint,config version/hash,topology version,CORE/HOST_STRESS label,validation report. Nếu rescale/cap, lưu thuật toán và tỷ lệ biến đổi thực; không khẳng định S2b/S5 luôn đạt rho danh nghĩa.

Lịch phải phủ warm-up 60s + measurement 360s theo pilot. Theo Guide mục 3.4, chỉ phát hành forecast khi `forecast_end≤measurement_end`; với H*=10, cutoff cuối hợp lệ là350. M1–M3 dùng cùng policy; oracle dùng cùng tập target khi so sánh. Label tail là thay đổi cần duyệt/freeze riêng (Guide mục 11), không mặc định thêm tail hoặc trộn tail vào metrics chính.

Split trước window theo seed/run family. **Tỷ lệ train/validation/test đã xác nhận là70/15/15**, tương ứng14/3/3 khi tập chia có20 seeds; Guide mục3.5, YAML mục3.6 và bảng mục6 dùng thống nhất tỷ lệ này. Còn MUST_LOCK là danh sách seed cụ thể, phạm vi áp dụng theo scenario/cấu hình và hash của `split_manifest`. Tổng schedules/runs lấy từ `benchmark_matrix` đã khóa; không mặc định225 runs hoặc coi mỗi cấu hình rho là một scenario có20 seeds nếu chưa được xác nhận.

RunManifest ghi experiment/run/schedule/method/scenario,split,parent family,schedule/model/solver/order/fault seeds,repeat/attempt/order index,code/env/model/config/topology hashes,Δ/T_ctl/MA_window,**ml_config_ref/hash/version và L*/H*/epsilon* đã được ML chọn offline**,initial_paths_hash,calibration_id,origins UTC+mono,measurement_end/drain_end,timeout/guard/deadline policy,files/hashes,status/quality/inclusion/reason/supersedes_run_id. Runner freeze config trước run; Data đọc H* từ config/manifest này để kiểm48×H*, không suy H từ số rows nhận được hoặc yêu cầu ML gửi config theo mỗi forecast. Những trường chỉ biết sau run bổ sung khi hoàn tất, không thay config đã khóa hoặc giả trị số.

Method M3 không đổi thành M1 khi fallback. Data runs `ok/failed/discarded` là reporting view; lưu status chi tiết ở manifest/source table: CREATED/RUNNING/COMPLETED/FAILED/INVALID_INFRASTRUCTURE. Inclusion riêng INCLUDED/EXCLUDED/PARTIAL_METRICS/PENDING_QA. Giữ mọi attempt.

## 5. IF03 Dữ liệu đo thô

| Log | Trường bắt buộc |
| --- | --- |
| Sender | run_id,flow_group_id,sequence_number,send_at,send_mono_ns,payload_bytes,send_success,socket_error,scheduled_offset_s,sender_lag_ms |
| Receiver | run_id,flow_group_id,sequence_number,received_at,receive_mono_ns,payload_bytes,receiver_id |
| Probe | run_id,probe_id,flow_group_id,sent_at,received_at nullable,status,rtt_ms nullable; path evidence refs nếu có |
| Counter | run_id,sample_id,link_id/switch_id+port_no+direction,xid,request_at,received_at,sample_at,available_at,raw_counter_bytes,counter_epoch,counter_source,measurement_point,counter_layer,valid/reason |

Sequence duy nhất theo run/group, không reset mỗi giây. Nếu restart phải có epoch trong khóa hoặc invalid run. Giữ duplicate raw. send_success chỉ nghĩa syscall nhận payload, chưa chứng minh qua qdisc. Tính loss bằng join sequence/cohort/deadline, không trừ byte hai cửa sổ nhận/gửi riêng.

Counter chuẩn một chiều dùng PORT_TX ở nguồn; không cộng TX nguồn+RX đích. Mapping host TX không nhất thiết có OpenFlow port ở host: Network cung cấp nguồn host counter riêng hoặc ghi unavailable; MLU chỉ yêu cầu16 fabric. Mẫu đầu/reset/epoch đổi/Δt≤0: delta/throughput=null,valid=false; lấy baseline mới. Không giả counter đọc đúng tick; sample_time_basis và uncertainty phải khai báo nếu dùng request/reply estimate. SDN hoặc Data có thể tính derivative nhưng chỉ một implementation là authoritative, Data recompute đối chiếu được.

S0 hiệu chuẩn `k_e=median(port_tx_mbps/offered_payload_mbps)` theo từng directed link ở tải thấp không mất gói, lặp nhiều mức, lưu số mẫu/độ phân tán/sai số/point/version. Mẫu số là payload thực phát từ sender; không dùng receiver goodput hoặc requested rate trong lúc sender bị throttle. Chưa đủ calibration dùng `k_e=1.0` và gắn `UNCALIBRATED`; không nhân k_e lên throughput/actual MLU đo lần nữa. Điểm đếm trước shaping phải được gắn nhãn offered load; không tuyên bố egress utilization khi chưa chứng minh.

## 6. IF04 TrainingBundle

Khóa sample_id nối X,y_vector,sample_metadata một–một; ML/Testing có thể tính y_mean để đánh giá offline theo cùng sample_id. Metadata: schedule/group,split,parent family,cutoff t,origin,Δ,L,H,feature schema/order,dtypes,source hashes. Split trước window, cùng lịch/parent family ở cùng split; seed và fingerprint không rò chéo. Không nối lịch/run/group khi tạo window.

Với Δ1s và L30,H5,t100: X từ bins70..99; y_vector từ100..104; y_mean=sum(y_vector)/5. Nếu cần thứ tự demand_lag_1 gần nhất nhưng vector cũ→mới, feature_schema phải nêu mapping rõ. X/y_vector là đầu ra Data; nếu triển khai còn chỉ có nhãn scalar thì phải bổ sung multi-output trước tích hợp. y_mean là phép tính đánh giá offline của ML/Testing, không dùng thay nhãn train H bước hoặc giao Data tính mean dự báo cho Optimization.

Offline dùng lịch đầy đủ để tạo nhãn hợp lệ; online không truy cập lịch tương lai. Fit preprocessing trên train; tuning/epsilon/MA window trên validation; test độc lập. Scenario/seed/schedule future params không vào feature. One-hot group ID chỉ nhận diện48 nhóm, không tự cấm nếu đúng ML design; mapping version phải khóa.

## 7. IF05 Snapshot online

Chữ ký logic theo Guide mục4.2:

```python
# SDN tạo và phát hành; lookback_L lấy từ frozen ML config
create_snapshot(run_id, cutoff, lookback_L) -> Snapshot
# Data validate, lưu nguyên bản và chỉ đọc lại bản SDN đã phát hành
ingest_snapshot(sdn_snapshot) -> SnapshotStoreAck
get_stored_snapshot(run_id, cutoff) -> Snapshot
```

Snapshot có snapshot_id,snapshot_as_of_utc,history_cutoff_utc,state_version,config_version/hash,topology_version/hash,content_hash,origin refs,flow_group_ids,history[],routing_state[],link_samples refs,quality_status,missing_groups,routing_state_complete,unknown_flow_group_ids. SDN chốt ID/hash/nội dung; Data không tự gán ID, tạo lại snapshot từ query hoặc sửa snapshot sau ingest. Metadata nhận/lưu của Data nằm riêng. SDN chịu trách nhiệm causality chính; Data kiểm schema/ID/hash/cutoff tại boundary và từ chối bản sai.

Mỗi history item: flow_group_id,window_start_utc/window_end_utc,requested_mbps,released_at_utc,available_at_utc,valid/reason,release_record_id. Snapshot VALID phải đủ48 nhóm×L* bins, đáp ứng cả ba bất đẳng thức ở §2.2. Network release bin đã hoàn tất; Data ghi available time thực. **API/kênh cấp requested_mbps đã release/available tới SDN vẫn MUST_LOCK** giữa Network/SDN/Data; không dùng goodput hoặc port counters để suy demand, không coi lịch tạo trước run là available, không giả availability. Online M1–M3 chỉ thấy snapshot đã lưu; forecast labels/oracle store tách quyền. Metamorphic test sửa tương lai ẩn nhưng giữ history thì M1–M3 output không đổi.

Age từ history cutoff/nguồn dùng thực, không từ lúc worker trả output. T_stale đề xuất2Δ cần khóa. Nếu latest bin chưa available: chờ hữu hạn trong budget hoặc skip/fallback theo policy đã freeze; không dịch target âm thầm và không gắn VALID cho snapshot thiếu L* bins. History không hợp lệ hoặc batch thiếu thì skip tick. Nếu route UNKNOWN/INCONSISTENT thì skip toàn tick, log KEPT. Snapshot trong apply/reconcile bị chặn hoặc mang routing_state_complete=false; không cấp confirmed state giả. PARTIAL/INCONSISTENT phải reconcile trước tick tiếp theo.

## 8. IF06 H-step predictions và mean adapter

### 8.1. Input và output ML→Data

`predict_batch(snapshot, frozen_ml_config) -> list[PredictionRow]`: input là snapshot SDN do Data cấp lại và config L*/H*/epsilon*/feature schema do ML tự chọn offline. Không có labels/future schedule. Δ=1s nên H* giây tương ứng H* bước. ML tạo48 vector×H* giá trị nội bộ rồi chỉ xuất48×H* hàng final dưới đây. Model/version/latency/reason thuộc artifact/audit riêng, không phải trường bổ sung của result gửi Data. Chuẩn hóa âm theo công thức §8.3; output final âm/NaN/inf bị reject.

Mỗi PredictionRow có **đúng 7 trường**:

| Cột | Kiểu / semantics |
| --- | --- |
| run_id | TEXT; đúng run |
| flow_group_id | TEXT canonical Network hex, ví dụ `0x103` |
| issued_at | UTC timestamp; cùng toàn đợt phát hành, không tạo từng group bằng now() |
| forecast_start | REAL offset measurement origin, đầu một bin |
| forecast_end | REAL offset measurement origin, cuối bin=start+1 |
| predicted_mbps | REAL final prediction của bin; ML đã áp epsilon* theo frozen config đúng một lần |
| source | TEXT provider, ví dụ rf_v1/persistence/moving_avg/fallback_persistence/oracle; cùng source trong batch |

**PK:** `(run_id,flow_group_id,issued_at,forecast_start)`. Khóa3 trường cũ không chứa được H dòng; triển khai còn dùng khóa cũ phải migrate trước tích hợp. Mỗi run có một đợt forecast tại một issued_at; registry bắt conflict nhiều source cho cùng group/bin. Model khác dùng run hoặc thời điểm phát hành khác; thêm batch ID ngoài bảng không tránh được xung đột PK này. Không overwrite một giá trị.

### 8.2. Batch registry và completeness

Data thực hiện `commit_prediction_rows(rows_7_fields) -> ForecastBatchRef` và tự tạo registry nội bộ: run_id,forecast_batch_id,snapshot_id,issued_at,source,config/schema/model refs,full target UTC range,H* từ frozen config,expected_rows=48×H*,actual_rows,ready_at,status,content_hash. Snapshot/config refs lấy từ ngữ cảnh tick do Runner/Data quản lý; không buộc ML gửi thêm trường ngoài7 fields. Nếu không liên kết duy nhất được rows với tick/config thì reject, không đoán. `forecast_batch_rows` map batch với PK các rows.

WRITING → READY chỉ sau validate đủ48 nhóm×H* contiguous bins, không duplicate/gap/mixed issued_at/**source**/target/config. H* lấy từ config/RunManifest đã khóa, không suy từ số hàng nhận. NOT_READY/INVALID/EXPIRED không được Optimization đọc như complete. Ingest lại cùng rows+hash idempotent, trả lại cùng batch ref; cùng khóa khác nội dung conflict. Chỉ đọc atomically batch READY, ready_at≤decision time. Transaction/commit marker và cách gắn tick là chi tiết vận chuyển MUST_LOCK; registry không phải output ML.

### 8.3. Vector, mean và epsilon

Data→Optimization trả **H* rows/flow** từ batch READY, không tạo scalar mean. Optimization tự tính `b_f=mean(predicted_mbps[0:H*])` rồi tạo vector48 scalar cho solver; mean chỉ là derived audit của Optimization. Fixture `[2,2.5,3,3.5,3]` → b=2.8 Mbps; H*=5,F=48→240 rows.

**ML-owned epsilon theo Guide mục5.3:** `final=max(0,(1+epsilon*)×raw)`, Data lưu final, Optimization không nhân epsilon nữa. ML chọn L*/H*/epsilon* trên validation, lưu `ml_config.yaml`/model metadata riêng và Runner pin hash trong RunManifest; raw forecast nếu cần chỉ ở artifact nội bộ ML. Không gửi L/H/epsilon/raw/final/mean/epsilon_owner/horizon_steps hoặc batch metadata trong payload ML→Data; `predicted_mbps` đã là final. Epsilon candidates0/0.05/0.1/0.2 là đề xuất; không cải thiện hoặc hòa với0 thì chọn0. Baseline/Oracle config phải freeze riêng; epsilon0 cho các phương pháp này là đề xuất, chưa phải kết quả được duyệt. Muốn kết luận riêng lợi ích RF cần ablation hoặc calibration policy chung đã khóa.

Fixture raw4,eps.1,k1 → final/b_used4.4, không4.84. Tolerance numeric_epsilon không phải epsilon dự báo. Không có policy Optimization-owned trong v1.4.

Fallback khi RF lỗi nhưng history hợp lệ có thể dùng Persistence với `source=fallback_persistence`. Batch READY không trộn RF/fallback source. **Đề xuất cần freeze:** tạo lại toàn bộ48×H* hàng Persistence, cùng source và issued_at, mọi giá trị vẫn causal/còn hạn; reason lưu audit riêng. Nếu history không hợp lệ, batch thiếu hoặc chưa có policy fallback được khóa thì skip toàn tick. M3 vẫn method=M3; degraded/fallback là nhãn quality riêng, không thay readiness READY. Báo riêng native/fallback frequency; M1/M2 dùng cùng target và history availability. M0 không tạo dự báo giả; M4 đọc future demand chỉ trong evaluation offline, không deploy.

## 9. IF07 Optimization và audit

`get_optimization_input(run_id,forecast_batch_id)` do Data cung cấp gồm run/snapshot/forecast_batch IDs,expected_state_version,config/hash,đủ48×H* prediction rows,16 fabric capacities,k_e/calibration refs,candidate paths/current verified paths/last_successful_change_at,cutoff/target UTC/deadline. Optimization tra H* từ frozen config và tự mean thành b vector; TH/N/h/B/hold/lambda và numeric tolerances từ config chung. Thiếu rows hoặc current UNKNOWN thì skip tick. Không cộng counter load của chính F lên predicted load lần nữa.

Mô hình chuẩn khi ML đã hiệu chỉnh: `L_e=k_e Σ_f,p b_f a_fpe x_fp`; `Σ_p x_fp=1`; `L_e≤U C_e`; `y_f=1−x_f,current`; `Σ y_f≤B`; hold chưa đủ thì x_current=1; x/y binary,U≥0. F cố định48 kể cả nhóm0. Không hard cap U≤.9 hay1. Dùng J=U+lambda×n_changes/48; current có n_changes0 nên J_old=U_current.

Trigger strict U_current>TH trong N tick hợp lệ liên tiếp. Baseline đề xuất N3; invalid/stale/busy/reconciled-state-change reset count; sau batch final reset. Acceptance: actual proposed changes≥1,J_old>numeric_epsilon,J_new≤(1−h)J_old theo tolerance khóa, đủ hạn/state đúng. Independent validator recompute constraints/U/J, không chỉ tin solver output.

Output `OptimizationResult`/audit: decision_batch_id,control_tick_id,forecast_batch_id,snapshot_id,state/config refs,target UTC,U_old/U_new,J_old/J_new,solver_status,solve_ms/gap/bound,has_incumbent,trigger_count,full_routes,changes,validation_status,disposition/reason. `solver_status` theo Guide mục5.4: OPTIMAL/FEASIBLE/INFEASIBLE/TIME_LIMIT/ERROR; mapping từ solver cụ thể phải khóa. NOT_RUN/NO_INCUMBENT/INVALID_RESULT là outcome/reason kiểm thử, không thay enum trên wire; chưa solve thì solver_status=null. Pilot ưu tiên OPTIMAL. FEASIBLE hoặc TIME_LIMIT có incumbent chỉ dùng khi config cho phép, có max_gap đã khóa và đáp ứng gap, integer feasible, validator độc lập pass, đủ improvement/state/deadline. Chưa khóa policy/max_gap thì giữ đường với nghiệm chưa chứng minh OPTIMAL. Không có nghiệm hợp lệ→KEPT, không bịa OPTIMAL. Chỉ Runner/Orchestrator tạo ApplyRequest; Optimization không gọi Ryu trực tiếp.

Ví dụ F48,lambda.1,U_new.58: một đổi J_new=.5820833333; bốn đổi J_new=.5883333333. Ví dụ .61 của Optimization không khớp các tham số này. Fixture F2,b6/6,k1,C10 cùng nhánh→U_old1.2; đổi1→U_new.6,J_new.65. Phân biệt mẫu số F2 fixture và F48 production.

## 10. IF08 ApplyRequest và ApplyResponse

Theo Guide mục4/5/13, canonical request `decision_batch_id/changes` do Runner/Orchestrator tạo sau OptimizationResult hợp lệ. Metadata solver lưu audit riêng, không buộc SDN giải lại MILP. Ví dụ dưới đây cùng timeline §2.2, giá trị chỉ là fixture.

```json
{
  "run_id":"run_001","decision_batch_id":"batch_001","snapshot_id":"snap_100",
  "expected_state_version":15,"config_version":"v1",
  "forecast_start":"2026-11-12T09:02:40Z","forecast_end":"2026-11-12T09:02:45Z",
  "apply_deadline_utc":"2026-11-12T09:02:44Z",
  "changes":[{"flow_group_id":"0x103","old_path_id":"0x103_P1","new_path_id":"0x103_P2"}]
}
```

Ví dụ lõi; envelope ở §2.3 mang topology/config hashes và payload_hash đi cùng transport/audit. Lệnh dùng full path ID từ catalog; P1/P2 chỉ là path_label, không chấp nhận làm path_id thiếu flow context. `forecast_start/end` ở ApplyRequest là UTC, khác offset của PredictionRows. target_interval5s không đủ thay target timestamps. Validation trước write: run/config/topology/snapshot/version đúng, old_path VERIFIED, membership hợp lệ, changes không trùng và1..B, hold-down theo confirmed time, chưa qua apply_deadline_utc và còn guard trước forecast_end, không batch khác flight.

```json
{
  "run_id":"run_001","decision_batch_id":"batch_001","status":"APPLIED",
  "state_version":16,"completed_at":"2026-11-12T09:02:40.850Z",
  "results":[{"flow_group_id":"0x103","actual_path_id":"0x103_P2","status":"APPLIED",
  "verification_status":"VERIFIED","reason":null,"rule_count":1,
  "applied_at":"2026-11-12T09:02:40.850Z"}]
}
```

| Batch final | Điều kiện |
| --- | --- |
| APPLIED | Tất cả changes đã xác minh thành công |
| REJECTED | Validation fail trước FlowMod; version không đổi |
| FAILED | Không hoàn thành, trạng thái cuối biết rõ; không còn thay đổi chưa đối soát |
| ROLLED_BACK | Mọi thao tác bị tác động đã khôi phục và xác minh |
| PARTIAL | Kết quả per-group khác nhau, tất cả actual đều biết |
| INCONSISTENT | Có ít nhất một UNKNOWN; actual=null cho nhóm đó |

Group final: APPLIED/KEPT/FAILED/ROLLED_BACK/UNKNOWN. PROPOSED/APPLYING/RECONCILING là trạng thái trung gian trong event/batch record, không final response. Status không đồng nhất reason code.

Duplicate `(run,decision_batch_id)` cùng payload_hash→trả kết quả cũ, không thêm FlowMod; nếu chưa final trả ack PROCESSING theo transport khóa, không giả final APPLIED. Khác payload→BATCH_ID_CONFLICT; batch mới khi bận→BUSY. Timeout phía client không có nghĩa Controller chưa write; phải query/reconcile trước retry. Retry cùng ID giữ semantics, không tạo ID mới để né dedup.

Preinstall downstream hai nhánh theo baseline; khi cần cài/cập nhật đường mới, downstream trước → Barrier/ack → source uplink3↔4 sau → verify → mới dọn rule cũ. Barrier chỉ là evidence xử lý thông điệp; verify read-back rule và probe/counter đúng nhóm/nhánh khi phù hợp. Traffic0 dùng probe; check đến đích và đúng nhánh riêng. Một batch liên switch không atomic; mọi FlowMod kể cả lỗi/rollback được đếm. `rule_count` phải đo/đếm thực, không hard-code theo ví dụ.

Forecast expiry/apply deadline phải kiểm trước write. Nếu write rồi verify trễ forecast_end, ghi deadline_missed và reconcile actual; không che thay đổi có thật bằng REJECTED hoặc đường cũ. Kết quả xác minh thành công muộn vẫn ghi actual cùng late flag, là lỗi vận hành deadline để QA đánh giá. PARTIAL/INCONSISTENT→chặn tick tiếp theo đến khi reconcile; UNKNOWN không được điền old_path làm actual. Ryu restart cũng phải reconcile actual routes trước nhận request mới; chi tiết phục hồi cần freeze. Không queue proposal cũ để apply sau.

## 11. Verified state và Data ETL

| Source table / khóa | Nội dung phải bảo toàn |
| --- | --- |
| routing_state / (run,group) | path_id nullable,state_version,confirmed_at,last_successful_change_at,verification_status,config_version |
| routing_history / (run,group,valid_from) | path_id,valid_from/to,status INITIAL/APPLIED/ROLLED_BACK,batch_id nullable |
| decision_batches / (run,batch) | payload_hash,snapshot/forecast refs,expected/result version,target/received/completed,U/J/solver,status/reason |
| routing_decisions / (run,batch,group) | old/proposed/actual path,status/reason,applied_at,rule_count,evidence refs |
| link_statistics / (run,link,sample_id) | raw bytes,delta,time/source/point/epoch,valid/reason |
| event_log / (run,event_id) | component,event_at,severity,batch/flow/switch/xid,detail JSON |

**SDN là nguồn sự thật duy nhất của live routing state/history; Data là DB writer duy nhất**, ghi mirror/event theo kết quả SDN, không tự đổi actual route hoặc tự tăng version. Chi tiết đề xuất cần freeze: tăng global state_version tối đa1 lần ở final batch nếu confirmed state hoặc độ chắc chắn thay đổi; đồng bộ mọi state row của run, bao gồm mốc hold-down/confirmed metadata thay đổi có ý nghĩa; reject trước write không tăng. Không tăng vì tiếp nhận proposal. Trạng thái vật lý có thể chuyển qua nhiều bước trong batch: giữ event trace, chưa công bố confirmed snapshot mới đến final. Final rollback về path cũ nhưng timestamp lần đổi verified thay đổi cũng phải invalidate snapshot cũ; owner xác nhận rule này trước freeze. Controller retry event cùng event_id khi chưa được Data ack; Data ingest idempotent, restart phải reconcile.

Hold-down dùng lần chuyển actual path đã verify, kể cả verified rollback làm đổi path; không cập nhật bởi proposal/thao tác lỗi chưa biết. Bootstrap last_successful_change_at=null, Optimization hiểu chưa có lần đổi/không khóa. last_optimization_change_at có thể lưu riêng cho metric chính.

History các khoảng verified không chồng, tối đa1 open row/group. Khi UNKNOWN đóng khoảng trước ở mốc mất xác nhận; path_id=null ở state và gap+event riêng, không ép tạo history path_id=null trái schema SDN. Muốn truy vấn đầy đủ timeline tạo derived view gồm verified intervals và uncertainty intervals. Không khẳng định mốc verify chính là lúc packet đầu chuyển nhánh.

Data giữ bảng báo cáo gốc nhưng migrate/extend source tables. `decisions` là view từ routing_decisions/batch, không bỏ actual/batch/version. `raw_counter_bytes→counter_bytes`, `link_statistics→link_samples`, `event_log→events`; lower-case status chỉ trong view có mapping enum. Event auto integer trong DB không thay producer event ID: thêm unique producer ID/run để ingest lặp không nhân bản. Source event_id phân namespace process, không va khi gom log.

Invariants: response.state_version=batch.result_state_version=state snapshot toàn run tại final; response per-group=stored routing_decisions; UNKNOWN→actual null/state không VERIFIED; history không overlap; duplicate không thêm writes; ingest raw/clean không double count. Nếu sau đó có batch mới, so version theo snapshot final cũ lưu riêng, không so response cũ với current state mới. DB với OVS không phải transaction nguyên tử; cần outbox/replay/reconciliation có ID.

## 12. IF09 Audit, giữ đường và metrics

Mỗi control tick có control_tick_id,time,method,snapshot/forecast refs,trigger count,disposition=KEPT/APPLY_PROPOSAL/REJECTED,reason. Không ép KEPT vào enum decision_batches SDN (không có KEPT): lưu `control_ticks` riêng; chỉ tạo batch execution khi có ApplyRequest. Không giả batch FlowMod cho M0.

Reason tối thiểu: NO_TRIGGER,BELOW_IMPROVEMENT,HOLDDOWN,DATA_MISSING,DATA_STALE,DATA_INVALID,FORECAST_TIMEOUT,FORECAST_INVALID,FALLBACK_DEMAND,NO_INCUMBENT,INVALID_RESULT,VERSION_MISMATCH,STALE,BUSY,BATCH_ID_CONFLICT,FLOWMOD_ERROR,VERIFY_TIMEOUT,ROLLBACK_FAILED,DEADLINE_MISSED. Adapter aliases phải có bảng cố định, không nhầm reason với status.

MetricRecord: experiment/run/schedule/scenario/method,metric_name/version,value/unit,group nullable,numerator/denominator,n_samples,coverage,valid_duration,measurement/cohort refs,quality/reason,input hashes,pipeline/analysis version. Công thức/cohort/CI/exclusions theo test_plan mục8/10/11. Raw packet/probe/counter/state phải đủ tính lại. Metric null không chuyển0; method gặp timeout/fallback vẫn giữ trong kết quả.

## 13. Contract fixtures và điều kiện khóa

| Bộ fixture | Nội dung / case |
| --- | --- |
| Topology | 48/96/16/32, canonical Network IDs, reject ID cũ, sai port/path, reverse/same-edge; TC-N01/I01 |
| Time/causality | t100,L30,H5; warm-up60s; cả release/available cutoff; SDN tạo/Data lưu cùng ID/hash; exact boundary/late/sửa hidden future; TC-D04/I02/I03 |
| Forecast storage | Đúng7 fields, H*=5 lấy từ config→240 rows; reject thêm config/raw/mean fields, thiếu1/dup/mixed issue/source; TC-D06 |
| Epsilon/mean | vector mean2.8; raw4→final4.4, không4.84; TC-M01/M04 |
| MILP | F2 và F48 toy; λ0,y chính xác; N3,hold/B/h/timeout, FEASIBLE/max_gap policy; TC-O01..O05 |
| Apply/state | valid/stale/wrong path,duplicate/conflict/BUSY,PARTIAL+rollback+UNKNOWN,restart reconciliation,expiry sau write; TC-S01..S03/D07 |
| Metrics | 100send/90unique/2duplicates,late/cohort/counter reset; TC-D05/E01 |

Mỗi producer giao valid/invalid input, expected output, schema/version/hash; consumer validate và ghi actual/evidence. JSON Schema, parser và executable fixtures là đầu ra triển khai tiếp theo, chưa có trong bộ tài liệu này. QA không ghi PASS chỉ vì ví dụ JSON parse được.

Owner sign-off dùng decision log trong [test_plan_v2.md](test_plan_v2.md); gates theo Guide là **G0–G8**. Ownership/ID/7-field payload/ML-owned epsilon/Optimization mean đã có quyết định v1.4; tỷ lệ split70/15/15 đã được người dùng xác nhận. Xác nhận triển khai và executable evidence vẫn cần hoàn tất. Các mục MUST_LOCK: kênh demand lịch sử tới SDN,transport/ack/retry/timeouts,origins/Δ,config L*/H*/epsilon* do ML chọn offline và MA window,baseline/Oracle/fallback policy,TH/N/h/B/hold/lambda,allow_feasible/max_gap,tolerances,k_e/point/calibration,initial paths/ARP,restart/version policy,pacing/headroom,probe/deadline/drain/coverage,rebin,danh sách seed/phạm vi trong split_manifest/benchmark_matrix và policy cuối run. Chỉ freeze khi có giá trị thực, owner/time và hashes; đổi semantics phải bump version và regression các case liên quan. Không mở lại tỷ lệ split đã chốt khi khóa manifest.
