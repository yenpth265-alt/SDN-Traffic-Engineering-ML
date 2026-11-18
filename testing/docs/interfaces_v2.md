# Giao diện tích hợp tổng hợp cho kiểm thử SDN

- Phiên bản `testing_v2 2026-11-10`.
- API mục tiêu `sdn_qa_api_v2`.
- Phụ trách Testing/QA/QC.
- Trạng thái: hoàn thiện đặc tả để owner xác nhận, chưa nghiệm thu triển khai.

Đây là hợp đồng đích đề xuất dựa trên file từng role và báo cáo rà soát; chưa phải xác nhận API đã hiện thực. Các owner cần xác nhận C01–C16 trong test_plan_v2.md trước freeze. Giữ nguyên ranh giới: Network tạo tải; Data lưu/cấp snapshot; ML dự báo; Optimization quyết định; SDN thực thi/xác minh; Testing đối chiếu độc lập. Không sửa file thành viên trong đợt này.

## 1. Danh mục và quyền sở hữu

| ID | Producer → Consumer | Nội dung | Owner xác nhận |
| --- | --- | --- | --- |
| IF01 | Network → SDN/Data/Optimization | TopologyBundle: host/switch/link/flow/path/initial map | Network; SDN đối soát port |
| IF02 | Network → Data/Runner | ScheduleBundle: CSV + manifest/hash | Network+Data |
| IF03 | Network/SDN → Data/Testing | PacketLog, ProbeLog, CounterLog | Producer; Data kiểm nguồn đo |
| IF04 | Data → ML | TrainingBundle: X/y_vector/y_mean/metadata/split | Data+ML |
| IF05 | Data+SDN → ML/Optimization | Snapshot lịch sử causal và confirmed state | Data+SDN |
| IF06 | ML/baseline → Data → Optimization | H-step predictions, batch READY, mean adapter | ML+Data+Optimization |
| IF07 | Data/Coordinator → Optimization | Demand mean+state → proposal/audit | Optimization |
| IF08 | Optimization/adapter → SDN → Data/Optimization | ApplyRequest/ApplyResponse, verified state/history | Optimization+SDN+Data |
| IF09 | Tất cả → Data/QA | Events, control ticks, metrics và provenance | Data+QA |
| IF10 | Runner/QA → tất cả | RunManifest/frozen config/seed/order | Runner+QA, các owner echo version |

Coordinator là vai trò điều phối logic, có thể nằm trong runner; không bắt buộc thêm service. Transport thực tế (Python IPC/REST/queue), encoding, timeout/retry/ack cần owner khóa và chạy contract tests; hiện MUST_LOCK. Không coi chữ ký logic trong tài liệu là API đang tồn tại.

## 2. Quy ước định danh và thời gian

### 2.1. Namespace chuẩn và adapter

| Khái niệm | Canonical | Nguồn cũ và chuyển đổi |
| --- | --- | --- |
| Nhóm có hướng | `flow_group_id=h1_h3` | `0x103` là cookie; `h1-h3`/nhom → tra mapping |
| OpenFlow cookie | `flow_cookie=0x103` | Theo Network src×256+dst; không dùng thay primary ID |
| Đường | `path_id=h1_h3_c1` / `h1_h3_c2` | P1/P2,c1/c2 chỉ resolve khi có flow + topology |
| Nhãn đường | `path_label=P1` / `P2` | P1 qua c1, P2 qua c2 |
| Link có hướng | `link_id=e1_to_c1` | e1>c1,e1_c1 → mapping; c1_to_e1 là link khác |
| Batch forecast | `forecast_batch_id=fc_001` | Khác batch điều khiển; nhóm nhiều prediction rows |
| Batch thực thi | `decision_batch_id=batch_001` | Optimization.batch_id → decision_batch_id |
| Trạng thái | `state_version` integer toàn run | Không dùng bộ đếm riêng từng flow |
| Phiên bản | config_version/topology_version/schema_version/api_version | Version có hash nội dung kèm; không dùng chuỗi v1 thay checksum |

Network xuất mapping máy đọc được; Data/consumer không tự parse alias mơ hồ. Thứ tự vector48 nhóm theo cookie tăng dần trong manifest, song consumer phải join bằng flow_group_id và kiểm tập ID, không chỉ zip theo vị trí.

### 2.2. Origin và khoảng thời gian

Mỗi run có ba mốc rõ: run_started_at_utc (khởi động tiến trình), traffic_started_at_utc (bắt đầu replay, gồm warm-up), measurement_started_at_utc (bắt đầu360s đo). Không giả chúng trùng nhau. Dùng timestamp UTC RFC3339 trong API, offset số thực trong bảng vật lý theo origin khai báo. Duration dùng clock monotonic; không trừ hai monotonic khác miền clock.

Baseline đề xuất:

- `demand_plan.time_offset_s`: từ traffic_started_at_utc.
- `traffic_actual.window_start/end`: từ traffic_started_at_utc, giữ semantics Data §6.1.
- `predictions.forecast_start/end`: từ measurement_started_at_utc, giữ semantics ML §6.3.
- Snapshot/ApplyRequest UTC; mọi interval `[start,end)`. ML d[k] là giá trị trong [k,k+1) theo measurement origin; các bin warm-up âm có thể là lịch sử hợp lệ.
- Quy đổi `utc=declared_origin+offset_s`; measurement=t0+70s và forecast bin100 thì schedule bin170. Không cộng warm-up hai lần.

Ví dụ TC-I03: traffic10:00:00Z; measurement10:01:10Z; forecast offsets100..105 → UTC10:02:50Z..10:02:55Z. Ví dụ run origin trong báo cáo chỉ áp khi field đó thực sự khai báo run origin.

Ở cutoff t, X lấy [t−L,t), Y lấy [t,t+H); mẫu online chỉ dùng khi window_end≤cutoff và available_at≤snapshot_as_of≤decision time. `issued_at` là lúc forecast phát hành, không phải forecast_start và không bị backdate để khớp bin. `forecast_start` là target anchor t. Late history đến sau snapshot_as_of không được nhập lại snapshot bất biến.

### 2.3. Đơn vị và envelope

Mbps=1.000.000 bit/s; payload bao gồm metadata ứng dụng. requested_mbps, sent_bytes, received_unique_bytes và Port TX bytes có semantics khác nhau. Dữ liệu thiếu=null+reason; 0 là giá trị thật. Demand hữu hạn≥0,capacity>0,k_e>0; dự báo lớn hơn capacity có thể hợp lệ. NaN/Infinity không hợp lệ JSON; ghi null+reason trong lỗi. Không clip utilization về1.

Envelope batch/API gồm api_version,schema_version,run_id,producer,produced_at,config_version,config_hash,topology_version. Bảng predictions vẫn7 cột; các trường envelope/audit lưu ở batch registry/manifest liên kết, không bắt buộc lặp trong7 cột. Ingest có khóa/record ID, provenance raw file+line/hash; conflict không overwrite im lặng.

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

Ví dụ h1_h3_c1: [h1_to_e1,e1_to_c1,c1_to_e2,e2_to_h3]. Match phải lấy IP/MAC từ config; không tự suy MAC. Tham số baseline10Mbps mỗi chiều,1ms host/5ms fabric,queue50 là cấu hình thiết kế; k_e/measurement point có calibration_id, chưa đo không ghi đã hiệu chuẩn.

## 4. IF02 ScheduleBundle và IF10 RunManifest

CSV khóa `(schedule_id,flow_group_id,time_offset_s)`; cột requested_mbps; core dense48 dòng/bin, zero explicit. Manifest ghi scenario/rho/seed/parent_family_id/split, origin,Δ=1s,duration_s,generator version/params,packet payload,content hash,series fingerprint,config version/hash,topology version,CORE/HOST_STRESS label,validation report. Nếu rescale/cap, lưu thuật toán và tỷ lệ biến đổi thực; không khẳng định S2b/S5 luôn đạt rho danh nghĩa.

Lịch phải phủ warm-up+360s; nhãn forecast của tick cuối cần lịch đủ đến target_end (thêm tối đa H_max nếu chấm tất cả tick). Đo truyền chính vẫn kết thúc sau360s, không phát thêm tail vào cohort; tương lai tail chỉ oracle/offline evaluator được đọc. Không đủ nhãn tail thì báo exclusion/coverage đồng nhất mọi provider.

RunManifest ghi experiment/run/schedule/method/scenario,split,parent family,schedule/model/solver/order/fault seeds,repeat/attempt/order index,code/env/model/config/topology hashes,Δ/T_ctl/L/H/MA_window,epsilon_owner/ratio,initial_paths_hash,calibration_id,origins UTC+mono,measurement_end/drain_end,timeout/guard/deadline policy,files/hashes,status/quality/inclusion/reason/supersedes_run_id. Những trường chưa biết ban đầu bổ sung khi hoàn tất, không giả trị số.

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

S0 hiệu chuẩn k_e=link_rate/payload_sent_rate ở tải thấp không mất gói, lặp nhiều mức, lưu residual/sai số/point/version. Không ước lượng k_e bằng requested rate trong lúc sender bị throttle, không nhân k_e lên throughput đo lần nữa. Điểm đếm trước shaping phải được gắn nhãn offered load; không tuyên bố egress utilization khi chưa chứng minh.

## 6. IF04 TrainingBundle

Khóa sample_id nối X,y_vector,y_mean,sample_metadata một–một. Metadata: schedule/group,split,parent family,cutoff t,origin,Δ,L,H,feature schema/order,dtypes,source hashes. Split trước window, cùng lịch/parent family ở cùng split; seed và fingerprint không rò chéo. Không nối lịch/run/group khi tạo window.

Với Δ1s và L30,H5,t100: X từ bins70..99; y_vector từ100..104; y_mean=sum(y_vector)/5. Nếu cần thứ tự demand_lag_1 gần nhất nhưng vector cũ→mới, feature_schema phải nêu mapping rõ. X/y_vector là sản phẩm Data cần bổ sung; file y.parquet scalar hiện mô tả chưa đủ multi-output. y_mean có thể là derived view, không dùng thay nhãn train H bước.

Offline dùng lịch đầy đủ để tạo nhãn hợp lệ; online không truy cập lịch tương lai. Fit preprocessing trên train; tuning/epsilon/MA window trên validation; test độc lập. Scenario/seed/schedule future params không vào feature. One-hot group ID chỉ nhận diện48 nhóm, không tự cấm nếu đúng ML design; mapping version phải khóa.

## 7. IF05 Snapshot online

`get_snapshot(run_id,as_of_utc,history_cutoff_utc)` là chữ ký logic. Snapshot bất biến, cùng run/config và global state version. Có snapshot_id,created_at,available_at,as_of_utc,history_cutoff_utc,origin refs,flow_group_ids,history[],routing_state[],link_samples refs,quality_status,missing_groups,routing_state_complete,unknown_flow_group_ids.

Mỗi history item: group,window_start/end,requested_mbps,available_at,valid/reason,release_record_id. Network/Data ghi demand release ledger khi bin hoàn tất; không coi file lịch tạo trước run là đã available. Có thể derive availability ở provider nhưng phải giữ audit tái hiện được. Online M1–M3 chỉ thấy ledger đã phát hành; forecast labels/oracle store tách quyền. Metamorphic test sửa tương lai ẩn nhưng giữ history thì M1–M3 output không đổi.

Age từ history cutoff/nguồn dùng thực, không từ lúc worker trả output. T_stale đề xuất2Δ cần khóa. Nếu latest bin chưa available: chờ hữu hạn trong budget hoặc fallback còn mới/skip theo policy khóa; không dịch target âm thầm. Nếu UNKNOWN state hoặc thiếu bất kỳ demand cần thiết sau fallback thì skip toàn tick, log KEPT. Snapshot phát hành trong batch apply/reconcile bị chặn hoặc mang routing_state_complete=false; không cấp state confirmed giả.

## 8. IF06 H-step predictions và mean adapter

### 8.1. Request và output logic

Request: envelope,forecast_batch_id,snapshot_id,target_start_utc,target_end_utc,L_s,H_s,Δ_s,feature_schema_version,history đủ48 nhóm. Không chứa labels/future schedule. Δ1s nên H giây tương ứng H bước. predict_batch trả H giá trị hữu hạn không âm mỗi nhóm, nguồn/status/reason,model_version,latency,completed_at. RF native có thể clip âm→0 theo ML §3.1 nhưng phải ghi raw/clipping_count; output sau chuẩn hóa vẫn âm/NaN/inf bị reject.

Data giữ7 cột vật lý ML yêu cầu:

| Cột | Kiểu / semantics |
| --- | --- |
| run_id | TEXT; đúng run |
| flow_group_id | TEXT canonical h1_h3 |
| issued_at | UTC timestamp; cùng toàn đợt phát hành, không tạo từng group bằng now() |
| forecast_start | REAL offset measurement origin, đầu một bin |
| forecast_end | REAL offset measurement origin, cuối bin=start+1 |
| predicted_mbps | REAL final prediction của bin, đã áp epsilon nếu metadata khai báo |
| source | TEXT versioned provider, ví dụ rf_v1/persistence/moving_avg/fallback/oracle |

**PK bắt buộc sửa Data v1:** `(run_id,flow_group_id,issued_at,forecast_start)`. Khóa3 trường cũ không chứa được H dòng. Mỗi run có một forecast batch hoạt động tại một issued_at; registry bắt conflict nhiều source cho cùng group/bin. Thí nghiệm model khác dùng run/batch khác hoặc schema khóa mở rộng có version; không overwrite một giá trị.

### 8.2. Batch registry và completeness

Bảng/manifest `forecast_batches` chứa run_id,forecast_batch_id,snapshot_id,issued_at,config/schema/model refs,source map từng nhóm,full target UTC range,H/Δ/L,expected_rows=48H,actual_rows,ready_at,status,epsilon metadata,raw_forecast_refs,content_hash. `forecast_batch_rows` map batch với các PK7-field rows; row source giữ nguồn thực nếu fallback.

WRITING → READY chỉ sau validate đủ48 nhóm×H contiguous bins, không duplicate/gap/mixed issued_at/target/config. NOT_READY/INVALID/EXPIRED không được Optimization đọc như complete. Ingest lại cùng batch+hash idempotent; cùng ID khác nội dung conflict. Chỉ read atomically batch READY, ready_at≤decision time. File manifest commit marker hoặc transaction đều được nếu owner chứng minh không đọc nửa batch.

### 8.3. Vector, mean và epsilon

Data→Optimization trả vector hoặc H rows từ batch READY. Optimization/adapter tính `b_f=mean(predicted_mbps[0:H])`. Có thể trả derived forecast_mean_mbps để audit nhưng phải kiểm bằng H rows; không bắt ML thêm cột mean vào predictions.

Fixture: [2,2.5,3,3.5,3] → b=2.8 Mbps; H5,F48→240 rows. Lưu raw vector ref nếu có hiệu chỉnh. **Policy v2 chọn ML-owned theo ML §6.5:** final=max(0,(1+epsilon_ml)×raw), Data lưu final, Optimization không nhân epsilon nữa. Registry ghi epsilon_owner=ML,epsilon_applied=true/false,epsilon_ratio,raw_forecast_ref. Baselines và oracle mặc định epsilon0; M3 adjusted là một phương pháp đầy đủ gồm RF+calibration. Muốn kết luận riêng lợi ích RF phải có ablation raw RF/epsilon0 hoặc policy calibration chung đã khóa.

Nếu owner chọn phương án opt-owned từ Rà soát C07 thì đổi protocol: ML xuất raw/epsilon_applied=false, Optimization nhân một lần và ghi b_raw/b_used. Không cho phép hai policy chạy chung vô tình. Fixture raw4,eps.1,k1 → final/b_used4.4, không4.84. Tolerance numeric_epsilon không phải epsilon dự báo.

Fallback đề xuất: RF lỗi/timeout → persistence còn mới cho nhóm lỗi; cả H bước cùng value cuối; source=fallback và reason. Không đủ cho một nhóm→skip toàn tick. M3 vẫn method=M3; batch DEGRADED nếu trộn nguồn. Source/vectors được chấm tách native/fallback; baseline M1/M2 cũng dùng cùng khoảng target và history availability. M0 không tạo dự báo giả; M4 đọc tương lai trong lane ORACLE riêng.

## 9. IF07 Optimization và audit

Input gồm run/snapshot/forecast_batch IDs,expected_state_version,config/hash,full48-group b vector,16 fabric capacities,k_e/calibration refs,candidate paths/current paths/last_successful_change_at,TH/N/h/B/hold/lambda,numeric tolerances,cutoff/target UTC/deadline. Không cộng counter load của chính F lên predicted load lần nữa.

Mô hình chuẩn khi ML đã hiệu chỉnh: `L_e=k_e Σ_f,p b_f a_fpe x_fp`; `Σ_p x_fp=1`; `L_e≤U C_e`; `y_f=1−x_f,current`; `Σ y_f≤B`; hold chưa đủ thì x_current=1; x/y binary,U≥0. F cố định48 kể cả nhóm0. Không hard cap U≤.9 hay1. Dùng J=U+lambda×n_changes/48; current có n_changes0 nên J_old=U_current.

Trigger strict U_current>TH trong N tick hợp lệ liên tiếp. Baseline đề xuất N3; invalid/stale/busy/reconciled-state-change reset count; sau batch final reset. Acceptance: actual proposed changes≥1,J_old>numeric_epsilon,J_new≤(1−h)J_old theo tolerance khóa, đủ hạn/state đúng. Independent validator recompute constraints/U/J, không chỉ tin solver output.

Output audit: decision_batch_id,control_tick_id,forecast_batch_id,snapshot_id,state/config refs,target UTC,U_old/U_new,J_old/J_new,solver_status,solve_ms/gap/bound,trigger_count,full_routes,changes,validation_status,disposition/reason. Solver enum NOT_RUN/OPTIMAL/FEASIBLE_TIME_LIMIT/NO_INCUMBENT/INFEASIBLE/ERROR/INVALID_RESULT. Time-limit incumbent chỉ dùng khi integer feasible, pass validator và còn hạn. Không có nghiệm hợp lệ→KEPT, không bịa OPTIMAL.

Ví dụ F48,lambda.1,U_new.58: một đổi J_new=.5820833333; bốn đổi J_new=.5883333333. Ví dụ .61 của Optimization không khớp các tham số này. Fixture F2,b6/6,k1,C10 cùng nhánh→U_old1.2; đổi1→U_new.6,J_new.65. Phân biệt mẫu số F2 fixture và F48 production.

## 10. IF08 ApplyRequest và ApplyResponse

Theo sdn_interfaces.md §4–5, canonical request `decision_batch_id/changes`. Metadata solver lưu audit riêng, không buộc SDN giải lại MILP.

```json
{
  "run_id":"run_001","decision_batch_id":"batch_001","snapshot_id":"snap_100",
  "expected_state_version":15,"config_version":"v1",
  "forecast_start":"2026-11-10T10:01:40Z","forecast_end":"2026-11-10T10:01:45Z",
  "changes":[{"flow_group_id":"h1_h3","old_path_id":"h1_h3_c1","new_path_id":"h1_h3_c2"}]
}
```

Ví dụ lõi; envelope ở §2.3 đi cùng transport/audit. Adapter map old_path/new_path P1/P2 sang full path bằng danh mục; target_interval5s không đủ thay target timestamps. Validation trước write: run/config/topology/snapshot/version đúng, old_path VERIFIED, membership hợp lệ, changes không trùng và1..B, hold-down theo confirmed time, đủ guard trước forecast_end, không batch khác flight.

```json
{
  "run_id":"run_001","decision_batch_id":"batch_001","status":"APPLIED",
  "state_version":16,"completed_at":"2026-11-10T10:01:41Z",
  "results":[{"flow_group_id":"h1_h3","actual_path_id":"h1_h3_c2","status":"APPLIED",
  "reason":null,"rule_count":1,"applied_at":"2026-11-10T10:01:41Z"}]
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

Preinstall downstream hai nhánh, đổi source uplink3↔4. Barrier chỉ là evidence xử lý thông điệp; verify read-back rule và probe/counter đúng nhóm/nhánh khi phù hợp. Traffic0 dùng probe; check đến đích và đúng nhánh riêng. Một batch liên switch không atomic; mọi FlowMod kể cả lỗi/rollback được đếm.

Forecast expiry phải kiểm trước write. Nếu write rồi verify trễ forecast_end, ghi deadline_missed và reconcile actual; không che thay đổi có thật bằng REJECTED hoặc đường cũ. Kết quả xác minh thành công muộn vẫn ghi actual cùng late flag, là lỗi vận hành deadline để QA đánh giá. UNKNOWN→pause toàn cục đến resync trong core policy. Không queue proposal cũ để apply sau.

## 11. Verified state và Data ETL

| Source table / khóa | Nội dung phải bảo toàn |
| --- | --- |
| routing_state / (run,group) | path_id nullable,state_version,confirmed_at,last_successful_change_at,verification_status,config_version |
| routing_history / (run,group,valid_from) | path_id,valid_from/to,status INITIAL/APPLIED/ROLLED_BACK,batch_id nullable |
| decision_batches / (run,batch) | payload_hash,snapshot/forecast refs,expected/result version,target/received/completed,U/J/solver,status/reason |
| routing_decisions / (run,batch,group) | old/proposed/actual path,status/reason,applied_at,rule_count,evidence refs |
| link_statistics / (run,link,sample_id) | raw bytes,delta,time/source/point/epoch,valid/reason |
| event_log / (run,event_id) | component,event_at,severity,batch/flow/switch/xid,detail JSON |

SDN là writer duy nhất state/history. Theo SDN §2/5: tăng global state_version tối đa1 lần ở final batch nếu confirmed state hoặc độ chắc chắn thay đổi; đồng bộ mọi state row của run. Bao gồm mốc hold-down/confirmed metadata thay đổi có ý nghĩa; reject trước write không tăng. Không tăng vì tiếp nhận proposal. Trạng thái vật lý có thể chuyển qua nhiều bước trong batch: giữ event trace, chưa công bố confirmed snapshot mới đến final. Final rollback về path cũ nhưng timestamp lần đổi verified thay đổi cũng phải invalidate snapshot cũ; owner xác nhận rule này trước freeze.

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
| Topology | 48/96/16/32, alias mapping, sai port/path, reverse/same-edge; TC-N01/I01 |
| Time/causality | t100,L30,H5; bin âm warm-up; exact boundary/late/sửa hidden future; TC-D04/I02/I03 |
| Forecast storage | 240 rows cùng issue,H5; thiếu1/dup/mixed issue/source; TC-D06 |
| Epsilon/mean | vector mean2.8; raw4→final4.4, không4.84; TC-M01/M04 |
| MILP | F2 và F48 toy; λ0,y chính xác; N3,hold/B/h/timeout; TC-O01..O05 |
| Apply/state | valid/stale/wrong path,duplicate/conflict/BUSY,partial+rollback+UNKNOWN,expiry sau write; TC-S01..S03/D07 |
| Metrics | 100send/90unique/2duplicates,late/cohort/counter reset; TC-D05/E01 |

Mỗi producer giao valid/invalid input, expected output, schema/version/hash; consumer validate và ghi actual/evidence. JSON Schema, parser và executable fixtures là đầu ra triển khai tiếp theo, chưa có trong bộ tài liệu này. QA không ghi PASS chỉ vì ví dụ JSON parse được.

Owner sign-off C01–C16 dùng decision log trong test_plan. Mọi mục còn OPEN ở bản này. Các field MUST_LOCK: transport/ack/retry/timeouts,origins/Δ/L/H/MA window/epsilon policy,TH/N/h/B/hold/lambda,tolerances,k_e/point/calibration,initial paths/ARP,pacing/headroom,probe/deadline/drain/coverage,rebin,seed/split/run matrix. Chỉ freeze khi đã có giá trị thực, owner/time và hashes; đổi semantics phải bump version và regression các case liên quan.
