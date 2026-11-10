# Giao diện tổng hợp giữa các module

## 1. Trách nhiệm và luồng giao tiếp

| ID | Producer → Consumer | Giao diện/đầu ra | Trách nhiệm kiểm tra |
| --- | --- | --- | --- |
| IF01 | Network → SDN, Data, Optimization | TopologyBundle, DPID/port map, groups/paths | Network+SDN đồng xác nhận cổng; Data kiểm FK; Optimization kiểm path |
| IF02 | Network → Data, Runner | ScheduleBundle: demand.csv + manifest | Data kiểm schema/hash/split; Runner replay nguyên file |
| IF03 | Network, SDN → Data | PacketLog, ProbeLog, CounterLog | Producer giữ raw; Data chuẩn hóa và xác nhận phép đo |
| IF04 | Data → ML | TrainingBundle X,y,sample_metadata,split_manifest | Data đảm bảo chronology/split; ML xác nhận feature/label |
| IF05 | Data + SDN state → demand-provider, Optimization | Snapshot | Data chốt availability; SDN cung cấp state đã xác minh |
| IF06 | Provider adapter (M1–M4), ML → Coordinator | ForecastRequest/ForecastResponse | ML/adapter xác nhận demand vector/H/source/status |
| IF07 | Coordinator → Optimization → Coordinator | OptimizeRequest/DecisionBatch | Optimization kiểm đủ F, R1–R7/J; Coordinator kiểm stale/version |
| IF08 | Coordinator/Optimization → SDN | ApplyRequest/ApplyResult | SDN là writer duy nhất của verified routing_state/history |
| IF09 | Các module → Data, QA/Evaluation | EventLog, decision/state history, run metrics | Data tính; QA/Evaluation kiểm trace/recompute/paired |
| IF10 | Runner/QA → mọi producer | RunManifest và frozen config | Mọi producer echo run/config/schema version, từ chối mismatch |

Một quyết định có thể dùng nhiều forecast; ghép bằng batch_id và prediction_id/decision_prediction_links. M1 không bắt buộc có prediction_id nếu adapter chỉ đưa demand vector, nhưng luôn có demand_source và target_interval. M0 không gọi optimizer/apply, vẫn có snapshot/metrics/events cho quan sát.

## 2. Quy ước nền

### 2.1. Định danh, phiên bản và thời gian

| Trường | Kiểu/quy tắc |
| --- | --- |
| `experiment_id` | String định danh một protocol/config khóa và chiến dịch chạy |
| `schedule_id` | String bất biến của lịch; replay không đổi; khác `run_id` |
| `parent_family_id` | String nhóm lịch chung nguồn; dùng split trước window |
| `run_id` | String duy nhất mỗi attempt; ví dụ `exp01_S3_test006_M3_a01`; không parse để thay metadata |
| `flow_group_id` | String có hướng, ví dụ `h1_h3`; F cố định trong topology, đầy đủ 48 khi benchmark |
| `link_id` | String có hướng, ví dụ `e1_c1`; chiều `c1_e1` riêng |
| `path_id` | ID toàn cục trong topology, ví dụ `h1_h3__via_c1`; `via_spine` chỉ là c1/c2 |
| `snapshot_id`, `batch_id`, `decision_batch_id`, `prediction_id` | String duy nhất trong run; giữ quan hệ ngoại khóa rõ |
| `state_version` | Integer không âm, version **toàn run**, tăng khi actual/verification state thay đổi được ghi nhận |
| `api_version`, `schema_version` | String bắt buộc ở envelope; major mismatch bị reject, không đoán adapter |
| `config_hash`, `topology_version`, `git_commit`, `model_hash` | Lấy từ artifact thật; SHA-256 cho hash nội dung; ghi encoding/canonicalization |
| `*_at` | RFC3339 UTC có độ chính xác đủ, ví dụ `2026-11-08T02:00:10.000Z` |
| `*_mono_ns` | Integer monotonic nanosecond trong clock domain khai báo; dùng đo duration |
| `*_offset_s` | Float giây từ traffic_start, không trộn với UTC; field forecast_start/end trong Data.md là dạng này |
| `window_start/end` | UTC canonical của khoảng nửa mở [start,end); lưu offset/mono tương đương nếu cần |
| `available_at` | Khi producer đã hoàn tất và dữ liệu được cấp cho consumer; không giả bằng window_end |

Dữ liệu online dùng chỉ khi window_end≤issued_at và available_at≤issued_at. Snapshot ghi `history_cutoff` là window_end mới nhất được dùng, **không** phải thời điểm dự báo hoàn thành. Clock anchor gồm traffic_start_utc,traffic_start_mono_ns,clock_domain_id. Khi đồng hồ wall clock nhảy, duration dựa mono; timestamp quy đổi có quality flag. Khác máy không được trừ monotonic trực tiếp; one-way delay chỉ báo sau kiểm chứng clock.

### 2.2. Đơn vị, miền và chất lượng

1 Mbps=1.000.000 bit/s. `requested_mbps`/`demand_mbps` là UDP payload yêu cầu, bao gồm metadata ứng dụng; `tx_mbps` là payload gửi thành công; `goodput_mbps` là unique payload nhận. Counter byte ở link ghi layer/measurement_point riêng. Không dùng các đại lượng thay nhau.

Float nhu cầu/capacity/k_e phải hữu hạn; demand≥0,capacity>0,k_e>0. Lớn hơn capacity không tự là dữ liệu lỗi. Invalid numeric trong JSON dùng null+reason, không ghi NaN/Infinity. `null` khác zero; nhóm zero hợp lệ vẫn thuộc F. Không clip overload/utilization về 1.

Mọi envelope có `run_id` (trừ bundle ngoại tuyến), `api_version`, `schema_version`, `produced_at`, `producer`, `config_hash`; payload raw tối thiểu thêm record_id/sequence hoặc khóa tự nhiên để ingest idempotent. `quality_flags` là array string và `reason` nullable. Trường tùy chọn vắng mặt khác với trường bắt buộc nhưng null vì chưa biết. Lỗi input phải trả mã, không silent coercion.

## 3. IF01 — TopologyBundle

Owner: Network; SDN đồng sở hữu port mapping. Consumer: Data, SDN, Optimization.

| Thành phần | Field tối thiểu |
| --- | --- |
| `nodes[]` | node_id,node_type(host/edge/spine),ip/mac với host,dpid với switch (hex string cố định, không ép số float) |
| `links[]` | link_id,src_node_id,dst_node_id,src_port,dst_port,capacity_mbps,configured_delay_ms,queue_packets,link_type(host/fabric),counter_source,measurement_point,overhead_factor |
| `flow_groups[]` | flow_group_id,src_host_id,dst_host_id,src_edge_id,dst_edge_id |
| `paths[]` | path_id,flow_group_id,via_spine,ordered_link_ids |
| `initial_routes[]` | flow_group_id,path_id; mapping xác định giống mọi method |
| `control` | OpenFlow version,controller endpoint,fail_mode,ARP policy,table-miss policy |

Validation: host IP/MAC không trùng; dpid/cổng map đúng link hai chiều; path liên tục từ src tới dst, đúng một spine, không vòng; đủ hai candidate/group; F=48, fabric=16 chiều; current/initial route nằm trong candidate. Mọi capacity/delay/queue lấy config chung. k_e và measurement_point chưa hiệu chuẩn phải có calibration_status=unverified, chặn claim MLU egress.

ID ví dụ: `path_id=h1_h3__via_c1`, ordered links=`[h1_e1,e1_c1,c1_e2,e2_h3]`. Với Data.md lưu old_path/new_path là c1/c2, adapter phải kết hợp flow_group_id và topology_version để ra path_id đầy đủ; không dùng c1 như path_id toàn cục.

## 4. IF02 — ScheduleBundle và IF10 — RunManifest

### 4.1. Lịch

CSV `demand.csv` có đúng khóa `(schedule_id,flow_group_id,time_offset_s)`, thêm `requested_mbps`; mỗi dòng có hiệu lực [offset,offset+Δ). Nếu không đủ 48 dòng mỗi tick, phải có policy sparse rõ trong manifest; đề xuất core dùng dense để zero không bị nhầm missing.

Manifest: schedule_id,parent_family_id,scenario,schedule_seed,generator_version,generator_parameters,Δ_s,duration_s,packet_payload_bytes,config_hash,topology_version,content_sha256,series_fingerprint,created_at,split,overload_label,validation_report_hash. Split có thể được Data bổ sung sau; không đổi nội dung lịch âm thầm. `duration_s` phải phủ warm-up+measurement; nếu chấm forecast gần cuối cần schedule thật đến target end, phần này chỉ oracle/evaluator được đọc tương lai.

Không đưa seed/scenario/generator future parameters/witness vào feature hay online request M1–M3. Generator và offline evaluator biết lịch tương lai là quyền riêng, không truyền toàn CSV vào forecaster.

### 4.2. Lượt chạy

RunManifest bắt buộc trước start: experiment_id,run_id,schedule_id,method,scenario,split,parent_family_id,schedule_seed,model_seed/solver_seed/order_seed (nullable có lý do),repeat_index,attempt_index,run_order_index,topology_version,config_hash,code_commit,environment_id,model_id/hash khi M3,packet_payload_bytes,Δ_s,T_ctl_s,L_s,H_s,W_s,measurement_duration_s,warmup_policy,initial_routes_hash.

Khi kết thúc bổ sung traffic_start/measurement_start/measurement_end/drain_end UTC+mono,run_status,quality_flags,inclusion_status,exclusion_reason,supersedes_run_id,files/checksums và calibration_id. `method` bất biến, không đổi M3 thành M1 khi fallback.

Run status đề xuất: CREATED,RUNNING,COMPLETED,FAILED,INVALID_INFRASTRUCTURE. `inclusion_status` riêng: INCLUDED,EXCLUDED,PARTIAL_METRICS,PENDING_QA. Adapter Data.md `ok/failed/discarded` không được xóa thông tin chi tiết. Fallback/timeout có thể là COMPLETED với quality flag, không tự INVALID_INFRASTRUCTURE.

## 5. IF03 — PacketLog, ProbeLog, CounterLog

### 5.1. Sender/receiver

| Log | Field bắt buộc |
| --- | --- |
| Sender | run_id,flow_group_id,sequence_number,send_time,send_mono_ns,payload_bytes,send_success,socket_error nullable,scheduled_time/offset,sender_lag_ms |
| Receiver | run_id,flow_group_id,sequence_number,receive_time,receive_mono_ns,payload_bytes,receiver_id |
| Probe | run_id,probe_id,flow_group_id,sent_at/sent_mono_ns,received_at/received_mono_ns nullable,status,rtt_ms nullable,forward_path_id/reverse_path_id nếu quan sát được |

Sequence duy nhất theo (run_id,flow_group_id), không reset giữa các cửa sổ. Nếu sender khởi động lại thì run invalid hoặc có stream_epoch được đặc tả; không ghép sequence trùng mơ hồ. Receiver raw giữ duplicate, Data xác định unique theo khóa packet; không loại duplicate tại nguồn làm mất chứng cứ. Gói lạc run/malformed ghi event riêng, không cộng metric.

Chỉ `send_success=true` thuộc mẫu số packet loss. `send_success` nghĩa syscall chấp nhận payload, không khẳng định packet đã qua qdisc. `received_unique_payload_bytes` là tổng packet ghép đúng và không trùng; log chỉ có tổng byte/giây không đủ nghiệm thu loss deadline.

### 5.2. Counter

Bắt buộc: run_id,sample_id,link_id hoặc dpid+port+direction có mapping,request_id/xid,request_at,reply_at,sample_at hoặc timestamp_semantics,available_at,counter_bytes,counter_packets nếu hỗ trợ,counter_epoch,counter_source,measurement_point,counter_layer,quality_flags.

Nếu không có timestamp đọc counter thực trong switch, ghi sample_time_basis=request_or_reply_estimate và sai số/độ trễ quan sát; không mô tả là thời điểm chính xác. Chọn một nguồn counter/chiều. Counter giảm/epoch đổi/Δt≤0→delta và throughput null,valid=false,reason=COUNTER_RESET hoặc INVALID_TIME; lấy mốc mới. Không diễn giải reset thành traffic âm.

## 6. IF04 — TrainingBundle

Data bàn giao X,y,sample_metadata và split_manifest với khóa `sample_id` 1–1. Metadata gồm schedule_id,parent_family_id,flow_group_id,issued_at_offset_s,history_cutoff_offset_s,forecast_start/end_offset_s,H_s,L_s,split,source_hash và feature_schema_version. Với dữ liệu chỉ tạo từ lịch offline, dùng schedule reference, không tạo run đo giả.

Feature chỉ có giá trị quá khứ hợp lệ và history_age_s; lag/thống kê không vượt L. Tiền xử lý fit trên train; final fit train+validation chỉ sau khóa lựa chọn và lưu train_final_manifest. Label:

`y(t,H) = integral(requested_mbps(s), s in [t,t+H)) / H`.

Nếu H chia hết Δ và đúng biên, y là trung bình các bước; không trả riêng vector H bước rồi để optimizer tự đoán. Sample thiếu đầu vào/nhãn có exclusion reason, không nối lịch/nhóm. Bundle manifest ghi feature_order,dtypes,missing_policy,Δ,L,H,split_hash,training_schedule_ids,generator/schema version và file hashes. Leakage test độc lập chèn late available_at và ID/seed trá hình.

## 7. IF05 — Snapshot

`get_snapshot(run_id, cutoff_at) -> Snapshot` là chữ ký logic đề xuất. Snapshot bất biến sau phát hành; không trả object RAM còn bị producer cập nhật.

| Field | Nội dung |
| --- | --- |
| snapshot_id,run_id,issued_at,created_at | Định danh/mốc chốt |
| history_cutoff,oldest_required_window_end | Mốc lịch sử dùng, phục vụ kiểm age |
| topology_version,config_hash,state_version | Cùng baseline; version toàn run |
| flow_group_ids | Tập F cố định, đủ 48 khi core |
| demand_history[] | group,window_start/end,available_at,demand_mbps,valid,reason |
| link_samples[] | link_id,window/sample times,available_at,throughput_mbps,valid,measurement_point |
| routing_state[] | group,path_id nullable,verification_status,confirmed_at,last_successful_change_at |
| quality_status,quality_flags | OK/DEGRADED/INVALID, lỗi cụ thể |

Age rule đề xuất: lúc dùng snapshot, `now-history_cutoff≤T_stale`; mọi nguồn được dùng không vượt max age riêng đã khóa; kiểm lại trước apply. T_stale ban đầu 2Δ, không tính từ lúc worker trả response để che tuổi history. Snapshot đủ demand history nhưng state UNKNOWN thì không được tối ưu. Zero demand hợp lệ vẫn có row; thiếu một nhóm demand cần thiết thì giữ toàn tick.

Measured load chỉ dùng giám sát/hiệu chuẩn model; không cộng nguyên counter load của cùng F thêm vào k_e×forecast. Tải residual ngoài F nếu có phải có hợp đồng riêng trước khi dùng.

## 8. IF06 — ForecastRequest/Response và adapter M1–M4

Chữ ký logic `predict_batch(request) -> response`. Request gồm run_id,batch_id,snapshot_id,issued_at,history_cutoff,H_s,Δ_s,L_s,feature_schema_version,history[] theo nhóm. Không chứa nhãn/giá trị tương lai. ML trả:

| Field | Quy tắc |
| --- | --- |
| run_id,batch_id,snapshot_id | Echo không đổi |
| issued_at,history_cutoff,forecast_start,forecast_end | forecast_start=issued_at; forecast_end=start+H_s |
| completed_at,feature_ms,forecast_ms | Mốc thực; thời gian feature tách inference; total batch đo wall/mono |
| model_id,model_hash,feature_schema_version | RF artifact; null có lý do cho baseline |
| batch_status | OK/DEGRADED/INVALID/TIMEOUT |
| items[] | prediction_id nullable,flow_group_id,predicted_mbps nullable,source,status,error_reason |

Source canonical: PERSISTENCE,MOVING_AVERAGE,RF,FALLBACK_PERSISTENCE,ORACLE. Item status: OK,FALLBACK,INVALID,MISSING,TIMEOUT. Mỗi group xuất hiện đúng một lần, output sort theo group chỉ để thuận kiểm, consumer join bằng ID. Thiếu/dup group, sai H/batch/model/unit, NaN/âm/inf→reject. forecast_end≤completed_at hoặc còn ít hơn apply budget+guard→không dùng cho apply; ghi expired/insufficient budget.

Policy fallback: RF lỗi trên một hoặc nhiều nhóm có thể thay từng nhóm bằng persistence **còn đủ mới**; source ở item là authoritative, batch DEGRADED nếu trộn nguồn. Thiếu nguồn hợp lệ cho bất kỳ nhóm cần thiết→bỏ tối ưu toàn tick, không lấp 0. M3 luôn method=M3. MA có thiếu/gãy lịch sử trả INVALID theo policy core; thay đổi policy imputation chỉ sau validation và khóa.

Ví dụ rút gọn hợp lệ ở contract fixture F=2 (benchmark thật phải đủ F=48; hash minh họa chưa là baseline khóa):

```json
{
  "api_version": "sdn_qa_api_v1",
  "schema_version": "schema_qa_v1",
  "producer": "demand_provider",
  "produced_at": "2026-11-08T02:00:10.020Z",
  "config_hash": "fixture-config-unlocked",
  "run_id": "fixture_M3_a01",
  "batch_id": "fc_001",
  "snapshot_id": "snap_010",
  "issued_at": "2026-11-08T02:00:10.000Z",
  "history_cutoff": "2026-11-08T02:00:10.000Z",
  "forecast_start": "2026-11-08T02:00:10.000Z",
  "forecast_end": "2026-11-08T02:00:15.000Z",
  "completed_at": "2026-11-08T02:00:10.020Z",
  "feature_ms": 3.0,
  "forecast_ms": 17.0,
  "model_id": "rf_fixture_H5",
  "model_hash": "fixture-model-unlocked",
  "feature_schema_version": "features_v1",
  "batch_status": "DEGRADED",
  "quality_flags": ["RF_PARTIAL_ERROR"],
  "items": [
    {"prediction_id":"pred_001","flow_group_id":"h1_h3","predicted_mbps":6.0,"source":"RF","status":"OK","error_reason":null},
    {"prediction_id":"pred_002","flow_group_id":"h2_h4","predicted_mbps":6.0,"source":"FALLBACK_PERSISTENCE","status":"FALLBACK","error_reason":"RF_EXCEPTION"}
  ]
}
```

Cách hiểu H: ví dụ H_s=5,Δ_s=1 có 5 bước label để lấy trung bình; response vẫn một giá trị/group cho cả khoảng. Nếu sau này dùng multi-step vector phải tạo API version hoặc thêm forecast_type và aggregator rõ, không thay ý nghĩa predicted_mbps âm thầm.

## 9. IF07 — OptimizeRequest và DecisionBatch

OptimizeRequest có run_id,decision_batch_id,snapshot_id,expected_state_version,topology_version,config_hash,issued_at,history_cutoff,target_interval,deadline_at,flow_group_ids,demand_items (group,b_mbps,source,prediction_ids),capacities/overhead_factors,candidate_paths,current_routes,last_successful_change_at,verification_status,measured_load cùng quality,parameters TH/N/h/lambda/B/T_hold/numeric tolerances. Config lớn có thể truyền ref+hash thay chép mọi tick nếu đã nạp bất biến.

Coordinator chọn provider và quản lý tick; logic trigger có thể nằm trong Optimization nhưng chỉ có **một implementation chung** cho M1–M4. Input timestamp đủ tính hold-down/age. Demand item thiếu hoặc target interval không thống nhất không được solve với zero bù.

DecisionBatch trả tối thiểu:

| Nhóm field | Field |
| --- | --- |
| Trace | run_id,decision_batch_id,snapshot_id,expected_state_version,config_hash,topology_version |
| Demand/time | issued_at,history_cutoff,forecast_start/end,demand_source (MIXED nếu cần),prediction_ids,completed_at,solve_ms |
| Trigger | U_current,trigger_threshold,trigger_count,triggered,reason |
| Solver | solver_status,gap_if_available nullable,objective_bound nullable,solver_version |
| Điểm/constraints | U_old,U_new,J_old,J_new,n_changes_proposed,validation_status,validation_errors |
| Đường | full_routes[] đủ F; changes[] chỉ nhóm đổi, old_path_id/proposed_path_id/reason/prediction_ids |
| Quyết định | disposition=APPLY_PROPOSAL hoặc KEPT hoặc REJECTED; reason_code |

Solver_status: NOT_RUN,OPTIMAL,FEASIBLE_TIME_LIMIT,NO_INCUMBENT,INFEASIBLE,ERROR,INVALID_RESULT. Không có incumbent thì U_new/J_new/full_routes có thể null; trường null có reason. KEPT_NO_TRIGGER có thể trả full_routes=current và điểm giữ được tính, không bịa solver optimal.

Validator độc lập R1–R7 tính lại U và J từ x/routes, cùng b/k/C/F/current; không chỉ tin float objective solver trả. Gate apply: n_changes≥1,J_old>numeric_epsilon,J_new≤(1−h)J_old theo tolerance; đủ mới/đúng version/horizon/pending; không hard cap 90% hoặc 100%. Time-limit incumbent chỉ dùng sau kiểm. `current` luôn khả thi với mô hình lõi đúng, nên infeasible cần điều tra input/implementation.

Tổng ngân sách forecast+solve+apply+guard phải phù hợp T_ctl được khóa; 0,2 s chỉ là timeout solver khởi đầu, không là deadline toàn chu kỳ. Deadline apply không được muộn hơn forecast_end. Nếu không thể đáp ứng ở validation thì chỉnh T_ctl/H/Δ trước test; không tự đổi giữa lượt test.

## 10. IF08 — ApplyRequest/Result, state và version

ApplyRequest gồm envelope,decision_batch_id,snapshot_id,expected_state_version,topology_version,issued_at,history_cutoff,forecast_end,deadline_at,changes[]. Mỗi change: flow_group_id,old_path_id,new_path_id. Có thể thêm verifier_plan_ref/decision_hash để đối chiếu proposal đã qua validator; controller không nhận một list thiếu trace.

Kiểm tra trước write: config/topology đúng, current version khớp, old path khớp verified state, group/path thuộc F/candidate, dữ liệu đủ mới, không pending, đủ thời gian verify trước deadline. Không tự sửa prediction hoặc chọn đường khác proposal. Giới hạn B kiểm ở validator và controller phòng lỗi.

Cài đặt đề xuất: downstream cả hai nhánh cài trước; đổi rule chọn uplink ở source edge. Ghi xid/error, gửi Barrier, đối chiếu rule dump và counter/probe. Barrier phản ánh xử lý thứ tự lệnh, không tự chứng minh data plane end-to-end. Traffic=0 thì probe cùng IP match. Counter rule bị reset phải dùng epoch hoặc downstream rule để tránh sai delta.

ApplyResult gồm decision_batch_id,status,started_at,completed_at,state_version_before,state_version_after,flowmod_sent_count,flowmod_error_count,apply_ms,groups[]. Mỗi group: old/proposed/actual_path_id (actual nullable khi UNKNOWN),status,reason,flowmod_xids,barrier_xid,verification_evidence_refs,verified_at,last_successful_change_at,rollback_status.

| Trạng thái batch | Ý nghĩa |
| --- | --- |
| PROPOSED/APPLYING | Đang đề xuất/thực thi; chưa xác nhận toàn bộ |
| APPLIED | Mọi thay đổi yêu cầu được xác minh thành công |
| REJECTED | Từ chối trước khi write rule |
| FAILED | Không hoàn thành, có bằng chứng không có thay đổi hiệu lực còn lại |
| ROLLED_BACK | Đã đổi rồi khôi phục được xác minh |
| PARTIAL | Một phần giữ đường mới đã xác minh; phần khác thất bại/khôi phục |
| INCONSISTENT | Ít nhất một nhóm UNKNOWN; cần đối soát trước tối ưu tiếp |

Trạng thái từng nhóm: KEPT,APPLIED,FAILED,ROLLED_BACK,UNKNOWN. Reason_code riêng, không ép reason thành status. Một batch có thể tạm trạng thái trung gian khác nhau từng nhóm, không giả định atomic.

**Writer duy nhất:** SDN. Khi xác minh một thay đổi actual path hoặc verification status, ghi state/history và tăng global state_version; version tăng bao nhiêu lần/batch phụ thuộc số transition, consumer chỉ yêu cầu đơn điệu và compare chính xác. Apply guard/pending serializes batch; thay đổi nội bộ của chính batch không bị coi external conflict. Retry của cùng decision_batch_id phải idempotent: trả result đã lưu, không gửi FlowMod lần hai. Request cùng ID khác nội dung là conflict.

Sau partial: dừng tiếp, thử rollback có log. Rollback xác minh→actual=old; rollback hỏng nhưng new xác minh→actual=new,PARTIAL; chưa rõ→actual=null,UNKNOWN/INCONSISTENT, tạm dừng tối ưu toàn cục đến resync. Giữ `last_confirmed_path_id` riêng nếu cần, không giả làm actual. Không áp proposal cũ sau khi state_version thay đổi.

`last_successful_change_at` theo lần đổi đường thực đã xác minh, gồm rollback làm đổi actual path; proposal/FlowMod lỗi không làm mới hold-down. Có thể lưu thêm `last_optimization_change_at` để phân tích. History dùng [valid_from,valid_to), initial row có nguồn INITIALIZED, sau đó OPTIMIZATION/ROLLBACK/RECOVERY; giữ cả khoảng UNKNOWN. Thời điểm hiệu lực là mốc verify/commit quan sát được, không tuyên bố packet chuyển đúng ngay issued_at.

## 11. IF09 — EventLog, quyết định giữ đường và metrics

Event JSONL có event_id (UUID/string ở producer; DB có thể map INTEGER auto tăng),run_id,component,event_at,event_mono_ns,event_type,severity,reason_code,detail object và snapshot/batch/group nullable. ID producer không được va chạm khi gom nhiều process. Cột TEXT detail ở Data.md lưu JSON chuẩn, không chuỗi khó parse.

Reason chuẩn tối thiểu: NO_TRIGGER,BELOW_IMPROVEMENT,HOLDDOWN,BUDGET_LIMIT,DATA_MISSING,DATA_INVALID,DATA_STALE,COUNTER_RESET,MODEL_LOAD_FAIL,FORECAST_INVALID,FORECAST_TIMEOUT,RF_EXCEPTION,FALLBACK_DEMAND,NO_INCUMBENT,SOLVER_ERROR,INVALID_RESULT,STATE_VERSION_MISMATCH,EXPIRED_FORECAST,INSUFFICIENT_APPLY_BUDGET,APPLY_PENDING,FLOWMOD_ERROR,FAILED_TIMEOUT,VERIFY_FAILED,ROLLBACK_FAILED,OVERLOAD.

Ghi mọi tick kể cả không solve/không đổi: một decision_batches/tick có status KEPT và reason; chi tiết per-group nếu bị khóa hoặc giữ. Không phát sinh prediction giả cho M0. Với M1 adapter không tạo prediction row thì demand trace nằm trong batch source refs/snapshot.

MetricRecord (dạng dài để hỗ trợ per-group): experiment_id,run_id,schedule_id,scenario,method,metric_name,metric_version,value nullable,unit,flow_group_id nullable,measurement_start/end,cohort_definition_ref,numerator nullable,denominator nullable,valid_duration_s nullable,coverage_ratio,n_samples,quality_status,reason,input_hashes,pipeline_commit,analysis_version. Khi xuất wide run_metrics.csv, giữ metadata và bảng phụ định nghĩa metric. Không ghép stats baseline vào run RF.

Định nghĩa công thức, phân vị, paired CI và exclusion dùng test_plan mục 8/10/11. Dataset X/y và metrics export đều có manifest/hash/version, không cần lập dịch vụ database riêng. Mỗi run có evidence index trỏ raw và verification để QA đối chiếu.

## 12. Mapping Data.md với schema tích hợp

Tên vật lý của Data có thể giữ nguyên; cung cấp view/adapter canonical. Không duy trì hai giá trị nguồn mâu thuẫn cho cùng field. Bảng dưới là yêu cầu mở rộng, không phải báo schema hiện đã hoàn tất.

| Data.md nháp | Canonical theo PDF/hợp đồng | Phần cần thêm hoặc làm rõ |
| --- | --- | --- |
| flow_groups | flow_groups | topology_version,src/dst edge và FK |
| links | network_links | direction,port/capacity/delay/queue,type,counter source/point,k_e,calibration |
| Chưa có nodes/path tables | network_nodes,network_paths,path_links | DPID/IP/MAC,2 path/group,ordered hops,topology version |
| schedules | traffic_schedules | parent_family,generator parameters/version,payload,config/hash/fingerprint |
| demand_plan | Lịch con của traffic_schedules | Alias requested_mbps→demand_mbps chỉ khi semantics payload giống hệt |
| runs | experiment_runs | Toàn manifest,measurement/drain,env/model/config/hash,quality/inclusion/rerun |
| traffic_actual | traffic_demand + transmission_metrics | Tách demand/tx/rx; requested bytes,send errors,cohort/deadline/probe; giữ packet raw |
| link_samples | link_statistics | request/reply/sample/available times,epoch,source/point,valid/reason; không gán tick giả |
| predictions | traffic_predictions | prediction_id,batch/snapshot,history_cutoff,completed_at,model/hash/status/reason |
| decisions | decision_batches + routing_decisions | Batch solver/U/J,target/version; per-group old/proposed/actual/verify; không overwrite mất proposal |
| Chưa có state/history | routing_state,routing_history | Writer SDN,version,confirmed_at,hold-down,unknown,valid_from/to |
| Chưa có liên kết nhiều forecast | decision_prediction_links | decision_batch_id,flow_group_id,prediction_id; FK và provenance |
| events | event_log | Severity,reason_code,detail JSON,producer ID namespace |

`issued_at` trong predictions là UTC; forecast_start/end dạng REAL ở Data.md là offset từ traffic_start và cần adapter rõ. Dùng canonical UTC trong API, bảo tồn offset raw. Một khóa `(run,group,decided_at)` không đủ phân biệt proposal/result và retry; dùng decision_batch_id + group, còn transition append-only ở event/history. Không ghi status applied chỉ từ optimizer output.

## 13. Versioning, lỗi hợp đồng và bảo toàn log

- API major thay đổi semantics/unit/time/key phải bump và cập nhật producer/consumer cùng lúc; schema additive chỉ dùng khi consumer chấp nhận rõ.
- Mỗi producer cung cấp golden valid fixture, missing/null/zero/NaN-encoded-invalid fixture, unknown ID, stale version và boundary-time fixture; consumer xác nhận trước integration.
- Chỉ retry an toàn đối với ingest/predict có ID và apply idempotent đã đặc tả. Không retry FlowMod mù khi timeout chưa biết kết quả; phải reconcile actual state.
- Raw append-only; parser giữ file hash/source line; clean chỉ chuẩn hóa có provenance. Conflict quarantine, không tự giữ bản cuối.
- Receiver/request timeout là trạng thái vận hành có evidence, không là bằng chứng đường cũ vẫn hiệu lực.

## 14. Checklist owner xác nhận trước khóa G1/G5

| Mục | Owner xác nhận | Trạng thái |
| --- | --- | --- |
| F=48, paths/DPID/ports, fabric directions, initial map | Network + SDN + Optimization | Pending |
| Clock/offset/availability và packet/probe/counter raw | Network + SDN + Data | Pending |
| Split/fingerprint/feature label/alias Data.md | Data + ML + QA | Pending |
| H giây trung bình, feature order, fallback/source | ML + Optimization + SDN | Pending |
| J normalized, TH/N/h/B/hold,epsilon tách margin | Optimization + SDN + QA | Pending |
| Batch version/idempotency/partial/UNKNOWN và verify evidence | SDN + Optimization + Data + QA | Pending |
| Deadline/drain/coverage/calibration và config thống nhất | Network + ML + SDN + Data + QA | Pending |
| M0–M4 access policy/order/seed/exclusion/metric endpoints | Evaluation/Testing + các owner | Pending |

QA tổng hợp quyết định D01–D07 vào decision log: decision_id,source_conflict,chosen_contract,owners,confirmed_at,config/schema hashes. Chưa có xác nhận thật thì giữ Pending; không ghi “đã chốt” chỉ vì hợp đồng đã được viết.
