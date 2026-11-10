# Tiêu chí nghiệm thu từng module

## 1. Cách sử dụng và quyết định nghiệm thu

- `Bắt buộc`: tính đúng đắn/chống leakage/truy vết; phải có evidence PASS trước khi mở gate liên quan.
- `Hiệu chuẩn`: chỉ có ngưỡng sau pilot/validation và khóa trước test; chưa có trị số thì BLOCKED cho benchmark, không tự cho PASS.
- `Nghiên cứu`: báo số đo và kết luận trung thực; không yêu cầu RF luôn thắng hoặc đạt một tỷ lệ cải thiện đặt trước.
- Trạng thái case: NOT_RUN, PASS, FAIL, BLOCKED; trạng thái sign-off: pending, accepted, rejected. Hiện tại toàn bộ case là NOT_RUN, sign-off pending.
- PASS cần expected khớp actual, đúng phiên bản/hash và có raw evidence. Thiếu evidence không là PASS. Mọi thay đổi điều kiện phải ghi version/reason và retest.

Severity blocker: leakage, APPLIED giả, state không phản ánh thực tế, sai metric chính không thể truy vết. Major: sai hợp đồng/đơn vị/constraints/tái lập. Minor: tài liệu hoặc hiển thị không làm sai kết quả. Gate benchmark: 100% case bắt buộc PASS, không blocker/major mở, đủ các ngưỡng hiệu chuẩn khóa; không dùng tỷ lệ pass 95% để bỏ qua lỗi trọng yếu.

## 2. Network

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-N01 Bắt buộc | Đúng 8 host/6 switch; mỗi edge 2 host, nối cả c1,c2; không edge–edge/c1–c2; 16 chiều fabric+16 chiều host | TC-N01; so graph với manifest, DPID/port mapping | topology export, config/hash, dump link/port |
| AC-N02 Bắt buộc | F đầy đủ 48 nhóm có hướng khác edge; mỗi nhóm đúng 2 đường ngắn; ngược chiều riêng; một nhánh/group ổn định | TC-N01; kiểm path continuity và probe ép cả hai nhánh | flow_groups, paths/path_links, probe/rule evidence |
| AC-N03 Bắt buộc | Cùng seed/config/generator tạo cùng lịch chuẩn hóa và hash; replay các method dùng cùng file; lịch đủ warm-up+đo+nhãn H nếu chấm cuối run | TC-N02; regenerate, compare, kiểm coverage | schedule CSV, manifest, seed và generator version |
| AC-N04 Bắt buộc + hiệu chuẩn | requested/tx/rx tách biệt, payload bao gồm metadata; send success/error và sequence đầy đủ; pacing trong dung sai khóa từ S0 | TC-N03; đối chiếu số packet/byte và lịch | sender/receiver raw, lag/send errors, calibration |
| AC-N05 Bắt buộc + hiệu chuẩn | Lịch core không nghẽn host theo overhead/headroom; khả năng phân bổ có chứng cứ; stress gắn nhãn | TC-N03; tổng demand vào/ra host, witness/checker | schedule validation report, k_e/headroom/hash; witness không cấp provider |
| AC-N06 Bắt buộc | ARP/IP/MAC tĩnh và probe đi cùng cặp IP match; không flood tùy ý gây loop; kênh controller ngoài fabric | TC-S01, TC-N01 | host ARP, switch flows, probe và control config |

Dung lượng 10 Mbps, queue 50, trễ 1/5 ms và payload 1200 byte là baseline khảo sát. Không bắt buộc RTT đúng 24 ms hoặc goodput đúng 10 Mbps. Trị số danh nghĩa 24 ms khứ hồi chỉ là tham chiếu tổng trễ cấu hình, cần số đo S0.

## 3. Data

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-D01 Bắt buộc | Kiểu/khóa/FK đúng; ID cùng namespace; đơn vị Mbps thập phân/byte/packet rõ; window_end>start | TC-D01; schema validation mọi loại log | schema version, invalid records/reason |
| AC-D02 Bắt buộc | NULL khác 0; âm/NaN/inf xử lý lỗi; burst theo lịch giữ nguyên; không biến dữ liệu thiếu thành 0; dự báo >capacity không bị loại chỉ vì lớn | TC-D01; fixture zero/missing/burst/out-of-domain | raw→clean diff, quality report |
| AC-D03 Bắt buộc | Ingest lại cùng log không nhân bản; duplicate giống hệt idempotent; duplicate mâu thuẫn không overwrite tùy ý | TC-D02; ingest 2 lần và chèn conflict | row counts/hashes, conflict report |
| AC-D04 Bắt buộc | Split theo schedule/parent family trước window; seed/họ/hash không rò chéo; replay cùng split; không split random từng dòng | TC-D03; intersections/fingerprint check | split_manifest, registry, checker result |
| AC-D05 Bắt buộc | Feature chỉ từ window_end≤issued_at và available_at≤issued_at; label trung bình đúng [t,t+H); không nối run/group | TC-D04; late sample, exact boundary và sample tính tay | sample_metadata, X/y fixture, history_cutoff |
| AC-D06 Bắt buộc | Counter raw giữ nguyên; reset/epoch/Δt≤0 invalid; một counter/chiều; sample/reply timestamp thật; missing coverage rõ | TC-D05; reset/delay/missing link | raw counters, epoch và throughput recomputation |
| AC-D07 Bắt buộc | Raw bất biến; clean/metric sinh lại từ raw+version/hash; một chuỗi trace end-to-end đầy đủ | TC-I01/TC-E01 | provenance/checksums, recompute diff |

Schema nháp Data.md cần mở rộng theo interfaces trước nghiệm thu tích hợp: packet/probe logs, availability, snapshot/batch/state/history và xác minh. Không bắt Data tự viết controller/sender thay các producer.

## 4. ML/AI

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-M01 Bắt buộc | Input quá khứ đúng L/Δ; output một trung bình Mbps/group trong H giây; feature/label đúng thứ tự và khoảng thời gian | TC-D04,TC-M01 | feature_schema, request/response, sample tính tay |
| AC-M02 Bắt buộc | Persistence lấy gần nhất hợp lệ; MA trung bình W theo thời lượng, W≤L; RF/MA/persistence chấm cùng target H và common support | TC-M01; lịch sử 2,3,4→M1=4,M2=3 | baseline outputs và sample list/hash |
| AC-M03 Bắt buộc | Model/hash/library/feature order/H khớp manifest; không fit trên test; không dùng seed/scenario/ID tiết lộ tương lai | TC-M02 + review train code/manifest | model artifact/hash, training manifest, feature list |
| AC-M04 Bắt buộc | RF lỗi/thiếu history/quá hạn→persistence còn mới hoặc giữ toàn tick nếu thiếu nhóm; ghi fallback/reason; không đổi method=M3 | TC-M03; exception, NaN, inf, âm, stale | events, forecast status/source, kept decision |
| AC-M05 Hiệu chuẩn | Batch feature+forecast nằm ngân sách khóa hoặc được ghi timeout/degraded; model nạp một lần | TC-M03; benchmark 48 nhóm/pilot, load event | p50/p95/max, eligible count, timeout rate, hardware |
| AC-M06 Nghiên cứu | MAE/RMSE Mbps đúng common samples; báo theo scenario/pha, native RF coverage và fallback, không suy ra network gain chỉ từ MAE | TC-E01 + recompute errors | predictions/y join, error report, latency report |

Không có tiêu chí “RF phải MAE thấp hơn M2 trên mọi kịch bản”. Nếu RF kém, giữ kết quả và phân tích; lỗi model/schema/leakage vẫn là không đạt chức năng.

## 5. Optimization

Mô hình lõi: R1 mỗi nhóm chọn một trong hai đường; R2 L_e=k_eΣb_f a_fpe x_fp; R3 L_e≤U C_e; R4 z_f=1−x_f,current; R5 Σz_f≤B; R6 hold-down khóa current; R7 x,z nhị phân,U≥0 và tham số hữu hạn không âm. Mục tiêu J=U+lambda×Σz_f/|F|, F cố định cả nhóm demand=0. Không đặt L_e≤0,9C_e hoặc L_e≤C_e trong lõi.

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-O01 Bắt buộc | Trigger U_current>TH đủ N tick hợp lệ liên tiếp; invalid/stale/commit reset; M0 không solve | TC-O01; đúng biên TH, đứt chuỗi | tick log, trigger_count và reason |
| AC-O02 Bắt buộc | Mọi phương án qua R1–R7, current có trong candidate, không chia nhóm; validator độc lập phát hiện sai B/path/hold/domain | TC-O02/O03; toy và enumeration | request, solver output, independent validator |
| AC-O03 Bắt buộc | J_old/New tính cùng snapshot/k_e/F/current; acceptance n_changes≥1,J_old>numeric_epsilon,J_new≤(1−h)J_old theo tolerance | TC-O02/O04; biên bằng/±δ | bảng recompute U/J, expected decisions |
| AC-O04 Bắt buộc | Hold-down tính từ verified successful change, không từ proposal; biên `<T_hold` khóa, `=T_hold` hết khóa; overload U>1 vẫn xét cải thiện | TC-O03; zero/B=0/overload | state timestamp, validator/load report |
| AC-O05 Bắt buộc + hiệu chuẩn | OPTIMAL có chứng nhận; time-limit incumbent chỉ dùng nếu integer feasible và qua mọi check; không incumbent/stale/version mismatch/hết hạn→giữ | TC-O05 | solver_status/gap/time, reject/kept reason |
| AC-O06 Bắt buộc | Không cộng tải đo của cùng nhóm lên tải dự báo gây double count; đủ F/16 fabric; optimizer không ghi routing_state | TC-I01; kiểm model load và state ownership | load reconciliation, code/interface record |
| AC-O07 Hiệu chuẩn | Worker không chặn controller; solve timeout và toàn pipeline đo được; late result không apply | TC-S03/TC-O05 | event-loop health, latency/deadline trace |

Toy bắt buộc: F=2, b=(6,6), C=10,k=1, cùng c1 → U_old=1,2; đổi một nhóm→U_new=0,6; lambda=0,1 →J_new=0,65. Khi F=48 với 46 nhóm zero, penalty một đổi là 0,1/48, không dùng 0,05 của fixture F=2. Các nghiệm đồng hạng có thể khác đường cụ thể; nghiệm thu tính khả thi/điểm mục tiêu theo tolerance, không ép một vector tùy ý.

## 6. SDN/Controller

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-S01 Bắt buộc | Bootstrap 6 switch OF1.3; preinstall downstream cả hai nhánh; source chọn một uplink; IPv4/probe cùng cặp theo cùng policy; ngược chiều riêng | TC-S01/N01 | connection/rule dump, match/priority/cookie, probe |
| AC-S02 Bắt buộc | Trước FlowMod kiểm expected_state_version, config/topology, snapshot age, horizon, old path và pending; không overlap apply | TC-S03/O05 | reject reason, version trace, pending log |
| AC-S03 Bắt buộc | APPLIED chỉ sau rule dump và counter/probe phù hợp; Barrier-only không đủ; zero demand dùng probe; state/history chỉ phản ánh đã xác minh | TC-S01/S02 | xid/error/barrier, verify timestamps/probe, state/history |
| AC-S04 Bắt buộc | FlowMod lỗi/mất reply/partial→dừng/đối soát/rollback; nếu thực mới ghi mới; chưa biết→UNKNOWN khóa tối ưu, không state cũ giả | TC-S02; fault 13–16 | per-group results, rollback verification, actual state |
| AC-S05 Bắt buộc + hiệu chuẩn | Solver/forecast không chặn event loop; collector giữ request/reply/sample/epoch và mapping; stale không dùng | TC-S03/D05 | heartbeats/PortStats, event latency, counter raw |
| AC-S06 Bắt buộc | Rule nhóm ngoài changes không bị đổi; mọi FlowMod gồm lỗi/rollback được đếm; decision giữ đường có reason | TC-S01/S02 | before/after flow diff, command count, events |

Batch không được quảng cáo nguyên tử toàn mạng. Khi PARTIAL, từng nhóm có thể khác trạng thái; version/state sau xác minh là nguồn cho tick sau. UNKNOWN không được tự điền old_path là actual_path.

## 7. Evaluation/Testing và nghiệm thu tích hợp

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-E01 Bắt buộc | Cohort theo send success, packet sequence và deadline/drain; warm-up loại khỏi cohort; duplicate không tăng nhận; denominator zero→N/A | TC-E01; fixture 100/90 | raw packet/probe, expected/actual metrics |
| AC-E02 Bắt buộc | MLU 16 chiều fabric đúng điểm đếm; missing coverage rõ; RTT không có reply không=0; phân vị/latency đúng định nghĩa | TC-D05/E01 | calibration, metric code/version, sample/time coverage |
| AC-E03 Bắt buộc | M1–M3 cùng lịch/F/H/config/khởi tạo/controller, chỉ thay provider; mỗi run đo mới; thứ tự/seed/hash đầy đủ | TC-E02 | run_order, manifests, raw per run, matrix completeness |
| AC-E04 Bắt buộc | Giữ run lỗi; chỉ loại infra theo rule khóa; không loại RF chậm/fallback; paired đúng schedule, báo n/missing/CI | TC-E03 | exclusions, rerun links, paired table/CI config |
| AC-E05 Nghiên cứu | Kết luận theo scenario dựa hiệu ứng/CI/chi phí; n=seed/lịch, không=giây; báo kết quả âm và giới hạn | Review report | run points, plots, report, uncertainty |
| AC-I01 Bắt buộc | Liên kết schedule→run→snapshot→forecast→decision→verified state→metric đầy đủ, không sai unit/time/id | TC-I01; một fixture end-to-end và một lượt pilot thật | evidence index, interface validation, state/history |
| AC-I02 Bắt buộc | D01–D07 có owner/xác nhận; một config hash, schema/API version và adapter Data.md được khóa | Review baseline và round-trip JSONL | decision log, signed confirmations, contract fixtures |

## 8. Bộ chèn lỗi và expected chung

| Fault PDF | Thao tác kiểm thử | Kết quả bắt buộc |
| --- | --- | --- |
| 01 thiếu mẫu | Bỏ mẫu với tỷ lệ/vị trí có seed | DATA_MISSING; NULL, fallback còn hợp lệ hoặc giữ |
| 03 sai giá trị/đơn vị | Chèn âm/×1000 theo hook có khai báo | DATA_INVALID; không âm thầm sửa thang đo/điền zero |
| 05 stale | Trễ collector vượt T_stale | DATA_STALE; không apply snapshot cũ |
| 06 thiếu L | Xóa đầu history | Fallback đủ mới hoặc kept toàn tick |
| 07 forecast lỗi/quá hạn | Exception/worker delay | FORECAST_TIMEOUT/ERROR; không apply horizon hết hạn |
| 08 forecast bất thường | NaN/âm/inf/sai unit | FORECAST_INVALID; >capacity riêng không đủ kết luận lỗi |
| 09 model load | Hỏng file/hash/feature manifest | MODEL_LOAD_FAIL; M3 degraded có log |
| 10 solver | Timeout có/không incumbent; inject invalid result | Chỉ dùng nghiệm kiểm chứng đủ hạn; còn lại giữ, không OPTIMAL giả |
| 13 mất reply switch | Ngắt phản hồi trong lab | FAILED_TIMEOUT; verify/reconcile, không APPLIED giả |
| 14 reject FlowMod | Hook lỗi operation | FAILED/PARTIAL; rollback có kiểm chứng |
| 15 partial update | Bỏ thao tác một nhóm trong batch | Per-group actual; rollback/PARTIAL/UNKNOWN đúng bằng chứng |
| 16 verify sai | Rule hợp cú pháp nhưng sai cổng | VERIFY_FAILED; không commit APPLIED, cách ly khi UNKNOWN |

Chèn lỗi vào bộ riêng trên S3 hoặc S5 với 3 seed/fault theo PDF; đối chứng sạch cùng lịch/cấu hình. Các bài counter reset, duplicate, overload, invalid route vẫn có unit/integration test bắt buộc. Controller restart (fault18) là mở rộng; không đưa thành blocker ngoài scope chính. Không cam kết “phục hồi trong 5 tick” hoặc “goodput≥90%” khi chưa có ngưỡng pilot và điều kiện tải rõ.

## 9. Ngưỡng phải chốt trước benchmark

| Trường | Cách xác định | Owner | Trạng thái hiện tại |
| --- | --- | --- | --- |
| `pacing_error_tolerance`, `s0_loss_tolerance` | Pilot S0 theo payload/config | Network + QA | MUST_LOCK |
| `counter_measurement_point`, `overhead_factor`, `host_headroom_ratio` | So sender/receiver/OVS/qdisc, kiểm witness/headroom | Network + SDN + Data | MUST_LOCK |
| `forecast_timeout_ms`, `apply_timeout_ms`, `apply_guard_ms`, `cycle_budget` | Latency pilot, đủ ngân sách T_ctl/H | ML + Optimization + SDN | MUST_LOCK |
| `packet_deadline_s`, `drain_s` | Packet delay/drain pilot; drain≥deadline theo policy đề xuất | Network + Data + QA | MUST_LOCK |
| `minimum_metric_coverage_ratio`, `minimum_rtt_samples`, `external_load_invalid_rule` | Chất lượng phép đo/nhiễu máy pilot | Data + Evaluation + QA | MUST_LOCK |
| `numeric_epsilon`, `feasibility_tolerance`, `objective_tolerance` | Đơn vị mô hình và solver/config | Optimization + QA | MUST_LOCK |
| `probe_interval_s`, `rebin_policy`, `warmup_policy`, `rerun_limit` | Ghi rule thống nhất, pilot kiểm khả năng quan sát | SDN + Data + QA | MUST_LOCK |

Coverage/latency là tiêu chí chất lượng phép đo hoặc vận hành có điều kiện; không dùng các ngưỡng này sau test để chọn bỏ run bất lợi do bản thân phương pháp.

## 10. Mẫu biên bản nghiệm thu

```csv
criterion_id,module,test_case_id,git_commit,config_hash,run_id,expected,actual,status,evidence_uri,defect_id,producer_confirmed_by,qa_reviewed_by,reviewed_at_utc
AC-N01,Network,TC-N01,,,,"6 switch 8 host đúng topology",,NOT_RUN,,,,,
```

Mỗi module có producer xác nhận evidence và QA đối chiếu; Testing tổng hợp sign-off. Không ghi tên người ký hoặc ngày nghiệm thu giả. Chấp nhận thiết kế giao diện không đồng nghĩa nghiệm thu hiệu năng hay kết luận RF tốt hơn.
