# Tiêu chí nghiệm thu từng module

- Phiên bản `testing_v2 2026-11-10`.
- Đồng bộ test_plan_v2.md và interfaces_v2.md.
- Căn cứ bổ sung: báo cáo rà soát thiết kế tích hợp và các file thành viên hiện nhận.
- Phụ trách Testing/QA/QC.
- Trạng thái: hoàn thiện đặc tả để owner xác nhận, chưa nghiệm thu triển khai.

Bộ này hoàn thiện tiêu chí thiết kế, không công bố module đã chạy đạt. Các quyết định C01–C16 còn OPEN/PENDING owner; toàn bộ case NOT_RUN. Case phụ thuộc contract chưa chốt được đánh BLOCKED khi lập lịch thực thi.

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
| AC-N06 Bắt buộc | IP/MAC đúng config, ARP tĩnh hoặc handler có kiểm soát và probe đi cùng cặp IP match; không flood tùy ý gây loop; kênh controller ngoài fabric | TC-S01, TC-N01 | host ARP, switch flows, probe và control config |

Dung lượng 10 Mbps, queue 50, trễ 1/5 ms và payload 1200 byte là baseline khảo sát. Không bắt buộc RTT đúng 24 ms hoặc goodput đúng 10 Mbps. Trị số danh nghĩa 24 ms khứ hồi chỉ là tham chiếu tổng trễ cấu hình, cần số đo S0.

## 3. Data

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-D01 Bắt buộc | Kiểu/khóa/FK đúng; ID cùng namespace; đơn vị Mbps thập phân/byte/packet rõ; window_end>start | TC-D01; schema validation mọi loại log | schema version, invalid records/reason |
| AC-D02 Bắt buộc | NULL khác 0; âm/NaN/inf xử lý lỗi; burst theo lịch giữ nguyên; không biến dữ liệu thiếu thành 0; dự báo >capacity không bị loại chỉ vì lớn | TC-D01; fixture zero/missing/burst/out-of-domain | raw→clean diff, quality report |
| AC-D03 Bắt buộc | Ingest lại cùng log không nhân bản; duplicate giống hệt idempotent; duplicate mâu thuẫn không overwrite tùy ý | TC-D02; ingest 2 lần và chèn conflict | row counts/hashes, conflict report |
| AC-D04 Bắt buộc | Split theo schedule/parent family trước window; seed/họ/hash không rò chéo; replay cùng split; không split random từng dòng | TC-D03; intersections/fingerprint check | split_manifest, registry, checker result |
| AC-D05 Bắt buộc | Feature chỉ từ window_end≤history_cutoff và available_at≤snapshot_as_of≤decision time; y_vector H bước và y_mean đúng [t,t+H); không nối run/group | TC-D04; late sample, exact boundary và sample tính tay | sample_metadata, X/y fixture, history_cutoff |
| AC-D06 Bắt buộc | Counter raw giữ nguyên; reset/epoch/Δt≤0 invalid; một counter/chiều; sample/reply timestamp thật; missing coverage rõ | TC-D05; reset/delay/missing link | raw counters, epoch và throughput recomputation |
| AC-D07 Bắt buộc | Raw bất biến; clean/metric sinh lại từ raw+version/hash; một chuỗi trace end-to-end đầy đủ | TC-I01/TC-E01 | provenance/checksums, recompute diff |
| AC-D08 Bắt buộc | PK predictions thêm forecast_start; lưu đủ48H dòng cùng issue; batch READY chỉ khi đủ/contiguous/unique, không trộn đợt hoặc trộn nguồn không khai báo; fallback có source map | TC-D06;240 rows H5, thiếu1/dup/conflict/2 issue | schema migration, batch registry/commit marker, invalid report, mean derivation |
| AC-D09 Bắt buộc | Bảo toàn batch/per-group/actual/state/history; ingest lặp không mất UNKNOWN hay tăng bản ghi | TC-D07; batch3 nhóm APPLIED/ROLLED_BACK/UNKNOWN | ApplyResponse/source tables/view round-trip, version/hash và uncertainty events |

Schema `Data.md` hiện nhận tự ghi schema_v1; cần mở rộng theo interfaces v2 trước nghiệm thu tích hợp: packet/probe logs, availability, snapshot/batch/state/history và xác minh. Không bắt Data tự viết controller/sender thay các producer.

## 4. ML/AI

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-M01 Bắt buộc | Input L bins causal; output H giá trị/group, không ép scalar; Data giữ H rows, mean adapter đúng; feature/label đúng thứ tự/khoảng | TC-D04,TC-M01 | feature_schema, request/response, sample tính tay |
| AC-M02 Bắt buộc | Persistence lấy gần nhất hợp lệ; MA trung bình W theo thời lượng, W≤L; RF/MA/persistence chấm cùng target H và common support | TC-M01; lịch sử 2,3,4→M1=[4]×H,M2=[3]×H | baseline outputs và sample list/hash |
| AC-M03 Bắt buộc | Model/hash/library/feature order/H khớp manifest; không fit trên test; không dùng seed/scenario/ID tiết lộ tương lai; one-hot group ID được phép theo feature_schema khóa | TC-M02 + review train code/manifest | model artifact/hash, training manifest, feature list |
| AC-M04 Bắt buộc | RF lỗi/thiếu history/quá hạn→persistence còn mới hoặc giữ toàn tick nếu thiếu nhóm; ghi fallback/reason; không đổi method=M3 | TC-M03; exception, NaN, inf, âm, stale | events, forecast status/source, kept decision |
| AC-M05 Hiệu chuẩn | Batch feature+forecast nằm ngân sách khóa hoặc được ghi timeout/degraded; model nạp một lần | TC-M03; benchmark 48 nhóm/pilot, load event | p50/p95/max, eligible count, timeout rate, hardware |
| AC-M06 Nghiên cứu | MAE/RMSE Mbps theo bước, pooled vector và mean-H tính riêng trên common samples; báo theo scenario/pha, native RF coverage và fallback, không suy ra network gain chỉ từ MAE | TC-E01 + recompute errors | predictions/y join, error report, latency report |
| AC-M07 Bắt buộc | to_prediction_rows xuất7 cột đúng;48H rows, canonical IDs và origin measurement; source/model/batch refs truy vết | TC-D06/TC-I03; readback H5 đủ240 rows | predictions rows, forecast_batches registry và model metadata |
| AC-M08 Bắt buộc | Policy v2 ML-owned epsilon: raw4,eps.1→final4.4; Optimization dùng4.4, không4.84; baseline/oracle epsilon0 | TC-M04 và TC-O02; kiểm cả vector/mean | raw/final prediction refs, epsilon_owner/applied/ratio, b_used/load recompute |

Không có tiêu chí “RF phải MAE thấp hơn M2 trên mọi kịch bản”. Nếu RF kém, giữ kết quả và phân tích; lỗi model/schema/leakage vẫn là không đạt chức năng.

## 5. Optimization

Mô hình lõi: R1 mỗi nhóm chọn một trong hai đường; R2 L_e=k_eΣb_f a_fpe x_fp; R3 L_e≤U C_e; R4 y_f=1−x_f,current (không chỉ ≥, kiểm cả λ=0); R5 Σy_f≤B; R6 hold-down khóa current; R7 x,y nhị phân,U≥0 và tham số hữu hạn không âm. Epsilon đã áp tại ML, không nhân thêm ở R2. Mục tiêu J=U+lambda×Σy_f/|F|, F cố định cả nhóm demand=0. Không đặt L_e≤0,9C_e hoặc L_e≤C_e trong lõi.

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-O01 Bắt buộc | Trigger U_current>TH đủ N tick hợp lệ liên tiếp; invalid/stale/commit reset; M0 không solve | TC-O01; N3 đề xuất, đúng biên TH, đứt chuỗi | tick log, trigger_count và reason |
| AC-O02 Bắt buộc | Mọi phương án qua R1–R7, current có trong candidate, không chia nhóm; validator độc lập phát hiện sai B/path/hold/domain | TC-O02/TC-O03; toy và enumeration | request, solver output, independent validator |
| AC-O03 Bắt buộc | J_old/New tính cùng snapshot/k_e/F/current; acceptance n_changes≥1,J_old>numeric_epsilon,J_new≤(1−h)J_old theo tolerance | TC-O02/TC-O04; biên bằng/±δ | bảng recompute U/J, expected decisions |
| AC-O04 Bắt buộc | Hold-down từ verified actual path change kể cả verified rollback; bootstrap null không khóa; không từ proposal; biên `<T_hold` khóa, `=T_hold` hết khóa; overload U>1 vẫn xét cải thiện | TC-O03; zero/B=0/overload | state timestamp, validator/load report |
| AC-O05 Bắt buộc + hiệu chuẩn | OPTIMAL có chứng nhận; time-limit incumbent chỉ dùng nếu integer feasible và qua mọi check; không incumbent/stale/version mismatch/hết hạn→giữ | TC-O05 | solver_status/gap/time, reject/kept reason |
| AC-O06 Bắt buộc | Không cộng tải đo của cùng nhóm lên tải dự báo gây double count; đủ F/16 fabric; optimizer không ghi routing_state | TC-I01; kiểm model load và state ownership | load reconciliation, code/interface record |
| AC-O07 Hiệu chuẩn | Worker không chặn controller; solve timeout và toàn pipeline đo được; late result không apply | TC-S03/TC-O05 | event-loop health, latency/deadline trace |
| AC-O08 Bắt buộc | Mẫu số F48 cố định; count từ routes thật; R4 đúng khi λ=0; ví dụ U_new=.58, một đổi có J=.5820833333, bốn đổi .5883333333 | TC-O02/TC-O03/TC-O04; enumeration và đối chiếu ví dụ gốc .61 | solver variables/routes, independent objective report |

Toy bắt buộc: F=2, b=(6,6), C=10,k=1, cùng c1 → U_old=1,2; đổi một nhóm→U_new=0,6; lambda=0,1 →J_new=0,65. Khi F=48 với 46 nhóm zero, penalty một đổi là 0,1/48, không dùng 0,05 của fixture F=2. Các nghiệm đồng hạng có thể khác đường cụ thể; nghiệm thu tính khả thi/điểm mục tiêu theo tolerance, không ép một vector tùy ý.

## 6. SDN/Controller

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-S01 Bắt buộc | Bootstrap 6 switch OF1.3; preinstall downstream cả hai nhánh; source chọn một uplink; IPv4/probe cùng cặp theo cùng policy; ngược chiều riêng | TC-S01/TC-N01 | connection/rule dump, match/priority/cookie, probe |
| AC-S02 Bắt buộc | Trước FlowMod kiểm expected_state_version, config/topology, snapshot age, horizon, old path và pending; không overlap apply | TC-S03/TC-O05 | reject reason, version trace, pending log |
| AC-S03 Bắt buộc | APPLIED chỉ sau rule dump và counter/probe phù hợp; Barrier-only không đủ; zero demand dùng probe; state/history chỉ phản ánh đã xác minh | TC-S01/TC-S02 | xid/error/barrier, verify timestamps/probe, state/history |
| AC-S04 Bắt buộc | FlowMod lỗi/mất reply/partial→dừng/đối soát/rollback; nếu thực mới ghi mới; chưa biết→UNKNOWN khóa tối ưu, không state cũ giả | TC-S02; F09–F12 | per-group results, rollback verification, actual state |
| AC-S05 Bắt buộc + hiệu chuẩn | Solver/forecast không chặn event loop; collector giữ request/reply/sample/epoch và mapping; stale không dùng | TC-S03/TC-D05 | heartbeats/PortStats, event latency, counter raw |
| AC-S06 Bắt buộc | Rule nhóm ngoài changes không bị đổi; mọi FlowMod gồm lỗi/rollback được đếm; decision giữ đường có reason | TC-S01/TC-S02 | before/after flow diff, command count, events |
| AC-S07 Bắt buộc | Global state_version tối đa+1/final batch khi confirmed state/độ chắc chắn/metadata hold-down đổi; mọi row/response cùng version; history UNKNOWN là gap+event | TC-D07/TC-S02; reject/bootstrap/partial/rollback/unknown | before/after state, response, history và event timeline |
| AC-S08 Bắt buộc | Retry cùng ID/payload không thêm FlowMod; ID khác payload conflict; pending batch→BUSY; không queue proposal cũ | TC-S03; duplicate lúc pending và sau final | payload hash, response cache, FlowMod count |
| AC-S09 Bắt buộc | Forecast hết hạn trước write→reject; verify muộn sau write→ghi actual+DEADLINE_MISSED và reconcile, không báo old/REJECTED giả | TC-S02/TC-S03; delay verify qua forecast_end | timestamps, actual state, deadline flag và evidence |

Batch không được quảng cáo nguyên tử toàn mạng. Khi PARTIAL, từng nhóm có thể khác trạng thái; version/state sau xác minh là nguồn cho tick sau. UNKNOWN không được tự điền old_path là actual_path.

## 7. Evaluation/Testing và nghiệm thu tích hợp

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-E01 Bắt buộc | Cohort theo send success, packet sequence và deadline/drain; warm-up loại khỏi cohort; duplicate không tăng nhận; denominator zero→N/A | TC-E01; fixture 100/90 | raw packet/probe, expected/actual metrics |
| AC-E02 Bắt buộc | MLU 16 chiều fabric đúng điểm đếm; missing coverage rõ; RTT không có reply không=0; phân vị/latency đúng định nghĩa | TC-D05/TC-E01 | calibration, metric code/version, sample/time coverage |
| AC-E03 Bắt buộc | M0–M4 cùng lịch/F/H/core config/khởi tạo/controller; M1–M3 chỉ thay provider/calibration đã khai báo; mỗi run đo mới; thứ tự/seed/hash đầy đủ | TC-E02 | run_order, manifests, raw per run, matrix completeness |
| AC-E04 Bắt buộc | Giữ run lỗi; chỉ loại infra theo rule khóa; không loại RF chậm/fallback; paired đúng schedule, báo n/missing/CI | TC-E03 | exclusions, rerun links, paired table/CI config |
| AC-E05 Nghiên cứu | Kết luận theo scenario dựa hiệu ứng/CI/chi phí; n=seed/lịch, không=giây; báo kết quả âm và giới hạn | Review report | run points, plots, report, uncertainty |
| AC-I01 Bắt buộc | Liên kết schedule→run→snapshot→forecast→decision→verified state→metric đầy đủ, không sai unit/time/id | TC-I01; một fixture end-to-end và một lượt pilot thật | evidence index, interface validation, state/history |
| AC-I02 Bắt buộc | C01–C16 có owner/xác nhận; một config hash, schema/API version và adapter Data(2).md được khóa | Review baseline và round-trip JSONL | decision log, signed confirmations, contract fixtures |
| AC-I03 Bắt buộc | Tách traffic/run/measurement origin; round-trip UTC–offset đúng; issued_at không giả forecast_start | TC-I03; traffic10:00,measure10:01:10,bin100→10:02:50Z | run manifest/time mapping, prediction rows và ApplyRequest |

## 8. Bộ chèn lỗi và expected chung

Mã F01–F12 dưới đây thuộc protocol testing v2, không gán thành số phụ lục đề bài chưa được đối chiếu trong lượt này. Mỗi loại3 fault seed trên S3 hoặc S5 riêng, kèm clean control cùng lịch khi cần; tổng36 lượt lỗi. Counter reset/duplicate/invalid route còn là contract cases bắt buộc ngoài36 lượt.

| Fault | Hook / thao tác có seed | Expected bắt buộc / case |
| --- | --- | --- |
| F01 | Bỏ history row một nhóm/bin | NULL+DATA_MISSING, fallback hợp lệ hoặc skip tick; TC-D01/TC-D04 |
| F02 | Giá trị âm/sai đơn vị×1000 theo fixture khai báo | Reject/invalid, không đoán sửa scale; TC-D01 |
| F03 | Trễ collector/release vượt T_stale | Không dùng snapshot stale; TC-D04/TC-D05 |
| F04 | Xóa đầu L-history | Fallback còn mới hoặc KEPT toàn tick; TC-M03 |
| F05 | Worker inference exception/timeout | FORECAST_TIMEOUT/ERROR, M3 degraded/kept; TC-M03 |
| F06 | Output NaN/inf/âm sau chuẩn hóa/sai shape | FORECAST_INVALID; hữu hạn >capacity riêng không là lỗi; TC-M02 |
| F07 | Hỏng model/hash/feature order | MODEL_LOAD_FAIL, không dùng artifact sai; TC-M02/TC-M03 |
| F08 | Solver timeout có/không incumbent hoặc invalid result | Chỉ dùng integer feasible, validator pass,còn hạn; TC-O05 |
| F09 | Mất reply OpenFlow/verify timeout | Reconcile actual, không APPLIED giả; TC-S02 |
| F10 | Reject FlowMod hook | FAILED/PARTIAL/rollback theo trạng thái đã verify; TC-S02 |
| F11 | Bỏ một thao tác trong batch nhiều nhóm | Per-group actual/UNKNOWN đúng; không atomic giả; TC-D07/TC-S02 |
| F12 | Rule đúng cú pháp nhưng sai port/nhánh | VERIFY_FAILED, không commit APPLIED; TC-S02 |

Không cam kết phục hồi5 tick/goodput90% khi chưa có điều kiện tải và ngưỡng pilot. Lỗi link/switch vật lý/controller restart là mở rộng riêng. Fault hooks cần log thời điểm/đối tượng/thời lượng/fault_seed và được reset trước clean run; không sửa raw log để giả lỗi phát sinh trong runtime.

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
| `ID_catalog`, `origin_policy`, `forecast_batch_completeness`, `epsilon_owner` | Golden round-trip; H-step PK và metadata; ML-owned proposal v2 | Network+Data+ML+Optimization+SDN | MUST_LOCK |
| `TH/N/h/B/hold/lambda`, `transport/ack/retry`, `state_version_policy` | Một config và executable contract fixtures | Optimization+SDN+Data+QA | MUST_LOCK |
| `split_manifest`, `benchmark_matrix`, `initial_paths_hash` | Baseline đề xuất10/5/5,45 test schedules×5methods=225runs, S5 chính | Network+Data+QA | MUST_LOCK |

Coverage/latency là tiêu chí chất lượng phép đo hoặc vận hành có điều kiện; không dùng các ngưỡng này sau test để chọn bỏ run bất lợi do bản thân phương pháp.

## 10. Mẫu biên bản nghiệm thu

```csv
criterion_id,module,test_case_id,git_commit,config_hash,run_id,expected,actual,status,evidence_uri,defect_id,producer_confirmed_by,qa_reviewed_by,reviewed_at_utc
AC-N01,Network,TC-N01,,,,"6 switch 8 host đúng topology",,NOT_RUN,,,,,
```

Mỗi module có producer xác nhận evidence và QA đối chiếu; Testing tổng hợp sign-off. Không ghi tên người ký hoặc ngày nghiệm thu giả. Chấp nhận thiết kế giao diện không đồng nghĩa nghiệm thu hiệu năng hay kết luận RF tốt hơn.

## 11. Kết luận nghiệm thu và việc làm tiếp

Hiện đủ cơ sở để lập đặc tả kiểm thử, chưa đủ cơ sở kết luận module PASS hay nhóm hoàn thành bao nhiêu phần trăm. Bản v2 đã phản ánh C01–C14 của báo cáo và bổ sung C15 (prediction PK/completeness), C16 (version/history testing cũ lệch SDN). C03 indexing, C07 epsilon, C09 N và C14 “upper bound” trong báo cáo được xử lý theo test_plan mục2, không sao chép mặc định.

Trình tự: owner xác nhận contract và sửa file mình → giao fixtures/schema/mã → QA chạy case và lập defect → pilot/S0 khóa phép đo → freeze → benchmark → tái tính metric/paired analysis → sign-off. QA có thể nghiệm thu chức năng khi RF không thắng baseline; kết quả nghiên cứu vẫn phải giữ và phân tích trung thực.
