# Tiêu chí nghiệm thu từng module

- Phiên bản `testing_v2-aligned-v1.4 2026-11-14`.
- Đồng bộ [test_plan_v2.md](test_plan_v2.md), [interfaces_v2.md](interfaces_v2.md) và [integration_requirements.md](integration_requirements.md).
- Căn cứ cập nhật: [Integration Guide v1.4](../../docs/architecture/SDN_Traffic_Engineering_Role_Integration_Guide_v1.4.md), đặc biệt mục 3–6, 10–13. Báo cáo conflict và tài liệu role cũ chỉ dùng bổ sung khi không trái v1.4.
- Phụ trách Testing/QA/QC.
- Trạng thái: hoàn thiện đặc tả để owner xác nhận, chưa nghiệm thu triển khai.

Bộ này quy định tiêu chí kiểm thử, không công bố module đã chạy đạt. Các quy tắc đã xác nhận trong v1.4 (ID theo Network; SDN tạo snapshot; ML gửi đúng 7 trường; Optimization tự tính mean) là căn cứ cập nhật, không tiếp tục coi là lựa chọn OPEN. Các chi tiết giao thức, ngưỡng pilot và quyết định chưa được khóa vẫn cần owner xác nhận. Toàn bộ case hiện NOT_RUN; case phụ thuộc contract chưa chốt được đánh BLOCKED khi lập lịch thực thi.

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
| AC-N02 Bắt buộc | F đầy đủ 48 nhóm có hướng khác edge, 96 paths và 192 path–fabric-link incidences; `flow_group_id=0x{src×256+dst:X}` (h1→h3 là `0x103`), path `0x103_P1`/`0x103_P2`, link đúng catalog Network như `e1>c1`; P1 qua c1, P2 qua c2; ngược chiều riêng | TC-N01; kiểm catalog, path continuity và probe ép cả hai nhánh | flow_groups, paths/path_links, mapping cookie/match, probe/rule evidence |
| AC-N03 Bắt buộc | Cùng seed/config/generator tạo cùng lịch chuẩn hóa và hash; replay các method dùng cùng file; lịch đủ warm-up và measurement; mặc định chỉ forecast khi `forecast_end≤measurement_end`, tail riêng chỉ khi được duyệt và không vào metric chính | TC-N02; regenerate, compare, kiểm coverage và cutoff cuối run | schedule CSV, manifest, seed và generator version |
| AC-N04 Bắt buộc + hiệu chuẩn | requested/tx/rx tách biệt, payload bao gồm metadata; send success/error và sequence đầy đủ; pacing trong dung sai khóa từ S0 | TC-N03; đối chiếu số packet/byte và lịch | sender/receiver raw, lag/send errors, calibration |
| AC-N05 Bắt buộc + hiệu chuẩn | Lịch core không nghẽn host theo overhead/headroom; khả năng phân bổ có chứng cứ; stress gắn nhãn | TC-N03; tổng demand vào/ra host, witness/checker | schedule validation report, k_e/headroom/hash; witness không cấp provider |
| AC-N06 Bắt buộc | IP/MAC đúng config, ARP tĩnh hoặc handler có kiểm soát và probe đi cùng cặp IP match; không flood tùy ý gây loop; kênh controller ngoài fabric | TC-S01, TC-N01 | host ARP, switch flows, probe và control config |

Dung lượng 10 Mbps, queue 50, trễ host-edge/edge-core 1/5 ms theo topology v1.4; payload 1200 byte là giá trị pilot cần khóa. RTT danh nghĩa 24 ms là tham chiếu trước queueing/processing; mục tiêu S0 ±5%, sai lệch throughput dưới 5% sau khi khóa overhead/cohort và CPU dưới 80% trong v1.4 là **đề xuất**, cần kiểm chứng và freeze trước nghiệm thu, không phải số đo đã đạt. Hiệu chuẩn k_e cần thêm mức không nghẽn như 2/4/6 Mbps; `flow_cookie` cùng trị số flow ID nhưng không thay thế match fields hay khóa đối tượng flow.

## 3. Data

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-D01 Bắt buộc | Kiểu/khóa/FK đúng; ID theo catalog Network như `0x103`, `0x103_P1`, `e1>c1`; không dùng namespace cũ trong interface; đơn vị Mbps thập phân/byte/packet rõ; window_end>start | TC-D01; schema validation mọi loại log, reject ID không thuộc catalog | schema version, invalid records/reason |
| AC-D02 Bắt buộc | NULL khác 0; âm/NaN/inf xử lý lỗi; burst theo lịch giữ nguyên; không biến dữ liệu thiếu thành 0; dự báo >capacity không bị loại chỉ vì lớn | TC-D01; fixture zero/missing/burst/out-of-domain | raw→clean diff, quality report |
| AC-D03 Bắt buộc | Ingest lại cùng log không nhân bản; duplicate giống hệt idempotent; duplicate mâu thuẫn không overwrite tùy ý | TC-D02; ingest 2 lần và chèn conflict | row counts/hashes, conflict report |
| AC-D04 Bắt buộc | Train/validation/test theo tỷ lệ đã chốt70/15/15 (20 seeds→14/3/3); chia theo schedule/parent family trước window; seed/họ/hash không rò chéo; replay cùng split; không split random từng dòng | TC-D03; kiểm tỷ lệ/số lượng, intersections/fingerprint | split_manifest, registry, checker result |
| AC-D05 Bắt buộc | Feature lấy từ snapshot SDN đã lưu: `window_end≤history_cutoff_utc`, `released_at_utc≤snapshot_as_of_utc`, `available_at_utc≤snapshot_as_of_utc`; X=[t−L,t), Y=[t,t+H), không nối run/group; label offline không được lộ vào snapshot online | TC-D04; late/released-late sample, exact boundary t100/L30/H5→X70–99,Y100–104 | sample_metadata, X/y fixture, snapshot ID/hash/cutoff |
| AC-D06 Bắt buộc | Counter raw giữ nguyên; reset/epoch/Δt≤0 invalid; một counter/chiều; sample/reply timestamp thật; missing coverage rõ | TC-D05; reset/delay/missing link | raw counters, epoch và throughput recomputation |
| AC-D07 Bắt buộc | Raw bất biến; clean/metric sinh lại từ raw+version/hash; một chuỗi trace end-to-end đầy đủ | TC-I01/TC-E01 | provenance/checksums, recompute diff |
| AC-D08 Bắt buộc | `predictions` đúng 7 trường và PK `(run_id,flow_group_id,issued_at,forecast_start)`; Data đọc H* từ frozen config/RunManifest để kiểm đủ 48×H* rows liên tục/unique, cùng `issued_at` và `source`; Data tự tạo registry/ForecastBatchRef, chỉ READY sau validate; không yêu cầu ML gửi batch/config/mean | TC-D06; H*=5 có 240 rows, thiếu1/dup/conflict/2 issue/mixed source/extra field/config mismatch | schema migration, config ref/hash, Data batch registry/commit marker, invalid report |
| AC-D09 Bắt buộc | Bảo toàn batch/per-group/actual/state/history; ingest lặp không mất UNKNOWN hay tăng bản ghi | TC-D07; batch3 nhóm APPLIED/ROLLED_BACK/UNKNOWN | ApplyResponse/source tables/view round-trip, version/hash và uncertainty events |
| AC-D10 Bắt buộc | Data chỉ validate/ingest/store/query snapshot do SDN tạo; giữ nguyên ID/version/hash/nội dung, không rebuild bằng query hay chèn late data vào snapshot cũ; Data là DB writer duy nhất, SDN là nguồn route actual | TC-D04/TC-I01; ingest/readback, late row và duplicate snapshot | snapshot SDN gốc, SnapshotStoreAck, readback/hash diff, quality/reject reason |

Schema/migration triển khai phải khớp contract đã freeze và v1.4: packet/probe logs, availability, snapshot storage, batch/state/history và xác minh. Mở rộng registry/envelope không được thêm cột vào 7-field `predictions`. Data không tạo snapshot, không tính mean dự báo và không viết controller/sender thay producer. Nguồn cấp `requested_mbps` đã release/available tới SDN vẫn MUST_LOCK; không được suy demand từ goodput/port counters.

## 4. ML/AI

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-M01 Bắt buộc | ML đọc snapshot SDN do Data cấp và frozen ML config; target `requested_mbps`; input L* bins causal, output H* giá trị/group; Data giữ H* rows, Optimization tự mean; feature/label đúng thứ tự/khoảng | TC-D04,TC-M01 | feature_schema, snapshot/config refs, 7-field rows, sample tính tay |
| AC-M02 Bắt buộc | Persistence lấy gần nhất hợp lệ; MA trung bình W theo thời lượng, W≤L; RF/MA/persistence chấm cùng target H và common support | TC-M01; lịch sử 2,3,4→M1=[4]×H,M2=[3]×H | baseline outputs và sample list/hash |
| AC-M03 Bắt buộc | ML tự chọn L*/H*/epsilon*/W/model params trên validation, lưu config/model metadata riêng; hash/library/feature order khớp manifest; không fit/tune bằng test; không dùng seed/scenario/ID tiết lộ tương lai; one-hot canonical flow ID được phép theo feature_schema khóa | TC-M02 + review train code/manifest | frozen ml_config/model artifact/hash, validation report, training manifest, feature list |
| AC-M04 Bắt buộc | RF lỗi nhưng history hợp lệ→có thể dùng Persistence theo policy freeze, `source=fallback_persistence`; thiếu history hợp lệ/batch thiếu→INVALID và skip tick; READY không trộn source; ghi fallback/reason, method vẫn M3 | TC-M03; exception, NaN, inf, âm, stale, mixed-source fallback | events, 7-field rows/source, Data batch status, skipped tick |
| AC-M05 Hiệu chuẩn | Batch feature+forecast nằm ngân sách khóa hoặc được ghi timeout/degraded; model nạp một lần | TC-M03; benchmark 48 nhóm/pilot, load event | p50/p95/max, eligible count, timeout rate, hardware |
| AC-M06 Nghiên cứu | MAE/RMSE Mbps theo bước, pooled vector và mean-H tính riêng trên common samples; báo theo scenario/pha, native RF coverage và fallback, không suy ra network gain chỉ từ MAE | TC-E01 + recompute errors | predictions/y join, error report, latency report |
| AC-M07 Bắt buộc | ML chỉ xuất `run_id, flow_group_id, issued_at, forecast_start, forecast_end, predicted_mbps, source`; đủ 48×H* rows, canonical IDs và origin measurement; `predicted_mbps` là final; không gửi L/H/epsilon/raw/mean/batch metadata | TC-D06/TC-I03; readback H*=5 đủ240 rows, reject extra fields | predictions rows, Data forecast registry, frozen config/model metadata riêng |
| AC-M08 Bắt buộc | ML áp epsilon đúng một lần: raw4,eps.1→final4.4, Optimization dùng4.4, không4.84; final=max(0,(1+epsilon)raw); chọn epsilon validation, không cải thiện hoặc tie→0; baseline/Oracle config phải freeze riêng | TC-M04 và TC-O02; kiểm vector/mean và config đã khóa | ML-local raw fixture/artifact nếu cần, frozen epsilon config/validation, final rows và b_used/load recompute |

Không có tiêu chí “RF phải MAE thấp hơn M2 trên mọi kịch bản”. Nếu RF kém, giữ kết quả và phân tích; lỗi model/schema/leakage vẫn là không đạt chức năng.

## 5. Optimization

Mô hình lõi: R1 mỗi nhóm chọn một trong hai đường; R2 L_e=k_eΣb_f a_fpe x_fp; R3 L_e≤U C_e; R4 y_f=1−x_f,current (không chỉ ≥, kiểm cả λ=0); R5 Σy_f≤B; R6 hold-down khóa current; R7 x,y nhị phân,U≥0 và tham số hữu hạn không âm. Epsilon đã áp tại ML, không nhân thêm ở R2. Mục tiêu J=U+lambda×Σy_f/|F|, F cố định cả nhóm demand=0. Không đặt L_e≤0,9C_e hoặc L_e≤C_e trong lõi.

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-O01 Bắt buộc | Trigger U_current>TH đủ N tick hợp lệ liên tiếp; invalid/stale/commit reset; M0 không solve | TC-O01; N3 đề xuất, đúng biên TH, đứt chuỗi | tick log, trigger_count và reason |
| AC-O02 Bắt buộc | Mọi phương án qua R1–R7, current có trong candidate, không chia nhóm; validator độc lập phát hiện sai B/path/hold/domain | TC-O02/TC-O03; toy và enumeration | request, solver output, independent validator |
| AC-O03 Bắt buộc | J_old/New tính cùng snapshot/k_e/F/current; acceptance n_changes≥1,J_old>numeric_epsilon,J_new≤(1−h)J_old theo tolerance | TC-O02/TC-O04; biên bằng/±δ | bảng recompute U/J, expected decisions |
| AC-O04 Bắt buộc | Hold-down từ verified actual path change kể cả verified rollback; bootstrap null không khóa; không từ proposal; biên `<T_hold` khóa, `=T_hold` hết khóa; overload U>1 vẫn xét cải thiện | TC-O03; zero/B=0/overload | state timestamp, validator/load report |
| AC-O05 Bắt buộc + hiệu chuẩn | OPTIMAL có chứng nhận; FEASIBLE/time-limit incumbent chỉ dùng khi policy freeze cho phép, có max_gap đã khóa và đạt gap, integer feasibility, independent validator cùng mọi check; không incumbent/stale/version mismatch/hết hạn→giữ | TC-O05 | solver_status/gap/time, policy/max_gap, reject/kept reason |
| AC-O06 Bắt buộc | Optimization tự tính b_f từ H* final rows do Data trả cùng READY batch; `[2,2.5,3,3.5,3]`→2.8 Mbps; không cộng tải đo của cùng nhóm gây double count hoặc áp epsilon lại; đủ48 flow/16 fabric; không ghi routing_state | TC-I01; kiểm mean/model load/state ownership | rows/config refs, mean/load recomputation, code/interface record |
| AC-O07 Hiệu chuẩn | Worker không chặn controller; solve timeout và toàn pipeline đo được; late result không apply | TC-S03/TC-O05 | event-loop health, latency/deadline trace |
| AC-O08 Bắt buộc | Mẫu số F48 cố định; count từ routes thật; R4 đúng khi λ=0; ví dụ U_new=.58, một đổi có J=.5820833333, bốn đổi .5883333333 | TC-O02/TC-O03/TC-O04; enumeration và đối chiếu ví dụ gốc .61 | solver variables/routes, independent objective report |

Toy bắt buộc: F=2, b=(6,6), C=10,k=1, cùng c1 → U_old=1,2; đổi một nhóm→U_new=0,6; lambda=0,1 →J_new=0,65. Khi F=48 với 46 nhóm zero, penalty một đổi là 0,1/48, không dùng 0,05 của fixture F=2. Các nghiệm đồng hạng có thể khác đường cụ thể; nghiệm thu tính khả thi/điểm mục tiêu theo tolerance, không ép một vector tùy ý.

## 6. SDN/Controller

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-S01 Bắt buộc | Bootstrap 6 switch OF1.3; preinstall downstream cả hai nhánh; source chọn một uplink; IPv4/probe cùng cặp theo cùng policy; ngược chiều riêng | TC-S01/TC-N01 | connection/rule dump, match/priority/cookie, probe |
| AC-S02 Bắt buộc | Trước FlowMod kiểm expected_state_version, config/topology, snapshot age, horizon/deadline, old path, candidate new path, B/hold-down và pending; không overlap apply | TC-S03/TC-O05 | reject reason, version trace, pending log |
| AC-S03 Bắt buộc | APPLIED chỉ sau rule dump và counter/probe phù hợp; Barrier-only không đủ; zero demand dùng probe; state/history chỉ phản ánh đã xác minh | TC-S01/TC-S02 | xid/error/barrier, verify timestamps/probe, state/history |
| AC-S04 Bắt buộc | FlowMod lỗi/mất reply/partial→dừng/đối soát/rollback; actual đã verify mới được ghi; chưa biết→per-flow UNKNOWN, batch INCONSISTENT; PARTIAL/INCONSISTENT đều khóa tick tiếp tới khi reconcile, không state cũ giả | TC-S02; F09–F12 | per-group results, rollback/reconcile verification, actual state |
| AC-S05 Bắt buộc + hiệu chuẩn | Solver/forecast không chặn event loop; collector giữ request/reply/sample/epoch và mapping; stale không dùng | TC-S03/TC-D05 | heartbeats/PortStats, event latency, counter raw |
| AC-S06 Bắt buộc | Rule nhóm ngoài changes không bị đổi; mọi FlowMod gồm lỗi/rollback được đếm; decision giữ đường có reason | TC-S01/TC-S02 | before/after flow diff, command count, events |
| AC-S07 Bắt buộc | SDN giữ global live state_version, Data mirror cùng version, không tự tăng; thay đổi đã verify tăng theo policy freeze; history UNKNOWN là gap+event; quy tắc +1/final batch và uncertainty/hold-down metadata cần được khóa trong contract | TC-D07/TC-S02; reject/bootstrap/partial/rollback/unknown/restart | frozen version policy, before/after state, response, history và event timeline |
| AC-S08 Bắt buộc | Retry cùng ID/payload không thêm FlowMod; ID khác payload conflict; pending batch→BUSY; không queue proposal cũ | TC-S03; duplicate lúc pending và sau final | payload hash, response cache, FlowMod count |
| AC-S09 Bắt buộc | Forecast hết hạn trước write→reject; verify muộn sau write→ghi actual+DEADLINE_MISSED và reconcile, không báo old/REJECTED giả | TC-S02/TC-S03; delay verify qua forecast_end | timestamps, actual state, deadline flag và evidence |
| AC-S10 Bắt buộc | SDN tạo immutable snapshot từ lịch sử requested_mbps hợp lệ và verified route/state; cutoff/release/available đúng, đủ L*×48 bins hoặc báo thiếu; không suy demand từ counter/goodput; restart reconcile actual trước nhận apply mới | TC-D04/TC-I01/TC-S03; source history, late data, missing bin, restart | kênh demand đã khóa, snapshot ID/hash/state/config refs, causality và reconcile evidence |

Batch không được quảng cáo nguyên tử toàn mạng. Batch status gồm APPLIED/REJECTED/FAILED/ROLLED_BACK/PARTIAL/INCONSISTENT; per-flow gồm APPLIED/KEPT/FAILED/ROLLED_BACK/UNKNOWN. PARTIAL/INCONSISTENT phải reconcile trước tick tiếp; UNKNOWN không được tự điền old_path là actual_path hay báo FAILED khi chưa biết actual. NO_TRIGGER/KEPT ở control tick không phải batch status của ApplyResponse.

## 7. Evaluation/Testing và nghiệm thu tích hợp

| ID/loại | Tiêu chí đạt | Cách kiểm tra | Bằng chứng bắt buộc |
| --- | --- | --- | --- |
| AC-E01 Bắt buộc | Cohort theo send success, packet sequence và deadline/drain; warm-up loại khỏi cohort; duplicate không tăng nhận; denominator zero→N/A | TC-E01; fixture 100/90 | raw packet/probe, expected/actual metrics |
| AC-E02 Bắt buộc | MLU 16 chiều fabric đúng điểm đếm; missing coverage rõ; RTT không có reply không=0; phân vị/latency đúng định nghĩa | TC-D05/TC-E01 | calibration, metric code/version, sample/time coverage |
| AC-E03 Bắt buộc | M0–M4 cùng schedule/seed/topology/core config/initial route; M0 static không forecast/MILP; M1–M3 cùng F/H*/trigger/solver/capacity/k_e/hold-down/apply, chỉ thay forecast provider/config đã khai báo; M4 dùng future demand qua evaluation offline với cùng MILP, không deploy; thứ tự/seed/hash đầy đủ | TC-E02 | run_order, frozen manifests, raw per run, Oracle provenance, matrix completeness |
| AC-E04 Bắt buộc | Giữ run lỗi; chỉ loại infra theo rule khóa; không loại RF chậm/fallback; paired đúng schedule, báo n/missing/CI | TC-E03 | exclusions, rerun links, paired table/CI config |
| AC-E05 Nghiên cứu | Kết luận theo scenario dựa hiệu ứng/CI/chi phí; n=seed/lịch, không=giây; báo kết quả âm và giới hạn | Review report | run points, plots, report, uncertainty |
| AC-I01 Bắt buộc | Trace schedule→released demand→SDN snapshot→Data store→ML 7-field rows→Data READY→Optimization mean/MILP→SDN verify→Data mirror→metric đầy đủ, không sai unit/time/id; G0–G8 theo v1.4, không mở G7 trước G1–G6 | TC-I01; một fixture end-to-end và một lượt pilot thật | evidence index, interface validation, snapshot/prediction/state refs |
| AC-I02 Bắt buộc | Conflict log đối chiếu v1.4 có owner/evidence; khóa schema/API/config hashes, canonical catalog, ML config ref, kênh demand tới SDN và transport/ack/retry; không freeze theo interface cũ trái v1.4 | Review baseline và round-trip qua transport đã chọn | decision log, confirmations, contract fixtures, RunManifest |
| AC-I03 Bắt buộc | Tách traffic/run/measurement origin; round-trip UTC–offset đúng; issued_at không giả forecast_start | TC-I03; traffic2026-11-12T09:00:00Z,measure09:01:00Z,bin100→09:02:40Z; warm-up60s | run manifest/time mapping, prediction rows và ApplyRequest |

## 8. Bộ chèn lỗi và expected chung

Mã F01–F12 dưới đây thuộc protocol Testing, không gán thành số phụ lục đề bài. Ma trận **đề xuất**: mỗi loại 3 fault seed trên S3 hoặc S5 riêng, kèm clean control cùng lịch khi cần, tương ứng 36 lượt lỗi nếu được freeze. Counter reset/duplicate/invalid route còn là contract cases bắt buộc ngoài ma trận này.

| Fault | Hook / thao tác có seed | Expected bắt buộc / case |
| --- | --- | --- |
| F01 | Bỏ history row một nhóm/bin | NULL+DATA_MISSING, history không hợp lệ→skip tick, không READY thiếu rows; TC-D01/TC-D04 |
| F02 | Giá trị âm/sai đơn vị×1000 theo fixture khai báo | Reject/invalid, không đoán sửa scale; TC-D01 |
| F03 | Trễ collector/release vượt T_stale | Không dùng snapshot stale; TC-D04/TC-D05 |
| F04 | Xóa đầu L-history | History không đủ theo frozen config→INVALID và skip tick; không tự lấp/đổi L*; TC-M03 |
| F05 | Worker inference exception/timeout | FORECAST_TIMEOUT/ERROR, M3 degraded/kept; TC-M03 |
| F06 | Output NaN/inf/âm sau chuẩn hóa/sai shape | FORECAST_INVALID; hữu hạn >capacity riêng không là lỗi; TC-M02 |
| F07 | Hỏng model/hash/feature order | MODEL_LOAD_FAIL, không dùng artifact sai; TC-M02/TC-M03 |
| F08 | Solver timeout có/không incumbent hoặc invalid result | Chỉ dùng nếu policy cho phép FEASIBLE, gap≤max_gap đã khóa, integer feasible, validator pass và còn hạn; TC-O05 |
| F09 | Mất reply OpenFlow/verify timeout | Reconcile actual, không APPLIED giả; TC-S02 |
| F10 | Reject FlowMod hook | FAILED/PARTIAL/rollback theo trạng thái đã verify; TC-S02 |
| F11 | Bỏ một thao tác trong batch nhiều nhóm | Per-group actual/UNKNOWN đúng; UNKNOWN→batch INCONSISTENT; PARTIAL/INCONSISTENT block tick tới reconcile; TC-D07/TC-S02 |
| F12 | Rule đúng cú pháp nhưng sai port/nhánh | VERIFY_FAILED, không commit APPLIED; TC-S02 |

Không cam kết phục hồi5 tick/goodput90% khi chưa có điều kiện tải và ngưỡng pilot. Lỗi link/switch vật lý là mở rộng riêng; controller restart phải có reconcile actual route trước nhận request mới theo v1.4, còn chi tiết recovery/timeout cần freeze. Fault hooks cần log thời điểm/đối tượng/thời lượng/fault_seed và được reset trước clean run; không sửa raw log để giả lỗi phát sinh trong runtime.

## 9. Ngưỡng phải chốt trước benchmark

| Trường | Cách xác định | Owner | Trạng thái hiện tại |
| --- | --- | --- | --- |
| `pacing_error_tolerance`, `s0_loss_tolerance` | Pilot S0 theo payload/config | Network + QA | MUST_LOCK |
| `counter_measurement_point`, `overhead_factor`, `host_headroom_ratio` | So sender/receiver/OVS/qdisc, kiểm witness/headroom | Network + SDN + Data | MUST_LOCK |
| `forecast_timeout_ms`, `apply_timeout_ms`, `apply_guard_ms`, `cycle_budget` | Latency pilot, đủ ngân sách T_ctl/H | ML + Optimization + SDN | MUST_LOCK |
| `packet_deadline_s`, `drain_s` | Packet delay/drain pilot; drain≥deadline theo policy đề xuất | Network + Data + QA | MUST_LOCK |
| `minimum_metric_coverage_ratio`, `minimum_rtt_samples`, `external_load_invalid_rule` | Chất lượng phép đo/nhiễu máy pilot | Data + Evaluation + QA | MUST_LOCK |
| `numeric_epsilon`, `feasibility_tolerance`, `objective_tolerance`, `max_gap` | Đơn vị mô hình, solver/config và policy cho phép FEASIBLE | Optimization + QA | MUST_LOCK |
| `probe_interval_s`, `rebin_policy`, `warmup_policy`, `rerun_limit` | Ghi rule thống nhất, pilot kiểm khả năng quan sát | SDN + Data + QA | MUST_LOCK |
| `ID_catalog`, `origin_policy`, `forecast_batch_completeness` | Semantics đã xác nhận: Network hex flow/link IDs, Data path IDs, measurement origin; Data kiểm 48×H* từ config, đúng7 fields; cần khóa catalog/schema/fixture version | Network+Data+ML+Optimization+SDN | Semantics theo v1.4; artifact/version MUST_LOCK |
| `ml_config`, baseline/Oracle config, fallback policy | ML tự chọn L*/H*/epsilon* trên validation, pin hash qua RunManifest; không truyền trong prediction rows; không mix source trong READY | ML+Data+Optimization+QA | Ownership đã xác nhận; giá trị/hash/policy MUST_LOCK |
| `released_demand_to_sdn` | Kênh lịch sử requested_mbps tới SDN và kiểm cutoff/released_at/available_at; không suy từ goodput/counter | Network+SDN+Data | OPEN / MUST_LOCK |
| `TH/N/h/B/hold/lambda`, `transport/ack/retry`, `state_version_policy` | Một config và executable contract fixtures | Optimization+SDN+Data+QA | MUST_LOCK |
| `split_manifest`, `benchmark_matrix`, `initial_paths_hash` | Tỷ lệ70/15/15 đã xác nhận; 20 seeds→14/3/3; khóa danh sách seed, cách áp dụng cho S1 levels/S2a/S2b và tổng run, đủ S1–S5; S0 calibration riêng | Network+Data+QA | Tỷ lệ đã chốt; danh sách seed/matrix/hash MUST_LOCK |

**Split đã thống nhất:** train/validation/test=70/15/15 theo xác nhận của người dùng; Guide mục3.5, YAML mục3.6 và bảng mục6 đã đồng bộ, 20 seeds tương ứng14/3/3. Tổng lượt chạy phải tính từ danh sách seed và phạm vi của `split_manifest` / `benchmark_matrix` được khóa; đây là việc hoàn thiện manifest, không phải chờ chọn lại tỷ lệ. Chính sách cuối run dùng mặc định mục3.4 (`forecast_end≤measurement_end`); nếu chọn label tail như lựa chọn còn mở ở mục11 thì phải ghi quyết định riêng trước benchmark.

Coverage/latency là tiêu chí chất lượng phép đo hoặc vận hành có điều kiện; không dùng các ngưỡng này sau test để chọn bỏ run bất lợi do bản thân phương pháp.

## 10. Mẫu biên bản nghiệm thu

```csv
criterion_id,module,test_case_id,git_commit,config_hash,run_id,expected,actual,status,evidence_uri,defect_id,producer_confirmed_by,qa_reviewed_by,reviewed_at_utc
AC-N01,Network,TC-N01,,,,"6 switch 8 host đúng topology",,NOT_RUN,,,,,
```

Mỗi module có producer xác nhận evidence và QA đối chiếu; Testing tổng hợp sign-off. Không ghi tên người ký hoặc ngày nghiệm thu giả. Chấp nhận thiết kế giao diện không đồng nghĩa nghiệm thu hiệu năng hay kết luận RF tốt hơn.

## 11. Kết luận nghiệm thu và việc làm tiếp

Các tiêu chí đã được đồng bộ với v1.4 về quyền tạo/lưu snapshot, ID, prediction payload, mean-H, epsilon, trạng thái apply và gates; tỷ lệ split70/15/15 đã xác nhận. Chưa có căn cứ kết luận module PASS hoặc phần trăm hoàn thành. Những lựa chọn còn mở phải được ghi trong decision log, đặc biệt kênh demand tới SDN, danh sách seed/phạm vi benchmark, config ML, latency và transport; không dùng tài liệu role cũ để đảo lại quyết định đã chốt.

Trình tự: owner xác nhận contract và sửa file mình → giao fixtures/schema/mã → QA chạy case và lập defect → pilot/S0 khóa phép đo → freeze → benchmark → tái tính metric/paired analysis → sign-off. QA có thể nghiệm thu chức năng khi RF không thắng baseline; kết quả nghiên cứu vẫn phải giữ và phân tích trung thực.
