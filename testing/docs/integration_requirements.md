# Yêu cầu thống nhất thiết kế và giải quyết xung đột tích hợp SDN

**Phiên bản:** requirements_v2 2026-11-14 — đồng bộ Integration Guide v1.4

**Người tổng hợp:** Testing / QA / QC  
**Đối tượng:** Network, Data, Machine Learning, Optimization, SDN Controller  
**Trạng thái:** Đã đồng bộ quy tắc v1.4; các điểm `OPEN` / `MUST-LOCK` cần owner xác nhận trước freeze. Chưa xác nhận triển khai hoặc nghiệm thu.

## 1. Mục đích và cách sử dụng

Tài liệu này chuyển 16 xung đột C01–C16 đã rà soát thành các yêu cầu có phương án chốt cụ thể, người chịu trách nhiệm và bằng chứng xác nhận. Nhóm có thể dùng làm agenda họp và danh sách việc cần sửa sau họp. Mục tiêu là các module đọc được dữ liệu của nhau, quyết định dựa trên đúng trạng thái mạng, và kết quả thực nghiệm có thể kiểm chứng.

Căn cứ cập nhật là [Integration Guide v1.4](../../docs/architecture/SDN_Traffic_Engineering_Role_Integration_Guide_v1.4.md), nhất là mục 2–6, 11 và 13; tài liệu đi kèm là [interfaces_v2.md](interfaces_v2.md), [test_plan_v2.md](test_plan_v2.md) và [acceptance_criteria_v2.md](acceptance_criteria_v2.md). Các đề xuất cũ trái với v1.4 không còn là lựa chọn mặc định. Đây không phải kết quả nghiệm thu mã nguồn.

Các quy tắc đã được v1.4 xác nhận gồm ID theo Network, SDN tạo snapshot, Data chỉ lưu/cấp lại, ML tự chọn cấu hình offline và chỉ gửi 7 trường dự báo, Optimization tự tính mean. Không mở lại các lựa chọn cũ bằng một default riêng trong Testing. **“Đề nghị chốt” / `[PROPOSED]` chỉ áp dụng cho chi tiết chưa freeze; `MUST-LOCK` là điều phải có quyết định được ghi nhận.** Thay đổi quy tắc phải sửa đồng bộ contract, producer, consumer, cấu hình và tests. Chấp thuận thiết kế không đồng nghĩa đã triển khai xong.

Hai cấp ưu tiên:

- **P0:** Có thể làm lỗi tích hợp, sai trạng thái hoặc sai kết quả. Phải chốt trước khi tích hợp giao diện liên quan.
- **P1:** Ảnh hưởng phép đo, tính tái lập hoặc công bằng. Phải chốt trước khi khóa benchmark; các giá trị phụ thuộc phép đo được xác định qua pilot.

Trạng thái quyết định: `ALIGNED_V1_4` = kế thừa quy tắc từ guide; `OPEN` → `AGREED` hoặc `REVISE` dành cho phần còn phải chốt. Trạng thái thực hiện riêng: `UNVERIFIED` → `IN_PROGRESS` → `VERIFIED` theo evidence thực tế. `ALIGNED_V1_4` không phải sign-off triển khai; `AGREED` cần người xác nhận thực tế và nội dung cuối cùng; `VERIFIED` cần đầu ra và bằng chứng kiểm tra. Testing tổng hợp, không ký thay thành viên.

## 2. Danh sách quyết định cần chốt

| Requirement | Conflict | Ưu tiên | Nội dung | Đảm nhiệm | Cùng xác nhận |
| --- | --- | --- | --- | --- | --- |
| REQ-01 | C01 | P0 | Bộ ID chung và mapping topology | Network | Data, ML, Optimization, SDN |
| REQ-02 | C02 | P0 | H bước dự báo, nhãn train và mean cho MILP | Data + ML | Optimization, Testing |
| REQ-03 | C03 | P0 | SDN tạo snapshot causal; đường cấp demand lịch sử còn mở | SDN + Network | Data, ML, Testing |
| REQ-04 | C04 | P0 | DecisionBatch → ApplyRequest/Response | Optimization + SDN | Data, Testing |
| REQ-05 | C05 | P0 | Lưu proposal, kết quả thực thi và trạng thái | Data | SDN, Optimization |
| REQ-06 | C06 | P0 | Mốc thời gian, offset và UTC | Data + Network | ML, Optimization, SDN |
| REQ-07 | C07 | P0 | Epsilon chỉ được áp một lần | ML + Optimization | Data, Testing |
| REQ-08 | C08 | P0 | R4 và công thức điểm mục tiêu J | Optimization | Testing |
| REQ-09 | C09 | P1 | Bộ tham số điều phối duy nhất | Optimization | SDN, ML, Testing |
| REQ-10 | C10 | P1 | Warm-up, split seed và ma trận thực nghiệm | Testing + Data | Network, ML |
| REQ-11 | C11 | P1 | Counter, điểm đo và hiệu chuẩn k_e | Network + SDN | Data, Optimization, Testing |
| REQ-12 | C12 | P1 | Tuyến ban đầu, MAC/ARP và same-edge | Network + SDN | Optimization, Testing |
| REQ-13 | C13 | P0 | Transport, deadline, timeout và lỗi thực thi | Optimization + SDN | Data, ML, Testing |
| REQ-14 | C14 | P1 | Đối chứng M0–M4 và metric | Testing | Data, ML, Optimization, SDN |
| REQ-15 | C15 | P0 | Khóa predictions và tính đầy đủ của batch | Data | ML, Optimization |
| REQ-16 | C16 | P0 | State version, rollback và UNKNOWN | SDN | Data, Optimization, Testing |

Các yêu cầu giữ mã REQ-01–REQ-16 để truy vết C01–C16. Phần semantics đã nêu trong v1.4 có trạng thái `ALIGNED_V1_4`; tỷ lệ split70/15/15 đã được người dùng xác nhận. Đường cấp demand lịch sử, transport, ngưỡng pilot và danh sách seed/phạm vi benchmark còn `OPEN` / `MUST-LOCK`. Mức triển khai của mọi yêu cầu là `UNVERIFIED` trong tài liệu này cho đến khi có evidence; không suy ra thành viên chưa làm việc.

## 3. Yêu cầu chi tiết và phương án đề nghị

### REQ-01 Thống nhất ID xuyên module

**Vấn đề:** Cùng nhóm lưu lượng đang được gọi bằng mã hex, `h1-h3` hoặc `h1_h3`; đường được gọi bằng P1/P2 hoặc c1/c2; link cũng có nhiều cách viết. Các khóa này không thể join trực tiếp.

**Quy tắc v1.4 (mục 3.2):**

| Thực thể | Tên và ví dụ dùng chung | Quy tắc |
| --- | --- | --- |
| Nhóm có hướng | `flow_group_id=0x103` | Chuỗi hex `0x{src×256+dst:X}`; chiều ngược `0x301` là nhóm riêng |
| Cookie | `flow_cookie=0x103` | Cùng trị số Network quy định; field OpenFlow có chức năng riêng, không thay flow match |
| Đường | `path_id=0x103_P1` hoặc `0x103_P2` | Theo Data schema `<flow_group_id>_P1` / `_P2`, thuộc catalog Network |
| Nhãn đường | `path_label=P1` hoặc `P2` | P1 qua c1; P2 qua c2 |
| Link có hướng | `link_id=e1>c1` | Lấy nguyên catalog Network; khác `c1>e1` |

Network xuất một danh mục máy đọc được cho host, switch, port, flow, path và link. Mọi producer/consumer sửa schema, fixture, API và serializer theo catalog cùng phiên bản; không chấp nhận hai namespace đồng thời hoặc đổi dữ liệu Network theo ID cũ. Dữ liệu legacy nếu cần lưu được giữ riêng với mapping/migration có version; không dùng alias làm khóa giao diện mới. Vector 48 nhóm sắp theo giá trị số mã hex, nhưng join theo `flow_group_id`, không theo index.

**Đầu ra cần giao:** `config.yaml` và mapping CSV/JSON, `topology_version`, checksum.

**Điều kiện đóng:** Đủ 48 nhóm, 96 đường, 16 chiều fabric, 32 chiều toàn mạng và 192 path–fabric-link incidences; một fixture `0x103` đi từ lịch → snapshot SDN → dự báo → quyết định → SDN → Data giữ cùng ID. Tham chiếu TC-N01, TC-I01.

### REQ-02 Giữ H bước và quy định rõ phép lấy mean

**Vấn đề:** ML thiết kế multi-output H bước, còn Data mô tả nhãn trung bình; MILP nhận một nhu cầu scalar mỗi nhóm.

**Quy tắc v1.4:** TrainingBundle giữ nhãn H bước đúng `[t,t+H)`; mean-H của nhãn là giá trị suy ra phục vụ đánh giá offline. ML tự chọn L*/H*/epsilon* trên validation và lưu `configs/ml_config.yaml` hoặc model metadata; Runner khóa version/hash trong RunManifest. ML chỉ gửi H* hàng/nhóm theo đúng 7 trường `run_id`, `flow_group_id`, `issued_at`, `forecast_start`, `forecast_end`, `predicted_mbps`, `source`. Data lưu rows và kiểm đủ `48×H*` bằng H* đọc từ frozen config. Optimization hoặc adapter thuộc Optimization tính `b_f = sum(predicted_mbps) / H*` từ đúng một batch READY. Data không tính mean dự báo; ML không gửi vector envelope, L/H/epsilon/raw/mean hoặc batch metadata cho Data.

Với Δ=1 giây, L=30, H=5, cutoff t=100: X chứa bins 70–99, y chứa bins 100–104. Vector `[2, 2.5, 3, 3.5, 3]` cho mean `2.8 Mbps`. Khi Δ khác 1 giây, phải khai báo số bước và thời lượng; không tiếp tục hiểu H vừa là số bước vừa là số giây mà không quy đổi.

**Đầu ra cần giao:** Data giao TrainingBundle/schema X/y và API lưu/đọc rows; ML giao rows mẫu cùng model/config artifact riêng; Optimization giao hàm tổng hợp có trace batch. Vector nội bộ ML không thay thế payload 7 trường.

**Điều kiện đóng:** Nhãn, vector lưu và mean tái tính khớp; đánh giá sai số theo bước và sai số mean-H tách biệt. TC-D04, TC-M01, TC-D06.

### REQ-03 Giới hạn dữ liệu online theo thời điểm khả dụng

**Vấn đề:** Lịch phát đã biết trước toàn bộ; chỉ lọc theo bin mà không kiểm availability vẫn có thể dùng dữ liệu chưa sẵn sàng.

**Quy tắc v1.4:** **SDN tạo và phát hành snapshot** từ lịch sử demand hợp lệ và routing state đã xác minh. Observation phải thỏa `window_end_utc <= history_cutoff_utc`, `released_at_utc <= snapshot_as_of_utc`, `available_at_utc <= snapshot_as_of_utc`. Data validate tại ingest rồi lưu/cấp lại nguyên bản theo snapshot ID/version/hash; không tạo snapshot, không tái dựng bằng query và không chèn late data vào snapshot cũ. M1–M3 không được đọc future `demand_plan`; M4 oracle và evaluator offline có lane riêng.

**[MUST-LOCK]** Network/SDN/Data chốt API/kênh chuyển `requested_mbps` lịch sử đã release/available tới SDN, owner timestamp và chứng cứ causality. Không được suy requested demand từ goodput hoặc port counters. Chưa có kênh này thì chưa đủ điều kiện đóng tích hợp snapshot.

Nếu thiếu lịch sử: có thể chờ trong budget hữu hạn để SDN tạo snapshot mới đủ L* bins đã release/available; nếu vẫn thiếu/stale/invalid thì giữ tuyến và skip tick. Không điền 0, rút L* hoặc dùng Persistence để hợp thức hóa snapshot thiếu. Fallback chỉ áp dụng khi RF lỗi nhưng history vẫn hợp lệ, theo policy đã freeze ở REQ-15.

**Đầu ra cần giao:** SDN giao snapshot/schema/hash và causality audit; Network/SDN/Data giao contract nguồn demand lịch sử; Data giao `ingest_snapshot` / `get_stored_snapshot`, chính sách reject/missing/stale và fixture tại đúng biên thời gian.

**Điều kiện đóng:** Giữ history không đổi và sửa mạnh tương lai ẩn thì M1–M3 output không đổi; mẫu late không lọt vào snapshot cũ. TC-D04, TC-I02.

### REQ-04 Thống nhất lệnh gửi SDN

**Vấn đề:** JSON Optimization dùng `batch_id`, `decisions`, P1/P2 và chuỗi `target_interval`; SDN dùng một contract khác.

**Quy tắc v1.4:** Hợp đồng tham chiếu ApplyRequest dùng `decision_batch_id`, `changes[]`, `old_path_id`, `new_path_id`, cùng run/snapshot/version/config và forecast timestamps UTC. Optimization sở hữu adapter chuyển proposal thành lệnh; SDN không tự sửa phương án hoặc giải lại MILP. Các U/J/solver fields lưu audit, không dùng thay xác minh data plane.

```json
{
  "run_id": "run_001",
  "decision_batch_id": "batch_001",
  "snapshot_id": "snap_100",
  "expected_state_version": 15,
  "config_version": "v1",
  "forecast_start": "2026-11-12T09:02:40Z",
  "forecast_end": "2026-11-12T09:02:45Z",
  "apply_deadline_utc": "2026-11-12T09:02:44Z",
  "changes": [
    {"flow_group_id": "0x103", "old_path_id": "0x103_P1", "new_path_id": "0x103_P2"}
  ]
}
```

Đây là ví dụ contract lõi; topology/config hashes và `payload_hash` nằm trong envelope theo [interfaces_v2.md](interfaces_v2.md). Optimization sở hữu proposal/adapter; Runner/Orchestrator gửi ApplyRequest tới SDN. Response phải chứa batch status, state_version, completed_at và kết quả actual/status/reason từng nhóm. Các trạng thái cuối gồm APPLIED, REJECTED, FAILED, ROLLED_BACK, PARTIAL, INCONSISTENT; nhóm chưa biết có `actual_path_id=null`, per-flow `UNKNOWN` và batch `INCONSISTENT`.

**Đầu ra cần giao:** Schema request/response, adapter và valid/invalid fixtures.

**Điều kiện đóng:** Hai module đọc cùng fixture; sai path/config/version hoặc thiếu deadline bị từ chối trước FlowMod. TC-I01, TC-S03.

### REQ-05 Bảo toàn dữ liệu cấp batch và từng nhóm

**Vấn đề:** Bảng decisions rút gọn chưa đủ lưu một batch có nhiều kết quả khác nhau, đường thực tế và phiên bản state.

**Đề nghị chốt:** Data giữ các bảng báo cáo nhưng bổ sung source tables hoặc cấu trúc tương đương cho `decision_batches`, `routing_decisions`, `routing_state`, `routing_history`, `event_log`. Lưu riêng old/proposed/actual; không ghi proposal như đã apply. Liên kết forecast → snapshot → decision batch → kết quả bằng ID, không chỉ timestamp gần nhau. SDN là nguồn sự thật của routing state đã xác minh; Data là DB writer duy nhất và sở hữu ingest/storage/view.

Mọi tick giữ tuyến vẫn có control tick record và reason; không ép `KEPT` vào enum execution batch SDN hoặc tạo FlowMod giả cho M0.

**Đầu ra cần giao:** Migration/ETL mapping, khóa và quan hệ liên kết, source/view mẫu.

**Điều kiện đóng:** Batch ba nhóm APPLIED/ROLLED_BACK/UNKNOWN được lưu và đọc lại đúng actual/status/version; ingest lại không nhân bản. TC-D07, TC-S02.

### REQ-06 Khai báo origin và chuyển đổi thời gian

**Vấn đề:** Run start, bắt đầu phát và bắt đầu đo có thể khác nhau; offset của Data và ML chưa cùng gốc.

**Đề nghị chốt:** Runner ghi đủ `run_started_at_utc`, `traffic_started_at_utc`, `measurement_started_at_utc` và mốc monotonic tương ứng. Trong phương án hiện tại: demand_plan và traffic_actual dùng traffic origin; predictions forecast offsets dùng measurement origin; API/snapshot/apply dùng UTC. Mỗi field phải có origin khai báo, kể cả adapter của nguồn cũ. Không gán `issued_at` bằng forecast_start để che latency.

Ví dụ theo Guide mục13.2: traffic bắt đầu `2026-11-12T09:00:00Z`, đo bắt đầu `09:01:00Z` sau warm-up60s; forecast offset100–105 tương ứng UTC09:02:40Z–09:02:45Z và schedule offset[160,165). Cửa sổ dùng `[start,end)`; duration dựa trên monotonic cùng clock domain. Core policy chỉ phát forecast khi `forecast_end <= measurement_end`; label tail cần quyết định riêng, không trộn vào metrics chính360giây.

**Đầu ra cần giao:** Bảng field–unit–origin, run manifest mẫu và round-trip converter.

**Điều kiện đóng:** Cùng một bin được Network/Data/ML/SDN quy về đúng UTC, không lệch warm-up hoặc một giây. TC-I03.

### REQ-07 Chỉ áp epsilon tại một nơi

**Vấn đề:** ML đã hiệu chỉnh dự báo, trong khi công thức tải Optimization tiếp tục nhân `(1+epsilon)`; epsilon 0.1 có thể thành hệ số 1.21.

**Quy tắc v1.4:** ML áp `final=max(0,(1+epsilon_ml)*raw)` đúng một lần rồi gửi final qua `predicted_mbps`; Data lưu final; Optimization dùng `L_e=k_e*sum(b_f*a*x)` và không nhân epsilon thêm. ML chọn epsilon trên validation, lưu trong config/model metadata riêng và khóa trước test; nếu không cải thiện so với epsilon=0 hoặc hòa thì chọn 0. Raw forecast, nếu cần, chỉ là artifact ML phục vụ phân tích offline. Không yêu cầu ML gửi owner/ratio/applied/raw trong payload hay registry forecast. `numeric_epsilon` là tolerance riêng, không phải hệ số hiệu chỉnh nhu cầu.

Phương án cũ “ML xuất raw, Optimization áp epsilon” không thuộc v1.4 và không được bật bằng config riêng của một role.

**Đầu ra cần giao:** Epsilon policy trong config/model metadata riêng, hash trong RunManifest và evidence offline đủ tái tính raw→final; payload online vẫn đúng 7 trường. Cấu hình baseline/oracle phải khóa riêng trong RunManifest; epsilon=0 cho các đối chứng là `[PROPOSED]`, không phải kết quả validation đã xác nhận. Báo M3 có epsilon dương là RF+calibration; ablation epsilon=0 phải định trước.

**Điều kiện đóng:** Raw 4 Mbps, epsilon=0.1, k_e=1 → b_used=4.4 Mbps; không thành 4.84. TC-M04.

### REQ-08 Sửa R4 và tái tính J

**Vấn đề:** `y_f >= 1-x_current` chưa xác định chính xác y khi lambda=0; ví dụ J_new=0.61 không khớp U_new=0.58, lambda=0.1, F=48 và B=4.

**Quy tắc v1.4:** Dùng `y_f = 1-x_f,current` với chọn đúng một đường; tính `n_changes` độc lập từ routes. Giữ `J=U+lambda*n_changes/|F|`, F cố định 48 kể cả nhóm demand=0. J_old=U_current do phương án giữ tuyến có n_changes=0. Không hard cap U<=0.9 hoặc U<=1 trong MILP lõi.

Với U_new=0.58: một đổi → J_new≈0.5820833333; bốn đổi → J_new≈0.5883333333. Toy F=2, demand=(6,6), C=10, k=1, cùng nhánh → U_old=1.2; đổi một nhóm → U_new=0.6 và J_new=0.65 khi lambda=0.1. Không lấy mẫu số 2 của toy dùng cho production 48.

**Đầu ra cần giao:** Optimization sửa công thức/ví dụ, giao independent recomputation và solver fixtures.

**Điều kiện đóng:** Ràng buộc và điểm mục tiêu khớp cả lambda=0, B=0, zero demand, overload và nghiệm đồng hạng. TC-O02, TC-O03, TC-O04.

### REQ-09 Khóa một bộ tham số điều phối

**Vấn đề:** Các bản thiết kế đề xuất N=2/3, TH=0.80/0.85, h=0.05/0.10, có nguy cơ mỗi thành viên dùng default khác.

**Đề nghị chốt bộ khởi đầu cho pilot:** Δ=T_ctl=1 giây; TH=0.85; N=3; h=0.05; T_hold=10 giây; B=4; lambda=0.1. Đây là lựa chọn đề xuất, không phải kết quả tuning hoặc SLA. Chọn N=3 để bám Optimization hiện nhận; N=2 có thể khảo sát riêng. L/H/MA window/epsilon chọn trên validation và phải cùng target semantics giữa các đối chứng.

Trigger là strict `U_current>TH` đủ N tick hợp lệ liên tiếp; tick invalid/stale/busy hoặc state được đối soát thay đổi thì reset chuỗi. Chấp nhận khi có thay đổi thật và `J_new <= (1-h)*J_old` theo tolerance đã khóa. Hold-down tính từ verified actual path change, không từ proposal; xem REQ-16.

**Đầu ra cần giao:** Một config version/hash; danh sách sweep tách khỏi baseline. Timeout solver 200 ms và T_stale=2Δ chỉ là ứng viên pilot, chưa tự coi đã chốt.

**Điều kiện đóng:** Mọi module đọc cùng config; fixture TH boundary và N=3 cho kết quả giống nhau. TC-O01, TC-O04, TC-O05.

### REQ-10 Thống nhất lịch chạy và quản lý seed

**Quyết định đã xác nhận:** Train/validation/test=**70/15/15** theo xác nhận của người dùng. Với20 seeds, chia14 train/3 validation/3 test. Guide mục3.5, YAML mục3.6 và conflict log mục6 đã đồng bộ; tỷ lệ không còn là mục OPEN.

**Quy tắc áp dụng:** Warm-up60giây, đo chính360giây; phải chứng minh đủ L* bins hoàn chỉnh và đã available trước forecast đầu tiên, không tự tăng warm-up thành70giây ở riêng Testing. Policy không điều phối trong warm-up cần freeze chung cho mọi method. Chia theo schedule/parent family với tỷ lệ70/15/15 trước tạo window; không để cùng lịch đổi tên hoặc replay lọt khác split. Giữ S5 trong bộ chính. `split_manifest` còn cần khóa danh sách seed thực, phạm vi áp dụng và hash; không chờ chọn lại tỷ lệ.

Các trường hợp scenario gồm S1 bốn mức rho, S2a/S2b/S3/S4/S5 mỗi loại một (9 cấu hình mở rộng). Guide chưa khóa rõ 20 seeds cho mỗi scenario hay mỗi cấu hình mở rộng. **Nếu** khóa 20 seeds cho từng cấu hình và split 14/3/3 thì có 27 lịch test × 5 methods = 135 lượt; đây chỉ là phép tính có điều kiện, không phải ma trận bắt buộc. Không tái dùng baseline cũ 45 lịch/225 lượt hoặc dự toán runtime tương ứng.

**[MUST-LOCK] Quyết định về nguồn lực:** Khóa phạm vi seed, phân bổ seed cụ thể theo tỷ lệ70/15/15 đã chốt, danh sách scenario/rho/seed và đầy đủ M0–M4 trong `benchmark_matrix` trước khi xem kết quả test. Tính số lượt/runtime từ manifest đã duyệt; nếu rút gọn phải ghi lý do và matrix mới, vẫn có S5. Pilot và fault run tách bộ test chính.

**Đầu ra cần giao:** seed_registry, split_manifest, run_order, benchmark_matrix và warm-up/drain policy. Tách seed sinh lịch/model/solver/thứ tự/bootstrap/fault; giữ generator version/hash.

**Điều kiện đóng triển khai:** Mỗi schedule đúng một split theo70/15/15, số lượng khớp manifest (20 seeds→14/3/3), đủ history ở đầu đo; các method replay cùng file và initial paths; thứ tự và số lượt khớp manifest đã khóa. Tỷ lệ đã xác nhận không đồng nghĩa manifest hoặc kiểm thử đã hoàn tất. TC-D03, TC-E02.

### REQ-11 Chốt nguồn đo và hiệu chuẩn

**Vấn đề:** Requested payload, dữ liệu thực gửi/nhận và PortStats khác nhau; chưa xác minh counter trước hay sau tc shaping. Các ngưỡng RTT/CPU/bandwidth trong Network chưa có evidence pilot.

**Đề nghị chốt:** Một counter chính cho mỗi chiều fabric, ưu tiên PORT_TX tại nguồn; không cộng TX nguồn và RX đích. Giữ raw bytes, sample/request/reply/available times, source/port/epoch/measurement point. Mẫu đầu/reset/Δt<=0 trả invalid và throughput=null, không traffic âm hay zero giả. Kiểm đủ 16 chiều mới báo MLU đầy đủ.

S0 so sender/receiver/OVS/qdisc ở nhiều mức tải thấp, lấy median của port-TX-rate/payload-sent-rate theo từng directed link và lưu calibration provenance/số mẫu/độ phân tán. Không dùng receiver goodput hoặc requested rate khi sender không phát đủ làm mẫu số; không nhân k_e lên throughput đo lần nữa. Chưa đủ calibration dùng k_e=1.0 và gắn UNCALIBRATED. Nếu counter trước shaping thì gọi đúng offered load, không gắn nhãn egress utilization chưa kiểm chứng.

**Đầu ra cần giao:** calibration report, mapping point/port/link, k_e theo link hoặc bằng chứng dùng chung; dung sai pacing/loss/coverage và điều kiện nhiễu máy. RTT danh nghĩa 24 ms, lệch 5% và CPU 80% là mục tiêu pilot cần kiểm tra, không tự chuyển thành chuẩn nghiệm thu vô điều kiện.

**Điều kiện đóng:** Data tính lại được throughput từ raw; counter reset và missing link có coverage đúng; nguồn đo được xác nhận. TC-N03, TC-D05.

### REQ-12 Chốt khởi tạo và xử lý traffic ngoài tập tối ưu

**Vấn đề:** Chưa có một file initial paths/MAC/ARP đầy đủ để các method khởi chạy giống nhau; table-miss flood có thể tạo vòng.

**Đề nghị chốt:** Network xuất IP/MAC/ARP mapping và initial path cho 48 nhóm, có hash. SDN preinstall downstream cả hai nhánh, source-edge chọn uplink 3 hoặc 4. Static ARP là phương án đơn giản ưu tiên; nếu dùng handler thì ghi rõ policy và kiểm loop. Traffic cùng edge chuyển nội bộ, ngoài tập F tối ưu. Unknown/miss dùng policy drop hoặc gửi controller có kiểm soát, không flood tự do qua fabric.

M0–M4 phải bắt đầu cùng initial map. Bootstrap `last_successful_change_at=null`, hiểu là chưa có lần đổi và không khóa hold-down; không bịa một thời điểm đổi trước run.

**Đầu ra cần giao:** host config, initial_paths, rule/miss/ARP policy và bootstrap evidence.

**Điều kiện đóng:** Ping same-edge/cross-edge, ép P1→P2→P1, kiểm chiều ngược độc lập; không đổi nhóm ngoài batch. TC-N01, TC-S01.

### REQ-13 Chốt transport và ngân sách vòng kín

**Vấn đề:** Chưa chốt cách truyền, ack/retry và thời hạn toàn vòng; timeout hoặc Barrier không tự chứng minh lệnh chưa/có tác dụng.

**Đề nghị chốt:** Optimization và SDN chọn một transport thực tế, khai báo schema, endpoint/queue hoặc chữ ký hàm, ack, timeout và cách truy vấn kết quả. Chỉ một batch apply/reconcile đang hoạt động mỗi run; tick bận thì skip và log, không xếp proposal cũ để áp sau. Solver/inference chạy tách khỏi event loop Ryu.

Cùng `(run_id,decision_batch_id)` và payload hash phải idempotent; khác payload cùng ID là conflict. Timeout client phải query/reconcile, không gửi FlowMod lại mù hoặc tạo batch ID mới để né dedup.

Ngân sách snapshot+inference+solve+apply+verify+guard được đo qua pilot. T_ctl là chu kỳ xem xét, forecast_end là hạn hiệu lực; cả cycle overrun lẫn horizon deadline miss đều phải ghi. Trước write, không đủ budget thì reject/skip. Sau write mà verify muộn: vẫn đối soát actual, ghi DEADLINE_MISSED; không ghi REJECTED/old path giả để che thay đổi có thật. APPLIED cần read-back và bằng chứng đúng nhánh/đích phù hợp, không chỉ Barrier. Sau `PARTIAL` hoặc `INCONSISTENT`, block tick tiếp theo đến khi reconcile xong.

Solver status theo v1.4 gồm `OPTIMAL`, `FEASIBLE`, `INFEASIBLE`, `TIME_LIMIT`, `ERROR`. Pilot ưu tiên OPTIMAL. Nếu cho phép FEASIBLE hoặc TIME_LIMIT có incumbent, phải khóa policy và `max_gap`, gap thực≤max_gap, có integer-feasible incumbent, qua validator độc lập, đúng version và còn deadline; không tự động apply mọi nghiệm feasible.

**Đầu ra cần giao:** Bảng timeout/guard/retry/query policy, latency p50/p95/max từng bước và end-to-end. Nếu H/T_ctl không đủ thì chỉnh trên validation, không đổi giữa test.

**Điều kiện đóng:** Duplicate/BUSY/stale/timeout/late-after-write cho state và response đúng; worker treo không khóa collector. TC-S02, TC-S03.

### REQ-14 Thống nhất đối chứng và tiêu chí đánh giá

**Đề nghị chốt phương pháp:**

| Method | Nguồn nhu cầu | Quyền dùng tương lai | Điều phối |
| --- | --- | --- | --- |
| M0 | Không dự báo để điều phối | Không cần | Giữ initial paths, vẫn đo đầy đủ |
| M1 | Persistence từ lịch sử hợp lệ gần nhất | Không | Cùng core Optimization/SDN |
| M2 | Moving average với window khóa từ validation | Không | Cùng core |
| M3 | RF + epsilon policy đã khóa; fallback có log | Không | Cùng core; fallback không đổi method=M3 |
| M4 | Nhu cầu thật trong horizon từ lịch | Có, lane oracle offline riêng | Cùng core/constraints/ngân sách thời gian; chỉ tham chiếu |

M4 không là cận trên tối ưu bảo đảm; M0 không là cận dưới tuyệt đối. Giữ cùng lịch, initial map, target H, mạng và cơ chế thực thi. M3 có calibration phải công bố, không quy toàn bộ lợi ích cho RF.

**Metric đề nghị:** Goodput cohort, packet loss theo sequence/deadline, MLU p95 đúng điểm đo, verified switching/FlowMod; bổ sung RTT/probe success, latency, fallback và coverage. Proposed không được đếm là đổi đường thành công. Rollback/recovery ghi riêng; UNKNOWN không suy ra actual. Gói duplicate không tăng unique bytes; missing không bằng 0.

**Đầu ra cần giao:** Metric definitions/cohort/deadline/drain, quality/exclusion policy, paired analysis script/report. Ghép cùng schedule; đơn vị thống kê là lịch/seed, không phải từng giây. Giữ timeout/fallback/loss cao do phương pháp; chỉ loại lỗi hạ tầng theo rule khóa trước.

**Điều kiện đóng:** Fixture 100 gói gửi, 90 nhận unique đúng hạn, 2 bản trùng cho loss=10%; raw đủ tái tính metric. Không có yêu cầu RF luôn thắng baseline. TC-E01, TC-E02, TC-E03.

### REQ-15 Sửa khóa predictions và công bố batch đầy đủ

**Vấn đề:** PK `(run_id,flow_group_id,issued_at)` của Data v1 va chạm khi mỗi nhóm phát H dòng cùng issued_at. Có PK đúng nhưng đọc nửa batch vẫn làm sai mean.

**Quy tắc v1.4:** PK thêm `forecast_start`: `(run_id,flow_group_id,issued_at,forecast_start)`. Giữ đúng 7 cột prediction của ML theo REQ-02. Data tự tạo `forecast_batches` / `ForecastBatchRef` sau ingest để lưu batch ID, snapshot/config refs, status, expected/actual row counts, ready_at và hash. H* lấy từ frozen config/RunManifest, không từ forecast payload; model/config provenance lấy từ manifest đã khóa, không yêu cầu ML gửi thêm batch metadata, L/H/epsilon/raw/mean.

Chỉ READY khi đủ 48×H* bins liên tục, đúng cùng issued_at/target/config và cùng `source`, không trùng hoặc thiếu; không trộn các issued_at/source theo mục 5.2 D4 của guide. Writer có thể dùng DB transaction hoặc file commit marker; consumer không được đọc nửa batch. Một issued_at trong run định danh duy nhất một đợt; muốn nhiều model/đợt va khóa phải đổi schema có version, không overwrite. `[PROPOSED]` Khi cần fallback, tạo đủ batch persistence với `source=fallback_persistence` nếu history hợp lệ; thiếu/invalid thì skip tick. Fallback trộn theo từng nhóm không thuộc core contract v1.4 và cần policy/schema version riêng nếu muốn bổ sung.

**Đầu ra cần giao:** Schema migration, write/read-complete protocol và valid/invalid rows.

**Điều kiện đóng:** H=5 lưu đủ 240 dòng; bỏ một dòng → NOT_READY; duplicate conflict bị phát hiện; Optimization không trộn các đợt. TC-D06.

### REQ-16 Thống nhất state version và các khoảng UNKNOWN

**Vấn đề:** Testing cũ cho version tăng nhiều lần theo transition; SDN đề xuất tăng một lần cuối batch. UNKNOWN/history và hold-down khi rollback chưa cùng cách hiểu.

**Đề nghị chốt:**

- `state_version` thuộc toàn run, không riêng flow. Batch final tăng tối đa một lần khi confirmed state/độ chắc chắn hoặc metadata hold-down có ý nghĩa thay đổi; mọi state row cùng version mới. Validation reject trước write không tăng.
- Trong apply/reconcile không phát snapshot có routing_state_complete=true. Intermediate transitions lưu event; response final và source tables phải đồng bộ ở cùng snapshot final.
- Rollback đã verify cũng là actual path transition: giữ timestamp hold-down phù hợp; rollback về cùng path ban đầu nhưng timestamp đổi có ý nghĩa vẫn invalidate snapshot cũ. Đây là chi tiết SDN/Optimization/Data cần xác nhận rõ.
- `UNKNOWN` → actual_path_id=null, batch INCONSISTENT nếu còn bất kỳ nhóm chưa biết; pause tối ưu toàn cục trong core policy đến resync. Không ghi old path như confirmed.
- Routing history chỉ giữ các khoảng path đã verified, không chồng. UNKNOWN là gap + uncertainty events hoặc bảng riêng; derived timeline có thể ghép gap vào. Không ép null path vào schema history vốn không cho null.

**Đầu ra cần giao:** State transition/version policy, history và final response mẫu; phân biệt counter version, snapshot ID và global routing state version.

**Điều kiện đóng:** Batch APPLIED/ROLLED_BACK/UNKNOWN có response, state, history và per-group kết quả khớp; retry không tăng version lần nữa. TC-D07, TC-S02, TC-S03.

## 4. Những giá trị cần xác nhận trong cuộc họp

Phân biệt quyết định semantics có thể chốt ngay với ngưỡng cần pilot; không để người lập trình tự chọn default khác nhau.

| Nhóm quyết định | Gợi ý lựa chọn | Điều còn cần xác nhận | Thời điểm |
| --- | --- | --- | --- |
| ID/topology | Mã hex Network, path `<flow_group_id>_P1/_P2`, link `from>to` | Mapping file/hash và người phát hành bản chuẩn | Trước tích hợp |
| Snapshot | SDN tạo, Data lưu/cấp lại immutable | API/kênh cấp requested demand lịch sử đã release/available tới SDN | MUST-LOCK trước tích hợp snapshot |
| Forecast | Đúng 7 trường; Data kiểm `48×H*`; Optimization tự tính mean | ML chọn L*/H*/W/feature order offline; Runner freeze config/hash | Semantics theo v1.4; trị số trên validation |
| Epsilon | ML-owned, Optimization không nhân lại | Config/model artifact riêng, validation evidence; baseline/oracle policy | Trước test chính |
| Storage/state/time | REQ-05/06/15/16 | Migration, origins, final version/rollback semantics | Trước tích hợp |
| Transport | Một lựa chọn IPC/REST/queue | Chủ trì adapter, ack/query/retry và serialization | Trước tích hợp |
| Điều phối pilot | TH=.85,N=3,h=.05,hold=10s,B=4,lambda=.1 | Một config, tolerances; thay đổi theo validation có version | Trước pilot / freeze |
| Timing | Một batch flight, skip tick khi busy | Timeout từng bước, stale threshold, guard | Đo pilot, khóa trước benchmark |
| Phép đo | PORT_TX có point rõ | k_e, headroom/pacing/loss/coverage tolerances | Đo S0/pilot |
| Packets | Cohort send-success, sequence/deadline | Payload, packet deadline, probe interval, drain >= deadline | Đo pilot, khóa trước benchmark |
| Thực nghiệm | Warm-up60s, đo360s, S5 chính; tỷ lệ70/15/15 đã xác nhận | Danh sách seed thực, phạm vi20seeds và matrix/count/order/rerun | Tỷ lệ đã chốt; manifest MUST-LOCK trước test chính |

Không tự điền số đo pilot hoặc tên người xác nhận nếu chưa có. Nhóm có thể giao một người kiêm Runner; cần ghi rõ tên thực tế sau họp.

## 5. Việc mỗi thành viên cần sửa và bàn giao

| Role | Sửa trong thiết kế / mã phụ trách | Bàn giao tối thiểu cho vòng tích hợp đầu |
| --- | --- | --- |
| Network | ID export, origins, MAC/ARP/initial map, lịch/split/warm-up và packet log | config + mapping + một schedule + manifest + sender/receiver sample |
| Data | TrainingBundle, validate/store/query snapshot SDN, predictions PK/READY theo H* frozen, state/batch ETL | Schema/migration + training sample + snapshot SDN đọc lại đúng hash + prediction batch 240 dòng khi H*=5 |
| ML | ID hex, đúng 7-field rows, tự chọn L*/H*/epsilon* offline, model/feature contract và fallback | Rows fixture; model/config/raw artifact riêng cho audit; latency khi có |
| Optimization | R4/J, không áp epsilon lại, tự tính mean từ rows, proposal/request adapter, trigger/timeout/max_gap | Toy solver/recompute + proposal + ApplyRequest adapter cho Runner + kept tick record |
| SDN | Tạo snapshot causal, chốt nguồn demand với Network/Data; transport/verify, rules, idempotency, state/version/history/counters | Snapshot ID/hash/cutoff + response success/partial/unknown + state/history/events/counter mẫu |
| Testing | Ghi quyết định thật, sửa bộ testing theo lựa chọn cuối và chạy contract cases | Decision log + fixture expected/actual + defect/evidence index |

Owner của producer và consumer cùng chịu trách nhiệm tương thích; không giao toàn bộ việc chuẩn hóa lại cho Data hay Testing.

## 6. Thứ tự chốt và kiểm chứng đề nghị

1. **Chốt dữ liệu nền:** REQ-01/02/06/07/15 — ID, H bước, origin, config ML riêng và batch đủ. Đầu ra là snapshot SDN → ML 7-field rows → Data READY → Optimization mean dùng cùng ID/time.
2. **Chốt điều khiển:** REQ-03/04/05/08/13/16 — nguồn demand lịch sử cho SDN, snapshot causal, request/response, storage/state và MILP. Đầu ra là fixture xuyên module, gồm duplicate/late/partial/UNKNOWN.
3. **Chốt điều kiện chạy:** REQ-09/10/11/12/14 — config, initial paths, S0, seed/matrix/metric. Các ngưỡng thiếu bằng chứng chuyển thành đầu việc pilot có owner.
4. **Smoke test:** Một nhóm `0x103` với `0x103_P1`→`0x103_P2`→`0x103_P1`, rồi đủ 48 nhóm; đối chiếu dữ liệu và state cuối. Ghi actual/evidence, không đánh PASS chỉ vì thống nhất được tài liệu.
5. **Freeze:** Khóa config/schema/API/model/calibration/split/run matrix theo hash; cập nhật ba file testing và chỉ sau đó chạy benchmark chính.

Không cần chốt mọi trị số hiệu chuẩn trước khi bắt đầu phát triển module riêng; cần chốt semantics của giao diện trước khi nối module. Các câu hỏi còn mở phải có người nhận xử lý và mốc do nhóm điền.

## 7. Mẫu sổ quyết định sau họp

| Requirement | Quyết định cuối | Người chủ trì thực tế | Producer / consumer xác nhận | Hạn hoàn tất | File / version cần sửa | Trạng thái quyết định | Trạng thái thực hiện | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| REQ-01 | ID theo Network; path theo Data schema | Chưa điền | Chưa điền | Chưa điền | config và mapping | ALIGNED_V1_4 | UNVERIFIED | Guide §3.2; chưa có test log |
| REQ-03 | SDN tạo snapshot; kênh demand lịch sử còn mở | Chưa điền | Chưa điền | Chưa điền | SDN/Network/Data contract | ALIGNED_V1_4; kênh OPEN | UNVERIFIED | Guide §4.1/§5.2 D3 |
| REQ-07 | ML áp một lần, config/artifact riêng | Chưa điền | Chưa điền | Chưa điền | ML/Optimization/config | ALIGNED_V1_4 | UNVERIFIED | Guide §5.3 M5; chưa có test log |
| REQ-10 | Tỷ lệ70/15/15 đã chốt; danh sách seed và matrix cần khóa | Chưa điền | Chưa điền | Chưa điền | split/run matrix/test_plan_v2 | AGREED (tỷ lệ); manifest OPEN | UNVERIFIED | Người dùng xác nhận70/15/15; Guide §3.5/§3.6/§6 đã đồng bộ |
| REQ-16 | SDN verified state, Data mirror; chi tiết version cần freeze | Chưa điền | Chưa điền | Chưa điền | SDN schema/interfaces_v2/Data | ALIGNED_V1_4; chi tiết OPEN | UNVERIFIED | Guide §5.5/§13.4 |

Nhân bản mẫu cho đủ REQ-01–REQ-16 khi sử dụng. Mỗi quyết định lưu thêm confirmed_at_utc, lý do, lựa chọn bị thay thế nếu có, config/schema/API hashes và test case liên quan. Nếu đổi quyết định sau freeze, tăng version và xác định case/run nào phải chạy lại.

## 8. Điều kiện kết thúc đợt giải quyết xung đột

- Đủ 16 yêu cầu có quyết định cuối hoặc điều chỉnh phạm vi được owner xác nhận; không dùng im lặng như đồng ý.
- Mỗi producer/consumer đã sửa tài liệu/cấu hình và giao fixture đúng lựa chọn cuối; các bản cũ không tiếp tục đóng vai trò default.
- Các contract cases liên quan đạt với bằng chứng, đặc biệt ID/time/epsilon/READY/actual/version; không còn P0 mở trước tích hợp tương ứng.
- Các ngưỡng cần calibration có kết quả pilot và config khóa trước benchmark; đủ run matrix/seed/initial state/metric policy thống nhất.
- Testing duy trì [interfaces_v2.md](interfaces_v2.md), [test_plan_v2.md](test_plan_v2.md), [acceptance_criteria_v2.md](acceptance_criteria_v2.md) cùng revision của guide và contract đã duyệt; không trỏ về tên file cũ. Mỗi thay đổi sau freeze phải cập nhật cả bộ và các fixture liên quan.

Kết quả mong đợi là một bộ quyết định có thể triển khai và kiểm thử, không phải cam kết RF luôn tốt hơn đối chứng hoặc xác nhận tiến độ bằng số lượng tài liệu.
