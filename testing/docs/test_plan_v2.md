# Kế hoạch kiểm thử và thực nghiệm SDN

- Phiên bản `testing_v2 aligned-guide-v1.4 2026-11-14`.
- Phụ trách Testing/QA/QC.
- Trạng thái: hoàn thiện đặc tả để owner xác nhận, chưa nghiệm thu triển khai.

Bộ tài liệu đồng bộ: [interfaces_v2.md](interfaces_v2.md), [acceptance_criteria_v2.md](acceptance_criteria_v2.md), [integration_requirements.md](integration_requirements.md) và tài liệu này. Căn cứ cập nhật là [Integration Guide v1.4](../../docs/architecture/SDN_Traffic_Engineering_Role_Integration_Guide_v1.4.md), đặc biệt mục 2–6, 10–13. Các quyết định v1.4 về ID, quyền tạo snapshot và hợp đồng dự báo thay thế quy ước cũ trong testing. Các giá trị pilot, transport và ma trận cụ thể vẫn là PROPOSED/MUST_LOCK; đồng bộ tài liệu không đồng nghĩa đã triển khai hoặc PASS kiểm thử.

## 1. Tổng quan đề tài và nguồn căn cứ

Hệ thống dùng lịch sử nhu cầu UDP để dự báo H* bước nhu cầu payload của từng cặp host nguồn–đích. Optimization tự tính mean H* từ các hàng dự báo, rồi MILP chọn đường qua một trong hai switch trung gian; Ryu cài và xác minh Flow Rule. Câu hỏi nghiên cứu là: khi giữ nguyên bộ tối ưu và cơ chế thực thi, Random Forest cùng calibration đã khóa có giúp hiệu năng mạng tốt hơn nhu cầu hiện tại hoặc trung bình trượt, và phải trả bao nhiêu chi phí đổi đường?

Mạng chính có 8 host, 4 switch biên e1–e4 và 2 switch trung gian c1,c2. e1 nối h1,h2; e2 nối h3,h4; e3 nối h5,h6; e4 nối h7,h8. Mỗi switch biên nối cả hai trung gian. Có 8 kết nối fabric hai chiều = 16 link fabric có hướng, thêm 16 link host có hướng. Tập F đầy đủ gồm 48 cặp host có hướng khác switch biên, mỗi nhóm có hai đường ngắn qua c1 hoặc c2. Chiều ngược là nhóm riêng. Một nhóm chỉ chọn một đường ở trạng thái ổn định.

Ngoại tuyến: Network sinh lịch → Data chia tập trước khi tạo cửa sổ → ML tự chọn L*/H*/epsilon* bằng train/validation và lưu config/model metadata. Vòng kín: Network release demand lịch sử → **SDN tạo snapshot** → **Data lưu/cấp nguyên bản** → ML xuất **đúng 7 trường/row** → Data kiểm đủ 48×H* rows theo frozen config → **Optimization tự tính mean H*** → trigger/MILP/validator → orchestrator tạo ApplyRequest → SDN apply/verify → Data lưu state/history → tính metric riêng từng run. Kênh cấp requested_mbps lịch sử hợp lệ cho SDN còn MUST_LOCK; goodput/counters không được thay demand.

Ngoài phạm vi chính: RL, chống tấn công, phục hồi lỗi link/switch vật lý, đa controller, chia một nhóm trên nhiều đường, TCP và Fat-tree. Đứt phản hồi kênh điều khiển được thử như lỗi thực thi, không biến thành đề tài phục hồi mạng vật lý.

## 2. Rà soát tiến độ thiết kế và xung đột

### 2.1. Nguồn và giới hạn kết luận

Tài liệu audit/role cũ chỉ dùng để truy nguyên xung đột. Namespace đang áp dụng theo v1.4 là `flow_group_id=0x103`, `path_id=0x103_P1`/`0x103_P2`, `link_id=e1>c1`; `flow_cookie` cùng trị số nhưng khác chức năng. Không suy ra schema thực nhận từ tên “Data_v2” hoặc trạng thái code từ mô tả thiết kế. Trạng thái triển khai trong kế hoạch này là CHƯA XÁC MINH cho đến khi có commit, fixture và run log tương ứng.

| Role | Bằng chứng đã có về thiết kế | Phần cần bàn giao để xác minh tiến độ tiếp theo |
| --- | --- | --- |
| Network | Topology/DPID/port, 48 nhóm/96 đường, S0–S5, seed và thời lượng | config/CSV thực, MAC/ARP/initial paths, generator/sender/receiver/runner và S0 logs |
| Data | Schema/ingestion, split, lưu/truy vấn snapshot SDN và prediction rows | Migration H-step PK, TrainingBundle, snapshot immutable/hash, forecast completeness theo frozen H*, state/batch ETL |
| ML | RF multi-output, baselines, L/H/epsilon, 7-field prediction rows | Chuẩn ID, model/metadata, train/validation kết quả, batch predict và latency thực |
| Optimization | MILP, trigger/hysteresis/hold/B, output JSON nháp | R4 chính xác, epsilon policy, ApplyRequest adapter, recompute U/J và solver tests |
| SDN | Producer snapshot; actual route/version, verify/partial theo v1.4 | Snapshot creator và nguồn demand đã release, Ryu, transport/timeouts, P1↔P2 evidence |
| Testing | Đã có kế hoạch/tiêu chí/interfaces | Bản v2 này sửa yêu cầu lỗi thời; tiếp theo là fixture, thực thi case, defect và biên bản |

### 2.2. Sổ xung đột và quyết định tích hợp đề xuất

P0: có thể sai dữ liệu/kết quả hoặc chặn tích hợp. P1: ảnh hưởng tái lập, phép đo hoặc công bằng thực nghiệm. Sổ C01–C16 giữ mã để truy vết; quyết định v1.4 đã được phản ánh trong thiết kế, còn evidence triển khai/contract freeze là PENDING. Không coi toàn bộ quyết định đã xác nhận ở guide là OPEN trở lại; chỉ các chi tiết được ghi MUST_LOCK/PROPOSED cần chốt tiếp.

| ID / mức | Căn cứ và ảnh hưởng | Quy tắc testing theo v1.4 / điểm còn mở | Owner / điều kiện đóng |
| --- | --- | --- | --- |
| C01/P0 | Namespace role cũ khác Network | `0x103` / `0x103_P1` / `e1>c1`; cookie cùng mã số, field riêng; vector theo giá trị số hex, join theo ID | Network chủ trì, tất cả consumer; TC-I01/TC-N01 round-trip |
| C02/P0 | Scalar-only hoặc Data tính mean gây sai quyền sở hữu | TrainingBundle giữ y_vector H bước; ML xuất H rows, Data lưu/trả rows, Optimization tự tính mean; mean label/metric do ML/Testing tính offline | Data+ML+Optimization; TC-D04/TC-M01/TC-D06 |
| C03/P0 | Snapshot/cutoff và nguồn lịch sử chưa thống nhất | SDN tạo snapshot immutable, Data chỉ validate/lưu/cấp; window_end≤cutoff và released_at/available_at≤snapshot_as_of; kênh requested_mbps tới SDN MUST_LOCK | Network+SDN+Data+ML; TC-D04/TC-I02 |
| C04/P0 | Optimization §V batch_id/decisions/P1/target_interval khác SDN §4 | Adapter thành decision_batch_id/changes/full path IDs/config_version/forecast UTC | Optimization+SDN; TC-I01/TC-S03 |
| C05/P0 | Data decisions thiếu batch/actual/version; SDN source tables phong phú hơn | Giữ bảng báo cáo, thêm source tables batch/per-group/state/history và trace forecast | Data+SDN; TC-D07/TC-S02 |
| C06/P0 | Offset run/traffic/measurement dễ bị đánh đồng | Giữ ba origin UTC explicit: run_started_at_utc, traffic_started_at_utc, measurement_started_at_utc; forecast offsets theo measurement | Network+Data+ML+SDN; TC-I03 |
| C07/P0 | Epsilon có thể bị nhân hai lần hoặc gửi thừa field | ML tự chọn/apply epsilon offline-config; chỉ final predicted_mbps gửi Data; raw chỉ ở artifact ML nếu cần, Optimization không nhân lại | ML+Optimization+Data; TC-M04; 4→4,4, không 4,84 |
| C08/P0 | Optimization R4 chỉ ≥; λ=0 có thể báo đổi giả; ví dụ J_new=.61 không khớp | y=1−x_current; J=U+λ n_changes/48; sửa ví dụ theo số đổi thật | Optimization; TC-O02/O03/O04 |
| C09/P1 | Optimization N=3, TH/h có 2 lựa chọn; testing cũ/Rà soát khuyến nghị N=2 | Baseline đề xuất N=3,TH=.85,h=.05; các giá trị khác là sweep; một config/hash | Optimization+QA; TC-O01; sign-off trước pilot |
| C10/P1 | Warm-up/split/ma trận cũ lệch guide | Warm-up60s, measurement360s; tỷ lệ70/15/15 đã xác nhận, 20 seeds→14/3/3; S1–S5; danh sách seed và ma trận cấu hình mở rộng mục5 cần khóa | Network+Data+ML+QA; TC-D03/TC-E02; tỷ lệ đã chốt, manifest khóa trước benchmark |
| C11/P1 | S0 Network đòi RTT24±5%, CPU<80%, phát10Mbps; k_e chưa hiệu chuẩn | Giữ làm mục tiêu pilot; đo payload/overhead/qdisc; ngưỡng khóa có điều kiện, không hard-code goodput10 | Network+SDN+Data+Optimization; TC-N03/D05 |
| C12/P1 | Thiếu MAC/ARP policy và initial_paths file; cùng-edge ngoài F | Một initial map/hash chung, static ARP hoặc handler kiểm soát; same-edge test riêng | Network+SDN; TC-N01/S01 |
| C13/P0 | T_ctl1s nhưng chưa có transport/latency budget; APPLIED cần verify | Một batch flight, timeout hữu hạn, check expiry trước write; quá hạn sau write vẫn reconcile thật | ML+Optimization+SDN+Data; TC-S02/S03 |
| C14/P1 | Phạm vi M0/M4 và chi phí partial chưa đủ; Rà soát gọi M4 upper bound | M0 tĩnh, M4 oracle tham chiếu không phải cận trên bảo đảm; switching theo verified transition | QA+Data+SDN; TC-E01/E02/E03 |
| C15/P0 bổ sung | PK thiếu forecast_start va chạm H rows; metadata online làm lệch schema | ML gửi đúng 7 trường; Data tạo registry nội bộ, H* lấy frozen config/RunManifest; READY chỉ đủ 48H rows cùng issued_at/source, không trộn nguồn | Data+ML; TC-D06 |
| C16/P0 bổ sung | interfaces testing cũ tăng version theo nhiều transition; SDN tăng một lần cuối batch; history UNKNOWN khác nhau | Global version tối đa+1/final batch; UNKNOWN là khoảng gap có log, không fake path; response khớp DB | SDN+Data+Optimization; TC-D07/S02 |

Với d[k] cho [k,k+1), X=[t−L,t), Y=[t,t+H); kiểm cả biên t để tránh lệch một giây. Epsilon thuộc ML theo quyết định v1.4, không còn lựa chọn opt-owned trong contract hiện hành. Nếu cần thay đổi, phải sửa contract/config/tests và ghi nhận quyết định mới trước khi tích hợp. Quy tắc chi tiết về version/transport vẫn cần freeze với SDN và Data.

### 2.3. Thứ tự xử lý

1. Đóng C01/C02/C06/C07/C15: danh mục ID, X–y_vector–mean, origin, epsilon và prediction batch.
2. Đóng C03/C04/C05/C08/C13/C16: snapshot causal, adapter ApplyRequest, state và MILP fixtures.
3. Chốt C09–C12/C14 và các ngưỡng calibration; chạy smoke một flow rồi đủ48 nhóm.
4. Qua G0–G7, freeze mọi MUST_LOCK và chạy benchmark G8. QA kiểm evidence, owner sửa tài liệu/mã của mình; bản này không thay mặt ký xác nhận.

## 3. Phân công trách nhiệm

| Role | Chịu trách nhiệm đầu ra | QA kiểm chứng/bàn giao nhận |
| --- | --- | --- |
| Network | Topology, DPID/port mapping cùng SDN, lịch, UDP sender/receiver, S0 | Hai đường, 48 nhóm, tính tái lập lịch, pacing, packet log |
| Data | Schema, split, TrainingBundle, lưu/cấp snapshot SDN, prediction registry, raw→clean→metric | Không tạo snapshot/tính mean; H* từ frozen config, hash, NULL, ingest idempotent |
| ML/AI | Tự chọn L*/H*/epsilon* offline, model/config, 7-field PredictionRows, baselines | Đúng H/đơn vị/feature; config tách payload, sai số/latency/fallback |
| Optimization | Mean-H adapter, trigger, MILP, validator, OptimizationResult | R1–R7, J, hold-down/B, timeout/stale; orchestrator tạo ApplyRequest |
| SDN | Tạo snapshot từ lịch sử hợp lệ, Ryu, counter, apply/verify, routing state | Causality/immutable, không APPLIED giả, partial/rollback/UNKNOWN, event loop |
| Testing/QA/QC | Test plan, tiêu chí, hợp đồng tổng hợp, case/defect/evidence, kiểm soát seed/run | Kiểm tra độc lập; tổng hợp kết quả; không tự xác nhận thay producer |
| Evaluation (có thể do Testing kiêm nhiệm) | Chạy benchmark, bảng ghép cặp/biểu đồ/kết luận | Đơn vị phân tích theo lịch, quy tắc loại run và giới hạn |

## 4. Đối chứng M0–M4

| Mã | Demand-provider tại t | Tối ưu/trigger | Vai trò và giới hạn |
| --- | --- | --- | --- |
| M0 Static | Không dùng để điều phối | Không chạy tối ưu; giữ mapping ban đầu | Baseline bổ sung; vẫn thu đầy đủ metric; không là cận dưới tuyệt đối |
| M1 Current/Persistence | Giá trị nhu cầu hợp lệ gần nhất đã available tại t, giữ cho [t,t+H) | Cùng core với M2–M4; vector H bước trước mean | Đối chứng chính; không dùng goodput/counter thay nhu cầu |
| M2 Moving Average | Trung bình theo thời lượng W giây lịch sử hợp lệ; W≤L, khóa trước test | Cùng core | Không phải giá trị cuối; không dùng centered rolling |
| M3 Random Forest | Dự báo mô hình và epsilon ML khóa; khi lỗi có thể tạo toàn batch persistence nếu đủ history hợp lệ | Cùng core | Giữ method=M3; source=fallback_persistence, reason riêng; không trộn source trong READY batch |
| M4 Oracle | H* giá trị requested_mbps thật từ lịch trong [t,t+H*); Optimization tự tính mean | Cùng core, gồm hold-down/B và apply latency | Chỉ tham chiếu thông tin tương lai trong evaluation offline, không deploy; không là cận trên toàn cục |

Policy fallback toàn batch ở M3 là **PROPOSED** để đáp ứng quy tắc không trộn source của guide §5.2: nếu RF lỗi, ML có thể phát lại đủ 48×H* rows với `source=fallback_persistence` và cùng issue mới khi toàn bộ lịch sử còn hợp lệ; nếu không thì skip tick. Không hợp thức hóa batch trộn RF/persistence bằng source map. Guide §5.6 gọi M4 là “upper bound offline”; ở đây hiểu là oracle tham chiếu thông tin tương lai, không có chứng minh cận trên hiệu năng toàn cục dưới hold-down/B/latency.

## 5. Ma trận thực nghiệm đề xuất theo v1.4

Đơn vị schedule là (scenario, cấu hình scenario, seed). S0 là calibration riêng. **Tỷ lệ train/validation/test đã xác nhận là70/15/15**. Với20 seeds, chia **14 train/3 validation/3 test**; Guide §3.5, YAML §3.6 và bảng §6 đã đồng bộ. `split_manifest` còn cần khóa danh sách seed cụ thể, phạm vi áp dụng và hash; không chờ chọn lại tỷ lệ.

S1 có bốn cấu hình rho; S2a/S2b/S3/S4/S5 mỗi loại một cấu hình, tạo 9 cấu hình mở rộng. Guide chưa chốt 20 seeds áp dụng theo scenario hay theo từng cấu hình. Bảng dưới là **dự toán có điều kiện nếu nhóm chọn 20 seeds cho từng cấu hình trong 9 cấu hình**, chưa phải tổng số lượt bắt buộc. Ma trận, seed thật và số lượt cuối phải lấy từ manifest được freeze; mọi biến thể cùng parent family ở cùng split, seed namespaces giữa split rời nhau.

| Bộ | Lịch độc lập | Phương pháp | Số lượt / điều kiện |
| --- | --- | --- | --- |
| Offline có điều kiện | 9×20=180 lịch, trong đó 126 train/27 validation/27 test | Tạo X/y, học/chọn trên train/validation | Chỉ khi chốt 20 seeds/cấu hình; không bắt buộc chạy Mininet mỗi lịch offline |
| Pilot tích hợp | S0, S1-low, S3, S5 dùng seed pilot riêng | Smoke M0–M4, từng bước | Không nhập kết quả test; hiệu chuẩn/timing/feasibility |
| Benchmark mở rộng có điều kiện | 9 cấu hình×3 seed test=27 lịch | M0–M4 cùng mọi lịch | 135 lượt, chưa gồm rerun/drain; báo đủ S1–S5 |
| Fault riêng | S3 hoặc S5, 12 loại F01–F12, mỗi loại3 fault seed | M3 + đối chứng sạch cùng lịch khi cần | 36 lượt lỗi; không trộn benchmark sạch |
| Ablation/sensitivity | Validation hoặc tập bổ sung khóa trước | raw RF/epsilon; TH,N,h,B,hold,L/H | Không tuning lại trên tập test đã freeze |

S1 rho=.4/.6/.8/1.0 giữ theo Network; rho1.0 là stress chạm giới hạn host. Rho=sum requested/80 chỉ mô tả tổng, không chứng minh host/fabric khả thi. S2b các pha độc lập không bảo đảm tổng rho luôn .3–.9; tính lại envelope từ lịch. S3/S4/S5 phải lưu cả demand trước/sau rescale/cap và thuật toán điều chỉnh; cap theo tổng gửi/tổng nhận từng host, không cap riêng từng flow. Với UDP payload, giới hạn10Mbps payload chưa bảo đảm không nghẽn host vì overhead. Gắn nhãn CORE hoặc HOST_STRESS theo calibration; giữ stress trong báo cáo riêng, không loại bất lợi sau test.

Một lượt baseline đề xuất: **warm-up 60s + measurement 360s** + drain/reset đã hiệu chuẩn. Với ma trận mở rộng có điều kiện ở trên, 135×420s=56.700s=15,75 giờ trước drain/reset; đây là dự toán, không phải thời lượng đã chạy. SDN chỉ tạo snapshot VALID khi đủ L* bins đã release/available; 60s warm-up không cho phép giả dữ liệu đã sẵn có. Nếu nguồn lực không đủ, khóa ma trận rút gọn bằng protocol revision trước khi xem test. Không chạy nhiều Mininet benchmark đồng thời trên cùng máy.

## 6. Cấu hình khóa và quản lý seed

### 6.1. Cấu hình

Giá trị khởi đầu đề xuất để owner xác nhận theo v1.4: Δ=T_ctl=1 s; link 10 Mbps; trễ host–biên 1 ms/chiều, fabric 5 ms/chiều; queue 50 packet; payload 1200 byte; không chèn loss ngẫu nhiên; K=2; TH=0,85; N=3; h=0,05; T_hold=10 s; B=4; lambda=0,1; solver timeout 0,2 s và T_stale=2Δ là đề xuất pilot, chưa được đo. ML tự chọn epsilon*∈{0;0,05;0,10;0,20}, L*∈{10,30,60}s, H*∈{1,3,5,10}s bằng validation và lưu `configs/ml_config.yaml`/model metadata; nếu epsilon không cải thiện hoặc hòa thì ưu tiên 0. Optimization không áp epsilon. numeric_epsilon là tolerance riêng. Baseline/Oracle có policy epsilon riêng trong RunManifest, còn epsilon=0 là đề xuất so sánh cần freeze. Những giá trị này không phải SLA đã được xác nhận.

Trước test, lock config gồm ML config reference/hash (L*/H*/epsilon*, W, feature/model), tham số điều phối, solver/tolerance, `allow_feasible`/`max_gap` nếu dùng nghiệm chưa OPTIMAL, numeric_epsilon, overhead k_e/calibration_id, snapshot age rule, nguồn demand lịch sử cho SDN, policy missing/stale/fallback, timeouts, apply guard, packet deadline/drain, probe interval, rebin rule, calibration tolerance, headroom, quantile và rule loại run. Raw forecast nếu cần chỉ ở artifact đánh giá ML riêng; không thêm epsilon/raw/config metadata vào 7-field PredictionRows. Trường MUST_LOCK chưa có trị số làm benchmark BLOCKED. Chỉ ghi `locked=true` sau xác nhận của các owner; lưu người/thời điểm/hash.

### 6.2. Seed registry

| Trường | Ý nghĩa/quy tắc |
| --- | --- |
| `schedule_seed` | Sinh lịch; tập seed train/validation/test không giao nhau |
| `parent_family_id` | Các lịch xuất phát từ cùng mẫu/tham số gốc phải cùng split; khác seed chưa đủ độc lập |
| `split_seed` | Nếu chia ngẫu nhiên theo nhóm lịch, lưu thuật toán/seed/manifest; không chia từng dòng |
| `model_seed` | Random Forest và bước tiền xử lý ngẫu nhiên; khóa chung cho model chính |
| `solver_seed` | Seed solver nếu hỗ trợ; nếu không ghi unsupported, lưu threads/tolerance/version |
| `order_seed` | Sinh thứ tự chạy; tách namespace khỏi schedule_seed |
| `bootstrap_seed` | Tái lập CI; không dùng làm seed sinh tải |
| `fault_seed` | Vị trí/thời lượng lỗi; chỉ cho bộ fault, lưu faults.yaml |

Data giữ registry, Network sinh lịch, ML/Optimization khai báo seed nội bộ, QA kiểm tra đối chiếu. Lưu cả seed và phiên bản generator/config/hash file, không chỉ seed. Lịch cùng schedule_id dùng lại nguyên byte cho M0–M4; mỗi lần chạy có run_id mới. Không hứa seed tái tạo byte-identical kết quả truyền trên OS.

Quy tắc split: nhóm theo schedule_id + parent_family_id trước khi cắt X/y; mọi replay cùng split; hash nội dung và fingerprint chuỗi tải chuẩn hóa không trùng khác split. Fingerprint bỏ ID/timestamp tạo file để bắt lịch sao chép đổi tên. Tỷ lệ bắt buộc70/15/15 (14/3/3 nếu đơn vị chia có20 seeds); manifest ghi danh sách seed thật, phạm vi áp dụng, hash và owner xác nhận, không chỉ tỷ lệ. Train/validation/test cần đủ họ kịch bản theo thiết kế.

Với 3 lịch test/cấu hình và 5 methods trong ma trận mở rộng đề xuất, không thể tuyên bố cân bằng đủ vị trí bằng Latin square 5×5. Sinh thứ tự hoán vị có seed cho từng lịch, tối ưu cân bằng trên toàn ma trận và công bố số lần mỗi method ở mỗi vị trí; khóa `run_order.csv` trước test. Nếu cần cân bằng carry-over dùng repeat bổ sung có kế hoạch. Cùng lịch lặp không tăng số lịch độc lập. Mọi method có run_id/log đo riêng.

## 7. Quy trình chạy một lượt

1. QA kiểm tra gate G0–G7, config/model/split và ngưỡng đã khóa trước benchmark G8; tạo manifest và run_id duy nhất trước khi khởi chạy. Ghi method, schedule/hash, repeat/attempt và order.
2. Dừng sender/receiver/controller/Mininet cũ; dọn namespace/rule/tc theo script. Dựng topology sạch; kiểm tra 6 OpenFlow connections, DPID/port/IP/MAC/ARP và config hash.
3. Nạp model một lần với M3; khởi receiver, collector/probe. Cài và xác minh mapping ban đầu cho F, state/history khởi tạo. `last_successful_change_at` bootstrap=null, hiểu là chưa có lần đổi và không khóa hold-down; không bịa thời điểm đổi trước run. Chốt mốc UTC–monotonic.
4. Replay lịch đầy đủ gồm warm-up+360 s đo. Policy đề xuất: không điều phối trong warm-up cho tất cả phương pháp; vẫn thu lịch sử và log. Khi mở đo, reset trigger, không reset lịch sử; hold-down khởi tạo đồng nhất. Nếu nhóm chọn điều phối trong warm-up phải đổi policy chung trước test và tách các lần đổi warm-up.
5. Trong đo: SDN tạo snapshot causal → Data lưu nguyên bản → ML 7-field rows → Data READY → Optimization tự mean/trigger/solve → SDN apply/verify; không có hai batch apply chồng. PARTIAL/INCONSISTENT chặn tick tối ưu tiếp theo tới khi reconcile xong. Thu raw counter, packets, probes, state, quality, CPU/RAM/latency. Ghi mọi tick giữ đường, fallback hoặc quá hạn. Chỉ phát hành forecast nếu forecast_end≤measurement_end; H=10 thì cutoff cuối là 350. Label tail là thay đổi cần duyệt riêng, không trộn metrics chính.
6. Đến measurement_end dừng phát; tiếp tục receiver đến drain_end. Không sinh quyết định mới trong drain; giữ rule hiện tại để thu gói cuối. Đợi deadline cho cohort chính, giữ cả packet đến muộn với cờ late.
7. Đóng log, cập nhật manifest/status. Kiểm tra checksum, ID, timestamp, coverage và trạng thái UNKNOWN; tính metric từ raw bằng pipeline phiên bản khóa.
8. Ghi quality report, quyết định included/excluded và reason code theo quy tắc có trước; lưu run lỗi. Reset trước phương pháp tiếp theo. QA ký case/evidence, không sửa raw.

### Cohort và ranh giới thời gian

Cohort chính là packet **gửi thành công** với send_time∈[measurement_start,measurement_end). Mỗi packet có deadline=send_time+D_packet; nhận hợp lệ trước deadline và trong drain. Đề xuất điều kiện khóa T_drain≥D_packet để gói cuối đủ cơ hội. D_packet do pilot xác định, áp dụng chung. Packet gửi warm-up không nhập cohort dù nhận ở phần đo; packet cuối phần đo nhận trong drain vẫn được tính. Packet nhận trùng tính một lần theo run/group/sequence.

Nếu dùng arrival-window goodput, ghi rõ đây là series theo thời gian nhận, tách khỏi cohort goodput. Timestamp log UTC phục vụ ghép/truy vết; phép trừ thời gian dùng monotonic trên máy đã xác minh chung miền clock. Không tự coi RTT/2 là trễ một chiều.

## 8. Chỉ số và công thức

Ký hiệu: T=360 s đo chính; Q_req=tổng requested_mbps×1e6/8×thời lượng giao với khoảng đo; Q_tx=payload bytes gửi thành công của cohort; Q_rx=payload bytes nhận duy nhất đúng hạn của cohort; P_tx/P_rx là số sequence tương ứng. Mẫu số 0 trả NULL/not_applicable, không trả 0 giả.

| ID | Metric và công thức | Cách báo cáo |
| --- | --- | --- |
| MET01 | Goodput cohort=8Q_rx/(1e6T), Mbps | Tổng và từng nhóm; **chính** |
| MET02 | Tỷ lệ phục vụ=Q_rx/Q_req; phát thực/yêu cầu=Q_tx/Q_req | Tổng, từng nhóm; giải thích packetization/pacing; không tự cắt về 1 |
| MET03 | Packet loss=(P_tx−P_rx)/P_tx; % nhân 100 | **Chính**; theo sequence/deadline, không trừ hai cửa sổ giây |
| MET04 | Duplicate=packet nhận trùng; reorder theo thứ tự sequence đến, tách late | Count và rate với mẫu số ghi rõ; không tăng Q_rx do duplicate |
| MET05 | RTT p50/p95 và probe success=P_reply/P_probe | Toàn run/theo nhóm nếu đủ mẫu; timeout không là RTT=0; lưu n |
| MET06 | r_e=8Δcounter/(1e6Δt); u_e=r_e/C_e; MLU(t)=max fabric u_e(t) | 16 chiều fabric; time-weighted mean,p95,max; **MLU p95 chính** |
| MET07 | Time above θ=sum duration(t)×1[MLU(t)>θ]/T_valid, θ=0,85;0,90 | Ghi coverage=T_valid/T riêng; không lấy missing như không vượt |
| MET08 | Số đổi tối ưu xác minh thành công; số rollback/recovery riêng | Count/group/run và /phút; **chi phí chính** |
| MET09 | FlowMod gửi thành công/thất bại/rollback; apply success=verified changes/attempted changes | Giữ mọi FlowMod, cả thất bại; 0 attempt→N/A |
| MET10 | feature,forecast,solve,apply,total_ms; overrun=count(total_ms>T_ctl×1000)/eligible_ticks | p50,p95,max; ghi tick từ lúc bắt đầu đến terminal/UNKNOWN, không chỉ tick thành công |
| MET11 | data_age; coverage_remaining=max(0,forecast_end−verified_apply_at) | Phân phối, tỷ lệ hết horizon và skipped ticks |
| MET12 | MAE=mean(abs(y−ŷ)); RMSE=sqrt(mean((y−ŷ)^2)), Mbps | Chấm vector theo từng h=1..H và pooled; chấm mean-H riêng do ML/Testing tính offline; raw nếu có từ artifact ML riêng, final từ predictions; native/fallback tách, cùng support |
| MET13 | fallback_groups/eligible_group_forecasts; fallback_ticks/eligible_ticks; invalid/missing/stale rate | Ghi cả tử/mẫu; M3 vẫn là M3; native RF và all-provider report riêng |
| MET14 | CPU/RAM, generator lag/send errors, pending queue | Phân phối và môi trường; không loại run chỉ vì RF chậm |
| MET15 | Ping-pong: quay lại đường trước trong W_pingpong khóa trước | Phụ; tách rollback; cửa sổ MUST_LOCK nếu dùng |

Phân vị dùng quantile nội suy tuyến tính cho tập mẫu thô RTT/latency; MLU sau rebin Δ cố định dùng weighted empirical quantile nếu thời lượng cửa sổ khác nhau. Rebin chỉ dùng offline với quy tắc công bố; online không dùng counter tương lai để nội suy. Khoảng thiếu/reset đánh dấu invalid. Nếu một link fabric thiếu, không tính max của 15 link như MLU đủ 16; metric MLU tại khoảng đó NULL và giảm coverage.

MLU chỉ gọi là utilization egress khi S0 kiểm chứng điểm đếm; counter trước shaping phải ghi offered load, không gắn nhãn utilization. Mỗi chiều lấy một counter chính, không cộng Tx nguồn+Rx đích. Không clip u_e>1, giữ cờ và điều tra. Tải dự kiến U_old/U_new và U_measured là hai loại riêng. MLU thấp kèm loss cao/goodput thấp không chứng minh cân bằng tốt.

Ví dụ fixture: T=1 s, yêu cầu 100 packet×1200 byte, gửi thành công 100, nhận đúng hạn 90 unique và 2 bản trùng → goodput=0,864 Mbps, loss=10%, served=90%, duplicate=2. Packet cuối giây đến ở giây sau nhưng trước deadline vẫn thành công theo cohort.

## 9. Danh mục test và truy vết

| Case | Module/tiêu chí | Input/thao tác | Expected và bằng chứng |
| --- | --- | --- | --- |
| TC-N01 | AC-N01/N02 | Dựng topology, liệt kê paths/ports, ép từng nhánh | Đúng 6 switch/8 host/48 flows/96 paths/32 directed links/16 fabric/192 path–fabric incidences; `0x103`, `0x103_P1`, `e1>c1` từ Network; dump/probe |
| TC-N02 | AC-N03 | Sinh lịch hai lần cùng seed/config; đổi seed | Cùng hash khi tái tạo; khác seed không mặc nhiên đủ khác họ; manifest/diff |
| TC-N03 | AC-N04/N05 | S0 pacing thấp/cao, đối chiếu payload/counter/qdisc | Báo sai lệch/headroom/điểm đếm theo calibration, không gán goodput=10 |
| TC-D01 | AC-D01/D02 | NULL, zero, NaN, âm, burst đúng lịch, đơn vị ×1000 | NULL khác 0; invalid có reason; giữ burst; không đoán sửa scale |
| TC-D02 | AC-D03 | Hai bản ghi giống khóa và hai bản mâu thuẫn | Nạp lại idempotent; conflict quarantine/reject, không overwrite tùy ý |
| TC-D03 | AC-D04 | Manifest20 seeds theo70/15/15; thử số lượng sai; cùng lịch đổi ID/họ/seed, replay khác method | Đúng14 train/3 validation/3 test; từ chối số lượng sai; không lọt qua split bởi đổi tên; replay cùng split |
| TC-D04 | AC-D05/D10/M01/S10 | SDN tạo snapshot t=100,L=30 từ released/available history; late release/availability, window vượt cutoff, đổi run/group; Data ingest/readback | X=70–99,Y=100–104; đủ 48×L bins nếu VALID; không future/ghép run; Data giữ ID/version/hash và không build/patch snapshot cũ |
| TC-D05 | AC-D06 | Counter reset, reply trễ, thiếu một fabric link | Epoch mới; delta invalid; MLU thiếu→NULL, coverage đúng |
| TC-M01 | AC-M01/M02 | Lịch sử2,3,4; MA window=3s,H=5; RF fixture[2,2.5,3,3.5,3] | M1=[4]×5; M2=[3]×5; RF mean2.8; giữ5 bước và chấm MAE bước/mean riêng |
| TC-M02 | AC-M03 | Sai feature order/model hash/H; output final sau chuẩn hóa NaN/âm/>capacity | Sai hợp đồng bị chặn; final >capacity hữu hạn không tự coi lỗi; raw âm được xử lý theo công thức ML trước khi kiểm final |
| TC-M03 | AC-M04 | RF exception/timeout; thiếu L*/history stale; lỗi một nhóm | Thiếu/invalid history→INVALID/skip, không tự rút L*. RF lỗi khi history hợp lệ mới được fallback toàn batch theo policy freeze; source=fallback_persistence, không trộn nguồn; method=M3 |
| TC-O01 | AC-O01 | Dãy U=[TH,TH+δ,TH+δ,TH+δ], N=3 | Strict >; trigger chỉ sau 3 tick hợp lệ; stale/commit reset |
| TC-O02 | AC-O02/O03 | Toy 2 nhóm 6 Mbps cùng c1, k=1,C=10 | U_old=1,2,U_new=0,6,J_new=0,65 khi F=2,lambda=0,1; xác minh enumeration |
| TC-O03 | AC-O02/O04 | λ=0 và y chính xác; B=0, nhóm trong/đúng biên hold-down, toàn zero, overload | Không đổi khi khóa/zero; biên đủ T_hold được xét; overload không tự infeasible |
| TC-O04 | AC-O03 | J_new ngay dưới/bằng/trên (1−h)J_old; epsilon | Chấp nhận biên theo tolerance khóa; h áp lên J, không chỉ U |
| TC-O05 | AC-O05/S02 | OPTIMAL/FEASIBLE/INFEASIBLE/TIME_LIMIT/ERROR; timeout có/không incumbent, thiếu/vượt max_gap, state đổi/hết hạn | OPTIMAL qua validator; nghiệm chưa OPTIMAL chỉ dùng khi policy cho phép, gap≤max_gap, integer feasible, validator pass, còn hạn; thiếu policy/gap hoặc stale→giữ |
| TC-S01 | AC-S01/S03 | Đổi một nhóm, probe 0 demand, chiều ngược | Rule dump+probe xác minh; nhóm ngoài batch/chiều ngược không bị đổi |
| TC-S02 | AC-S04 | Error/Barrier-only, reject FlowMod, mất reply, bỏ một update; verify trễ qua expiry sau write | Không APPLIED giả; partial/rollback/UNKNOWN phản ánh thực; UNKNOWN→batch INCONSISTENT; PARTIAL/INCONSISTENT block tick tới khi reconcile |
| TC-S03 | AC-S02/S05/S07 | Hai apply cùng version, solver treo worker; duplicate cùng/khác payload và pending/final; Ryu restart | Không overlap; stale result bị reject; event loop vẫn xử lý health; restart phải reconcile actual routes/version trước nhận request mới theo recovery policy đã freeze |
| TC-E01 | AC-E01/E02 | Fixture 100/90 packets và duplicate/late/warm-up | Đúng metric mục 8; N/A mẫu số 0; recompute từ raw |
| TC-E02 | AC-E03 | Ma trận replay/order và manifest seed/config | M0–M4 cùng core; M1–M3 chỉ đổi provider/calibration khai báo; từng run có log đo riêng |
| TC-E03 | AC-E04 | Ghép bảng theo schedule_id, fault run, run lỗi infra | Không ghép chéo seed; loại có lý do; giữ fallback/timeout |
| TC-D06 | AC-D08/M07 | Frozen H*=5; insert 240 rows đúng 7 trường; thiếu 1/dup/conflict/mixed issue/source, thêm H/epsilon/raw/mean, thiếu config | Không va PK; READY chỉ đủ 48×5 liên tục/unique/cùng issue/source; từ chối field thừa/config thiếu; Data tạo registry nội bộ và trả rows, Optimization tự mean |
| TC-D07 | AC-D09/S07 | Batch3 nhóm APPLIED/ROLLED_BACK/UNKNOWN; ingest twice | actual/status/response/state version khớp; UNKNOWN=null; history không chồng |
| TC-M04 | AC-M08 | Artifact ML fixture raw4, frozen epsilon=.1,k=1; ML→Data→Optimization | predicted_mbps4.4,b_used4.4, không4.84; raw/config chỉ ở artifact ML, không thêm field dự báo; Optimization tự mean vector |
| TC-I02 | AC-D05/M03 | Giữ lịch sử không đổi, sửa mạnh tương lai trong schedule ẩn; chèn late history; thử forecast cuối run | M1–M3 output bất biến với tương lai; M4 có thể đổi; late không vào snapshot cũ/X; H10 cutoff350 hợp lệ,351 không phát hành |
| TC-I03 | AC-I03 | run origin riêng,traffic2026-11-12T09:00:00Z,measurement09:01:00Z,t=100,H=5 | DB forecast100..105→UTC09:02:40..45Z; schedule offset[160,165); issued_at là thời điểm phát hành thật; round-trip không lệch |
| TC-I01 | AC-I01/D10/S10 | Một lịch end-to-end qua tất cả interfaces | Network demand→SDN snapshot→Data immutable→ML 7 fields→Data READY→Optimization mean/solve→orchestrator ApplyRequest→SDN verify→Data state→metric |

Mỗi case có test record gồm case_id, requirement_id, owner, input/hash, expected, actual, status, evidence_uri, defect_id, git/config hash và thời điểm. Trạng thái thiết kế hiện tại là NOT_RUN; không điền PASS nếu chưa có actual/evidence. Fault dùng F01–F12 của [acceptance_criteria_v2.md](acceptance_criteria_v2.md) mục 8; không gán số lỗi từ PDF chưa được cung cấp để đối chiếu; TC-D05 và TC-O03 vẫn bắt buộc kiểm tra logic dù không thuộc 12 fault lượt ưu tiên.

## 10. Gate, lỗi và quy tắc loại run

Gate dùng đúng G0–G8 của guide §5.6; bộ G1–G6 cũ không còn áp dụng:

| Gate | Phạm vi | Điều kiện qua / evidence |
| --- | --- | --- |
| G0 | Contract/config/schema | Các boundary dùng cùng schema/version/catalog; C01–C16 có quyết định và cấu hình smoke đủ rõ |
| G1 | Network S0 | Topology, ping, P1/P2, counters và calibration có evidence |
| G2 | Network→Data | Demand/telemetry ingest đúng ID/unit/time, idempotent |
| G3 | SDN→Data→ML | SDN tạo snapshot từ nguồn demand hợp lệ đã chốt; Data lưu/cấp nguyên bản; causality, split/X-y đúng |
| G4 | ML→Data→Optimization | Đúng 7 fields/row; Data READY đủ 48×H* theo frozen config; Optimization tự mean; baseline/fallback/epsilon đúng |
| G5 | Optimization standalone | Fixture, objective/constraints, trigger/hold-down/timeout và validator đúng |
| G6 | Optimization→SDN | Orchestrator adapter ApplyRequest; h1→h3 P1↔P2 verify, stale/idempotency/deadline đúng |
| G7 | Full 48 flows | Batch/state/version/rollback/partial/fault đúng; reconcile hết UNKNOWN trước đi tiếp |
| G8 | Benchmark | Ngưỡng/split/ma trận freeze; M0–M4 cùng điều kiện, đủ raw/metrics, recompute/paired analysis và báo cáo |

Không vào G7 khi G1–G6 chưa qua; benchmark chỉ bắt đầu sau G0–G7 và các MUST_LOCK đã khóa. QA chỉ mở gate khi có evidence; người phụ trách module xác nhận phần mình. Một gate chưa chạy giữ NOT_RUN.

Severity: blocker=leakage, APPLIED giả, state sai/không kiểm soát, thiếu bằng chứng metric; major=sai đơn vị/nhãn, R1–R7, không tái lập; minor=lỗi trình bày không ảnh hưởng tính đúng. Release benchmark yêu cầu 100% case bắt buộc PASS, không blocker/major mở; một case chưa chạy là chưa đạt, không coi waived như PASS.

| Loại tình huống | Cách xử lý |
| --- | --- |
| Dựng sai topology/config, log hỏng/mất tới mức metric không tính được, host benchmark bị tác vụ ngoài thiết kế làm nhiễu theo rule pilot | Giữ run, đánh dấu invalid_infrastructure; ghi root cause/evidence, rerun có ID mới |
| RF chậm, solver timeout, fallback, nhiều FlowMod, loss cao do chính phương pháp | Giữ trong hiệu năng phương pháp; báo quality/degraded/overrun, không loại |
| Metric riêng thiếu coverage | NULL/chất lượng cho metric đó theo ngưỡng khóa; không tự loại toàn run hoặc điền 0 |
| Một method thiếu run hợp lệ trong block | Primary paired analysis dùng block hoàn chỉnh theo từng metric; công bố số/lý do missing và toàn bộ run; rerun theo rule khóa |

Quy tắc rerun đề xuất: tối đa 2 attempt thay thế cho lỗi hạ tầng, chọn **attempt hợp lệ đầu tiên**, giữ mọi attempt; nếu hết thì block incomplete. Không chọn attempt có goodput tốt nhất. Quyết định valid dựa health/log/config, không dựa M3 thắng hay thua. Giới hạn attempt phải khóa trước test.

Defect record: defect_id, severity, case, run/batch/group, expected/actual, raw evidence, bước tái hiện, owner, fix commit, retest result. Lỗi đã fix phải retest case liên quan và regression tích hợp, không sửa số metric để đóng lỗi.

## 11. Phân tích và kết luận

Định trước endpoint chính: goodput cohort và packet loss cho hiệu quả truyền; MLU p95 cho tập trung tải; số đổi xác minh/FlowMod cho chi phí. RTT/latency/fallback/coverage là bằng chứng giải thích và giới hạn vận hành. Tính chênh lệch M3−M1, M3−M2 theo cùng schedule/scenario; dấu có lợi là dương với goodput, âm với loss/MLU/chi phí. Báo hiệu số tuyệt đối; phần trăm chỉ khi baseline khác 0 và có ý nghĩa.

Mỗi lịch/seed độc lập là đơn vị tái lấy mẫu; repeated attempts/repeats cùng lịch không là mẫu độc lập. Bootstrap ghép cặp đề xuất B_boot=10.000, CI percentile 95%, seed riêng, lấy lại cả cặp trong từng kịch bản. Nếu có nhiều repeat đã định trước, tổng hợp theo lịch trước bootstrap. Với 3 lịch test/cấu hình trong ma trận mở rộng đề xuất, CI có độ phân giải và độ tin cậy hạn chế; công bố toàn bộ run points và n thực tế, không dùng 360 giây như n=360. Không gộp scenario thành một số chính tùy ý; summary phụ dùng trọng số bằng nhau đã định trước.

Không bắt buộc kiểm định p-value. Nếu dùng, khóa giả thuyết và họ kiểm định trước; số phép so sánh lấy từ scenario/cấu hình và endpoint trong ma trận freeze, không giữ số kịch bản cũ mặc định; có thể hiệu chỉnh Holm trong họ đã khai báo, chi phí báo mô tả trừ khi có họ riêng. Kiểm định đổi dấu cần giả định thích hợp, không gọi exact vô điều kiện. CI chính không tự được coi đã hiệu chỉnh đa so sánh.

Không đặt trước M3 phải tăng X%. Ngưỡng lợi ích thực tiễn nếu dùng phải chọn từ nhiễu pilot và nhu cầu nghiên cứu trước test. Kết luận theo kịch bản: cải thiện có bằng chứng, đánh đổi, chưa đủ bằng chứng, hoặc kém hơn. CI chứa 0 không chứng minh hai phương pháp bằng nhau. Oracle không chứng minh tối ưu toàn cục; MAE thấp không chứng minh mạng tốt hơn. M3 có epsilon ML khác0 là kết quả RF+calibration; để tách đóng góp RF dùng ablation epsilon0 đã khóa, không quy toàn bộ chênh lệch cho thuật toán RF.

## 12. Lưu trữ, kết quả và điều kiện hoàn thành

```text
experiments/<experiment_id>/
  protocol/ test_plan.md acceptance_criteria.md interfaces.md
  frozen/ config.yaml model_manifest.json env.json seed_registry.csv
          split_manifest.csv run_order.csv calibration.json checksums.sha256
  schedules/<schedule_id>/ demand.csv manifest.json
  runs/<run_id>/
    run_manifest.json
    raw/ sender.jsonl receiver.jsonl probes.jsonl link_counters.jsonl
         predictions.jsonl decisions.jsonl events.jsonl routing_history.jsonl
    clean/ validated_tables.parquet quality_report.json
    export/ metrics.json metric_series.csv verification_report.json
  aggregate/ run_metrics.csv paired_differences.csv ci.csv figures/ report.md
  qa/ test_results.csv defects.csv acceptance_record.csv exclusions.csv
```

Raw cần bổ sung `snapshots.jsonl` nguyên bản SDN (ID/version/hash), `forecast_batches.jsonl` do Data tạo, `decision_batches.jsonl`, `control_ticks.jsonl` và `routing_state.jsonl` theo interfaces v2; bảng nguồn lưu riêng để không mất trace. `predictions.jsonl` vẫn đúng 7 fields/row. Frozen artifacts gồm `ml_config.yaml`/model metadata với L*/H*/epsilon* và hash; raw ML forecast nếu cần đánh giá được lưu riêng trong artifact ML.

`validated_tables.parquet` là tên minh họa; thực tế tách mỗi bảng một file hoặc dùng DuckDB theo Data. Raw append-only, đóng file xong tính SHA-256; không sửa/xóa run bất lợi. Clean/export có pipeline version và input hash, sinh lại được; mỗi lần recompute có analysis_version, không ghi đè mất kết quả cũ. Raw packet có thể nặng; dung lượng và nơi sao lưu cần chọn trước chạy, không tự giảm sampling làm mất khả năng tính loss.

run_manifest có ít nhất experiment/run/schedule/scenario/method, các seed, parent_family/split, config/topology/schema/model/calibration/code/env version/hash, frozen ML config reference/hash, order/repeat/attempt, `run_started_at_utc`, `traffic_started_at_utc`, `measurement_started_at_utc`, mốc measure/drain end, status, quality_flags, inclusion/exclusion/reason, supersedes_run_id và danh sách file/hash. Đồng bộ `run_id` trong tên thư mục và mọi log; Data đọc H* qua manifest/config, không yêu cầu ML gửi lại.

Bàn giao hiện tại: bốn tài liệu testing đã đồng bộ với guide v1.4. Bàn giao khi chạy xong: test_results/defects/acceptance_record, calibration, manifests/hash, raw logs, run metrics/paired CI/figures và báo cáo giới hạn. Không xem việc có đủ tài liệu thiết kế là đã nghiệm thu các module.

## 13. Truy vết thay đổi và điểm cần owner xác nhận

Bản cập nhật v1.4 sửa namespace sang Network hex / Data path IDs / Network directed link IDs; giao SDN tạo snapshot, Data chỉ lưu/cấp; ML tự chọn config offline và chỉ gửi 7 fields; Data kiểm48×H* từ frozen config, Optimization tự mean. Đồng bộ warm-up60s, forecast cuối run và gatesG0–G8. **Split70/15/15 đã được người dùng xác nhận**, 20 seeds→14/3/3; guide và bộ Testing dùng cùng tỷ lệ. Danh sách seed và phạm vi theo scenario/cấu hình vẫn cần khóa trong `split_manifest` / `benchmark_matrix`. Giữ metric/cohort/raw/paired analysis và các mã test còn đúng.

Decision log: conflict_id,chosen_contract,source_sections,owner_names,confirmed_at_utc,protocol_version,config_hash,evidence_refs,status. Quyết định đã nêu xác nhận trong v1.4 ghi nguồn guide; tỷ lệ split ghi căn cứ người dùng xác nhận70/15/15 trong phiên làm việc. Không tự điền tên/ngày người ký; evidence triển khai vẫn PENDING/NOT_RUN. Transport, kênh demand tới SDN, danh sách seed/phạm vi ma trận, ngưỡng pilot, fallback toàn batch và chi tiết version/restart còn MUST_LOCK/PROPOSED. Owner cung cấp schema/fixtures/mã để QA kiểm, không dùng cập nhật tài liệu để tuyên bố hệ thống đã hết mọi lỗi runtime.
