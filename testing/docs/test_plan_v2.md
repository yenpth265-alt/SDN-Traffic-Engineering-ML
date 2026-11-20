# Kế hoạch kiểm thử và thực nghiệm SDN

- Phiên bản `testing_v2 2026-11-10`.
- Phụ trách Testing/QA/QC.
- Trạng thái: hoàn thiện đặc tả để owner xác nhận, chưa nghiệm thu triển khai.

Bộ tài liệu đồng bộ: `test_plan_v2.md`, `interfaces_v2.md`, `acceptance_criteria_v2.md`. Mọi quy ước chọn trong bộ này là đề xuất tích hợp; chưa có biên bản owner xác nhận hoặc kết quả chạy kèm theo. Không suy ra tỷ lệ hoàn thành lập trình từ số lượng file thiết kế.

## 1. Tổng quan đề tài và nguồn căn cứ

Hệ thống dùng lịch sử nhu cầu UDP để dự báo nhu cầu trung bình ngắn hạn của từng cặp host nguồn–đích. MILP chọn đường qua một trong hai switch trung gian; Ryu cài và xác minh Flow Rule. Câu hỏi nghiên cứu là: khi giữ nguyên bộ tối ưu và cơ chế thực thi, Random Forest cùng calibration đã khóa có giúp hiệu năng mạng tốt hơn nhu cầu hiện tại hoặc trung bình trượt, và phải trả bao nhiêu chi phí đổi đường?

Mạng chính có 8 host, 4 switch biên e1–e4 và 2 switch trung gian c1,c2. e1 nối h1,h2; e2 nối h3,h4; e3 nối h5,h6; e4 nối h7,h8. Mỗi switch biên nối cả hai trung gian. Có 8 kết nối fabric hai chiều = 16 link fabric có hướng, thêm 16 link host có hướng. Tập F đầy đủ gồm 48 cặp host có hướng khác switch biên, mỗi nhóm có hai đường ngắn qua c1 hoặc c2. Chiều ngược là nhóm riêng. Một nhóm chỉ chọn một đường ở trạng thái ổn định.

Ngoại tuyến: Network sinh lịch → Data chia tập trước khi tạo cửa sổ → ML học/chọn mô hình bằng train/validation. Vòng kín: snapshot → demand-provider → trigger → MILP → kiểm tra độc lập → controller apply/verify → state/history → tính metric riêng từng run. Testing tổng hợp giao diện, kiểm tra tính tương thích, chất lượng bằng chứng và tính công bằng của thực nghiệm.

Ngoài phạm vi chính: RL, chống tấn công, phục hồi lỗi link/switch vật lý, đa controller, chia một nhóm trên nhiều đường, TCP và Fat-tree. Đứt phản hồi kênh điều khiển được thử như lỗi thực thi, không biến thành đề tài phục hồi mạng vật lý.

## 2. Rà soát tiến độ thiết kế và xung đột

### 2.1. Nguồn và giới hạn kết luận

Rà soát là nguồn tham khảo phân tích, không phải biên bản chốt. Một số ví dụ trong báo cáo phản ánh bản trước: SDN hiện đã dùng h1_h3; ML hiện dùng hex, đã quy định H dòng và Optimization tự tính mean. Data(2).md tự ghi schema_v1, không phải “Data_v2” mà ML dẫn. Không dùng tên phiên bản được dẫn lại thay cho schema thực nhận. Không có source code, run log, model hay kết quả thực nghiệm trong bộ này; trạng thái triển khai là CHƯA XÁC MINH.

| Role | Bằng chứng đã có về thiết kế | Phần cần bàn giao để xác minh tiến độ tiếp theo |
| --- | --- | --- |
| Network | Topology/DPID/port, 48 nhóm/96 đường, S0–S5, seed và thời lượng | config/CSV thực, MAC/ARP/initial paths, generator/sender/receiver/runner và S0 logs |
| Data | Schema v1, raw→clean→db→export, split, snapshot provider dự kiến | Migration H-step PK, X/y_vector, snapshot/forecast completeness, state/batch ETL và fixtures |
| ML | RF multi-output, baselines, L/H/epsilon, 7-field prediction rows | Chuẩn ID, model/metadata, train/validation kết quả, batch predict và latency thực |
| Optimization | MILP, trigger/hysteresis/hold/B, output JSON nháp | R4 chính xác, epsilon policy, ApplyRequest adapter, recompute U/J và solver tests |
| SDN | Ba đặc tả khá đầy đủ; đã chuẩn hóa ID, version, verify/partial | Mã Ryu, transport/timeouts, output counter/state thật và P1↔P2 evidence |
| Testing | Đã có kế hoạch/tiêu chí/interfaces | Bản v2 này sửa yêu cầu lỗi thời; tiếp theo là fixture, thực thi case, defect và biên bản |

### 2.2. Sổ xung đột và quyết định tích hợp đề xuất

P0: có thể sai dữ liệu/kết quả hoặc chặn tích hợp. P1: ảnh hưởng tái lập, phép đo hoặc công bằng thực nghiệm. Mỗi dòng hiện OPEN, owner sign-off PENDING. “Đề xuất” chưa có nghĩa thành viên đã sửa file hoặc đồng ý.

| ID / mức | Căn cứ và ảnh hưởng | Đề xuất dùng trong testing v2 | Owner / điều kiện đóng |
| --- | --- | --- | --- |
| C01/P0 | Network §5/Phụ lục dùng cookie hex; ML §2.1 dùng hex làm ID; Optimization §V dùng h1-h5; Data §3 dùng h1_h3; SDN đã chuẩn hóa | h1_h3 / h1_h3_c1 / e1_to_c1; hex là flow_cookie; P1 là path_label | Network chủ trì, tất cả consumer; TC-I01/TC-N01 round-trip |
| C02/P0 | Data pipeline mô tả y scalar; ML §2,6 là H bước; testing cũ cũng ép scalar | Data cấp y_vector và y_mean; giữ H dự báo, Optimization tính mean; không ép ML thêm cột mean | Data+ML+Optimization; TC-D04/TC-M01/TC-D06 |
| C03/P0 | demand_plan không có available_at; lịch biết trước, snapshot chưa có chứng cứ hiện thực | Online release ledger/buffer chỉ lịch sử đã khép và sẵn có; M4 tách quyền | Network+Data+ML; TC-D04/TC-I02 |
| C04/P0 | Optimization §V batch_id/decisions/P1/target_interval khác SDN §4 | Adapter thành decision_batch_id/changes/full path IDs/config_version/forecast UTC | Optimization+SDN; TC-I01/TC-S03 |
| C05/P0 | Data decisions thiếu batch/actual/version; SDN source tables phong phú hơn | Giữ bảng báo cáo, thêm source tables batch/per-group/state/history và trace forecast | Data+SDN; TC-D07/TC-S02 |
| C06/P0 | Data offsets theo run, ML theo measurement; SDN UTC; Rà soát §4 C06 gợi ý một origin | Giữ hai origin explicit: traffic_started_at và measurement_started_at; adapter quy đổi từng field, cấm đoán | Network+Data+ML+SDN; TC-I03 |
| C07/P0 | ML §6.5 đã hiệu chỉnh epsilon; Optimization R2 vẫn nhân (1+epsilon) | Theo ML hiện nhận: epsilon chỉ áp tại ML, Optimization k_e×mean(final); raw/adjusted metadata; fixture 4→4,4 | ML+Optimization+Data; TC-M04. Rà soát đề xuất opt-owned là phương án thay thế, không bật cả hai |
| C08/P0 | Optimization R4 chỉ ≥; λ=0 có thể báo đổi giả; ví dụ J_new=.61 không khớp | y=1−x_current; J=U+λ n_changes/48; sửa ví dụ theo số đổi thật | Optimization; TC-O02/O03/O04 |
| C09/P1 | Optimization N=3, TH/h có 2 lựa chọn; testing cũ/Rà soát khuyến nghị N=2 | Baseline đề xuất N=3,TH=.85,h=.05; các giá trị khác là sweep; một config/hash | Optimization+QA; TC-O01; sign-off trước pilot |
| C10/P1 | Network 30s warm-up/20 seed chia10/5/5; ML L tới60; Data gợi ý70/15/15; testing cũ72 lượt và S5 phụ | Warm-up70s đề xuất, ≥L và đủ availability; split10/5/5 theo manifest; S5 chính; ma trận mục5 | Network+Data+ML+QA; TC-E02 |
| C11/P1 | S0 Network đòi RTT24±5%, CPU<80%, phát10Mbps; k_e chưa hiệu chuẩn | Giữ làm mục tiêu pilot; đo payload/overhead/qdisc; ngưỡng khóa có điều kiện, không hard-code goodput10 | Network+SDN+Data+Optimization; TC-N03/D05 |
| C12/P1 | Thiếu MAC/ARP policy và initial_paths file; cùng-edge ngoài F | Một initial map/hash chung, static ARP hoặc handler kiểm soát; same-edge test riêng | Network+SDN; TC-N01/S01 |
| C13/P0 | T_ctl1s nhưng chưa có transport/latency budget; APPLIED cần verify | Một batch flight, timeout hữu hạn, check expiry trước write; quá hạn sau write vẫn reconcile thật | ML+Optimization+SDN+Data; TC-S02/S03 |
| C14/P1 | Phạm vi M0/M4 và chi phí partial chưa đủ; Rà soát gọi M4 upper bound | M0 tĩnh, M4 oracle tham chiếu không phải cận trên bảo đảm; switching theo verified transition | QA+Data+SDN; TC-E01/E02/E03 |
| C15/P0 bổ sung | Data §7.1 PK (run,group,issued_at) va chạm H dòng; ML §6.3 đòi thêm forecast_start | PK thêm forecast_start; batch manifest READY chỉ khi đủ48H dòng, không trộn issue; nguồn từng nhóm theo source map | Data+ML; TC-D06 |
| C16/P0 bổ sung | interfaces testing cũ tăng version theo nhiều transition; SDN tăng một lần cuối batch; history UNKNOWN khác nhau | Global version tối đa+1/final batch; UNKNOWN là khoảng gap có log, không fake path; response khớp DB | SDN+Data+Optimization; TC-D07/S02 |

Rà soát C03 đánh chỉ số “đến t / từ t+1”; bản ML hiện tại định nghĩa d[k] cho [k,k+1), X=[t−L,t), Y=[t,t+H). Testing dùng định nghĩa khoảng của ML, kiểm cả biên t để tránh lệch một giây. Rà soát C07 cho phép hai cách sở hữu epsilon; bản này chọn ML-owned để giữ pipeline ML mới nhất. Nếu nhóm chọn opt-owned phải đổi đồng bộ ML/metadata/R2/fixtures và tăng protocol version trước test. Không mở cả hai như hai default.

### 2.3. Thứ tự xử lý

1. Đóng C01/C02/C06/C07/C15: danh mục ID, X–y_vector–mean, origin, epsilon và prediction batch.
2. Đóng C03/C04/C05/C08/C13/C16: snapshot causal, adapter ApplyRequest, state và MILP fixtures.
3. Chốt C09–C12/C14 và các ngưỡng calibration; chạy smoke một flow rồi đủ48 nhóm.
4. Freeze G5 và chạy ma trận benchmark. QA kiểm evidence, owner sửa tài liệu/mã của mình; bản này không thay mặt ký xác nhận.

## 3. Phân công trách nhiệm

| Role | Chịu trách nhiệm đầu ra | QA kiểm chứng/bàn giao nhận |
| --- | --- | --- |
| Network | Topology, DPID/port mapping cùng SDN, lịch, UDP sender/receiver, S0 | Hai đường, 48 nhóm, tính tái lập lịch, pacing, packet log |
| Data | Schema, split, X/y, snapshot, raw→clean→metric | Thời gian, chống leakage, hash, NULL, nạp lặp và tính lại |
| ML/AI | Feature/label, model manifest, predict_batch, baseline dự báo | Đúng H/đơn vị/feature, sai số và latency, fallback |
| Optimization | Trigger, MILP, kiểm tra nghiệm, DecisionBatch | R1–R7, J, hold-down/B, timeout/stale |
| SDN | Ryu, cài rule, counter, apply/verify, routing state | Không báo thành công giả, partial/rollback/UNKNOWN, event loop |
| Testing/QA/QC | Test plan, tiêu chí, hợp đồng tổng hợp, case/defect/evidence, kiểm soát seed/run | Kiểm tra độc lập; tổng hợp kết quả; không tự xác nhận thay producer |
| Evaluation (có thể do Testing kiêm nhiệm) | Chạy benchmark, bảng ghép cặp/biểu đồ/kết luận | Đơn vị phân tích theo lịch, quy tắc loại run và giới hạn |

## 4. Đối chứng M0–M4

| Mã | Demand-provider tại t | Tối ưu/trigger | Vai trò và giới hạn |
| --- | --- | --- | --- |
| M0 Static | Không dùng để điều phối | Không chạy tối ưu; giữ mapping ban đầu | Baseline bổ sung; vẫn thu đầy đủ metric; không là cận dưới tuyệt đối |
| M1 Current/Persistence | Giá trị nhu cầu hợp lệ gần nhất đã available tại t, giữ cho [t,t+H) | Cùng core với M2–M4; vector H bước trước mean | Đối chứng chính; không dùng goodput/counter thay nhu cầu |
| M2 Moving Average | Trung bình theo thời lượng W giây lịch sử hợp lệ; W≤L, khóa trước test | Cùng core | Không phải giá trị cuối; không dùng centered rolling |
| M3 Random Forest | Dự báo mô hình và epsilon ML khóa; fallback persistence còn đủ mới khi lỗi | Cùng core | Giữ method=M3 khi fallback, ghi source/reason từng nhóm |
| M4 Oracle | Trung bình requested_mbps thật từ lịch trong [t,t+H) | Cùng core, gồm hold-down/B và apply latency | Chỉ tham chiếu thông tin tương lai; không là cận trên toàn cục |

## 5. Ma trận thực nghiệm đề xuất đồng bộ Network

Đơn vị schedule là (scenario, cấu hình scenario, seed). S0 là calibration riêng. S1 có bốn cấu hình rho; S2a/S2b/S3/S4/S5 mỗi loại một cấu hình: tổng 9 cấu hình. Chọn 20 seed/cấu hình theo Network, chia 10 train/5 validation/5 test bằng manifest của Data. Data 70/15/15 chỉ là gợi ý, không đồng thời áp vào bộ 10/5/5. Seed namespaces giữa split phải rời nhau; mọi biến thể cùng parent family ở cùng split.

| Bộ | Lịch độc lập | Phương pháp | Số lượt / điều kiện |
| --- | --- | --- | --- |
| Offline | 9×20=180 lịch, trong đó90 train/45 validation/45 test | Tạo X/y, học/chọn trên train/validation | Không bắt buộc chạy Mininet cho mỗi lịch offline |
| Pilot tích hợp | S0, S1-low, S3, S5 dùng seed pilot riêng | Smoke M0–M4, từng bước | Không nhập kết quả test; hiệu chuẩn/timing/feasibility |
| Benchmark đầy đủ | 9 cấu hình×5 seed test=45 lịch | M0–M4 cùng mọi lịch | 225 lượt, chưa gồm rerun/drain; S5 là kịch bản báo cáo chính |
| Fault riêng | S3 hoặc S5, 12 loại F01–F12, mỗi loại3 fault seed | M3 + đối chứng sạch cùng lịch khi cần | 36 lượt lỗi; không trộn benchmark sạch |
| Ablation/sensitivity | Validation hoặc tập bổ sung khóa trước | raw RF/epsilon; TH,N,h,B,hold,L/H | Không tuning lại trên45 lịch test |

S1 rho=.4/.6/.8/1.0 giữ theo Network; rho1.0 là stress chạm giới hạn host. Rho=sum requested/80 chỉ mô tả tổng, không chứng minh host/fabric khả thi. S2b các pha độc lập không bảo đảm tổng rho luôn .3–.9; tính lại envelope từ lịch. S3/S4/S5 phải lưu cả demand trước/sau rescale/cap và thuật toán điều chỉnh; cap theo tổng gửi/tổng nhận từng host, không cap riêng từng flow. Với UDP payload, giới hạn10Mbps payload chưa bảo đảm không nghẽn host vì overhead. Gắn nhãn CORE hoặc HOST_STRESS theo calibration; giữ stress trong báo cáo riêng, không loại bất lợi sau test.

Một lượt baseline đề xuất: warm-up70s + measurement360s + drain/reset đã hiệu chuẩn. 225×430s=96.750s≈26,88 giờ trước drain/reset; đây là dự toán, không phải thời lượng đã chạy. Nếu nguồn lực không đủ, khóa một ma trận rút gọn bằng protocol revision trước khi xem test; không âm thầm dùng72 lượt của bản cũ. Không chạy nhiều Mininet benchmark đồng thời trên cùng máy.

## 6. Cấu hình khóa và quản lý seed

### 6.1. Cấu hình

Giá trị khởi đầu đề xuất để owner xác nhận theo tài liệu hiện nhận: Δ=T_ctl=1 s; link 10 Mbps; trễ host–biên 1 ms/chiều, fabric 5 ms/chiều; queue 50 packet; payload 1200 byte; không chèn loss ngẫu nhiên; K=2; TH=0,85; N=3; h=0,05; T_hold=10 s; B=4; lambda=0,1; solver timeout 0,2 s và T_stale=2Δ là đề xuất pilot, chưa được đo. Epsilon_ml∈{0;0,05;0,10;0,20} chọn validation và áp tại ML; epsilon_opt=0 (không nhân thêm). numeric_epsilon là tolerance riêng. L∈{10,30,60}s,H∈{1,3,5,10}s chọn bằng validation. Những giá trị này không phải SLA đã được xác nhận.

Trước test, lock config gồm L,H,W, feature/model/hash, tham số điều phối, solver/tolerance, numeric_epsilon, overhead k_e/calibration_id, epsilon_owner/epsilon_applied/raw forecast refs, snapshot age rule, policy missing/stale, timeouts, apply guard, packet deadline/drain, probe interval, rebin rule, calibration tolerance, headroom, quantile và rule loại run. Trường MUST_LOCK chưa có trị số làm benchmark BLOCKED. Chỉ ghi `locked=true` sau xác nhận của các owner; lưu người/thời điểm/hash.

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

Quy tắc split: nhóm theo schedule_id + parent_family_id trước khi cắt X/y; mọi replay cùng split; hash nội dung và fingerprint chuỗi tải chuẩn hóa không trùng khác split. Fingerprint bỏ ID/timestamp tạo file để bắt lịch sao chép đổi tên. Baseline đề xuất10/5/5 theo Network; manifest phải ghi danh sách seed thật và owner xác nhận, không chỉ tỷ lệ. Train/validation/test cần đủ họ kịch bản theo thiết kế.

Với5 lịch test/cấu hình và5 method, dùng Latin square5×5 cho vị trí chạy, rồi hoán vị hàng/cột theo order_seed và khóa run_order.csv trước test. Mỗi method xuất hiện một lần mỗi vị trí trong mỗi cấu hình; nếu cần cân bằng carry-over dùng repeat bổ sung có kế hoạch. Cùng lịch lặp không tăng số lịch độc lập. Mọi method có run_id/log đo riêng.

## 7. Quy trình chạy một lượt

1. QA kiểm tra gate G1–G4, config/model/split đã khóa; tạo manifest và run_id duy nhất trước khi khởi chạy. Ghi method, schedule/hash, repeat/attempt và order.
2. Dừng sender/receiver/controller/Mininet cũ; dọn namespace/rule/tc theo script. Dựng topology sạch; kiểm tra 6 OpenFlow connections, DPID/port/IP/MAC/ARP và config hash.
3. Nạp model một lần với M3; khởi receiver, collector/probe. Cài và xác minh mapping ban đầu cho F, state/history khởi tạo. `last_successful_change_at` bootstrap=null, hiểu là chưa có lần đổi và không khóa hold-down; không bịa thời điểm đổi trước run. Chốt mốc UTC–monotonic.
4. Replay lịch đầy đủ gồm warm-up+360 s đo. Policy đề xuất: không điều phối trong warm-up cho tất cả phương pháp; vẫn thu lịch sử và log. Khi mở đo, reset trigger, không reset lịch sử; hold-down khởi tạo đồng nhất. Nếu nhóm chọn điều phối trong warm-up phải đổi policy chung trước test và tách các lần đổi warm-up.
5. Trong đo: chạy chu kỳ snapshot/provider/trigger/solve/apply; không có hai batch apply chồng. Thu raw counter, packets, probes, state, quality, CPU/RAM/latency. Ghi mọi tick giữ đường, fallback hoặc quá hạn.
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
| MET12 | MAE=mean(abs(y−ŷ)); RMSE=sqrt(mean((y−ŷ)^2)), Mbps | Chấm vector theo từng h=1..H và pooled; chấm mean-H riêng sau aggregator; raw/adjusted/native/fallback tách; cùng support, không đánh đồng hai loại lỗi |
| MET13 | fallback_groups/eligible_group_forecasts; fallback_ticks/eligible_ticks; invalid/missing/stale rate | Ghi cả tử/mẫu; M3 vẫn là M3; native RF và all-provider report riêng |
| MET14 | CPU/RAM, generator lag/send errors, pending queue | Phân phối và môi trường; không loại run chỉ vì RF chậm |
| MET15 | Ping-pong: quay lại đường trước trong W_pingpong khóa trước | Phụ; tách rollback; cửa sổ MUST_LOCK nếu dùng |

Phân vị dùng quantile nội suy tuyến tính cho tập mẫu thô RTT/latency; MLU sau rebin Δ cố định dùng weighted empirical quantile nếu thời lượng cửa sổ khác nhau. Rebin chỉ dùng offline với quy tắc công bố; online không dùng counter tương lai để nội suy. Khoảng thiếu/reset đánh dấu invalid. Nếu một link fabric thiếu, không tính max của 15 link như MLU đủ 16; metric MLU tại khoảng đó NULL và giảm coverage.

MLU chỉ gọi là utilization egress khi S0 kiểm chứng điểm đếm; counter trước shaping phải ghi offered load, không gắn nhãn utilization. Mỗi chiều lấy một counter chính, không cộng Tx nguồn+Rx đích. Không clip u_e>1, giữ cờ và điều tra. Tải dự kiến U_old/U_new và U_measured là hai loại riêng. MLU thấp kèm loss cao/goodput thấp không chứng minh cân bằng tốt.

Ví dụ fixture: T=1 s, yêu cầu 100 packet×1200 byte, gửi thành công 100, nhận đúng hạn 90 unique và 2 bản trùng → goodput=0,864 Mbps, loss=10%, served=90%, duplicate=2. Packet cuối giây đến ở giây sau nhưng trước deadline vẫn thành công theo cohort.

## 9. Danh mục test và truy vết

| Case | Module/tiêu chí | Input/thao tác | Expected và bằng chứng |
| --- | --- | --- | --- |
| TC-N01 | AC-N01/N02 | Dựng topology, liệt kê paths/ports, ép từng nhánh | Đúng 6 switch/8 host/48 nhóm/2 đường; dump topology/rule/probe |
| TC-N02 | AC-N03 | Sinh lịch hai lần cùng seed/config; đổi seed | Cùng hash khi tái tạo; khác seed không mặc nhiên đủ khác họ; manifest/diff |
| TC-N03 | AC-N04/N05 | S0 pacing thấp/cao, đối chiếu payload/counter/qdisc | Báo sai lệch/headroom/điểm đếm theo calibration, không gán goodput=10 |
| TC-D01 | AC-D01/D02 | NULL, zero, NaN, âm, burst đúng lịch, đơn vị ×1000 | NULL khác 0; invalid có reason; giữ burst; không đoán sửa scale |
| TC-D02 | AC-D03 | Hai bản ghi giống khóa và hai bản mâu thuẫn | Nạp lại idempotent; conflict quarantine/reject, không overwrite tùy ý |
| TC-D03 | AC-D04 | Cùng lịch đổi ID/họ/seed, replay khác method | Không lọt qua split bởi đổi tên; replay cùng split |
| TC-D04 | AC-D05/M01 | Sample late available_at, window vượt t, đổi run/group | Không dùng tương lai, không nối nhóm/run; label đúng H |
| TC-D05 | AC-D06 | Counter reset, reply trễ, thiếu một fabric link | Epoch mới; delta invalid; MLU thiếu→NULL, coverage đúng |
| TC-M01 | AC-M01/M02 | Lịch sử2,3,4; MA window=3s,H=5; RF fixture[2,2.5,3,3.5,3] | M1=[4]×5; M2=[3]×5; RF mean2.8; giữ5 bước và chấm MAE bước/mean riêng |
| TC-M02 | AC-M03 | Sai feature order/model hash/H; RF NaN/âm/>capacity | Sai hợp đồng bị chặn; >capacity hữu hạn không tự coi lỗi |
| TC-M03 | AC-M04 | RF exception/timeout/thiếu L; persistence mới/cũ | Fallback có log hoặc giữ toàn chu kỳ; method=M3 |
| TC-O01 | AC-O01 | Dãy U=[TH,TH+δ,TH+δ,TH+δ], N=3 | Strict >; trigger chỉ sau 3 tick hợp lệ; stale/commit reset |
| TC-O02 | AC-O02/O03 | Toy 2 nhóm 6 Mbps cùng c1, k=1,C=10 | U_old=1,2,U_new=0,6,J_new=0,65 khi F=2,lambda=0,1; xác minh enumeration |
| TC-O03 | AC-O02/O04 | λ=0 và y chính xác; B=0, nhóm trong/đúng biên hold-down, toàn zero, overload | Không đổi khi khóa/zero; biên đủ T_hold được xét; overload không tự infeasible |
| TC-O04 | AC-O03 | J_new ngay dưới/bằng/trên (1−h)J_old; epsilon | Chấp nhận biên theo tolerance khóa; h áp lên J, không chỉ U |
| TC-O05 | AC-O05/S02 | Timeout có/không incumbent; state đổi/snapshot hết hạn | Chỉ dùng incumbent qua validator và còn hạn; stale bị chặn |
| TC-S01 | AC-S01/S03 | Đổi một nhóm, probe 0 demand, chiều ngược | Rule dump+probe xác minh; nhóm ngoài batch/chiều ngược không bị đổi |
| TC-S02 | AC-S04 | Error/Barrier-only, reject FlowMod, mất reply, bỏ một update; verify trễ qua expiry sau write | Không APPLIED giả; partial/rollback/UNKNOWN phản ánh thực |
| TC-S03 | AC-S02/S05 | Hai apply cùng version, solver treo worker; duplicate cùng/khác payload và pending/final | Không overlap; stale result bị reject; event loop vẫn xử lý health |
| TC-E01 | AC-E01/E02 | Fixture 100/90 packets và duplicate/late/warm-up | Đúng metric mục 8; N/A mẫu số 0; recompute từ raw |
| TC-E02 | AC-E03 | Ma trận replay/order và manifest seed/config | M0–M4 cùng core; M1–M3 chỉ đổi provider/calibration khai báo; từng run có log đo riêng |
| TC-E03 | AC-E04 | Ghép bảng theo schedule_id, fault run, run lỗi infra | Không ghép chéo seed; loại có lý do; giữ fallback/timeout |
| TC-D06 | AC-D08/M07 | Cùng issued_at insert48H rows; thiếu1/dup/conflict/mixed issue/nguồn không khai báo | Không va PK; chưa đủ→NOT_READY; đọc atomic một batch; mean tính từ đúng H bước |
| TC-D07 | AC-D09/S07 | Batch3 nhóm APPLIED/ROLLED_BACK/UNKNOWN; ingest twice | actual/status/response/state version khớp; UNKNOWN=null; history không chồng |
| TC-M04 | AC-M08 | raw4,epsilon=.1,k=1; đưa qua ML→Data→Optimization | adjusted4.4,b_used4.4; không4.84; mean(raw vector) và mean(final) truy vết |
| TC-I02 | AC-D05/M03 | Giữ lịch sử không đổi, sửa mạnh tương lai trong schedule ẩn; chèn late history | M1–M3 output bất biến với tương lai; M4 có thể đổi; late không vào X |
| TC-I03 | AC-I03 | traffic10:00:00Z,measurement10:01:10Z,t=100,H=5 | DB forecast100..105→UTC10:02:50..55Z; schedule bin170..175; round-trip không lệch |
| TC-I01 | AC-I01 | Một lịch end-to-end qua tất cả interfaces | Truy vết schedule→run→snapshot→forecast→batch→verify→metric |

Mỗi case có test record gồm case_id, requirement_id, owner, input/hash, expected, actual, status, evidence_uri, defect_id, git/config hash và thời điểm. Trạng thái thiết kế hiện tại là NOT_RUN; không điền PASS nếu chưa có actual/evidence. Fault dùng F01–F12 của acceptance_criteria.md mục8; không gán số lỗi từ PDF chưa được cung cấp để đối chiếu; TC-D05 và TC-O03 vẫn bắt buộc kiểm tra logic dù không thuộc 12 fault lượt ưu tiên.

## 10. Gate, lỗi và quy tắc loại run

G1: các contract P0 trong C01–C16 và cấu hình smoke được owner xác nhận → G2: Network/SDN S0 và log packet/counter đúng → G3: split/X-y/model và baseline đúng → G4: MILP fixtures + apply/rollback/fault tích hợp đạt → G5: đóng toàn bộ C01–C16, khóa ngưỡng/ma trận benchmark → G6: recompute/paired analysis và bàn giao. QA chỉ mở gate khi có evidence; người phụ trách module xác nhận phần mình.

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

Mỗi lịch/seed độc lập là đơn vị tái lấy mẫu; repeated attempts/repeats cùng lịch không là mẫu độc lập. Bootstrap ghép cặp đề xuất B_boot=10.000, CI percentile 95%, seed riêng, lấy lại cả cặp trong từng kịch bản. Nếu có nhiều repeat đã định trước, tổng hợp theo lịch trước bootstrap. Với5 lịch mỗi cấu hình, CI còn bất định; không dùng 360 giây như n=360. Không gộp scenario thành một số chính tùy ý; summary phụ dùng trọng số bằng nhau đã định trước.

Không bắt buộc kiểm định p-value. Nếu dùng, khóa giả thuyết và họ kiểm định trước; ví dụ 4 scenario×2 đối chứng×3 endpoint hiệu quả=24 phép, hiệu chỉnh Holm trong họ đó; chi phí báo mô tả trừ khi có họ riêng. Kiểm định đổi dấu cần giả định thích hợp, không gọi exact vô điều kiện. CI chính không tự được coi đã hiệu chỉnh đa so sánh.

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

Raw cần bổ sung forecast_batches.jsonl, decision_batches.jsonl, control_ticks.jsonl và routing_state.jsonl theo interfaces v2; bảng nguồn lưu riêng để không mất trace.

`validated_tables.parquet` là tên minh họa; thực tế tách mỗi bảng một file hoặc dùng DuckDB theo Data. Raw append-only, đóng file xong tính SHA-256; không sửa/xóa run bất lợi. Clean/export có pipeline version và input hash, sinh lại được; mỗi lần recompute có analysis_version, không ghi đè mất kết quả cũ. Raw packet có thể nặng; dung lượng và nơi sao lưu cần chọn trước chạy, không tự giảm sampling làm mất khả năng tính loss.

run_manifest có ít nhất experiment/run/schedule/scenario/method, các seed, parent_family/split, config/topology/model/code/env/hash, order/repeat/attempt, mốc warm-up/measure/drain, status, quality_flags, inclusion/exclusion/reason, supersedes_run_id và danh sách file/hash. Đồng bộ `run_id` trong tên thư mục và mọi log.

Bàn giao hiện tại: ba tài liệu thiết kế, checklist acceptance và hợp đồng đề xuất. Bàn giao khi chạy xong: test_results/defects/acceptance_record, calibration, manifests/hash, raw logs, run metrics/paired CI/figures và báo cáo giới hạn. Không xem việc có đủ ba file thiết kế là đã nghiệm thu các module.

## 13. Truy vết thay đổi và điểm cần owner xác nhận

Bản v2 thay các giả định lỗi thời: N2→N3 đề xuất; scalar-only→vector H bước/mean;72 lượt→225 lượt đề xuất theo Network; S5 mở rộng→chính; path/link IDs theo SDN; version nhiều lần→tối đa1 lần/batch. Loại tham chiếu trang PDF chưa kiểm tra. Giữ metric/cohort/raw/paired analysis còn đúng; sửa acceptance và interfaces tương ứng.

Decision log khi họp: conflict_id,chosen_contract,source_sections,owner_names,confirmed_at_utc,protocol_version,config_hash,evidence_refs,status. Hiện owner_names/confirmed_at để trống; status=OPEN. Kế tiếp cần owner sửa source docs và cung cấp fixtures trước code tích hợp. Không yêu cầu có chữ ký giả để hoàn tất thiết kế testing.
