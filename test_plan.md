# Kế hoạch kiểm thử và thực nghiệm SDN

## 1. Tổng quan đề tài và nguồn căn cứ

Hệ thống dùng lịch sử nhu cầu UDP để dự báo nhu cầu trung bình ngắn hạn của từng cặp host nguồn–đích. MILP chọn đường qua một trong hai switch trung gian; Ryu cài và xác minh Flow Rule. Câu hỏi nghiên cứu là: khi giữ nguyên bộ tối ưu và cơ chế thực thi, Random Forest có giúp hiệu năng mạng tốt hơn nhu cầu hiện tại hoặc trung bình trượt, và phải trả bao nhiêu chi phí đổi đường?

Mạng chính có 8 host, 4 switch biên e1–e4 và 2 switch trung gian c1,c2. e1 nối h1,h2; e2 nối h3,h4; e3 nối h5,h6; e4 nối h7,h8. Mỗi switch biên nối cả hai trung gian. Có 8 kết nối fabric hai chiều = 16 link fabric có hướng, thêm 16 link host có hướng. Tập F đầy đủ gồm 48 cặp host có hướng khác switch biên, mỗi nhóm có hai đường ngắn qua c1 hoặc c2. Chiều ngược là nhóm riêng. Một nhóm chỉ chọn một đường ở trạng thái ổn định.

Ngoại tuyến: Network sinh lịch → Data chia tập trước khi tạo cửa sổ → ML học/chọn mô hình bằng train/validation. Vòng kín: snapshot → demand-provider → trigger → MILP → kiểm tra độc lập → controller apply/verify → state/history → tính metric riêng từng run. Testing tổng hợp giao diện, kiểm tra tính tương thích, chất lượng bằng chứng và tính công bằng của thực nghiệm.

Ngoài phạm vi chính: RL, chống tấn công, phục hồi lỗi link/switch vật lý, đa controller, chia một nhóm trên nhiều đường, TCP và Fat-tree. Đứt phản hồi kênh điều khiển được thử như lỗi thực thi, không biến thành đề tài phục hồi mạng vật lý.

## 2. Các điểm chưa thống nhất và quy tắc xử lý

| Mã | Quan sát từ tài liệu | Quy ước đề xuất của QA | Điều kiện đóng |
| --- | --- | --- | --- |
| D01 | Đề cương tích hợp tr.7 và Optimization dùng N=2,h=0,05; bảng tr.57 dùng N=3,h=0,10 | Lấy phần tích hợp làm baseline thiết kế: N=2,h=0,05; cả hai còn là cấu hình khảo sát | Optimization và SDN xác nhận một config chung trước benchmark |
| D02 | Phần tích hợp dùng J=U+lambda*n_changes/\|F\|; phần sau có α,w_f,γ | Hợp đồng lõi dùng J chuẩn hóa ở phần tích hợp; không trộn hai hàm mục tiêu | Optimization bàn giao công thức, fixture và config hash |
| D03 | Bảng phân công nói H bước, PDF dùng H giây | H_s là giây; n_horizon_steps=H_s/Δ_s khi chia hết; không mặc định vector nhiều bước | ML/Data xác nhận nhãn trung bình một khoảng và response |
| D04 | Data.md là schema nháp rút gọn; PDF có batch/state/history, thời điểm sẵn dùng | Giữ tên bảng Data.md qua adapter; bổ sung trường/bảng trong interfaces | Data và producer chạy round-trip mẫu |
| D05 | ε trong phần sau là biên dự phòng nhu cầu; Optimization cũng dùng epsilon gần 0 | Tách `numeric_epsilon` và `demand_margin_ratio`; margin lõi=0, chỉ khảo sát riêng khi đã đặc tả | ML/Optimization thống nhất tên, vị trí áp dụng và hash |
| D06 | Chưa có trị số deadline packet, drain, forecast/apply timeout, headroom và dung sai S0 đã đo | Đánh dấu MUST_LOCK; xác định trên pilot/validation, không tự gán chuẩn 5% hay CPU 80% | Hồ sơ calibration và config khóa đủ trường |
| D07 | Data.md chưa đủ raw packet/probe để tính loss/RTT chính xác | Network cung cấp log packet, probe và epoch; SDN cung cấp raw counter và verify | Fixture liên module tính lại được bằng tay |

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
| M1 Current/Persistence | Giá trị nhu cầu hợp lệ gần nhất đã available tại t, giữ cho [t,t+H) | Cùng core với M2–M4 | Đối chứng chính; không dùng goodput/counter thay nhu cầu |
| M2 Moving Average | Trung bình theo thời lượng W giây lịch sử hợp lệ; W≤L, khóa trước test | Cùng core | Không phải giá trị cuối; không dùng centered rolling |
| M3 Random Forest | Dự báo mô hình khóa; fallback persistence còn đủ mới khi lỗi | Cùng core | Giữ method=M3 khi fallback, ghi source/reason từng nhóm |
| M4 Oracle | Trung bình requested_mbps thật từ lịch trong [t,t+H) | Cùng core, gồm hold-down/B và apply latency | Chỉ tham chiếu thông tin tương lai; không là cận trên toàn cục |

## 5. Ma trận thực nghiệm

| Mức | Kịch bản/phương pháp | Số lượt dự kiến | Mục đích |
| --- | --- | --- | --- |
| Gating | S0 và S1-low; một rồi nhiều nhóm; hai nhánh | Theo kế hoạch calibration | Kiểm tra phép đo và hoạt động, không đưa vào 72 lượt |
| Cốt lõi | S1-high, S2b chu kỳ, S3 hotspot không nghẽn host, S4 burst × M1,M2,M3 × 6 lịch/seed | 72 | Phép so sánh xác nhận chính |
| Tham chiếu đề xuất | S1-high và S3 × M0,M4 × cùng 6 lịch test ở cốt lõi | 24 bổ sung | M0–M4 trên hai cấu hình; chọn trước khi xem test |
| Chèn lỗi ưu tiên | 12 lỗi 01,03,05,06,07,08,09,10,13,14,15,16 × 3 seed | 36 lượt lỗi, cộng đối chứng sạch khi cần | Bộ riêng, không trộn metric benchmark sạch |
| Mở rộng | S2a ramp, S5 hỗn hợp, ablation, sensitivity, Fat-tree | Khóa kế hoạch riêng | Không là điều kiện bắt buộc của 72 lượt |

S1-high phải có bằng chứng khả năng phân bổ và headroom host, không chỉ rho<1. Tải danh nghĩa rho=sum requested/80 không chứng minh khả thi. S3 giới hạn tổng vào/ra từng host sau quy đổi overhead; stress quá tải host phải gắn nhãn riêng. S4 giữ burst thật, không lọc như outlier. Mức rho cụ thể lấy từ pilot; S1 rho=0,8 ở phần sau PDF là ứng viên, không tự áp cứng nếu overhead/headroom không đạt.

Một lượt: warm-up≥L_max+H_max (tham chiếu 70 s), đo chính 360 s, drain/reset theo pilot. 72×430 s=8,6 giờ chỉ cho warm-up+đo; 96 lượt tương ứng khoảng 11,47 giờ trước drain/reset/lỗi. Dự toán thời gian thực phải cập nhật từ pilot. Không chạy đồng thời nhiều Mininet trên cùng máy benchmark.

## 6. Cấu hình khóa và quản lý seed

### 6.1. Cấu hình

Giá trị khởi đầu theo bản tích hợp: Δ=T_ctl=1 s; link 10 Mbps; trễ host–biên 1 ms/chiều, fabric 5 ms/chiều; queue 50 packet; payload 1200 byte; không chèn loss ngẫu nhiên; K=2; TH=0,85; N=2; h=0,05; T_hold=10 s; B=4; lambda=0,1; solver timeout 0,2 s; T_stale=2Δ. L∈{10,30,60}s,H∈{1,3,5,10}s chọn bằng validation. Những giá trị này không phải SLA đã được xác nhận.

Trước test, lock config gồm L,H,W, feature/model/hash, tham số điều phối, solver/tolerance, numeric_epsilon, overhead k_e, snapshot age rule, policy missing/stale, timeouts, apply guard, packet deadline/drain, probe interval, rebin rule, calibration tolerance, headroom, quantile và rule loại run. Trường MUST_LOCK chưa có trị số làm benchmark BLOCKED. Chỉ ghi `locked=true` sau xác nhận của các owner; lưu người/thời điểm/hash.

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

Quy tắc split: nhóm theo schedule_id + parent_family_id trước khi cắt X/y; mọi replay cùng split; hash nội dung và fingerprint chuỗi tải chuẩn hóa không trùng khác split. Fingerprint bỏ ID/timestamp tạo file để bắt lịch sao chép đổi tên. Tỷ lệ 70/15/15 chỉ gợi ý; số lịch từng split chưa được tài liệu chốt, cần manifest thật. Train/validation/test cần đủ họ kịch bản theo thiết kế.

Thứ tự M1–M3 đề xuất cho 6 lịch: 123,231,312,132,321,213; gán permutation của các thứ tự bằng order_seed trước test, lưu run_order.csv. Trong block 5 phương pháp M0–M4 ở S1-high/S3, tạo thứ tự cân bằng gần đúng bằng order_seed và lưu sai lệch số vị trí; mỗi method xuất hiện một lần mỗi block. Không tính hai lần M1–M3 ở ma trận bổ sung. Lặp cùng lịch để đo nhiễu máy không làm tăng số lịch độc lập.

## 7. Quy trình chạy một lượt

1. QA kiểm tra gate G1–G4, config/model/split đã khóa; tạo manifest và run_id duy nhất trước khi khởi chạy. Ghi method, schedule/hash, repeat/attempt và order.
2. Dừng sender/receiver/controller/Mininet cũ; dọn namespace/rule/tc theo script. Dựng topology sạch; kiểm tra 6 OpenFlow connections, DPID/port/IP/MAC/ARP và config hash.
3. Nạp model một lần với M3; khởi receiver, collector/probe. Cài và xác minh mapping ban đầu cho F, state/history khởi tạo. `last_successful_change_at` ban đầu đủ sớm để không khóa hold-down ngoài ý muốn. Chốt mốc UTC–monotonic.
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
| MET12 | MAE=mean(abs(y−ŷ)); RMSE=sqrt(mean((y−ŷ)^2)), Mbps | Cùng H/group/target/sample; theo kịch bản/pha và coverage; không dùng MAPE chính |
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
| TC-M01 | AC-M01/M02 | Lịch sử 2,3,4; target thật 5; W=3Δ | M1=4, M2=3; label=5 nếu H=Δ và alignment đúng |
| TC-M02 | AC-M03 | Sai feature order/model hash/H; RF NaN/âm/>capacity | Sai hợp đồng bị chặn; >capacity hữu hạn không tự coi lỗi |
| TC-M03 | AC-M04 | RF exception/timeout/thiếu L; persistence mới/cũ | Fallback có log hoặc giữ toàn chu kỳ; method=M3 |
| TC-O01 | AC-O01 | Dãy U=[TH,TH+δ,TH+δ], N=2 | Strict >; trigger chỉ sau 2 tick hợp lệ; stale/commit reset |
| TC-O02 | AC-O02/O03 | Toy 2 nhóm 6 Mbps cùng c1, k=1,C=10 | U_old=1,2,U_new=0,6,J_new=0,65 khi F=2,lambda=0,1; xác minh enumeration |
| TC-O03 | AC-O02/O04 | B=0, nhóm trong/đúng biên hold-down, toàn zero, overload | Không đổi khi khóa/zero; biên đủ T_hold được xét; overload không tự infeasible |
| TC-O04 | AC-O03 | J_new ngay dưới/bằng/trên (1−h)J_old; epsilon | Chấp nhận biên theo tolerance khóa; h áp lên J, không chỉ U |
| TC-O05 | AC-O05/S02 | Timeout có/không incumbent; state đổi/snapshot hết hạn | Chỉ dùng incumbent qua validator và còn hạn; stale bị chặn |
| TC-S01 | AC-S01/S03 | Đổi một nhóm, probe 0 demand, chiều ngược | Rule dump+probe xác minh; nhóm ngoài batch/chiều ngược không bị đổi |
| TC-S02 | AC-S04 | Error/Barrier-only, reject FlowMod, mất reply, bỏ một update | Không APPLIED giả; partial/rollback/UNKNOWN phản ánh thực |
| TC-S03 | AC-S02/S05 | Hai apply cùng version, solver treo worker | Không overlap; stale result bị reject; event loop vẫn xử lý health |
| TC-E01 | AC-E01/E02 | Fixture 100/90 packets và duplicate/late/warm-up | Đúng metric mục 8; N/A mẫu số 0; recompute từ raw |
| TC-E02 | AC-E03 | Ma trận replay/order và manifest seed/config | M1–M3 chỉ đổi provider; từng run có log đo riêng |
| TC-E03 | AC-E04 | Ghép bảng theo schedule_id, fault run, run lỗi infra | Không ghép chéo seed; loại có lý do; giữ fallback/timeout |
| TC-I01 | AC-I01 | Một lịch end-to-end qua tất cả interfaces | Truy vết schedule→run→snapshot→forecast→batch→verify→metric |

Mỗi case có test record gồm case_id, requirement_id, owner, input/hash, expected, actual, status, evidence_uri, defect_id, git/config hash và thời điểm. Trạng thái thiết kế hiện tại là NOT_RUN; không điền PASS nếu chưa có actual/evidence. Fault dùng bảng mapping tương ứng phụ lục PDF; TC-D05 và TC-O03 vẫn bắt buộc kiểm tra logic dù không thuộc 12 fault lượt ưu tiên.

## 10. Gate, lỗi và quy tắc loại run

G1: hợp đồng/config/ID và D01–D07 đóng → G2: Network/SDN S0 và log packet/counter đúng → G3: split/X-y/model và baseline đúng → G4: MILP fixtures + apply/rollback/fault tích hợp đạt → G5: freeze benchmark → G6: recompute/paired analysis và bàn giao. QA chỉ mở gate khi có evidence; người phụ trách module xác nhận phần mình.

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

Mỗi lịch/seed độc lập là đơn vị tái lấy mẫu; repeated attempts/repeats cùng lịch không là mẫu độc lập. Bootstrap ghép cặp đề xuất B_boot=10.000, CI percentile 95%, seed riêng, lấy lại cả cặp trong từng kịch bản. Nếu có nhiều repeat đã định trước, tổng hợp theo lịch trước bootstrap. Với 6 lịch, CI còn bất định; không dùng 360 giây như n=360. Không gộp scenario thành một số chính tùy ý; summary phụ dùng trọng số bằng nhau đã định trước.

Không bắt buộc kiểm định p-value. Nếu dùng, khóa giả thuyết và họ kiểm định trước; ví dụ 4 scenario×2 đối chứng×3 endpoint hiệu quả=24 phép, hiệu chỉnh Holm trong họ đó; chi phí báo mô tả trừ khi có họ riêng. Kiểm định đổi dấu cần giả định thích hợp, không gọi exact vô điều kiện. CI chính không tự được coi đã hiệu chỉnh đa so sánh.

Không đặt trước M3 phải tăng X%. Ngưỡng lợi ích thực tiễn nếu dùng phải chọn từ nhiễu pilot và nhu cầu nghiên cứu trước test. Kết luận theo kịch bản: cải thiện có bằng chứng, đánh đổi, chưa đủ bằng chứng, hoặc kém hơn. CI chứa 0 không chứng minh hai phương pháp bằng nhau. Oracle không chứng minh tối ưu toàn cục; MAE thấp không chứng minh mạng tốt hơn.

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

`validated_tables.parquet` là tên minh họa; thực tế tách mỗi bảng một file hoặc dùng DuckDB theo Data. Raw append-only, đóng file xong tính SHA-256; không sửa/xóa run bất lợi. Clean/export có pipeline version và input hash, sinh lại được; mỗi lần recompute có analysis_version, không ghi đè mất kết quả cũ. Raw packet có thể nặng; dung lượng và nơi sao lưu cần chọn trước chạy, không tự giảm sampling làm mất khả năng tính loss.

run_manifest có ít nhất experiment/run/schedule/scenario/method, các seed, parent_family/split, config/topology/model/code/env/hash, order/repeat/attempt, mốc warm-up/measure/drain, status, quality_flags, inclusion/exclusion/reason, supersedes_run_id và danh sách file/hash. Đồng bộ `run_id` trong tên thư mục và mọi log.

Bàn giao hiện tại: ba tài liệu thiết kế, checklist acceptance và hợp đồng đề xuất. Bàn giao khi chạy xong: test_results/defects/acceptance_record, calibration, manifests/hash, raw logs, run metrics/paired CI/figures và báo cáo giới hạn. Không xem việc có đủ ba file thiết kế là đã nghiệm thu các module.
