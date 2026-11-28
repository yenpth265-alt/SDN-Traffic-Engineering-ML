# ML dự báo nhu cầu lưu lượng SDN

Pipeline cho role ML theo Architecture v1.4 và Testing v2: chuẩn bị TrainingBundle, huấn luyện RF, chọn cấu hình bằng validation, đánh giá test và dự báo từ snapshot SDN do Data cung cấp.

**Nếu đang thấy quá nhiều file:** đọc [Hướng dẫn ML: file nào làm gì, luồng chạy ra sao](docs/HUONG_DAN_ML.md) trước. Bạn chỉ cần chạy `python -m ml ...`; không cần chạy từng file Python.

Đã có **Persistence**, **Moving Average** và **một Random Forest dùng chung cho 48 flow, đầu ra nhiều bước**. Target là requested_mbps. Dự báo gửi Data chỉ có đúng 7 trường; không gửi L/H/epsilon/raw/mean hoặc batch metadata.

## 1. Cài và chạy thử ngay

Chạy terminal tại thư mục gốc dự án:

~~~powershell
python -m pip install -r ml/requirements.txt
python -m ml demo --out ml/artifacts/demo-local
~~~

Demo tạo dữ liệu **tổng hợp**, chia 20 seed theo 10/5/5, chuẩn bị bundle, train/validation, evaluate test và dự báo online thử. Không dùng số đo demo để viết kết quả thực nghiệm mạng. Dùng thư mục đầu ra mới mỗi lần chạy.

File dữ liệu mẫu đầy đủ nằm trong ml/artifacts/demo-local/input/. Snapshot và context do demo tạo chỉ dùng kiểm thử, sẽ hết hạn rất nhanh; không lấy làm snapshot online thật.

## 2. Khi cần chuẩn bị dữ liệu offline mới hoặc huấn luyện lại

Chỉ với phạm vi offline mới, ML mới phối hợp Data xuất hai file theo [hợp đồng đầu vào](docs/DATA_CONTRACT.md):

- history.csv: nhu cầu theo run, flow và bin một giây, gồm cả warm-up.
- runs.json: nguồn thời gian, scenario, seed, family và split đã thống nhất.

**Hai file này không cần cho tích hợp/inference với model đã huấn luyện.** Luồng online ở mục 5 nạp `selected.yaml` và artifact đã khóa, lấy run metadata cùng snapshot SDN qua Data API; lịch sử L bins nằm trong snapshot. Không yêu cầu Data bổ sung export CSV/JSON như điều kiện chạy model hiện tại. Bộ offline Network đã dùng để huấn luyện cũng không cần xuất lại chỉ để inference.

~~~powershell
python -m ml run --history data/exports/history.csv --runs data/exports/runs.json --out ml/artifacts/run001
~~~

Lệnh trên thực hiện prepare → train/validation → evaluate test. Không tự chia ngẫu nhiên cửa sổ, không tự đổi nhãn split. Cấu hình tìm kiếm ở [training.yaml](../configs/ml/training.yaml); chọn file khác bằng --config.

Điều kiện dữ liệu:

- Đủ 48 flow đúng catalog Network, không thiếu bin, trùng khóa, NaN/inf hoặc nhu cầu âm.
- Chia theo seed/family **trước khi tạo window**, theo tỷ lệ 50/25/25. Với 20 seed: 10 train (1–10), 5 validation (11–15), 5 test (16–20), theo manifest Network. Replay cùng seed/family/schedule phải cùng split.
- Bắt bản sao nguyên chuỗi bị đổi tên rồi đưa sang split khác bằng fingerprint.
- Origin của CSV là traffic start; origin của dự báo là measurement start.
- Target nằm trong khoảng đo; không tạo label tail ngoài measurement end.

**Trạng thái dữ liệu ngày 22/11/2026:** đã tích hợp nhánh `data/ml-offline-dataset`. Network `demand_plan.zip`, manifest và ML thống nhất split 10/5/5. Có 120 lịch: 60 train, 30 validation, 30 test; tổng 2.419.200 dòng cho 48 flow. ML giữ nguyên nhãn của nguồn.

Các file `ml/data/train.csv`, `validation.csv`, `test.csv` là bản xuất Data với offset từ measurement start (-60 đến 359); chúng chưa phải `history.csv`/`runs.json` cho lệnh `run`. Lệnh `from-schedule` dưới đây đọc trực tiếp ZIP Network với offset từ traffic start (0–419), kiểm manifest và tạo đầu vào đúng định dạng cho pipeline.

### Chạy offline ngay trên schedule Network

~~~powershell
python -m ml from-schedule --schedule "network/traffic_schedules/demand_plan.zip" --warmup-seconds 60 --measurement-seconds 360 --config configs/ml/pilot.yaml --out ml/artifacts/network-run001
~~~

Mở `ml/artifacts/network-run001/evaluation/REPORT.md` để xem kết quả. Đây là học/đánh giá trên **lịch demand được sinh**, chưa phải số đo mạng thực.

`pilot.yaml` thử đủ 3 giá trị L × 4 giá trị H, học tối đa 5.000 mẫu với 40 cây để chạy thử trên máy cá nhân. `training.yaml` tìm kiếm rộng hơn: 50.000 mẫu và hai cấu hình RF 100/150 cây; đổi `--config` khi đủ tài nguyên. Mọi lần chạy vẫn đánh giá đầy đủ validation/test của bundle.

`from-schedule` đọc CSV/ZIP, đối chiếu `seed_allocations` trong manifest, kiểm đủ 48 flow/bin và toàn bộ seeds mỗi scenario, rồi chạy import → prepare → train/validation → evaluate test. Cùng seed giữ cùng family nếu CSV chưa cung cấp `parent_family_id`; Network vẫn cần xác nhận các họ dữ liệu trước benchmark.

Hai cờ thời gian là **giả định offline khai báo tường minh**, chưa sửa hay khóa warm-up/drain của Network. Với CSV 420 bins, 60+360 hợp lệ; 70+360 bị từ chối vì thiếu 10 bins. Adapter dùng UTC giả định 01/01/2000 làm hệ tọa độ và lưu provenance `offline_network_schedule`, `online_eligible=false`; không tạo release/availability hoặc snapshot online. Với Data export đã có mốc thực, dùng lệnh `run` ở trên.

## 3. Chạy riêng từng bước

~~~powershell
python -m ml prepare --history data/exports/history.csv --runs data/exports/runs.json --max-lookback 60 --max-horizon 10 --stride 5 --out ml/artifacts/bundle001
python -m ml train --bundle ml/artifacts/bundle001 --config configs/ml/training.yaml --out ml/artifacts/model001
python -m ml evaluate --bundle ml/artifacts/bundle001 --artifact ml/artifacts/model001 --out ml/artifacts/evaluation001
~~~

Data có thể cung cấp TrainingBundle đúng định dạng và bắt đầu ngay ở train. Prepare là bộ chuyển đổi file ngoại tuyến mà Data có thể gọi lại; không truy cập/ghi DB và không tạo snapshot sản xuất.

Stride=5 lấy mẫu mỗi 5 giây khi tạo tập học để giảm chi phí; dùng --stride 1 nếu cần mọi cutoff. Mọi L/H và baseline được so sánh trên cùng danh sách sample hợp lệ theo L/H lớn nhất. H nhỏ không tự được thêm mẫu ở cuối run.

## 4. Artifact và đánh giá

Với lệnh run, kết quả nằm dưới ml/artifacts/run001/:

| Đường dẫn | Nội dung |
| --- | --- |
| bundle/bundle.json | Schema, run/split, source hashes, hash các file dữ liệu |
| model/model.joblib | RF đã huấn luyện |
| model/ml_config.json | L*, H*, epsilon*, W*, feature order, thứ tự flow và baseline config |
| model/deployment.yaml | YAML đóng băng cấu hình đã chọn, đường dẫn model tương đối và hash để online nạp |
| model/metadata.json | Model/config hash, phiên bản thư viện, provenance và sample hash |
| model/requirements-lock.txt | Phiên bản thư viện để tái tạo môi trường |
| model/validation.json | Các lượt tìm kiếm và so sánh RF/Persistence/MA |
| model/MODEL_CARD.md | Target, đặc trưng, chia tập, giới hạn |
| evaluation/evaluation.json | MAE/RMSE pooled, theo bước, mean-H riêng, bias, underprediction, theo scenario; latency 48 flow |
| evaluation/REPORT.md | Báo cáo tiếng Việt để đọc trước: cấu hình, so sánh 3 mô hình, từng scenario/horizon và giới hạn |
| evaluation/predictions.npz | Sample ID, y thật và dự báo từng phương án để Testing tính lại |
| evaluation/sample_metadata.csv | Metadata để join theo sample ID |

RF được chọn bằng **pooled vector MAE trên validation**. Epsilon ưu tiên 0 nếu hòa hoặc không cải thiện. W của MA được chọn riêng, W ≤ L*. Baseline dùng epsilon riêng trong config, mặc định 0. Test chỉ đọc ở evaluate, không dùng để chọn tham số; không refit train+validation. `evaluation.json` còn có `by_run` để Testing ghép theo seed/run, `by_flow` cho 48 nhóm và provenance của dữ liệu gốc.

Mặc định fit tối đa 50.000 sample train, lấy mẫu có seed và ghi hash; tăng max_train_samples nếu đủ tài nguyên. Không dùng seed/scenario/schedule làm feature. Feature gồm lag, thống kê lịch sử, xu hướng và one-hot 48 flow.

Chọn H bằng MAE không chứng minh H đó tối ưu hiệu năng mạng. Muốn kết luận lợi ích RF cần benchmark M1/M2/M3 với cùng MILP và điều kiện chạy.

## 5. Dự báo online

### 5.0. Khóa L/H bằng offline và dùng lại YAML khi online

L/H được chọn bằng **validation pooled vector MAE** trong lưới offline; test chỉ đánh giá sau khi chọn xong. Online dùng nguyên cấu hình đã đóng băng, không tự chọn lại L/H theo từng tick. File dùng chung của lần chạy 26/11/2026 là `configs/ml/selected.yaml`, tham chiếu model tại `ml/saved_models/offline-L30-H1/`.

Mỗi lần `train` tự sinh `model/deployment.yaml`. Để xuất YAML sang vị trí bàn giao:

~~~powershell
python -m ml export-config --artifact ml/saved_models/offline-L30-H1 --out configs/ml/selected.yaml
~~~

YAML chứa `lookback_L`, `horizon_H`, `ma_window`, epsilon và hash model/config. Loader kiểm chúng khớp model đã huấn luyện; sửa riêng L/H trong YAML sẽ bị từ chối. Cần bàn giao cả YAML lẫn thư mục model được tham chiếu, giữ cấu trúc đường dẫn tương đối. Runner đăng ký `lookback_bins`/`horizon_bins` từ cấu hình đã nạp và lấy `ml_config_hash` từ `ml_config_sha256` trong YAML, không dùng hash bytes YAML thay thế.

### 5.1. Qua Data API (`python -m ml online`, khuyến nghị khi tích hợp)

Chi tiết endpoint, `X-API-Key` và schema v1.4 xem tại [Hợp đồng tích hợp online v1.4](docs/ONLINE_INTEGRATION_CONTRACT.md).

Để chạy model bàn giao trên môi trường mới, cài đúng lock của artifact; loader kiểm phiên bản NumPy, scikit-learn và joblib thay vì tự chấp nhận mọi phiên bản trong requirements huấn luyện:

~~~powershell
python -m pip install -r ml/saved_models/offline-L30-H1/requirements-lock.txt
~~~

~~~powershell
$env:SDN_ML_KEY = "<ml-key>"
$env:SDN_READER_KEY = "<reader-key>"
python -m ml online --deployment-config configs/ml/selected.yaml --base-url http://127.0.0.1:8000 --run-id run001 --snapshot-id snap_run001_100 --emit-event
# Hoặc giữ model nạp 1 lần cho Runner đọc từng dòng snapshot_id từ stdin:
python -m ml online --deployment-config configs/ml/selected.yaml --base-url http://127.0.0.1:8000 --run-id run001 --watch-stdin --emit-event
~~~

Adapter lưu forecast và audit event vào journal SQLite cục bộ **trước POST**, mặc định `ml/artifacts/online-replay.sqlite3`. Dùng `--replay-db <path>` hoặc `SDN_ML_REPLAY_DB` để đặt đường dẫn bền vững do Runner quản lý. Đây là cache replay của ML, Data vẫn ghi các bảng dữ liệu thí nghiệm.

Khi Runner gửi lại cùng run/snapshot/batch sau mất ACK hoặc restart, ML dùng nguyên payload và `issued_at` đã lưu, không inference lại. Cùng ID mà đổi snapshot/model/context bị từ chối; forecast hết hạn vẫn skip, không được làm mới timestamp để gửi lại. `--dry-run` không ghi journal. Journal giữ ACK và payload đã gửi; hiện chưa tự quét/gửi mọi mục pending khi khởi động, Runner cần gửi lại đúng tick IDs. Audit `event_delivery=PENDING` cho biết event cần được retry khi còn đủ điều kiện; không làm mất kết quả forecast Data đã ACK.

Trước runtime cần thống nhất: Data/SDN về tick đầu và history warm-up; SDN cung cấp producer snapshot L=30; Optimization dùng đúng H và không áp epsilon lần hai; QA/Runner chốt cách gọi worker, replay, timeout/fallback và ngân sách deadline. Chi tiết phân vai nằm ở mục 10 của hợp đồng online.

### 5.2. Chế độ file cục bộ (`python -m ml predict`)

~~~powershell
python -m ml predict --artifact ml/artifacts/run001/model --snapshot data/exports/snapshot_tick100.json --context data/exports/context_tick100.json --out ml/artifacts/forecast_tick100.json --audit ml/artifacts/audit_tick100.json
~~~

Mỗi tick dùng tên output riêng. Khi invalid/timeout, CLI trả mã lỗi 2 và chỉ ghi audit; không tạo payload dự báo thiếu/giả. Dùng --method persistence hoặc --method moving_average để chạy baseline cùng H/history.

Trong vòng điều khiển, giữ một instance để model chỉ nạp một lần:

~~~python
from ml.inference.predict import Predictor
from ml.common import ContractError

predictor = Predictor("configs/ml/selected.yaml")
# snapshot do Data.get_stored_snapshot(...) cung cấp; context do Runner cung cấp.
try:
    rows = predictor.predict_batch(snapshot, run_context, method="rf")
except ContractError:
    audit = predictor.last_audit
    # Runner ghi audit và skip tick, giữ đường hiện tại.
else:
    audit = predictor.last_audit  # Lưu riêng ở ML/Runner.
    # Data.commit_prediction_rows(rows) khi adapter Data đã triển khai.
~~~

Rows gồm 48×H* hàng như:

~~~json
{"run_id":"run001","flow_group_id":"0x103","issued_at":"2026-11-12T09:02:40.150000Z","forecast_start":100,"forecast_end":101,"predicted_mbps":4.4,"source":"rf_v1"}
~~~

Forecast start/end là offset theo **measurement origin**. Issued_at lấy lúc phát hành, chung cho batch. ML áp max(0, (1+epsilon) × raw) đúng một lần. Dự báo hữu hạn trên 10 Mbps không bị cắt về dung lượng link.

Snapshot thiếu/invalid/stale, sai hash/flow/order, route chưa xác minh hoặc target vượt measurement end đều bị chặn. RF lỗi chỉ fallback khi context đặt rõ fallback_policy=persistence: toàn batch chuyển sang fallback_persistence, epsilon fallback khai báo riêng. Không fallback cho history lỗi hoặc artifact sai hash. Frequency/reason nằm trong audit, không thêm vào payload.

## 6. Kiểm thử và giới hạn tích hợp

~~~powershell
python -m pip install -r ml/requirements-dev.txt
python -m pytest ml/tests -q
~~~

Kiểm tra bám TC-D03/D04, TC-M01–M04 và TC-I03: leakage, alignment, baseline, epsilon, 240 rows khi H=5, origin, missing/future/stale history, hash, fallback toàn batch, không dùng test để train và H=1.

- JSON/CSV/NPZ là **file adapter có version được đề xuất**, vì Data/SDN chưa có API/schema executable. Cần thống nhất format/hash/transport hoặc viết adapter; chưa xác nhận toàn hệ thống đã tích hợp.
- Timeout đo bằng monotonic và chặn xuất kết quả quá hạn; RF đồng bộ không bị cưỡng bức ngắt giữa chừng. Hard deadline cần Runner quản lý worker ở cấp process.
- Ngưỡng tuổi snapshot, latency, fallback và epsilon fallback do Runner khóa; giá trị demo chưa là ngưỡng benchmark đã duyệt.
- Chỉ nạp model do nhóm tin cậy tạo. Loader kiểm hash và phiên bản NumPy/scikit-learn/joblib; hash kiểm toàn vẹn, không thay xác thực nguồn.
- Chưa có dữ liệu thật thì chỉ xác nhận pipeline chạy đúng hợp đồng; chưa kết luận sai số thực tế hoặc lợi ích mạng.

Nguồn: [Architecture §5.3](../docs/architecture/SDN_Traffic_Engineering_Role_Integration_Guide_v1.4.md), [Interfaces IF04–IF06](../testing/docs/interfaces_v2.md), [Acceptance AC-M01–M08](../testing/docs/acceptance_criteria_v2.md), [Test plan](../testing/docs/test_plan_v2.md).

API thư viện: [RandomForestRegressor](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestRegressor.html), [NumPy load](https://numpy.org/doc/stable/reference/generated/numpy.load.html). NPZ được đọc với allow_pickle=False.
