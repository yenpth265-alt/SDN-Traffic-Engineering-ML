> **TÀI LIỆU LỊCH SỬ — không dùng làm đặc tả runtime hiện hành.** Giữ nguyên nội dung và bằng chứng tại mốc ghi bên dưới. Đối chiếu [baseline QA 28/11/2026](../baselines/2026_11_28/README.md), [ánh xạ ID](../baselines/2026_11_28/reference/legacy_id_crosswalk.md) và [runtime contract](../../integration/orchestration/README.md). [CI main@71f120a](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/actions/runs/46134738871) đạt 289 module, 19 Data/QA và 3 SDN–PostgreSQL; không đồng nghĩa sign-off/live.

# Bàn giao phần ML — 18/11/2026

Đã hoàn thiện đường chạy ML độc lập từ dữ liệu đầu vào đến model, đánh giá và payload dự báo; chạy được trên schedule Network hiện có ở main. Chưa nghiệm thu tích hợp với Data/SDN runtime hoặc benchmark mạng. Nhánh controller chưa được merge; các lỗi Network/manifest đã audit không bị sửa trong công việc ML này.

## 1. Đã làm những gì?

Phần có sẵn được giữ và kiểm chứng: Persistence, Moving Average, shared multi-output Random Forest, feature lịch sử, chia bundle, tuning validation, model registry, kiểm snapshot/fallback và payload 7 trường. Trước thay đổi, bộ test có 35 case đạt.

Phần bổ sung/hoàn thiện lần này:

| Thay đổi | File chính | Kết quả |
|---|---|---|
| Import CSV/ZIP schedule trực tiếp | `ml/preprocessing/schedule.py`, `ml/__main__.py` | Có lệnh `from-schedule`: import → bundle → train/validation → evaluate |
| Kiểm đầu vào trước học | `schedule.py` | Từ chối split lệch manifest, thiếu/trùng bin, flow lạ, NaN, metadata không nhất quán và scenario thiếu seed |
| Khai báo rõ thời gian offline | `schedule.py` | Warm-up/measurement bắt buộc truyền rõ; 60+360 khớp CSV, không tự sửa cấu hình Network 70/360/10 |
| Giữ nguồn gốc dữ liệu | `bundle.py`, `train.py`, `evaluate.py` | Provenance từ schedule đi xuyên input → bundle → metadata model → báo cáo; không coi schedule là observation online |
| Báo cáo dễ đọc | `ml/validation/evaluate.py` | Sinh `REPORT.md`; JSON thêm kết quả từng run/seed, flow, RF raw và cấu hình đã chọn |
| Profile pilot dùng lại được | `configs/ml/pilot.yaml` | 12 cấu hình L/H, tối đa 5.000 mẫu train, 40 cây; profile đầy đủ trong `training.yaml` vẫn được giữ |
| Sửa hai lỗi nhỏ khi chạy | `ml/__main__.py`, `ml/demo.py` | Lỗi đọc snapshot sau nạp model vẫn có audit/reason; snapshot demo mặc định còn thời hạn khi H=1 |
| Hướng dẫn đọc code | `ml/docs/HUONG_DAN_ML.md`, `ml/README.md`, `ml/docs/DATA_CONTRACT.md` | Giải thích từng file, thuật ngữ, hai luồng và cách bàn giao |
| Bổ sung kiểm thử | `ml/tests/` | Tổng 48 case đạt, tăng 13 case so với trước |

## 2. Dữ liệu và cấu hình thực sự đã chạy

- Base main: `ff73cddbdb086b4d05afa0886b575896ffe6721d`.
- Nguồn: `network/traffic_schedules/demand_plan (2).zip`.
- SHA-256 ZIP: `2f1b9d11b55a7e01e5d12acbe277c779f16f1d0f172c2c4f2e81dff5ebb53e6f`.
- 2.419.200 dòng = 120 schedules × 420 bins × 48 flow. Gồm S1, S2a, S2b, S3, S4, S5; mỗi loại 20 seeds.
- Theo seed manifest: 1–14 train, 15–17 validation, 18–20 test. Nhãn CSV được giữ nguyên.
- Giả định **offline**: warm-up 60s, measurement 360s. UTC 01/01/2000 là hệ tọa độ nhân tạo, được gắn nhãn trong provenance; không phải timestamp đo thực và không tạo release/availability/route evidence.
- Tạo common windows bằng Lmax=60, Hmax=10, stride=5: **286.272 train / 61.344 validation / 61.344 test**. Tất cả cấu hình dùng chung cutoffs; H=1 không được thêm các mẫu cuối run mà H=10 không có.
- Pilot fit **5.000/286.272** mẫu train, lấy mẫu với seed 2026. Validation và test dùng toàn bộ số mẫu nêu trên.
- Pilot tìm L ∈ {10,30,60}, H ∈ {1,3,5,10}, epsilon ∈ {0,0.05,0.1,0.2}; RF 40 cây, max_depth=12, min_samples_leaf=2; baseline epsilon=0. MA chọn W riêng từ các giá trị hợp lệ.
- Có 12 trial RF, chọn bằng pooled vector MAE trên validation. Lưới đầy đủ 24 trial/50.000 mẫu đã dừng trước khi hoàn tất; **không dùng kết quả đó để tuyên bố đã chạy full search**. Model được bàn giao là pilot. File config thực thi được giữ tại `ml/artifacts/network-2026-11-18/pilot-search.yaml`, có nội dung tham số tương đương `configs/ml/pilot.yaml`.

## 3. Kết quả

Validation chọn **L=60, H=1, epsilon=0**, MA **W=5**; validation MAE của RF là **0,057673 Mbps**. H=1 nghĩa là nhìn 60 giây trước để dự báo bin một giây kế tiếp. Đây không phải kết luận H=1 tối ưu cho điều khiển mạng; tiêu chí đang chọn là sai số dự báo.

Test gồm 18 runs/schedules (6 scenario × 3 seeds test), 61.344 mẫu chung:

| Phương án | MAE (Mbps) | RMSE (Mbps) | Bias (Mbps) |
|---|---:|---:|---:|
| Random Forest | 0,058202 | 0,098819 | -0,001791 |
| Persistence | 0,068198 | 0,105776 | -0,000634 |
| Moving Average | 0,059102 | 0,105542 | -0,000419 |

RF có MAE tổng thấp hơn hai baseline trong lượt này. Tuy nhiên MA có MAE thấp hơn RF tại S1, S2a, S3 và S5; không kết luận RF thắng mọi scenario hoặc chênh lệch có ý nghĩa thống kê.

Đo 100 batch × 48 flow khi model đã nạp: feature + RF + epsilon có **p50=6,476 ms, p95=9,693 ms** trên máy chạy Windows hiện tại. Không gồm transport, snapshot validation, Data, MILP hoặc SDN; không coi đây là latency end-to-end hay SLA. Thông tin môi trường được ghi trong `evaluation.json`/`metadata.json`.

Model RF khoảng 4,78 MB (4.780.177 bytes), được nạp lại thành công:

- Model SHA-256: `5334e6c62befc9667c999d781cc820eac20b69c603151004afe17f739de9908f`.
- ML config SHA-256: `eecde9f70b349c6c1e593cdedac6c51fd891d11e4a89a17bfa96f6f991bf72b5`.

## 4. Kiểm chứng đã thực hiện

1. `python -m pytest ml/tests -q --junitxml=ml/artifacts/pytest-2026-11-18.xml`: **48 passed in 9.75s**. Bao phủ leakage, causal window, split, 7 fields, H=1/H=5, missing/stale/future snapshot, epsilon đúng một lần, model/hash, toàn batch fallback, timeout và import schedule lỗi.
2. `python -m ml demo --out ml/artifacts/demo-2026-11-18`: chạy đủ prepare → train/validation → evaluate → predict trên dữ liệu tổng hợp; chọn L=5/H=3/epsilon=0 và xuất **144 hàng**. Chỉ là demo kiểm đường chạy.
3. Schedule main đã import và tạo bundle đầy đủ, pilot train trên train/validation, sau đó evaluate riêng trên test. Các số test tại mục 3 đến từ model đã đóng băng; không tuning theo test, không refit train+validation.
4. Nạp lại model pilot bằng `Predictor`, dùng snapshot **giả lập** và đồng hồ kiểm thử; RF, Persistence, MA đều xuất **48 hàng đúng 7 trường**, đủ 48 flow, H=1, không fallback. Bằng chứng ở `ml/artifacts/network-2026-11-18/smoke/summary.json`, có nhãn `synthetic_snapshot_smoke_not_live_network`.
5. Không thay đổi file tracked của Network, manifest, guide main hoặc controller. Công việc ML và tài liệu bàn giao hiện nằm trong workspace, chưa commit/push.

## 5. Mở những file nào trước?

1. **[Hướng dẫn ML](../../ml/docs/HUONG_DAN_ML.md)**: có sơ đồ offline/online, ví dụ L/H/W/epsilon và bảng giải thích toàn bộ file mã nguồn/đầu ra.
2. **[Báo cáo kết quả pilot](../../ml/artifacts/network-2026-11-18/evaluation/REPORT.md)**: cấu hình, bảng metric, từng scenario/horizon và cách đọc artifact.
3. **[README chạy lệnh](../../ml/README.md)**: cài môi trường, chạy schedule/Data export, demo và predict.
4. **[Hợp đồng Data/Runner](../../ml/docs/DATA_CONTRACT.md)**: định dạng input/output để nối các role.

Model dùng được ở `ml/artifacts/network-2026-11-18/model/`. Payload thử nằm ở `smoke/rf_rows.json`, `smoke/persistence_rows.json`, `smoke/moving_average_rows.json`; snapshot ở đó là fixture, không dùng cho tick online thật. `ml/artifacts/` bị ignore trong Git: khi bàn giao model cần gửi thư mục model qua kênh artifact đã thống nhất, không chỉ gửi source code.

Để chạy lại một lượt tương đương vào thư mục mới:

```powershell
python -m ml from-schedule --schedule "network/traffic_schedules/demand_plan (2).zip" --warmup-seconds 60 --measurement-seconds 360 --config configs/ml/pilot.yaml --out ml/artifacts/network-rerun
```

## 6. Luồng hoạt động tóm tắt

**Offline:** schedule hoặc Data export → kiểm input/split → tạo X/y → feature quá khứ → RF học train → validation chọn L/H/epsilon và W của MA → lưu model/config → evaluate test → báo cáo.

**Online sau khi tích hợp:** Network công bố demand quá khứ → SDN tạo snapshot → Data lưu/cấp → ML kiểm snapshot và model → dự báo → áp epsilon một lần → trả 48×H hàng 7 trường cho Data → Optimization tự mean H và chọn đường → SDN thực thi.

ML không truy cập DB, không tạo snapshot sản xuất, không dùng future schedule trong inference và không cài flow rule. Data/Runner vẫn cần thống nhất schema/hash/transport với controller feature; bộ test độc lập không chứng nhận đã tương thích native snapshot serializer của controller đó.

## 7. Những phần chưa thể nghiệm thu toàn hệ thống

- Chưa có luồng Data/SDN runtime thật trên main để chạy end-to-end. Cần kiểm tương thích snapshot và adapter thực khi controller được đưa lại vào main.
- Network/AI/Test còn cần chốt warm-up/drain/time origin và sửa reader manifest. Các tham số 60/360 trong pilot chỉ áp dụng cho việc đánh giá offline của ML.
- Cần đồng bộ guide main đang ghi split 10/5/5 với allocation/CSV 14/3/3; kiểm độc lập family/demand release và hash trước benchmark chính thức.
- Model pilot chưa qua tìm kiếm đầy đủ, số đo mạng thật hoặc benchmark M1/M2/M3 cùng MILP. Chưa có bằng chứng giảm nghẽn/loss hoặc tăng goodput.
- Timeout hiện chặn công bố dự báo quá hạn; cưỡng bức dừng RF đang chạy cần Runner quản lý worker process.

