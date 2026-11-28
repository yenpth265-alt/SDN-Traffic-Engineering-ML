> Cập nhật mã tích hợp 28/11/2026: xem [runtime contract](../../integration/orchestration/README.md). Các đề xuất version/hash/gate và SHA producer cũ bên dưới là lịch sử. Baseline hiện dùng topology `v1.0`, hash raw từ bundle, warm-up 60 s, L30/H1; G1–G6 QA và gate Network/S0 là hai namespace riêng. Xác nhận owner và nghiệm thu live vẫn PENDING.

# API online Data — các nội dung cần team chốt

Cập nhật ngày 24/11/2026 theo `main@db2dacd`. Các phương án tích hợp vẫn chờ đầu mối phụ trách xác nhận.

## Luồng thống nhất tạm thời

Runner đăng ký lần chạy → Network gửi dữ liệu → SDN tạo snapshot → AI dự báo → Optimization chọn đường → SDN áp dụng và báo kết quả. Data kiểm tra, lưu và cung cấp dữ liệu giữa các bước.

Snapshot là bản dữ liệu cố định tại một thời điểm. Runner là chương trình điều phối một lần chạy; cần phân công người phụ trách.

## Mỗi module cần xác nhận gì?

| Module | Phương án tạm | Nội dung cần chốt |
|---|---|---|
| **Data** | Một địa chỉ API chung, mỗi vai trò một API key | Máy chạy API/DB, địa chỉ và cổng truy cập, HTTPS, cách cấp key; cập nhật tài liệu sau khi team xác nhận |
| **Network** | Gửi demand từng giây đã hoàn tất; ghép sender/receiver rồi gửi traffic; gửi lại cùng ID khi lỗi | Định nghĩa requested_mbps và thời điểm phát hành; ai ghép số liệu gửi–nhận; xử lý dữ liệu thiếu/đến muộn; Network hay SDN gửi số liệu liên kết |
| **SDN** | Tạo snapshot; công bố routing version; kiểm tra lại version và hạn áp dụng trước khi đổi đường; công bố routing trước khi báo kết quả cuối | Cấu trúc snapshot và mốc đếm giây; quy tắc tăng version; cách báo từ chối/thất bại/khôi phục đường cũ; ý nghĩa FAILED và UNKNOWN |
| **AI/ML** | Đọc snapshot hợp lệ; gửi đủ 48 × H dòng dự báo, gắn đúng snapshot | L (số giây lịch sử), H (số giây dự báo), phiên bản mô hình/cấu hình; thứ tự nhóm luồng; cách xử lý thiếu lịch sử |
| **Optimization** | Gửi đủ mọi nhóm luồng, kể cả giữ nguyên; kèm snapshot, forecast, version và hạn áp dụng | Gửi toàn bộ hay chỉ nhóm đổi đường; hạn áp dụng tính từ đâu và bao lâu; ai tạo tick_id (ID mỗi lượt tối ưu); cách báo không có nghiệm/bỏ qua lượt |
| **Test/Runner** | Đăng ký run qua API; chuyển decision_batch_id cho SDN; kết thúc run bằng ok/failed/discarded | Người viết Runner; cách thông báo ID cho SDN; tiêu chí kết thúc/hủy lần chạy; thời gian chờ dữ liệu muộn; bài kiểm thử tích hợp |

## Quy tắc chung cần tất cả module thống nhất

- **Thời gian:** dùng UTC và đồng bộ đồng hồ. Bắt đầu đo = bắt đầu phát lưu lượng + thời gian khởi động; các mốc cố định khi đăng ký run. Nếu khởi động lệch, cần thống nhất hủy run hay sửa cơ chế đăng ký.
- **Cách đếm giây:** demand/snapshot đếm từ lúc phát lưu lượng; dự báo đếm từ lúc đo chính thức. Ví dụ khởi động 60 giây thì giây 60 của demand tương ứng giây 0 của dự báo.
- **Snapshot:** SDN tạo, Data kiểm tra/lưu/trả; thống nhất một cấu trúc. Không đưa trực tiếp history `format=sdn` vào snapshot hiện tại vì khác trường và mốc đếm giây.
- **Dữ liệu online:** không lấy lịch tương lai từ demand_plan để bù lịch sử thiếu.
- **Gửi lại:** giữ nguyên ID và nội dung; bên gửi lưu bản chưa được xác nhận. Chốt thời gian chờ, số lần thử; 409 là xung đột nội dung, 422 là dữ liệu không hợp lệ, cần kiểm tra trước khi gửi lại.
- **Kết quả SDN:** APPLIED = đường đề xuất; KEPT/ROLLED_BACK = đường cũ; UNKNOWN = chưa xác minh được đường thực tế. Chốt cách tổng hợp trạng thái cả lô và ghi sự kiện lỗi/khôi phục.
- **Giao nhận:** tạm dùng Runner chuyển ID; chưa có API danh sách quyết định chờ hay thông báo tự động. Topology có thể đọc qua API, cần chốt dùng API hay file cấu hình chung.

## API theo module

Các đường dẫn bắt đầu bằng `/v1`, trừ health. `{run_id}` là ID lần chạy.

| Module | API sử dụng |
|---|---|
| Network | POST `/v1/network/demand`, `/v1/network/traffic`; POST `/v1/sdn/links` nếu được giao thu thập liên kết |
| SDN | POST `/v1/sdn/routing`, `/v1/sdn/snapshots`, `/v1/sdn/apply-results`; GET `/v1/runs/{run_id}/demand-history`, `/v1/runs/{run_id}/decisions/{decision_batch_id}` |
| AI/ML | GET `/v1/runs/{run_id}/snapshots/{snapshot_id}` bằng Reader key; POST `/v1/ml/forecasts` bằng ML key |
| Optimization | GET `/v1/runs/{run_id}/forecasts/{forecast_batch_id}`; POST `/v1/optimization/decisions` |
| Test/Runner | POST `/v1/runs`; GET `/v1/runs/{run_id}`; PATCH `/v1/runs/{run_id}/status` |
| Dùng chung | GET `/v1/runs/{run_id}/topology`; POST `/v1/runs/{run_id}/events/{component}` với component là network/sdn/ml/optimization/runner và key tương ứng; GET `/health/live`, `/health/ready` |

Xem trường dữ liệu và thử API tại `/docs` khi dịch vụ chạy. Code nằm trong `data/ingestion/online_api.py`, `online_models.py`, `online_store.py` và `management_api.py`.

## Ưu tiên chốt và kiểm thử

Chốt trước: **người phụ trách Runner → địa chỉ/key → thời gian → L/H và snapshot → cách giao quyết định → hạn áp dụng và kết quả SDN**.

Kiểm tra vòng 48 nhóm luồng, gửi trùng, dữ liệu thiếu, dữ liệu đến muộn, đổi version và hết hạn theo các mục dưới đây. Bài mất kết nối và xác minh trên mạng thật thuộc đợt kiểm thử pilot.

## Truy vết và xác nhận giao diện ngày 23/11/2026

Các SHA dưới đây ghim mã nguồn dùng để tái hiện. Người trong cột xác nhận là đầu mối được đề nghị theo lịch sử commit của module. Trạng thái ký xác nhận của từng đầu mối là **đang chờ**.

| Mã | Commit | Module | Đầu mối xác nhận |
|---|---|---|---|
| N | [e5db9bc](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/commit/e5db9bc487094382fe18e9d3eb35cafee36f353e) | Network | `daylauyen2006-alt` |
| D | [1ab3836](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/commit/1ab3836ab08a3f692edb49927528872c282382f6) | Data | `VanNgHong` |
| S | [ca405ec](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/commit/ca405ecf6f6f595df979656d490e96e2fdce3cb8) | SDN | `PhamAn56` |
| M | [f619544](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/commit/f6195442c202539f8973328cc899eaccfd767e09) | ML | `yenpth265-alt` |
| O | [f0aaa26](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/commit/f0aaa26cfb6febbf1361a3be39715764c0b097e9) | Optimization | `dangtquang0509` |
| Q | [e5db9bc](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/commit/e5db9bc487094382fe18e9d3eb35cafee36f353e) | Testing | `ndtchumoa` |

Mã D tham chiếu commit của Data. Nhánh QA lấy `main` tại [6e5e3b9](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/commit/6e5e3b9d18ac1919d47111b901a454154ed8f7af). Từ `e5db9bc` đến commit này, hai file sender và receiver đổi. API Data, schema và collector telemetry trùng phiên bản đã dùng để bắt fixture. File kiểm thử mới và fixture nằm trong commit của nhánh QA. Việc phân công lập trình Runner, địa chỉ triển khai và cơ chế chuyển `decision_batch_id` cần quyết định của nhóm.

| Giao diện | Module, commit và file | Fixture và ca kiểm | Kết quả kỳ vọng | Xác nhận |
|---|---|---|---|---|
| Runner → Data, đăng ký run và kết thúc | Q + D · `data/ingestion/management_api.py`, `configs/experiments/runner_manifest.json` | Run có warm-up 60 s, chạy lại cùng ID · `test_runner_registration_replay`, `test_runner_and_event_replay`, TC-I03 | Đăng ký `1 → 0`, warm-up sai `422`, kết thúc lại `1 → 0`, đổi trạng thái cuối `409` | `ndtchumoa`, `VanNgHong` |
| Data → các module, topology | N + D · `configs/network/topology_bundle.json`, `management_api.py` | Topology thật, fixture routing và telemetry · `test_telemetry_retry_and_topology`, TC-I01 | GET đúng topology 48 nhóm luồng, sai cổng đo `422` trong bộ kiểm thử Data | `daylauyen2006-alt`, `VanNgHong`, `PhamAn56` |
| Network → Data, demand và traffic | N + D · `network/traffic_generator/sender.py`, `receiver.py`, `data/ingestion/online_store.py` | Demand dựa trên lịch sử fixture snapshot · TC-I02, `test_traffic_fk_rolls_back_batch` | Demand replay `1 → 0`, sửa giá trị cùng ID `409`. Traffic có FK sai `422` và rollback lô. Đầu mối ghép sender/receiver chờ chốt | `daylauyen2006-alt`, `VanNgHong` |
| Network → Data, telemetry | N + D · `network/telemetry/telemetry_collector.py`, `data/ingestion/online_models.py` | [telemetry.json](../../testing/fixtures/online_contract/telemetry.json) · `test_telemetry_retry_and_topology` | 24 link trong dry-run, replay `24 → 0`, sai count `422`, ID cũ nội dung khác `409`. Mỗi link một dòng trong lô | `daylauyen2006-alt`, `VanNgHong` |
| Data ↔ SDN, history và routing version | D + S · `online_store.py`, `sdn/adapters/data_api_v1_adapter.py`, `sdn/monitoring/snapshot_creator.py` | [snapshot.json](../../testing/fixtures/online_contract/snapshot.json), [apply.json](../../testing/fixtures/online_contract/apply.json) · TC-I02, TC-I03, `test_version_change_deadline_and_apply_replay` | Loại bin tương lai và bản ghi đến sau `as_of`. Chấp nhận v1 → v2. Nhảy v1 → v3, snapshot mới hoặc decision mới theo v1 sau khi v2 có sẵn trả `409` | `VanNgHong`, `PhamAn56` |
| SDN → Data → ML, snapshot | S + D + M · `snapshot_creator.py`, `data_api_v1_adapter.py`, `data/ingestion/online_api.py`, `ml/inference/predict.py` | `snapshot.json` chứa bản của SDN và bản wire Data v1 · TC-I02, TC-I03 | `L=2` cần 96 dòng VALID. Sau `as_of` cũ chỉ thấy 95 dòng khi một dòng đến muộn. Ép VALID trả `422`. Với đủ lịch sử, POST `1 → 0` và GET trả đúng byte cùng SHA-256 | `PhamAn56`, `VanNgHong`, `yenpth265-alt` |
| ML → Data → Optimization, forecast | M + D + O · `ml/inference/predict.py`, `online_models.py`, `data/schemas/003_forecast_measurement_origin.sql`, `optimization/input_validator.py` | [forecast.json](../../testing/fixtures/online_contract/forecast.json) · TC-I02, TC-I03, TC-D06 | JSON float nguyên dạng `100.0` tái hiện `422`. Khi truyền integer, đủ 48 × 5 dòng ở các bin `[100,105)`, lưu 1 batch với 240 dòng và retry `0`. Thiếu dòng hoặc dùng traffic offset `422` | `yenpth265-alt`, `VanNgHong`, `dangtquang0509` |
| Optimization → Data/SDN, decision và hạn áp dụng | O + D + S · `optimization/result_serializer.py`, `management_api.py`, `sdn/controller/decision_handler.py` | `apply.json` chứa `request` và `expired_response` · `test_version_change_deadline_and_apply_replay`, TC-S03 | Lô audit đủ 48 nhóm. Deadline hết trả `422` và không lưu tick/batch. SDN từ chối `DEADLINE_EXPIRED` trước khi tăng version. Decision hợp lệ retry `1 → 0` | `dangtquang0509`, `VanNgHong`, `PhamAn56` |
| SDN → Data, kết quả apply | S + D · `decision_handler.py`, `data_api_v1_adapter.py`, `management_api.py` | `apply.json` chứa `result` 1 dòng và `result_full` 48 dòng do adapter SDN sinh · `test_version_change_deadline_and_apply_replay`, TC-D07 | Chưa công bố routing v2 trả `409`. Kết quả 1 dòng trả `422`. Bản đầy đủ có 1 APPLIED, 47 KEPT. Retry `updated: 1 → 0` và vẫn chỉ một decision batch | `PhamAn56`, `VanNgHong` |
| Sự kiện và readiness | D + Q · `management_api.py`, `online_api.py` | Event READY, retry và sai vai trò · `test_runner_and_event_replay` | `/health/ready` thấy đủ migration 1–3. Sai vai trò `401`, event đúng `1 → 0`, ID cũ khác nội dung `409` | `VanNgHong`, `ndtchumoa` |

## Các mục C01–C16 liên quan

Theo [sổ C01–C16 trong kế hoạch kiểm thử](../../testing/docs/test_plan_v2.md). Bảng dưới đây cập nhật phạm vi bằng chứng và đầu mối. Những mục cần pilot hoặc benchmark giữ trạng thái chờ.

| Mục | Commit, module và file | Fixture, ca kiểm hoặc bước xác nhận | Đầu mối chủ trì |
|---|---|---|---|
| C01 | N/S/D · `topology_bundle.json`, `topology_registry.py`, `online_store.py` | `apply.json` và `telemetry.json` dùng ID topology hiện tại. Xác nhận ID chính thức và alias ở TC-I01/TC-N01 | `daylauyen2006-alt` |
| C02 | M/D/O · `predict.py`, `003_forecast_measurement_origin.sql`, `input_validator.py` | `forecast.json` có 5 bước mỗi flow. Đối chiếu nhãn và mean bằng TC-D04/TC-M01/TC-D06 | `VanNgHong`, `yenpth265-alt` |
| C03 | D/S/M · `online_store.py`, `snapshot_creator.py`, `predict.py` | `snapshot.json`, TC-I02. Lịch sử chỉ từ demand đã lưu đúng thời điểm | `VanNgHong` |
| C04 | O/S · `result_serializer.py`, `apply_models.py` | `apply.json`, TC-S03. `ApplyRequest` qua strict parser và `DecisionHandler` | `dangtquang0509`, `PhamAn56` |
| C05 | D/S · `management_api.py`, `data_api_v1_adapter.py` | `apply.json`, TC-D07. Dữ liệu audit truy vết snapshot/forecast/version | `VanNgHong` |
| C06 | N/D/S/M · `sender.py`, `online_store.py`, `data_api_v1_adapter.py`, `predict.py` | `snapshot.json`, `forecast.json` và TC-I03. Traffic 160, measurement 100 tại `10:02:40Z` | `VanNgHong`, `daylauyen2006-alt` |
| C07 | M/O · `predict.py`, `load_estimator.py` | TC-M04 chờ ca epsilon 4 → 4,4 trên đường vào MILP | `yenpth265-alt`, `dangtquang0509` |
| C08 | O · `optimization/milp/model.py`, `objective.py` | TC-O02/O03/O04 chờ đối chiếu số đổi thật và J ở nghiệm MILP | `dangtquang0509` |
| C09 | O/Q · `configs/optimization/optimization.yaml`, `optimization/routing_policies/trigger.py` | TC-O01 chờ chốt giá trị N, TH, h và hash config của pilot | `dangtquang0509` |
| C10 | Q/N/M · `runner_manifest.json`, `ml/demo.py`, `test_plan_v2.md` | TC-I03 dùng warm-up 60 s theo phạm vi đợt QA. Ma trận seed và pilot vẫn theo TC-E02 chờ chốt | `ndtchumoa` |
| C11 | N/S/D/O · `counter_parser.py`, `telemetry_collector.py`, `online_models.py`, `load_estimator.py` | `telemetry.json` kiểm hợp đồng Data. TC-N03/D05 chờ phép đo OVS và hiệu chuẩn hệ số | `daylauyen2006-alt`, `PhamAn56` |
| C12 | N/S · `configs/network/initial_paths.csv`, `sdn/flow_management/rule_installer.py` | `apply.json` xác minh bootstrap trên driver in-memory. TC-N01/S01 chờ same-edge, MAC/ARP trên mạng thật | `daylauyen2006-alt`, `PhamAn56` |
| C13 | O/S/D · `result_serializer.py`, `decision_handler.py`, `management_api.py` | `apply.json`, TC-S02/S03. Kiểm hết hạn trước khi ghi. Đo latency và reconcile lỗi sau ghi chờ pilot | `dangtquang0509`, `PhamAn56` |
| C14 | Q/D/S · `test_plan_v2.md`, `online_store.py`, `state_manager.py` | TC-I02 kiểm ranh giới dữ liệu tương lai. TC-E01/E02/E03 chờ chạy đối chứng và metric | `ndtchumoa` |
| C15 | D/M · `003_forecast_measurement_origin.sql`, `predict.py` | `forecast.json`, TC-D06/TC-I03. Kiểm count 240, scope của batch và retry | `VanNgHong`, `yenpth265-alt` |
| C16 | S/D/O · `decision_handler.py`, `online_store.py`, `management_api.py` | `apply.json` kiểm v1 → v2 khi APPLIED. TC-D07/S02 còn nhánh UNKNOWN, rollback và verified gap | `PhamAn56`, `VanNgHong` |

## Tái hiện trên PostgreSQL

Các fixture do [script bắt output](../../testing/scripts/capture_contract_fixtures.py) tạo từ producer đang chạy với dữ liệu QA kiểm soát. Telemetry dùng `--dry-run`, SDN dùng `InMemorySwitchDriver` và ML persistence dùng artefact huấn luyện từ dữ liệu demo với `L=2`, `H=5`. Kết quả áp dụng mạng vật lý và benchmark có ca nghiệm thu riêng. Bản ghi gốc được giữ trong bốn file JSON. Script tạo lại có thể đổi thời điểm thu telemetry và ID decision do producer cấp.

[Suite PostgreSQL](../../testing/scripts/test_postgresql_contracts.py) dùng FastAPI `TestClient` và kết nối PostgreSQL qua driver của repo. Suite dùng topology thật, đăng ký run qua API, cập nhật `run_id` và các trường availability/issue theo đồng hồ DB, và tính lại hash byte của snapshot. Trường hợp dương của forecast chuyển các bin nguyên từ JSON float sang JSON integer trong harness. Trường hợp dương của apply dùng trực tiếp output của tùy chọn `expand_unchanged_as_kept=True` trong adapter SDN. Hai ca âm giữ định dạng forecast float và kết quả apply một dòng để tái hiện lỗi hợp đồng.

Các checkout producer cần đưa vào `PYTHONPATH` ở commit M, O và S. Trong một cơ sở dữ liệu PostgreSQL dùng riêng cho QA, đặt `SDN_DATABASE_URL` và cài `requirements-online.txt`, `ml/requirements.txt` ở commit M cùng `httpx`. Chạy theo thứ tự sau.

```sh
psql "$SDN_DATABASE_URL" -X -v ON_ERROR_STOP=1 \
  -f data/schemas/schema_postgresql_dbeaver.sql \
  -f data/schemas/002_online_routing.sql \
  -f data/schemas/003_forecast_measurement_origin.sql
python -m unittest -v data.ingestion.test_management_api \
  data.ingestion.test_online_ingestion testing.scripts.test_postgresql_contracts
```

## Kết quả chạy PostgreSQL

Lượt xác nhận chạy trên GitHub Actions Ubuntu, Python 3.12 và PostgreSQL 17.12. Workflow kết nối qua TCP vào DB kiểm thử riêng `sdn_qa`, xác nhận `SELECT version(), current_database()` và đọc lại đủ migration `1, 2, 3`. FastAPI `TestClient` gọi các route và validator thật của Data. Driver `psycopg2-binary 2.9.13` kết nối PostgreSQL để lưu và đọc lại dữ liệu. Commit chạy suite là [44acd20](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/commit/44acd200bb4892dfd259d3e1d3523cba5d993738). Xem bước 9 và 10 của [job kiểm thử PostgreSQL](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/actions/runs/45223682285/job/137864381051).

Đây là lượt CI gần nhất tại thời điểm rà soát. Sau PR #5, `main` có thay đổi ở telemetry collector tại `db2dacd`. Kết quả chưa được chạy lại trên mốc mã mới. [Sổ kết quả Testing](../../testing/results/README.md) ghi trạng thái đã kiểm, phần dùng fixture và điều kiện nghiệm thu còn mở.

**Kết quả suite: 16/17 ca đạt, một ca lỗi đăng ký run lặp.** Các ca kiểm có payload gốc bị từ chối được ghi riêng dưới đây. Trạng thái chấp nhận của từng module vẫn chờ đầu mối xác nhận.

| Ca kiểm | Kỳ vọng | Thực tế | Trạng thái | Bằng chứng trong job |
|---|---|---|---|---|
| DB và migration | Kết nối `sdn_qa`, đủ schema `1, 2, 3` | PostgreSQL `17.12`, DB `sdn_qa`, trả ba dòng `1, 2, 3` | Đạt | `Initialize disposable database` |
| Runner đăng ký lặp | HTTP `200` với `inserted: 0` khi gửi lại cùng ID và nội dung. DB vẫn có một run | HTTP `409`, `Existing runs record has different content`. `COUNT(runs) = 1` | **Không đạt** | `test_runner_registration_replay` |
| Telemetry, topology và gửi trùng | Lô producer lưu một dòng mỗi link. Replay thêm 0 dòng. Payload sai count hoặc counter status trả `422`. Nội dung hợp lệ khác với cùng ID trả `409` | Chèn `24`, replay `0`. PostgreSQL vẫn `24` dòng sau ca `422` và `409`. GET topology khớp bundle | Đạt | `test_telemetry_retry_and_topology` |
| Demand gửi trùng và traffic rollback | Cùng ID và nội dung, ACK `1 → 0`. Hai yêu cầu đồng thời chỉ tạo một dòng. Traffic có FK sai rollback toàn lô | Hai ACK đồng thời có tập `{0, 1}`. Lỗi FK traffic trả `422`, sau đó payload hợp lệ chèn lại được. Run TC-I02 giữ 95 dòng sau replay và xung đột `409` | Đạt | `test_network_concurrent_retry_and_invalid_timestamps`, `test_traffic_fk_rolls_back_batch`, `test_i02_cutoff_late_missing_and_future_schedule` |
| TC-I02, dữ liệu đến muộn và bin tương lai | History tại `as_of` cũ giữ 95 dòng. Bản ghi muộn chỉ vào history sau khi tiến `as_of`. Bin từ cutoff trở đi bị loại | `95` dòng tại bins `158, 159`, lần đọc lại cùng `as_of` khớp nguyên bản. `96` dòng với `as_of` mới dù DB có `97` observation. Ép thêm bản ghi muộn hoặc bin `160` trả `409` | Đạt | `test_i02_cutoff_late_missing_and_future_schedule` |
| TC-I02, thay lịch tương lai | Với lịch sử cố định, M1–M3 dự báo như cũ khi đổi demand_plan ở bin tương lai | Đổi tải bin `160` từ `999` lên `1999`. Ba producer mode `persistence`, `moving_average`, `rf` đều cho output giống nhau giữa hai lần | Đạt trong phạm vi M1–M3 | `TC-I02: 95 as-of rows; 96 after refresh; M1–M3 invariant to future schedule` |
| Thiếu bin, hash và snapshot replay | Snapshot thiếu bin ép VALID trả `422`. INCOMPLETE lưu được và bị chặn ở cửa forecast. Snapshot đủ replay chỉ lưu 1 bản cho ID đó | Ép VALID `422`. INCOMPLETE tồn tại, forecast `409`. Sai hash `422`. Hai snapshot lưu là một INCOMPLETE và một VALID. GET VALID khớp byte và SHA-256 sau replay | Đạt | `test_i02_cutoff_late_missing_and_future_schedule`, `test_i03_warmup_and_forecast_horizon` |
| TC-I03, mốc 60 giây | Cùng UTC, traffic bin `160` và measurement bin `100` | SQL đọc `[160, 100]` tại `10:02:40Z`. History Data và SDN lệch đúng `60` bin | Đạt | `PostgreSQL traffic bin and measurement bin` |
| Forecast producer nguyên dạng | Forecast đủ 48 × 5 đi qua validator của Data | `forecast_start` và `forecast_end` mang JSON float nguyên như `100.0`. Data yêu cầu integer nghiêm ngặt và trả `422`, DB không ghi forecast | **Lỗi giao diện ML → Data** | `raw ML float bin rejected by field validator` |
| TC-I03, H=5, batch và retry | 48 × 5 dòng, mốc `[100,105)`. Một batch và không thêm dự báo khi replay | Khi harness gửi offset dạng integer, lưu `1` batch và `240` dòng. GET READY trả `240/240`, kết thúc lúc `10:02:45Z`. Thiếu dòng hoặc dùng traffic offset trả `422`, cùng ID khác giá trị trả `409` | Đạt với kiểu wire integer | `test_i03_warmup_and_forecast_horizon`, `forecast_batches and predictions after replay and conflict` |
| Routing version thay đổi | Lưu v1 → v2. Từ chối v1 → v3 và decision mới dựa v1 sau khi có v2 | SQL trả `[1, 2]`. Nhảy version, decision cũ và snapshot mới tham chiếu v1 sau khi v2 sẵn có đều trả `409`. Snapshot v1 đã lưu vẫn replay ACK `0` | Đạt | `test_version_change_deadline_and_apply_replay` |
| Deadline hết trước áp dụng | Data trả `422` và không tạo tick/batch. SDN từ chối trước khi đổi routing version | HTTP `422`, SQL đọc `[0, 0]` từ `control_ticks` và `decision_batches`. Fixture SDN trả `REJECTED / DEADLINE_EXPIRED`, version `1` | Đạt | `control_ticks and decision_batches after expired deadline`, `apply.json/expired_response` |
| Kết quả apply mặc định | Kết quả có số nhóm luồng trùng decision đã lưu | Adapter mặc định chỉ có một dòng thay đổi. Data trả `422` vì decision có 48 nhóm. Decision vẫn chưa có response được lưu | **Lỗi giao diện SDN → Data khi gửi bản một dòng** | `test_version_change_deadline_and_apply_replay`, `apply.json/result` |
| Apply đủ nhóm, thứ tự công bố và replay | Publish routing trước kết quả. Với bản đủ nhóm, DB ghi một kết quả rồi ACK `updated: 0` khi replay | Publish sai thứ tự trả `409`. Dùng output adapter mở rộng 48 dòng, DB xác nhận version `2`, `1 APPLIED`, `47 KEPT`, `49` dòng routing history và `1` decision batch. Replay `updated: 1 → 0` | Đạt với tùy chọn mở rộng của adapter SDN | `stored state_version, APPLIED count, KEPT count`, `apply.json/result_full` |
| Run status và sự kiện | Sai role `401`. Sự kiện trùng giữ một dòng. Trạng thái kết thúc lặp không ghi lại | Sự kiện lưu `1` dòng, đổi nội dung cùng ID `409`. PATCH trạng thái lặp `updated: 0`, đổi trạng thái đã khóa `409` | Đạt | `test_runner_and_event_replay` |

### Các điểm chờ người phụ trách xác nhận

- **Data và Runner.** Lỗi đăng ký lặp tái hiện cả khi run đang chạy. `create_run` đưa `manifest` dưới dạng `bytes`, còn `psycopg2` đọc `BYTEA` thành `memoryview`. Phép so sánh trực tiếp trong `online_store.insert_once` báo khác với cùng nội dung byte. `VanNgHong` và `ndtchumoa` xác nhận kỳ vọng ACK và quy tắc so sánh.
- **ML và Data.** `yenpth265-alt` và `VanNgHong` xác nhận kiểu wire của hai trường bin. Fixture gốc và ca `422` được giữ để kiểm lại sau khi hai module thống nhất.
- **SDN, Optimization và Data.** `PhamAn56`, `dangtquang0509` và `VanNgHong` xác nhận cách dùng kết quả đủ 48 nhóm và thứ tự publish routing trước apply. Fixture `result_full` có đủ dữ liệu để kiểm. Đường publish runtime hiện xếp kết quả trước routing và chưa truyền tùy chọn mở rộng.

- **SDN, Data và ML.** Chốt schema snapshot đọc từ Data, nguồn `available_at_utc` và phép kiểm hash. TC-I02 dùng dữ liệu history từ DB để dựng lại đầu vào trên snapshot SDN. Cần chạy trực tiếp GET snapshot từ Data vào Predictor.
- **Data và Optimization.** Nối batch READY dạng danh sách với vector nhóm trong planner. Ca hiện tại dựng bản audit decision trong bộ test. Điều kiện đóng là đọc forecast, chạy planner và truy vết decision thực đến SDN qua cùng ID.

Lượt [45222814157](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/actions/runs/45222814157) dừng các ca phụ thuộc ở bước đăng ký lặp. Lượt [45223263846](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/actions/runs/45223263846) tách ca đó và phát hiện biến thể telemetry sai counter status nhận `422`. Lượt xác nhận [45223682285](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/actions/runs/45223682285) kiểm riêng counter status sai và xung đột nội dung hợp lệ, rồi ghi lại kết quả ở bảng trên.
