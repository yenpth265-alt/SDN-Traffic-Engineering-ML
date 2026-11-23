# Fixture cho API online

Chạy `python -m testing.scripts.capture_contract_fixtures testing/fixtures/online_contract` với các checkout producer được ghim trong workflow PostgreSQL. Fixture dùng dữ liệu kiểm thử do QA kiểm soát.

| File | Nguồn | Ca tái hiện |
|---|---|---|
| `telemetry.json` | Network `telemetry_collector.py --dry-run` | Lô 24 link, gửi trùng, sai số lượng và xung đột nội dung |
| `snapshot.json` | SDN `SnapshotCreator` và adapter Data v1 | TC-I02, TC-I03, thiếu bin và dữ liệu đến muộn |
| `forecast.json` | ML `Predictor.predict_batch`, persistence | TC-I03, 240 dòng và kiểm tra kiểu bin |
| `apply.json` | Optimization serializer, SDN `DecisionHandler` với driver in-memory và adapter Data v1 | Routing version, deadline, thiếu dòng và replay kết quả |

Commit nguồn và đầu mối xác nhận được ghi trong [sổ quyết định API online](../../../docs/architecture/Online_API_Team_Decisions.md).

## Phạm vi sử dụng

Lượt PostgreSQL đã chạy các fixture qua API và validator Data. Telemetry là đầu ra dry-run, forecast dùng mô hình Persistence với dữ liệu demo và apply dùng driver in-memory. Ca dương forecast chuẩn hóa hai offset thành JSON integer trong bộ test. Ca dương apply dùng `result_full`, được adapter sinh với tùy chọn mở rộng. Giữ nguyên mẫu forecast dạng float và apply một dòng để tái hiện các phản hồi 422.

Xem [sổ kết quả](../../results/README.md) để đối chiếu phạm vi TC-I02/TC-I03 và các ca cần chạy lại với đầu ra producer nguyên dạng.
