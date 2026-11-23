# Fixture cho API online

Chạy `python -m testing.scripts.capture_contract_fixtures testing/fixtures/online_contract` với các checkout producer được ghim trong workflow PostgreSQL. Fixture dùng dữ liệu kiểm thử do QA kiểm soát.

| File | Nguồn | Ca tái hiện |
|---|---|---|
| `telemetry.json` | Network `telemetry_collector.py --dry-run` | Lô 24 link, gửi trùng, sai số lượng và xung đột nội dung |
| `snapshot.json` | SDN `SnapshotCreator` và adapter Data v1 | TC-I02, TC-I03, thiếu bin và dữ liệu đến muộn |
| `forecast.json` | ML `Predictor.predict_batch`, persistence | TC-I03, 240 dòng và kiểm tra kiểu bin |
| `apply.json` | Optimization serializer, SDN `DecisionHandler` với driver in-memory và adapter Data v1 | Routing version, deadline, thiếu dòng và replay kết quả |

Commit nguồn và đầu mối xác nhận được ghi trong [sổ quyết định API online](../../../docs/architecture/Online_API_Team_Decisions.md).
