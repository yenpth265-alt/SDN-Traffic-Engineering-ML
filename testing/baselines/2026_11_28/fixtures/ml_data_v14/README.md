# Batch ML bàn giao Data — 26/11/2026

`forecast_batch.json` được tạo bằng **Predictor thật và model đã khóa** trong `configs/ml/selected.yaml`: L=30, H=1, epsilon=0, W=5. Batch có 48 dòng, đủ 48 ID hex canonical; mỗi dòng chỉ có 7 trường Prediction, offset integer 100/101, chung run/issued_at/source.

`snapshot.json` là envelope v1.4 tổng hợp xác định, không phải dữ liệu live. Nó có 48×30 history rows và 48 verified routes; `context.json` gắn đúng hash/run/snapshot. `verification.json` ghi hash bytes từng file, content_hash và model hash. Không tự sửa nội dung để đưa qua validator.

Sinh lại từ thư mục gốc repo:

```powershell
python -m testing.scripts.generate_ml_data_fixture
python -m pytest ml/tests/test_data_handoff.py -q
```

Test sử dụng schema, HTTP endpoint, xác thực ML và hàm `ingest_forecast` hiện tại của Data; kết nối PostgreSQL được thay bằng cursor kiểm thử. Kết quả không thay cho kiểm chứng PostgreSQL thật.

Để đối chiếu trên Data API thật: Data cần có run tương ứng (M3, L=30/H=1, measurement origin và ml_config_hash trong context), topology đúng, cùng snapshot VALID đã lưu với demand/routing evidence tương ứng. Thời điểm snapshot available phải không sau issued_at trong batch. Sau đó POST nguyên `forecast_batch.json` tới `/v1/ml/forecasts` với ML key. Đây là fixture kiểm hợp đồng; không dùng cho tick điều khiển mạng thật.
