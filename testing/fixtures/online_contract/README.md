# Fixture API online

Capture lại từ cùng checkout bằng:

```bash
python -m testing.scripts.capture_contract_fixtures testing/fixtures/online_contract
```

`capture_provenance.json` ghi SHA Git, đường dẫn import và hash các file producer thực thi. `snapshot.json.sdn` là snapshot v1.4 đầy đủ; HTTP dùng `{ "sdn": ... }`. Phần `data_v1` là input compact dành riêng cho test chuyển đổi fixture trong `snapshot_factory.py`, không gửi trực tiếp lên API.

`forecast.json` hiện chứa integer bins do Predictor xuất nguyên dạng. Ca từ chối float tạo bản sao sai kiểu trong test. Fixture nhỏ L2/H5 phục vụ kiểm tra schema; model triển khai vẫn là L30/H1 trong `configs/ml/selected.yaml`. Fixture L30 hiện hành nằm tại `sdn/fixtures/data_v14_v10`.

Telemetry là dry-run 24 link đo được. Apply dùng driver in-memory và giữ bản một dòng cùng bản đủ 48 dòng để kiểm tra từ chối/round-trip. Đây là kiểm thử contract, không phải bằng chứng live OVS.
