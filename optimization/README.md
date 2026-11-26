# Optimization Module — SDN Traffic Engineering ML

Phân hệ lập lịch tối ưu định tuyến MILP cho 48 nhóm luồng trên hạ tầng SDN OpenFlow 1.3.

## 1. Cấu trúc thư mục
- `configs/optimization/optimization.yaml`: Tham số Trigger (`TH`, `N`), Hysteresis (`h`), Hold-down (`T_hold`), ngân sách đổi tuyến (`B`), trọng số phạt (`lambda_weight`), dung lượng liên kết mặc định (`C_e`) và quỹ thời gian (`deadline_ms`).
- `optimization/config_loader.py`: Nạp cấu hình và tính `config_hash` (SHA-256 canonical).
- `optimization/input_validator.py`: Kiểm tra `RoutingState` (`VERIFIED`, `data_sync_gate`, `state_version`) và `ForecastBatch` (`READY`, đủ `48 x H` phần tử, integer bin offset).
- `optimization/load_estimator.py`: Nạp topology từ `configs/network/topology_bundle.json` (48 flows, 32 links, 96 candidate paths) và tính tải dự báo trung bình $b_f$ trên $H$ bước (không cộng trùng `epsilon`).
- `optimization/milp/`: Xây dựng mô hình MILP (`model.py`, `objective.py`, `constraints.py`).
- `optimization/solver/solver.py`: Giải MILP qua PuLP (CBC/HiGHS) kèm bộ giải nhánh-và-cận dự phòng (`Exact Branch-and-Bound`).
- `optimization/result_serializer.py`: Xuất `DecisionBatch` đủ 48 dòng (gửi Data API) và `ApplyRequest` tối đa $B$ dòng đổi tuyến khớp `sdn/schemas/apply_models.py` (gửi SDN Controller).
- `optimization/service_client.py`: Cầu nối HTTP thực thi đúng thứ tự `POST /v1/optimization/decisions` (Data) trước `POST /v1/routing/apply` (SDN).
- `optimization/scripts/export_contract_fixtures.py`: Xuất bộ fixture JSON 48-flow dùng chung cho kiểm thử liên thông SDN / Data / QA.

## 2. Chạy kiểm thử độc lập
```bash
pip install -r optimization/requirements.txt
python -m optimization.scripts.export_contract_fixtures
python -m unittest discover -s optimization/tests -v
