# Kiểm tra pipeline ML — 15/11/2026

Phạm vi: module ML mới theo Architecture v1.4 và Testing v2, với adapter file CSV/JSON/NPZ. Chưa có dữ liệu vận hành thật hoặc API Data/SDN để nghiệm thu tích hợp.

## Kết quả thực thi

- 35 kiểm thử đạt: python -m pytest ml/tests -q.
- Demo chạy đầy đủ prepare → train/validation → test evaluation → predict, trên dữ liệu tổng hợp 20 seed, chia 14/3/3.
- Lệnh một bước python -m ml run cũng chạy thành công với bộ dữ liệu demo.
- Demo chọn L=5, H=3, epsilon=0 và MA W=2; xuất 144 hàng dự báo. Các giá trị này chỉ là kết quả của fixture tổng hợp, không phải cấu hình được chọn cho benchmark dự án.
- Fixture H=5 kiểm đúng 240 hàng; H=1 kiểm đúng 48 hàng.
- Kết quả pytest dạng XML lưu tại ml/artifacts/pytest-results.xml; artifact demo dưới ml/artifacts/ được bỏ qua trong Git.

## Đối chiếu yêu cầu

| Nhóm test | Nội dung đã kiểm tra |
| --- | --- |
| TC-D03 | Từ chối split 50/25/25, family chồng split và chuỗi demand giống nhau bị đổi run ID |
| TC-D04 / AC-M01 | X/y đúng cửa sổ và origin, join sample_id, thiếu/trùng bin, dữ liệu future/unavailable |
| TC-M01 | Persistence 2,3,4 → 4; MA W=3 → 3; lag_1 gần nhất; mean-H và pooled errors tách riêng |
| TC-M02 | Hash model/config sai, feature order sai, output sai shape, raw âm chuẩn hóa, output hữu hạn >capacity không bị clip |
| TC-M03 | Thiếu/invalid/stale history phải skip; RF NaN/exception/sai shape chỉ fallback khi policy cho phép; batch fallback cùng source; timeout |
| TC-M04 | Raw 4, epsilon 0.1 → final 4.4; không áp hai lần; hòa/không cải thiện chọn epsilon 0 |
| TC-I03 / AC-M07 | Đúng 7 trường, 48×H hàng, cùng issued_at/source, offset measurement 100–105 |
| Train/test boundary | Train chỉ load train+validation; evaluate mới load test; thay y không đổi features |
| CLI | Lỗi artifact chỉ ghi audit, không tạo prediction; từ chối dùng chung file rows/audit |

## Giới hạn

Chưa kiểm chứng độ chính xác trên dữ liệu thật, hiệu năng mạng, ngân sách latency môi trường triển khai, API vận chuyển, hoặc tính tương thích với serializer snapshot tương lai của Data/SDN. Các role cần thống nhất adapter và canonical hash tại ml/docs/DATA_CONTRACT.md.

Timeout hiện chặn công bố kết quả muộn, không cưỡng bức ngắt hàm RF đồng bộ. Hard deadline thuộc worker/Runner. Không coi test module đạt là các gate tích hợp G3/G4 hoặc benchmark đã nghiệm thu.
