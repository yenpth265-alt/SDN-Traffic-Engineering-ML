# Kiểm thử SDN Traffic Engineering

[Đợt PostgreSQL gần nhất](results/README.md) ghi 16/17 ca đạt tại `44acd20`. Bản rà soát tài liệu dùng `main@db2dacd` ngày 24/11/2026. Nghiệm thu TC-I01 và benchmark M0–M4 đang chờ.

| Tài liệu | Nội dung |
| --- | --- |
| [Kế hoạch kiểm thử](docs/test_plan_v2.md) | Ma trận ca, đối chứng M0–M4 và quy trình thực nghiệm. |
| [Tiêu chí nghiệm thu](docs/acceptance_criteria_v2.md) | Điều kiện đạt và bằng chứng bắt buộc theo module. |
| [Giao diện](docs/interfaces_v2.md) | Hợp đồng đích và các điểm cần chốt so với Data API v1. |
| [Yêu cầu tích hợp](docs/integration_requirements.md) | Trạng thái REQ-01–REQ-16 và điều kiện đóng. |
| [Sổ quyết định API](../docs/architecture/Online_API_Team_Decisions.md) | Commit, fixture, đầu mối và kết quả đọc lại DB. |
| [Fixture producer](fixtures/online_contract/README.md) | Nguồn dữ liệu và các ca tái hiện. |

Hướng dẫn tái hiện nằm trong [sổ quyết định](../docs/architecture/Online_API_Team_Decisions.md#tái-hiện-trên-postgresql) và [workflow CI](../.github/workflows/postgresql_interface_contracts.yml).
