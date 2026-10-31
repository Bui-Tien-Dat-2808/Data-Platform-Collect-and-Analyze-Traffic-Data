# Traffic Data Platform - Chuyên đề Tốt nghiệp Thăng Long University

> **Đề tài**: Xây dựng Data Platform tích hợp dữ liệu giao thông và phân tích hiện trạng giao thông đô thị  
> **Sinh viên thực hiện**: Bùi Tiến Đạt (A45352) - Nguyễn Trọng Cương (A46595)  
> **Giảng viên hướng dẫn**: Th.S Ngô Mạnh Cường  
> **Khoa**: Công nghệ Thông tin - Trường Đại học Thăng Long  

---

## 🚀 1. Tổng quan Kiến trúc Hệ thống

Dự án áp dụng kiến trúc **Medallion Lakehouse (Bronze -> Silver -> Gold)** với luồng xử lý **ELT**:
- **Ingestion**: Python Collector thu thập dữ liệu lưu thông thời gian thực từ **TomTom Traffic Flow API** định kỳ 15 phút.
- **Buffer / Streaming**: **Apache Kafka** phân tách tải và đảm bảo khả năng chịu lỗi.
- **Bronze Layer (MinIO)**: Lưu trữ JSON thô nguyên bản kèm metadata kiểm tra nguồn gốc (lineage).
- **Silver Layer (MinIO / PySpark)**: Chuẩn hóa schema, lọc null/outlier, khử trùng lặp và tính toán chỉ số suy giảm vận tốc (Parquet).
- **Gold Layer (PostgreSQL / dbt)**: Mô hình hóa dữ liệu dạng Star Schema (Kimball), tổng hợp theo khung giờ/ngày và kiểm tra chất lượng tự động (`dbt test`).
- **Orchestration**: **Apache Airflow** điều phối toàn diện DAGs.
- **Serving / BI**: **Power BI / Apache Superset** trực quan hóa bản đồ nhiệt và biểu đồ tắc nghẽn.

Chi tiết thiết kế kiến trúc xem tại: [`docs/DATA_PLATFORM_ARCHITECTURE.md`](./docs/DATA_PLATFORM_ARCHITECTURE.md)  
Danh mục tài liệu tham khảo và cách vận dụng xem tại: [`docs/REFERENCES_AND_APPLICATION.md`](./docs/REFERENCES_AND_APPLICATION.md)

---

## 📦 2. Cấu trúc Thư mục Dự án

```
Data Platform/
├── .agent/skills/             # 13 Agent Skills Data Engineering chuyên sâu (cloned)
├── skills/                    # Thư mục skills dự phòng tiện tra cứu
├── airflow/
│   └── dags/                  # Airflow DAG điều phối pipeline
├── collector/
│   ├── tomtom_collector.py    # Python Ingestion script gọi TomTom Traffic API
│   └── sample_batch.json      # Mẫu 12 bản ghi thực tế vừa thu thập từ API
├── config/
│   └── hanoi_corridors.json   # Cấu hình 5 trục giao thông trọng điểm Hà Nội
├── dbt/
│   └── models/                # Dimensional models (Gold layer) & schema tests
├── docker/
│   └── docker-compose.yml     # Khởi tạo Kafka, MinIO, Postgres
├── docs/
│   ├── DATA_PLATFORM_ARCHITECTURE.md
│   └── REFERENCES_AND_APPLICATION.md
├── spark/
│   └── jobs/                  # PySpark Bronze to Silver batch cleaning job
├── .env                       # Chứa TOMTOM_API_KEY và biến môi trường
├── .env.example
└── README.md
```

---

## 🛠️ 3. Hướng dẫn Chạy Thử nghiệm (Quick Start)

### 3.1. Cấu hình TomTom API
Khóa API đã được nạp sẵn vào file `.env`:
```bash
TOMTOM_API_KEY=kAk4RrBfvcgLurgaGP5yVeVER0Dz50zd
```

### 3.2. Chạy thử Collector cào dữ liệu Hà Nội
```powershell
python -m pip install requests python-dotenv
python collector/tomtom_collector.py
```
Dữ liệu 12 điểm trên 5 trục giao thông Hà Nội sẽ được lưu vào `collector/sample_batch.json`.

### 3.3. Khởi động môi trường Docker
```powershell
cd docker
docker-compose up -d
```
Truy cập:
- **MinIO Console**: http://localhost:9001 (User: `minioadmin` / Pass: `minioadmin`)
- **PostgreSQL**: `localhost:5432` (DB: `traffic_dw`, User: `postgres`)

