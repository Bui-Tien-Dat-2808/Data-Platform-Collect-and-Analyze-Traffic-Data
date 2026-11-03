## 🚀 1. Tổng quan Kiến trúc Hệ thống

Dự án áp dụng kiến trúc **Medallion Lakehouse (Bronze → Silver → Gold)** với luồng xử lý **ELT**:

| Tầng | Công nghệ | Vai trò |
| :--- | :--- | :--- |
| **Ingestion** | Python · TomTom Traffic API | Thu thập dữ liệu lưu thông & sự cố giao thông theo định kỳ |
| **Stream Buffer** | Apache Kafka (KRaft) | Phân tách tải, đảm bảo khả năng chịu lỗi |
| **Bronze Layer** | MinIO (S3-compatible) | Lưu trữ JSON thô nguyên bản kèm metadata lineage |
| **Silver Layer** | PySpark · MinIO | Chuẩn hóa schema, lọc null/outlier, khử trùng lặp, tính chỉ số phân tích (Parquet) |
| **Gold Layer** | PostgreSQL · dbt | Star Schema (Kimball), tổng hợp theo khung giờ/ngày, data quality tests |
| **Orchestration** | Apache Airflow | Điều phối toàn bộ DAGs |
| **Serving / BI** | Power BI | Bản đồ nhiệt, biểu đồ tắc nghẽn, dashboard thời gian thực |

Chi tiết thiết kế kiến trúc: [`docs/DATA_PLATFORM_ARCHITECTURE.md`](./docs/DATA_PLATFORM_ARCHITECTURE.md)  
Tài liệu tham khảo và vận dụng: [`docs/REFERENCES_AND_APPLICATION.md`](./docs/REFERENCES_AND_APPLICATION.md)

---

## 📦 2. Cấu trúc Thư mục Dự án

```
Data Platform/
├── airflow/
│   └── dags/
│       └── dag_traffic_pipeline.py   # DAG điều phối toàn bộ pipeline (15 phút/lần)
├── collector/
│   ├── tomtom_collector.py           # Ingestion: TomTom Flow Segment API (12 điểm cố định)
│   └── hanoi_incident_collector.py   # Ingestion: TomTom Incident API (toàn thành phố, bbox)
├── config/
│   └── hanoi_corridors.json          # Cấu hình 5 trục giao thông trọng điểm Hà Nội
├── dbt/
│   └── models/                       # Dimensional models (Gold layer) & schema tests
├── docker/
│   └── docker-compose.yml            # Khởi tạo PostgreSQL, MinIO, Kafka
├── docs/
│   ├── DATA_PLATFORM_ARCHITECTURE.md
│   └── REFERENCES_AND_APPLICATION.md
├── spark/
│   └── jobs/                         # PySpark batch jobs (Bronze → Silver)
├── .env.example                      # Template biến môi trường (KHÔNG chứa giá trị thật)
├── requirements.txt
└── README.md
```

---

## 🛠️ 3. Hướng dẫn Cài đặt & Chạy (Quick Start)

### 3.1. Điều kiện tiên quyết

- Python 3.10+
- Docker Desktop (để chạy PostgreSQL, MinIO, Kafka)
- TomTom API Key (đăng ký miễn phí tại [developer.tomtom.com](https://developer.tomtom.com))

### 3.2. Cấu hình biến môi trường

Sao chép file template và điền các giá trị thực tế:

```powershell
copy .env.example .env
# Mở file .env và điền TOMTOM_API_KEY cùng các thông tin khác
```

> **Bảo mật**: File `.env` đã được thêm vào `.gitignore` — **không bao giờ commit file `.env` lên Git**.

### 3.3. Cài đặt Python Dependencies

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

### 3.4. Khởi động hạ tầng Docker

```powershell
cd docker
docker-compose up -d

# Kiểm tra trạng thái
docker ps
```

Sau khoảng 30 giây, các service sau sẵn sàng:

| Service | URL / Địa chỉ | Credentials |
| :--- | :--- | :--- |
| MinIO Console | http://localhost:9001 | Xem file `.env` |
| PostgreSQL | `localhost:5433` · DB: `traffic_dw` | Xem file `.env` |
| Kafka Broker | `localhost:9092` | — |

> **Lưu ý**: PostgreSQL expose ra cổng **5433** (không phải 5432) để tránh xung đột với cài đặt PostgreSQL local.

### 3.5. Chạy thử Collector

```powershell
# Thu thập dữ liệu 12 điểm cố định (Flow Segment API)
python collector/tomtom_collector.py

# Thu thập toàn bộ sự cố nội thành Hà Nội (Incident API - bounding box)
python collector/hanoi_incident_collector.py
```

### 3.6. Chạy PySpark Transformation (Bronze → Silver)

```powershell
python spark/jobs/bronze_to_silver.py
python spark/jobs/bronze_to_silver_incidents.py
```

### 3.7. Chạy dbt Gold Models

```powershell
cd dbt
dbt debug    # Kiểm tra kết nối
dbt run      # Chạy toàn bộ models
dbt test     # Kiểm tra chất lượng dữ liệu
```

### 3.8. Kết nối Power BI

Dùng connector **PostgreSQL** trong Power BI Desktop với thông tin từ file `.env`.  
Khuyến nghị dùng **DirectQuery** để biểu đồ tự động cập nhật theo dữ liệu mới nhất.

