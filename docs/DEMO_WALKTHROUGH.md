# KỊCH BẢN CHẠY DEMO TOÀN DIỆN (END-TO-END DEMO GUIDE)
> Hướng dẫn từng bước để 2 bạn sinh viên (Đạt & Cương) chạy thử nghiệm và quay video / trình chiếu cho Giảng viên hướng dẫn.

---

## 🏁 BƯỚC 1: KHỞI ĐỘNG HẠ TẦNG DOCKER
Mở PowerShell tại thư mục `docker` và gõ:
```powershell
cd docker
docker compose up -d
```
Kiểm tra trạng thái các dịch vụ:
- **MinIO Console**: Mở trình duyệt vào [http://localhost:9001](http://localhost:9001) (User: `minioadmin` / Pass: `minioadmin`)
- **PostgreSQL**: Cổng `5433` (DB: `traffic_dw`, User: `postgres`, Pass: `postgres_traffic_2026`)
- **Kafka**: Cổng `9092`

---

## 📡 BƯỚC 2: CÀO DỮ LIỆU THỰC TẾ HÀ NỘI TỪ TOMTOM API (INGESTION -> BRONZE)
Tại thư mục gốc của dự án:
```powershell
python collector/tomtom_collector.py
```
**Điều gì xảy ra:**
- Hệ thống gọi TomTom API cho 12 điểm giao thông trên 5 trục đường chính Hà Nội (Nguyễn Trãi, Vành Đai 3, Cầu Giấy, Xã Đàn, Giải Phóng).
- Dữ liệu thô JSON tự động được lưu cục bộ và đẩy thẳng lên bucket **`lakehouse-bronze`** trên MinIO theo phân vùng `tomtom/flow/YYYY/MM/DD/`.
- F5 trình duyệt MinIO để xem file JSON thô vừa xuất hiện.

---

## ⚙️ BƯỚC 3: LÀM SẠCH VÀ TÍNH TOÁN CHỈ SỐ GIAO THÔNG (BRONZE -> SILVER)
Chạy lệnh:
```powershell
python spark/jobs/bronze_to_silver.py collector/sample_batch.json data/silver_traffic_parquet
```
**Điều gì xảy ra:**
- Dữ liệu được lọc bỏ bản ghi lỗi (tốc độ âm, confidence < 0.5), khử trùng lặp.
- Tính toán:
  - Tỷ lệ suy giảm tốc độ $\text{SRR} = \frac{\text{freeFlowSpeed} - \text{currentSpeed}}{\text{freeFlowSpeed}} \times 100\%$
  - Độ trễ di chuyển $\text{TDT} = \text{currentTravelTime} - \text{freeFlowTravelTime}$
  - Phân loại mức độ ùn tắc (`LOW / FREE_FLOW`, `MODERATE`, `HEAVY / CONGESTED`).
- Xuất dữ liệu Parquet phân vùng theo `corridor_id` và tự động đồng bộ lên bucket **`lakehouse-silver`** trên MinIO!

---

## 🗄️ BƯỚC 4: NẠP VÀO POSTGRESQL VÀ CHẠY DBT MODELING (SILVER -> GOLD)
Chạy lệnh nạp dữ liệu Silver vào bảng trung gian:
```powershell
python spark/jobs/load_to_postgres.py
```
Sau đó chạy dbt để tạo mô hình dữ liệu đa chiều (Star Schema):
```powershell
cd dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
```
**Điều gì xảy ra:**
- Tạo bảng chiều: `dim_corridor`
- Tạo bảng sự kiện 15 phút: `fact_traffic_flow_15min`
- Tạo bảng tổng hợp theo giờ: `fact_hourly_traffic_summary`
- Chạy tự động các test kiểm thử chất lượng dữ liệu (`not_null`, `unique`, `accepted_values`).

---

## 📊 BƯỚC 5: TRỰC QUAN HÓA (DBeaver / Power BI)
Kết nối DBeaver hoặc Power BI tới:
- **Host**: `127.0.0.1`
- **Port**: `5433`
- **Database**: `traffic_dw`
- **Username**: `postgres`
- **Password**: `postgres_traffic_2026`

Chạy câu truy vấn kiểm tra kết quả ngay trong PostgreSQL:
```sql
SELECT 
    corridor_name,
    point_name,
    current_speed_kmh,
    free_flow_speed_kmh,
    speed_reduction_ratio_pct,
    congestion_level
FROM fact_traffic_flow_15min
ORDER BY speed_reduction_ratio_pct DESC;
```

