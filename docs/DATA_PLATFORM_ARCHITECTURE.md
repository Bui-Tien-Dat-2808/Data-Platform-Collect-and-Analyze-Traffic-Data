# KIẾN TRÚC TỔNG THỂ DATA PLATFORM GIAO THÔNG ĐÔ THỊ HÀ NỘI
> **Đề tài Chuyên đề Tốt nghiệp**: Xây dựng Data Platform tích hợp dữ liệu giao thông và phân tích hiện trạng giao thông đô thị  
> **Sinh viên thực hiện**: Bùi Tiến Đạt (A45352) - Nguyễn Trọng Cương (A46595)  
> **Giảng viên hướng dẫn**: Th.S Ngô Mạnh Cường  
> **Đơn vị**: Khoa Công nghệ Thông tin - Trường Đại học Thăng Long  

---

## 1. TỔNG QUAN HỆ THỐNG VÀ NGUYÊN LÝ THIẾT KẾ

Hệ thống được thiết kế theo tư tưởng **ELT (Extract - Load - Transform)** kết hợp mô hình **Medallion Architecture (Bronze - Silver - Gold)** trên nền tảng **Lakehouse**. Mục tiêu cốt lõi là giải quyết bài toán thu thập dữ liệu giao thông định kỳ từ TomTom API, đảm bảo tính bất biến của dữ liệu gốc, chuẩn hóa làm sạch bằng xử lý phân tán, mô hình hóa dữ liệu nhiều chiều phục vụ phân tích chuyên sâu và trực quan hóa thời gian thực / lịch sử.

```
+---------------------------------------------------------------------------------------------------+
|                                     DATA SOURCES & INGESTION                                      |
|  +--------------------+       HTTP GET        +-----------------------+        Kafka Producer     |
|  | TomTom Traffic API | --------------------> | Python API Collector  | --------------------+     |
|  | (Flow Segment)     |  (Rate Limit/Retry)   | (State/Lineage Audit) |                     |     |
+--+--------------------+-----------------------+-----------------------+---------------------+-----+
                                                                                              |
+---------------------------------------------------------------------------------------------v-----+
|                                   STREAM BUFFER & LAKEHOUSE                                       |
|  +------------------------+      Kafka Consumer      +-----------------------------------------+  |
|  | Apache Kafka           | -----------------------> | MinIO (Object Storage)                  |  |
|  | Topic: traffic.raw.tt  |                          | - Bronze Layer: Raw JSON + Audit Ingest |  |
|  +------------------------+                          | - Silver Layer: Parquet Partitioned     |  |
+------------------------------------------------------+-+---------------------------------------+--+
                                                         |
                                                         | PySpark Processing
                                                         v
+---------------------------------------------------------------------------------------------------+
|                               PROCESSING & TRANSFORMATION (SILVER -> GOLD)                        |
|  +-----------------------+      Transform/Write      +-----------------------------------------+  |
|  | Apache Spark (PySpark)| ------------------------> | Silver Layer (MinIO - Cleaned Parquet)  |  |
|  | (Deduplicate, Clean)  |                           +--------------------+--------------------+  |
+--+-----------------------+                                                |                       |
                                                                            | dbt Engine            |
                                                                            v                       |
+---------------------------------------------------------------------------------------------------+
|                                 SERVING LAYER & DIMENSIONAL MODELING                              |
|  +---------------------------------------------------------------------------------------------+  |
|  | PostgreSQL Data Warehouse (Gold Layer)                                                      |  |
|  | - Dimensions: Dim_Corridor, Dim_Road_Segment, Dim_Date, Dim_TimeOfDay                         |  |
|  | - Facts: Fact_Traffic_Flow_15Min, Fact_Hourly_Traffic_Summary                               |  |
+--+---------------------------------------------------------------------------------------------+--+
                                  |                                       |
                                  v                                       v
+----------------------------------------------------+ +--------------------------------------------+
| POWER BI / APACHE SUPERSET (DASHBOARDS)            | | APACHE AIRFLOW (ORCHESTRATOR)              |
| - Bản đồ tắc nghẽn & Heatmap các tuyến đường       | | - DAG Ingestion (Schedule 15 phút)         |
| - Biểu đồ suy giảm vận tốc (Speed Reduction Ratio) | | - DAG Batch Transformation (Spark & dbt)   |
| - So sánh giờ cao điểm sáng/chiều & ngày lễ/thường | | - Sensor, Retry, Logging & Alerts         |
+----------------------------------------------------+ +--------------------------------------------+
```

---

## 2. CHI TIẾT CÁC TẦNG KIẾN TRÚC (MEDALLION LAYERS)

### 2.1. Tầng Thu Thập Dữ Liệu (Ingestion Layer)
- **Công nghệ**: Python 3.11+, Requests, Tenacity (Retry exponential backoff), Confluent-Kafka Python Client.
- **Nguồn cấp**: TomTom Traffic Flow Segment API (`/traffic/services/4/flowSegmentData/relative0/10/json`).
- **Chiến lược Ingestion**:
  - Tần suất quét: Định kỳ 15 phút/lần (thỏa mãn hạn mức gói Free 2,500 requests/ngày: với 11 điểm giám sát tại Hà Nội, mỗi ngày tiêu tốn `11 * 4 * 24 = 1,056 requests` < hạn mức 2,500).
  - Quản lý metadata: Bổ sung các trường audit ngay khi nhận phản hồi: `ingestion_timestamp_utc`, `collector_version`, `status`, `latency_ms`, `corridor_id`.
  - Cơ chế chịu lỗi: Tự động retry khi gặp lỗi mạng hoặc HTTP 429 (Rate Limit), circuit breaker ngắt nhịp khi TomTom API bảo trì.

### 2.2. Tầng Đệm & Truyền Tải (Buffer & Streaming Layer)
- **Công nghệ**: Apache Kafka (Zookeeper-less với KRaft mode hoặc Docker-compose Kafka).
- **Topic**: `traffic.raw.tomtom`
- **Phân vùng (Partitioning Key)**: `corridor_id` để đảm bảo thứ tự thời gian của từng tuyến đường.
- **Vai trò**: Tách rời (decoupling) quá trình cào dữ liệu từ API bên ngoài với quá trình ghi vào Lakehouse, chống nghẽn và rớt gói khi mạng biến động.

### 2.3. Tầng Bronze (Raw Storage - MinIO)
- **Công nghệ**: MinIO S3-Compatible Object Storage.
- **Format**: JSON gốc nén gzip (`.json.gz`).
- **Cấu trúc lưu trữ (Partitioning)**:
  ```
  s3://lakehouse-bronze/tomtom/flow/year=YYYY/month=MM/day=DD/traffic_raw_YYYYMMDD_HHmm.json
  ```
- **Nguyên tắc**: Dữ liệu lưu trữ bất biến (Immutable), giữ nguyên 100% payload từ TomTom API kết hợp header audit.

### 2.4. Tầng Silver (Cleaned & Standardized - MinIO / Spark)
- **Công nghệ**: Apache Spark 3.5 / PySpark.
- **Format**: Apache Parquet (tối ưu nén Snappy, lưu trữ dạng cột).
- **Nhiệm vụ PySpark Pipeline**:
  1. Đọc dữ liệu Bronze theo partition thời gian.
  2. Phẳng hóa (flatten) mảng JSON lồng nhau (`flowSegmentData.coordinates`, `currentSpeed`, `freeFlowSpeed`, `confidence`).
  3. Kiểm tra tính toàn vẹn (Data Quality Checks): loại bỏ bản ghi có `currentSpeed < 0` hoặc `freeFlowSpeed <= 0` hoặc `confidence < 0.5`.
  4. Chuẩn hóa kiểu dữ liệu: Timestamp UTC sang Giờ địa phương Việt Nam (`Asia/Ho_Chi_Minh` UTC+7).
  5. Khử trùng lặp (Deduplication) dựa trên khóa kép `(point_id, event_timestamp)`.
  6. Ghi vào Silver Lakehouse dưới định dạng Parquet theo phân vùng `year/month/day/corridor_id`.

### 2.5. Tầng Gold (Serving & Dimensional Modeling - PostgreSQL & dbt)
- **Mô hình hóa (Kimball Dimensional Modeling)**:
  - **Bảng chiều (Dimensions)**:
    - `dim_corridor`: Thông tin tuyến đường chính (Nguyễn Trãi, Vành Đai 3, Cầu Giấy,...).
    - `dim_road_segment`: Đoạn đường chi tiết, tọa độ, cấp đường (`FRC`).
    - `dim_date`: Ngày, thứ trong tuần, cờ ngày làm việc / ngày nghỉ / ngày lễ.
    - `dim_time_of_day`: Khung giờ (00:00 - 23:59), đánh dấu khung giờ cao điểm sáng (07:00 - 09:00), cao điểm chiều (17:00 - 19:30).
  - **Bảng sự kiện (Fact Tables)**:
    - `fact_traffic_flow_15min`: Lưu vận tốc hiện tại, vận tốc lưu thông tự do, thời gian di chuyển, tỷ lệ suy giảm vận tốc.
    - `fact_hourly_traffic_summary`: Tổng hợp chỉ số trung bình theo giờ để phục vụ truy vấn báo cáo siêu tốc.
- **Công nghệ chuyển đổi**: **dbt (data build tool)** thực thi các model SQL, kèm theo bộ kiểm thử dữ liệu tự động (`dbt test`: `not_null`, `unique`, `accepted_values`, `relationships`).

### 2.6. Tầng Trực Quan Hóa (BI & Dashboards)
- **Công cụ**: Power BI hoặc Apache Superset kết nối trực tiếp đến PostgreSQL Serving Layer.
- **Bộ chỉ số đo lường chính (Key Performance Indicators)**:
  - **Tỷ lệ suy giảm tốc độ (Speed Reduction Rate - SRR)**:
    $$\text{SRR} = \frac{\text{freeFlowSpeed} - \text{currentSpeed}}{\text{freeFlowSpeed}} \times 100\%$$
    - $\text{SRR} < 20\%$: Thông thoáng (Mức độ A/B).
    - $20\% \le \text{SRR} < 50\%$: Đông đúc, di chuyển chậm (Mức độ C/D).
    - $\text{SRR} \ge 50\%$: Ùn tắc nghiêm trọng (Mức độ E/F).
  - **Thời gian trễ hành trình (Travel Delay Time - TDT)**:
    $$\text{TDT} = \text{currentTravelTime} - \text{freeFlowTravelTime} \quad (\text{giây})$$
  - **Chỉ số tắc nghẽn tương đối (Congestion Index - CI)**:
    $$\text{CI} = \frac{\text{currentTravelTime}}{\text{freeFlowTravelTime}}$$

---

## 3. ĐIỀU PHỐI VÀ VẬN HÀNH (ORCHESTRATION & DEVOPS)

1. **Apache Airflow DAGs**:
   - `dag_tomtom_ingestion_15min`: Kích hoạt Collector mỗi 15 phút, kiểm tra tính sẵn sàng của Kafka & MinIO Bronze.
   - `dag_daily_lakehouse_transform`: Chạy định kỳ nửa đêm (hoặc mỗi giờ) kích hoạt PySpark gom batch Bronze sang Silver, sau đó chạy `dbt run` và `dbt test` nạp Gold vào PostgreSQL.
2. **Container hóa toàn diện (Docker & Docker Compose)**:
   - Các service bao gồm: Kafka, Zookeeper, MinIO, Spark Master, Spark Worker, PostgreSQL, Airflow Webserver/Scheduler, Superset.
   - Quản lý cấu hình tập trung qua file `.env`.

