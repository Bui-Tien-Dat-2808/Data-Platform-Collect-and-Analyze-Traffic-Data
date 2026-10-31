# DANH MỤC TÀI LIỆU THAM KHẢO CHUYÊN SÂU & VẬN DỤNG VÀO ĐỀ TÀI TỐT NGHIỆP
> **Đề tài**: Xây dựng Data Platform tích hợp dữ liệu giao thông và phân tích hiện trạng giao thông đô thị  
> **Sinh viên**: Bùi Tiến Đạt (A45352) - Nguyễn Trọng Cương (A46595)  
> **GVHD**: Th.S Ngô Mạnh Cường - Trường Đại học Thăng Long  

---

## 1. PHÂN TÍCH HẠN CHẾ CỦA DANH MỤC TÀI LIỆU BAN ĐẦU
Trong đề cương ban đầu, danh mục gồm 12 tài liệu nhưng tồn tại một số điểm cần khắc phục:
1. Nhiều tài liệu chỉ là link documentation thuần túy của các công cụ (Kafka, Airflow, Spark, dbt, MinIO, PostgreSQL) mà chưa có các giáo trình, sách kinh điển chuẩn mực và bài báo khoa học về Data Engineering & Traffic Analytics.
2. Có những tài liệu tốt nghiệp không liên quan chuyên môn Data Engineering (như [12] "Phát triển hệ thống hỗ trợ cố vấn học tập trong công tác quản lý sinh viên" hoặc [11] về "Hệ thống điện 110 kV").
3. Chưa có các công trình nghiên cứu chuẩn quốc tế (IEEE, ACM) về mô hình đánh giá tắc nghẽn giao thông đô thị dựa trên dữ liệu GPS/Traffic Sensor.

---

## 2. BẢNG TÀI LIỆU THAM KHẢO BỔ SUNG CHUẨN IEEE / HỌC THUẬT

### Nhóm 1: Nền tảng Kiến trúc Dữ liệu & Data Engineering (Core Data Engineering & Architecture)
1. **Kleppmann, M. (2017).** *Designing Data-Intensive Applications: The Big Ideas Behind Reliable, Scalable, and Maintainable Systems.* O'Reilly Media. ISBN: 978-1449373320.
   - **Đóng góp**: Cuốn sách chuẩn mực nhất thế giới về hệ thống dữ liệu lớn.
   - **Vận dụng**: Áp dụng Chương 10 (Batch Processing), Chương 11 (Stream Processing) và Chương 12 (The Future of Data Systems) để xây dựng kiến trúc Decoupling giữa Kafka - Storage và giải quyết bài toán Eventual Consistency, Idempotency khi nạp dữ liệu từ TomTom API.

2. **Reis, J., & Housley, M. (2022).** *Fundamentals of Data Engineering: Plan and Build Robust Data Systems.* O'Reilly Media. ISBN: 978-1098108304.
   - **Đóng góp**: Chuẩn hóa toàn bộ vòng đời kỹ thuật dữ liệu (Data Engineering Lifecycle: Ingestion $\rightarrow$ Storage $\rightarrow$ Transformation $\rightarrow$ Serving) và các dòng chảy ngầm (Undercurrents: Data Quality, Orchestration, Observability).
   - **Vận dụng**: Làm khung lý thuyết chính cho Chương 2 và Chương 3 để giải trình vì sao chọn mô hình ELT thay vì ETL truyền thống.

3. **Kimball, R., & Ross, M. (2013).** *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling (3rd Edition).* John Wiley & Sons. ISBN: 978-1118530801.
   - **Đóng góp**: Phương pháp luận mô hình hóa dữ liệu chiều (Star Schema, Fact/Dimension).
   - **Vận dụng**: 
     - Thiết kế tầng Gold: Bảng `fact_traffic_flow_15min` (dạng Periodic Snapshot Fact Table đo lường trạng thái định kỳ 15 phút).
     - Bảng chiều `dim_road_segment`, `dim_corridor` và `dim_time_of_day` (phân đoạn giờ cao điểm/thấp điểm).

### Nhóm 2: Kiến trúc Lakehouse, Medallion Architecture & Phân tán
4. **Armbrust, M., Ghodsi, A., Xin, R., & Zaharia, M. (2021).** "Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics." *Proceedings of the 11th Conference on Innovative Data Systems Research (CIDR 2021)*.
   - **Vận dụng**: Giải thích sự dịch chuyển từ Data Warehouse truyền thống sang Data Lakehouse. Minh chứng cho việc lưu file Parquet trên MinIO kết hợp PostgreSQL Serving Layer giúp tiết kiệm chi phí và tăng tốc độ xử lý.

5. **Armbrust, M., et al. (2020).** "Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores." *Proceedings of the VLDB Endowment (PVLDB)*, Vol. 13, No. 12, pp. 3411-3424.
   - **Vận dụng**: Vận dụng kiến trúc Medallion (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) để phân chia ranh giới rõ ràng: Bronze (JSON thô nguyên bản không mất dữ liệu), Silver (làm sạch, khử trùng lặp, chuẩn hóa schema), Gold (bảng sự kiện sẵn sàng cho BI).

### Nhóm 3: Phân tích Dữ liệu Giao thông Đô thị & Chỉ số Ùn tắc (Urban Traffic Analytics)
6. **Zheng, Y. (2015).** "Trajectory Data Mining: An Overview." *ACM Transactions on Intelligent Systems and Technology (TIST)*, Vol. 6, No. 3, Article 29. DOI: 10.1145/2743025.
   - **Vận dụng**: Cung cấp cơ sở khoa học để xử lý tọa độ GPS, phân đoạn đoạn đường (road segmentation), khử nhiễu tín hiệu định vị và nội suy tốc độ hành trình.

7. **TomTom International BV (2024).** "TomTom Traffic Index: Methodology and Congestion Calculation." *TomTom Whitepapers & Documentation Portal*.
   - **Vận dụng**: Áp dụng công thức chuẩn quốc tế của TomTom để tính chỉ số trễ hành trình (Travel Delay Time) và mức độ suy giảm vận tốc so với vận tốc lưu thông tự do lý tưởng (Free-Flow Speed).

8. **Transportation Research Board (TRB) (2016).** *Highway Capacity Manual (HCM 2016): A Guide for Multimodal Mobility Analysis (6th Edition).* Washington, D.C.: The National Academies Press.
   - **Vận dụng**: Áp dụng bảng phân loại mức độ dịch vụ giao thông (Level of Service - LOS từ A đến F) dựa trên tỷ lệ vận tốc thực tế / vận tốc tự do ($\text{Speed Ratio} = \frac{v}{v_0}$) để trực quan hóa màu sắc trên Dashboard (Xanh - Vàng - Cam - Đỏ).

### Nhóm 4: Công nghệ Chuyển đổi và Kiểm định Chất lượng Dữ liệu
9. **dbt Labs (2024).** *The Analytics Engineering Guidebook: Testing, Documentation, and Modular SQL Modeling.* 
   - **Vận dụng**: Viết các data tests (`not_null`, `unique`, `accepted_values`) trong tầng Gold nhằm phát hiện tự động các điểm bất thường của dữ liệu (Data Drift / Anomalies).

---

## 3. BẢNG ÁNH XẠ VẬN DỤNG CỤ THỂ VÀO NỘI DUNG KHÓA LUẬN

| Mục lục đề tài | Nội dung triển khai trong đồ án | Tài liệu tham khảo tương ứng được trích dẫn |
| :--- | :--- | :--- |
| **Chương 1: Mở đầu** | Đặt vấn đề ùn tắc giao thông Hà Nội, nhu cầu xây dựng nền tảng dữ liệu tự động thay thế ghi nhận thủ công; xác định phạm vi 5 trục giao thông huyết mạch. | TRB (2016) [HCM 2016], TomTom Traffic Index Whitepaper. |
| **Chương 2: Cơ sở lý thuyết & Công nghệ** | - Trình bày vòng đời dữ liệu (Ingestion $\rightarrow$ Storage $\rightarrow$ Transformation $\rightarrow$ Serving).<br>- Phân biệt chi tiết ETL vs. ELT.<br>- Kiến trúc Medallion (Bronze/Silver/Gold).<br>- Nguyên lý Message Queue (Kafka) và Distributed Storage (MinIO). | Reis & Housley (2022), Kleppmann (2017), Armbrust et al. (2021). |
| **Chương 3: Phân tích & Thiết kế hệ thống** | - Thiết kế Pipeline Ingestion chịu lỗi.<br>- Thiết kế Data Model dạng Ngôi sao (Star Schema) cho PostgreSQL.<br>- Thiết lập các công thức đo lường: Speed Reduction Rate, Congestion Index, Travel Delay. | Kimball & Ross (2013), Zheng (2015), TomTom Traffic Documentation. |
| **Chương 4: Cài đặt, Thực nghiệm & Đánh giá** | - Cài đặt Docker Compose toàn bộ hệ thống.<br>- Viết Collector Python tích hợp retry/rate limit.<br>- Viết PySpark job chuẩn hóa Bronze $\rightarrow$ Silver.<br>- Chạy dbt chuyển đổi Silver $\rightarrow$ Gold và test chất lượng dữ liệu.<br>- Trực quan hóa Dashboard so sánh giờ cao điểm và ngày trong tuần. | dbt Labs (2024), Armbrust et al. (2020), Kafka & Airflow Docs. |
| **Chương 5: Kết luận & Hướng phát triển** | Đánh giá tỷ lệ thành công của pipeline (99%+), độ trễ ingestion, các hạn chế quota API và hướng mở rộng sang mô hình dự báo thời gian thực. | Kleppmann (2017) [Chương Future of Data Systems]. |

