# 📋 KẾ HOẠCH & PHÂN CHIA NHIỆM VỤ DỰ ÁN (CẬP NHẬT)
> **Mục tiêu**: Xây dựng nền tảng Giám sát Giao thông Đô thị Đa Thành phố (**Hà Nội, TP.HCM, Đà Nẵng**) và **Website Bản đồ Giao thông Thời gian thực**.  
> **Thời hạn hoàn thành (Deadline)**: **09:00 AM — Thứ Năm, ngày 19/11/2026**

---

## 👨‍💻 PHẦN 1: BÙI TIẾN ĐẠT (Phụ trách: Data Pipeline & Web Backend API)
*Trạng thái: Đã hoàn thiện 100% Data Pipeline + Backend RESTful API.*

### Những gì Đạt đã xây dựng xong:
1. **Pipeline Ingestion**: Cào tự động 3 thành phố mỗi 5 phút/lần (`collector/multi_city_collector.py`).
2. **Lakehouse Storage**: Làm sạch và ghi Parquet Silver có phân vùng theo ngày và thành phố (`spark/jobs/bronze_to_silver_incidents.py`).
3. **Data Warehouse (PostgreSQL)**: Cập nhật Star Schema và dbt models (`fact_traffic_incidents`, `dim_city`, `agg_hourly_incident_summary`).
4. **Airflow Orchestrator**: Lập lịch tự động chạy toàn bộ pipeline mỗi **5 phút/lần**.
5. **Backend REST API (FastAPI - `web/server.py` & `web/database.py`)**:
   - `GET /api/cities`: Danh sách thành phố & tọa độ tâm (Hà Nội, TP.HCM, Đà Nẵng).
   - `GET /api/incidents?city_id=HAN`: Trả về danh sách điểm tắc mới nhất (kèm mã màu `#EA2027` đỏ, `#FFC312` vàng, `#10AC84` xanh theo độ trễ như Google Maps).
   - `GET /api/kpi?city_id=HAN`: Thống kê tổng số điểm tắc, tổng km kẹt, độ trễ trung bình, tuyến đường tắc nghiêm trọng nhất.
   - `GET /api/distribution?city_id=HAN`: Tỷ lệ các loại sự cố (Kẹt xe, Tai nạn, Thi công...).

---

## 👨‍💻 PHẦN 2: NGUYỄN TRỌNG CƯƠNG (Phụ trách: Web Frontend & Trải nghiệm Người dùng)

> **Mục tiêu**: Xây dựng giao diện Web Dashboard bản đồ tương tác (sử dụng **HTML5 + Tailwind CSS + Leaflet.js**) kết nối với Backend API do Đạt cung cấp. Đạt đã chuẩn bị sẵn khung giao diện ban đầu tại thư mục `web/`, Cương tinh chỉnh và hoàn thiện các tính năng sau:

### Danh sách nhiệm vụ chi tiết:

1. **Task 1 (Làm trước tiên — Deadline: Tối 17/11)**:
   * **Bản đồ Tương tác Leaflet.js (`web/static/app.js`)**:
     * Đảm bảo khi người dùng chọn Dropdown đổi thành phố (`HAN`, `SGN`, `DAD`), bản đồ tự động bay (`map.flyTo`) mượt mà về trung tâm thành phố đó.
     * Gọi API `GET /api/incidents?city_id=...` để vẽ các điểm tắc dưới dạng vòng tròn (`L.circleMarker`) với màu sắc lấy trực tiếp từ thuộc tính `item.color_hex`:
       - 🔴 **Màu Đỏ (`#EA2027`)**: Tắc nghẽn nghiêm trọng (trễ &ge; 5 phút).
       - 🟡 **Màu Vàng (`#FFC312`)**: Ùn ứ chậm (trễ 2 - 5 phút).
       - 🟢 **Màu Xanh (`#10AC84`)**: Thông thoáng / chậm nhẹ.
       - 🟣 **Màu Tím (`#E056FD`)**: Tai nạn giao thông.
     * Tinh chỉnh Popup khi click vào điểm tắc: Tên tuyến đường `road_from` &rarr; `road_to`, chiều dài km, thời gian trễ phút.

2. **Task 2 (Deadline: Trưa 18/11)**:
   * **Bản đồ nhiệt (Heatmap Layer) & Bộ lọc**:
     * Hoàn thiện tính năng Bật / Tắt nút bấm **"Bản đồ nhiệt" (`leaflet-heat`)**: Khi bật thì hiện vệt đỏ nhiệt độ ở các cụm tắc lớn; khi tắt thì chỉ hiện các Marker chấm tròn.
     * (Tùy chọn nâng cao): Thêm bộ lọc Checkbox để người dùng có thể lọc: *Chỉ xem Kẹt xe* hoặc *Chỉ xem Tai nạn*.

3. **Task 3 (Deadline: Chiều 18/11)**:
   * **Thanh KPI & Biểu đồ Thống kê (`web/templates/index.html`)**:
     * Hiển thị 4 thẻ thông số ở đầu trang: **Tổng số điểm tắc**, **Tổng chiều dài kẹt xe (km)**, **Độ trễ trung bình (phút)**, **Thời gian cập nhật gần nhất**.
     * Hoàn thiện biểu đồ Donut **Chart.js** ở góc phải hiển thị tỷ lệ % các loại sự cố.
     * Danh sách **Top 10 tuyến đường tắc nghiêm trọng nhất** ở góc dưới: Click vào dòng nào thì bản đồ tự động zoom đến đúng đoạn đường đó.

4. **Task 4 (Deadline: Tối 18/11)**:
   * **Tính năng Auto-Refresh (Tự làm mới)**:
     * Cài đặt bộ đếm thời gian tự động gọi lại API sau mỗi **60 giây** mà không cần người dùng tải lại trang web (hiện đồng hồ đếm ngược: `Tự làm mới: 59s...`).
     * Kiểm tra giao diện trên cả màn hình máy tính và điện thoại.

---

## 🛠️ HƯỚNG DẪN DÀNH CHO CƯƠNG ĐỂ CHẠY WEB LOCAL
Cương có thể chạy web trực tiếp trên máy bằng lệnh:

```powershell
# 1. Kích hoạt môi trường ảo
.\venv\Scripts\activate

# 2. Khởi động Web Server
python -m uvicorn web.server:app --reload --port 8000
```
Mở trình duyệt truy cập: **http://localhost:8000** để xem và sửa giao diện trực tiếp (code sửa ở `web/templates/index.html` và `web/static/app.js` sẽ tự cập nhật ngay lập tức).

---

## ⏰ MỐC THỜI GIAN PHỐI HỢP
* **20:00 Ngày 18/11/2026**: Hai bạn họp Sync 15 phút rà soát lần cuối toàn bộ Web và Airflow.
* **09:00 Sáng 19/11/2026**: Chốt toàn bộ sản phẩm hoàn chỉnh để sẵn sàng báo cáo.
