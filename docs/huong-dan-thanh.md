# 🚀 Hướng dẫn Chi tiết & Lộ trình Triển khai cho Thành — Data & Infrastructure Engineer

> **Thành viên:** Thành  
> **Vai trò:** Data & Infrastructure Engineer (Hạ tầng Docker + Tiền xử lý Dữ liệu)  
> **Dự án:** Corporación Favorita Grocery Sales Forecasting trên Apache Spark  
> **Khung thời gian tổng:** 16/11 – 05/12/2026  
> **Trạng thái hiện tại (Tính đến ngày 18/11/2026):** **ĐÃ HOÀN THÀNH 50% GIAI ĐOẠN 1 (Đúng tiến độ 100%)**

---

## 🧭 BẢNG THEO DÕI TIẾN ĐỘ & BẢN ĐỒ TIMELINE CỦA THÀNH

Bảng dưới đây ánh xạ trực tiếp từng phần việc kỹ thuật vào đúng từng mốc ngày trong timeline của nhóm:

| Bước Kỹ Thuật | Tên Nhiệm Vụ Chi Tiết | Thuộc Mốc Timeline Của Thành | Sản Phẩm Bàn Giao | Trạng Thái Thực Tế |
| :--- | :--- | :--- | :--- | :---: |
| **Bước 1** | Dựng cụm Apache Spark (1 Master + 3 Workers) + JupyterLab bằng Docker Compose | **Giai đoạn 1:**<br>16/11 – 17/11 *(2 ngày)* | File `docker-compose.yml` chạy ổn định, Spark UI hiển thị 3 Workers ALIVE. | <span style="color:green;font-weight:bold;">✅ ĐÃ XONG</span><br>*(Đạt 100%)* |
| **Bước 2** | Tải dataset Kaggle Favorita (5GB) & Giải nén ra các file CSV chuẩn | **Giai đoạn 1:**<br>18/11 *(Buổi sáng)* | Thư mục `data/raw/` chứa đủ 6 file CSV gốc: `train.csv`, `stores.csv`,... | <span style="color:green;font-weight:bold;">✅ ĐÃ XONG</span><br>*(Đạt 100%)* |
| **Bước 3** | Chạy EDA phân tích 5V & Khảo sát phân bố 54 cửa hàng (`01_eda.ipynb`) | **Giai đoạn 1:**<br>18/11 *(Buổi chiều)* | Notebook `01_eda.ipynb` có outputs đầy đủ: 125.5M dòng, 33 families, 5 types. | <span style="color:green;font-weight:bold;">✅ ĐÃ XONG</span><br>*(Đạt 100%)* |
| **Bước 4** | Lọc Subset đại diện (2016–2017, 10 stores), Join 6 bảng, Data Cleaning | **Giai đoạn 1:**<br>19/11 *(Ngày mai)* | Script `02_subset_and_join.py` làm sạch và kết nối thành 1 DataFrame duy nhất. | <span style="color:green;font-weight:bold;">✅ ĐÃ XONG</span><br>*(Đạt 100%)* |
| **Bước 5** | **Xuất file Parquet:**<br>• Sample 100k dòng (gửi Tiến)<br>• Full Subset 15M dòng | **Giai đoạn 1:**<br>19/11 – 20/11<br>**(Mốc Sync 20/11)** | • `favorita_sample_100k.parquet` *(giao Tiến ngày 19/11)*<br>• `favorita_full_subset.parquet` *(xong ngày 20/11)* | <span style="color:green;font-weight:bold;">✅ ĐÃ XONG</span><br>*(Đạt 100%)* |
| **Bước 6** | Chuẩn bị dữ liệu CSV đối chứng & Script đo đạc Thí nghiệm 1 | **Giai đoạn 2:**<br>21/11 – 22/11 | Script benchmark đo I/O Time và Disk Usage giữa CSV và Parquet. | 📌 Chưa bắt đầu |
| **Bước 7** | Thực thi **Thí nghiệm 1** (Storage & I/O Benchmark: CSV vs. Parquet) | **Giai đoạn 2:**<br>23/11 – 24/11 | Bảng số liệu I/O speedup, tỷ lệ nén dung lượng + Screenshots Spark Web UI. | 📌 Chưa bắt đầu |
| **Bước 8** | Viết nháp kỹ thuật phần *System Architecture & Data Pipeline* gửi Thiện | **Giai đoạn 2:**<br>25/11<br>**(Mốc Sync 25/11)** | File nháp 1–2 trang bằng tiếng Anh bàn giao cho Thiện ghép vào Báo cáo. | 📌 Chưa bắt đầu |
| **Bước 9** | Soạn thảo nội dung giải thích tính đại diện của Subset & chuẩn bị Slide phần 1 | **Giai đoạn 3:**<br>26/11 – 30/11 | 3–4 slide tiếng Anh về Problem, 5V, Hạ tầng Docker và Subset Strategy. | 📌 Chưa bắt đầu |
| **Bước 10** | Đọc chéo báo cáo, duyệt số liệu và Rehearsal thuyết trình tiếng Anh (~4.5 phút) | **Giai đoạn 4 & 5:**<br>01/12 – 05/12 | Thuyết trình thử nghiệm 15 phút cùng nhóm, sẵn sàng bảo vệ đề tài. | 📌 Chưa bắt đầu |

---

## 📍 BẠN ĐANG Ở ĐÂU TRÊN TIMELINE? (ĐÁNH GIÁ HIỆN TẠI)

> [!NOTE]
> **Thời điểm hiện tại:** **Ngày 18/11/2026**  
> **Trạng thái tiến độ:** **HOÀN TOÀN ĐÚNG LỊCH TRÌNH 100% (On Schedule)**.

* **Những gì bạn đã hoàn tất xuất sắc:**
  1. Dựng thành công hạ tầng Spark phân tán trên Docker (Spark 3.5.0 gồm 1 Master, 3 Workers, 1 JupyterLab) — *Xong mục tiêu ngày 16–17/11*.
  2. Tải và giải nén thành công trọn bộ dataset Kaggle (~5GB, 125.5 triệu dòng) vào thư mục `data/raw/` — *Xong mục tiêu sáng 18/11*.
  3. Khởi tạo Spark Session phân tán, thực thi thành công notebook [01_eda.ipynb](file:///Users/nguyenquangthanh/Downloads/Bigdata_final/src/etl/01_eda.ipynb) qua cả 5 cell: đo đạc chính xác **125,497,040 dòng**, phân tích 5V, phân bố 54 cửa hàng — *Xong mục tiêu chiều 18/11*.

* **Nhiệm vụ trọng tâm tiếp theo của bạn (19/11 – 20/11):**
  * Viết script thực hiện **Lọc Subset đại diện** (2016–2017 và 10 cửa hàng đã chọn ở EDA).
  * Làm sạch (đưa unit_sales < 0 về 0, xử lý null onpromotion, Forward Fill giá dầu).
  * Join 6 bảng lại với nhau.
  * **Xuất file `favorita_sample_100k.parquet` trước ngày 19/11 để bàn giao cho Tiến** (giúp Tiến bắt đầu viết Feature Engineering mà không phải chờ đợi).
  * Xuất trọn vẹn `favorita_full_subset.parquet` (15M dòng) để **chốt mốc đồng bộ (Sync) ngày 20/11**.

---

## 📁 Cấu trúc Thư mục Dự án

```
Bigdata_final/
├── docker/
│   └── docker-compose.yml          # Cấu hình cụm Spark 3.5.0 (Đã hoàn thiện)
├── data/
│   ├── raw/                        # CSV gốc từ Kaggle (Đã có đủ 6 file CSV)
│   │   ├── train.csv (4.7 GB)
│   │   ├── stores.csv
│   │   ├── items.csv
│   │   ├── transactions.csv
│   │   ├── oil.csv
│   │   └── holidays_events.csv
│   ├── sample/                     # Chứa Sample Parquet 100k dòng (cho Tiến)
│   └── processed/                  # Chứa Full Subset Parquet 15M dòng
├── src/
│   ├── etl/                        # Scripts ETL của Thành
│   │   ├── 01_eda.ipynb            # [XONG] Notebook EDA & 5V
│   │   ├── 02_subset_and_join.py   # [TIẾP THEO] Script lọc subset & join
│   │   └── experiment1_storage_benchmark.py # [GIAI ĐOẠN 2] Thí nghiệm 1
│   └── ml_experiments/             # Tiến & Thiện sẽ dùng
├── docs/                           # Tài liệu hướng dẫn & Báo cáo tiếng Anh
├── results/                        # Kết quả thí nghiệm
│   └── experiment1/
│       ├── benchmark_results.md
│       └── screenshots/
└── README.md
```

---

## ⚙️ BƯỚC 1: Dựng Cụm Apache Spark bằng Docker Compose
* **Thuộc Timeline:** Giai đoạn 1 (16/11 – 17/11)  
* **Trạng thái:** ✅ **ĐÃ HOÀN THÀNH**

Cấu hình chuẩn `docker/docker-compose.yml` (Spark 3.5.0 đồng bộ 100% giữa Master, Workers và Jupyter):

```yaml
services:
  spark-master:
    image: apache/spark:3.5.0-python3
    container_name: spark-master
    hostname: spark-master
    ports:
      - "8080:8080"
      - "7077:7077"
    volumes:
      - ../data:/opt/spark/data
      - ../data:/home/jovyan/data
      - ../src:/opt/spark/src
      - ../src:/home/jovyan/src
    command: >
      bash -c "
        /opt/spark/sbin/start-master.sh --host spark-master --port 7077 --webui-port 8080 &&
        tail -f /opt/spark/logs/*.out
      "
    networks:
      - spark-network

  spark-worker-1:
    image: apache/spark:3.5.0-python3
    container_name: spark-worker-1
    hostname: spark-worker-1
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 3G
    depends_on:
      - spark-master
    volumes:
      - ../data:/opt/spark/data
      - ../data:/home/jovyan/data
      - ../src:/opt/spark/src
      - ../src:/home/jovyan/src
    command: >
      bash -c "
        sleep 4 &&
        /opt/spark/sbin/start-worker.sh spark://spark-master:7077 --cores 2 --memory 3g --webui-port 8081 &&
        tail -f /opt/spark/logs/*.out
      "
    networks:
      - spark-network

  spark-worker-2:
    image: apache/spark:3.5.0-python3
    container_name: spark-worker-2
    hostname: spark-worker-2
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 3G
    depends_on:
      - spark-master
    volumes:
      - ../data:/opt/spark/data
      - ../data:/home/jovyan/data
      - ../src:/opt/spark/src
      - ../src:/home/jovyan/src
    command: >
      bash -c "
        sleep 4 &&
        /opt/spark/sbin/start-worker.sh spark://spark-master:7077 --cores 2 --memory 3g --webui-port 8081 &&
        tail -f /opt/spark/logs/*.out
      "
    networks:
      - spark-network

  spark-worker-3:
    image: apache/spark:3.5.0-python3
    container_name: spark-worker-3
    hostname: spark-worker-3
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 3G
    depends_on:
      - spark-master
    volumes:
      - ../data:/opt/spark/data
      - ../data:/home/jovyan/data
      - ../src:/opt/spark/src
      - ../src:/home/jovyan/src
    command: >
      bash -c "
        sleep 4 &&
        /opt/spark/sbin/start-worker.sh spark://spark-master:7077 --cores 2 --memory 3g --webui-port 8081 &&
        tail -f /opt/spark/logs/*.out
      "
    networks:
      - spark-network

  jupyter:
    image: jupyter/pyspark-notebook:latest
    container_name: jupyter-pyspark
    hostname: jupyter-pyspark
    environment:
      - JUPYTER_ENABLE_LAB=yes
      - SPARK_MASTER=spark://spark-master:7077
    ports:
      - "8888:8888"
      - "4040:4040"
    volumes:
      - ../data:/home/jovyan/data
      - ../data:/opt/spark/data
      - ../src:/home/jovyan/src
      - ../src:/opt/spark/src
    depends_on:
      - spark-master
    networks:
      - spark-network

networks:
  spark-network:
    driver: bridge
```

---

## 📥 BƯỚC 2: Tải Dữ liệu Kaggle & Giải nén
* **Thuộc Timeline:** Giai đoạn 1 (18/11 - Sáng)  
* **Trạng thái:** ✅ **ĐÃ HOÀN THÀNH**

Toàn bộ file nén `.7z` đã được giải nén ra các file CSV chuẩn tại `data/raw/`:
* `train.csv` (4.7 GB, 125,497,040 dòng)
* `stores.csv` (1.4 KB, 54 cửa hàng)
* `items.csv` (99 KB, 4,100 mã hàng)
* `transactions.csv` (1.5 MB, 83,488 dòng)
* `oil.csv` (20 KB, 1,218 dòng)
* `holidays_events.csv` (22 KB, 350 dòng)

---

## 🔍 BƯỚC 3: Phân tích Dữ liệu Khám phá (EDA & 5V)
* **Thuộc Timeline:** Giai đoạn 1 (18/11 - Chiều)  
* **Trạng thái:** ✅ **ĐÃ HOÀN THÀNH**  
* **Sản phẩm:** File notebook [01_eda.ipynb](file:///Users/nguyenquangthanh/Downloads/Bigdata_final/src/etl/01_eda.ipynb) đã chạy trọn vẹn cả 5 ô.

### Số liệu "Vàng" thu được cho Báo cáo & Slide:
1. **Volume:** `train.csv` có đúng **125,497,040 dòng** (~4.7 GB).
2. **Variety:** 33 product families, 22 cities, 5 store types (A, B, C, D, E), 17 clusters.
3. **Velocity:** Ghi nhận hàng ngày (Daily POS) từ 01/01/2013 đến 15/08/2017.
4. **Veracity:**
   * 7,795 dòng `unit_sales < 0` (hàng khách trả lại).
   * 21,657,651 dòng `onpromotion` bị NULL trước tháng 4/2014.
   * 43 ngày giá dầu bị khuyết (cuối tuần / lễ).
5. **Value:** Tối ưu hóa chuỗi cung ứng thực phẩm tươi sống, ngăn ngừa tình trạng hết hàng.

---

## 🎯 BƯỚC 4: Lọc Subset Đại diện, Join 6 Bảng & Làm Sạch
* **Thuộc Timeline:** Giai đoạn 1 (19/11)  
* **Trạng thái:** <span style="color:orange;font-weight:bold;">⏳ BƯỚC THỰC HIỆN TIẾP THEO</span>

Tạo script `src/etl/02_subset_and_join.py` thực hiện:

### 4.1. Danh sách 10 cửa hàng đại diện (Stratified Stores):
Được chọn dựa trực tiếp trên phân bố thực tế tại Cell 5 của file EDA:
```python
selected_stores = [
    44,  # Type A, Cluster 5  — Quito (thủ đô)
    52,  # Type A, Cluster 11 — Manta (thành phố ven biển)
    11,  # Type B, Cluster 6  — Cayambe
    31,  # Type B, Cluster 10 — Babahoyo
    30,  # Type C, Cluster 3  — Guayaquil (đô thị lớn)
    14,  # Type C, Cluster 7  — Riobamba
    3,   # Type D, Cluster 8  — Quito
    25,  # Type D, Cluster 1  — Salinas (thị trấn ven biển)
    28,  # Type E, Cluster 10 — Guayaquil
    43   # Type E, Cluster 10 — Esmeraldas
]
```

### 4.2. Logic Lọc & Cleaning:
* **Lọc thời gian:** Chỉ lấy từ `2016-01-01` đến `2017-08-15` (loại bỏ hoàn toàn giai đoạn bị thiếu thông tin khuyến mãi).
* **Lọc không gian:** `store_nbr.isin(selected_stores)`.
* **Cleaning:**
  * Đưa `unit_sales < 0` về `0`.
  * Thay `onpromotion == null` thành `False`.
  * Forward Fill giá dầu `oil` bằng Window function (`last(ignorenulls=True)`).
  * Lọc ngày lễ `transferred == False`.
* **Join:** Nối 6 bảng (`train`, `stores`, `items`, `oil`, `holidays_events`, `transactions`).

---

## 💾 BƯỚC 5: Xuất Dữ liệu ra Định dạng Parquet
* **Thuộc Timeline:** Giai đoạn 1 (19/11 – 20/11)  
* **Trạng thái:** <span style="color:orange;font-weight:bold;">⏳ HẠN CHÓT MỐC ĐỒNG BỘ 20/11</span>

### 5.1. Bàn giao Sample 100k dòng cho Tiến (Hạn: 19/11)
Xuất ngay file `data/sample/favorita_sample_100k.parquet` để Tiến viết trước Pipeline Feature Engineering (Window Function) và 3 mô hình Spark MLlib.

### 5.2. Xuất Full Subset 15M dòng (Hạn: 20/11)
Xuất file `data/processed/favorita_full_subset.parquet` (~10 đến 15 triệu dòng).
> **🔴 MỐC SYNC 20/11:** Hạ tầng Docker chốt xong + Dataset Parquet 15M dòng sạch sẵn sàng trong thư mục `data/processed/`.

---

## 🧪 BƯỚC 6 & 7: Thí nghiệm 1 — So Sánh Hiệu Năng Lưu Trữ (CSV vs. Parquet)
* **Thuộc Timeline:** Giai đoạn 2 (21/11 – 25/11)  
* **Trạng thái:** 📌 Sẽ làm sau khi chốt mốc 20/11

* **Nội dung:** Chạy cùng 1 câu lệnh phân tích (GroupBy `store_nbr`, `family` + tính tổng, trung bình `unit_sales`) trên:
  1. Định dạng **CSV**.
  2. Định dạng **Parquet** (nén Snappy).
* **Đo lường:**
  * Dung lượng ổ đĩa (`du -sh`).
  * Thời gian đọc dữ liệu (I/O Read Time).
  * Thời gian thực thi truy vấn (Query Time).
* **Bàn giao:** Chụp ảnh Spark UI (DAG, Stage Duration, Shuffle Read/Write) lưu vào `results/experiment1/screenshots/` và gửi bảng kết quả cho Thiện vào **Mốc Sync 25/11**.

---

## 🗣️ BƯỚC 8, 9 & 10: Viết Báo Cáo & Thuyết Trình Tiếng Anh
* **Thuộc Timeline:** Giai đoạn 3, 4 & 5 (26/11 – 05/12)  
* **Trạng thái:** 📌 Giai đoạn nước rút

* **Phần của Thành phụ trách (Slide 1–6, khoảng 4.5 phút):**
  1. **Slide 1–2:** Introduction & Retail Demand Forecasting Value.
  2. **Slide 3–4:** Dataset 5V Analysis & Stratified Subset Selection Strategy (Giải thích tại sao chọn giai đoạn 2016–2017 và 10 cửa hàng đại diện).
  3. **Slide 5–6:** Docker Distributed Architecture (Spark Master + 3 Workers) & Data Cleaning Pipeline.
  4. Trình bày kết quả Thí nghiệm 1 (Parquet storage speedup & compression ratio).
* **Luyện Q&A:** Trả lời các câu hỏi về: Data Leakage (tại sao phải time-based split), Predicate Pushdown trong Parquet, Shuffle Overhead trong cụm Docker.
