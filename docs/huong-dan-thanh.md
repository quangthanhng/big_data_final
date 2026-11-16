# 🚀 Hướng dẫn Chi tiết cho Thành — Data & Infrastructure Engineer

> **Vai trò:** Data & Infrastructure Engineer (Hạ tầng Docker + Tiền xử lý Dữ liệu)
> **Dự án:** Corporación Favorita Grocery Sales Forecasting trên Apache Spark
> **Thời gian:** 16/11 – 05/12/2026

---

## 📋 Tổng quan Nhiệm vụ

Thành chịu trách nhiệm **toàn bộ phần đầu** của pipeline:

| # | Nhiệm vụ | Sản phẩm bàn giao |
|---|----------|-------------------|
| 1 | Dựng cụm Apache Spark bằng Docker Compose | File `docker-compose.yml` hoạt động ổn định |
| 2 | Tải và EDA bộ dữ liệu Corporación Favorita | Notebook EDA + phân tích 5V, missing values |
| 3 | Viết script PySpark lọc Subset đại diện, join 6 bảng, làm sạch | Script PySpark `.py` hoặc notebook |
| 4 | Xuất dữ liệu sạch ra Parquet | File Parquet (Sample 100k + Full 15M dòng) |
| 5 | Chạy Thí nghiệm 1 (CSV vs. Parquet benchmark) | Bảng số liệu dung lượng + tốc độ đọc |
| 6 | Viết phần trình bày tiếng Anh (~4.5 phút) | Slides cho phần System & Data |

---

## 📁 Cấu trúc Thư mục Dự án

```
Bigdata_final/
├── docker/
│   ├── docker-compose.yml          # Cấu hình cụm Spark
│   └── jupyter/
│       └── Dockerfile              # (tuỳ chọn) Custom Jupyter image
├── data/
│   ├── raw/                        # CSV gốc từ Kaggle
│   │   ├── train.csv
│   │   ├── stores.csv
│   │   ├── items.csv
│   │   ├── transactions.csv
│   │   ├── oil.csv
│   │   └── holidays_events.csv
│   ├── sample/                     # Sample Parquet 100k dòng (cho Tiến)
│   └── processed/                  # Full Subset Parquet 15M dòng
├── src/
│   ├── etl/                        # Scripts ETL của Thành
│   │   ├── 01_eda.ipynb
│   │   ├── 02_subset_selection.py
│   │   ├── 03_data_cleaning_join.py
│   │   └── 04_export_parquet.py
│   └── ml_experiments/             # Tiến & Thiện sẽ dùng
├── docs/                           # Thiện quản lý
├── results/                        # Kết quả thí nghiệm
│   └── experiment1/
│       ├── csv_vs_parquet_benchmark.md
│       └── screenshots/
└── README.md
```

---

## ⚙️ BƯỚC 1: Dựng Cụm Apache Spark bằng Docker Compose

### 1.1. Cài đặt Docker Desktop

Kiểm tra Docker đã cài chưa:

```bash
docker --version
docker-compose --version
```

> [!IMPORTANT]
> Đảm bảo Docker Desktop được cấp **tối thiểu 12GB RAM** và **6 CPU cores** trong Settings → Resources. Nếu máy có 16GB RAM, nên cấp 12-14GB cho Docker.

### 1.2. Viết `docker-compose.yml`

Tạo file `docker/docker-compose.yml` với nội dung sau:

```yaml
version: '3.8'

services:
  spark-master:
    image: bitnami/spark:3.5
    container_name: spark-master
    hostname: spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_MASTER_HOST=spark-master
      - SPARK_MASTER_PORT=7077
      - SPARK_MASTER_WEBUI_PORT=8080
    ports:
      - "8080:8080"   # Spark Master Web UI
      - "7077:7077"   # Spark Master port
    volumes:
      - ../data:/opt/bitnami/spark/data
      - ../src:/opt/bitnami/spark/src
    networks:
      - spark-network

  spark-worker-1:
    image: bitnami/spark:3.5
    container_name: spark-worker-1
    hostname: spark-worker-1
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=3g
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 3G
    depends_on:
      - spark-master
    volumes:
      - ../data:/opt/bitnami/spark/data
      - ../src:/opt/bitnami/spark/src
    networks:
      - spark-network

  spark-worker-2:
    image: bitnami/spark:3.5
    container_name: spark-worker-2
    hostname: spark-worker-2
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=3g
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 3G
    depends_on:
      - spark-master
    volumes:
      - ../data:/opt/bitnami/spark/data
      - ../src:/opt/bitnami/spark/src
    networks:
      - spark-network

  spark-worker-3:
    image: bitnami/spark:3.5
    container_name: spark-worker-3
    hostname: spark-worker-3
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=3g
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 3G
    depends_on:
      - spark-master
    volumes:
      - ../data:/opt/bitnami/spark/data
      - ../src:/opt/bitnami/spark/src
    networks:
      - spark-network

  jupyter:
    image: jupyter/pyspark-notebook:latest
    container_name: jupyter-pyspark
    hostname: jupyter-pyspark
    environment:
      - JUPYTER_ENABLE_LAB=yes
      - PYSPARK_DRIVER_PYTHON=jupyter
      - PYSPARK_DRIVER_PYTHON_OPTS="lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root"
      - SPARK_MASTER=spark://spark-master:7077
    ports:
      - "8888:8888"   # JupyterLab
      - "4040:4040"   # Spark Driver UI (khi có job đang chạy)
    volumes:
      - ../data:/home/jovyan/data
      - ../src:/home/jovyan/src
    depends_on:
      - spark-master
    networks:
      - spark-network

networks:
  spark-network:
    driver: bridge
```

### 1.3. Khởi chạy cụm

```bash
cd docker/

# Khởi động toàn bộ cụm
docker-compose up -d

# Kiểm tra trạng thái
docker-compose ps

# Xem logs nếu có lỗi
docker-compose logs spark-master
docker-compose logs spark-worker-1
```

### 1.4. Kiểm tra cụm hoạt động

1. Mở trình duyệt vào **http://localhost:8080** → Spark Master Web UI → kiểm tra 3 Workers đã kết nối
2. Mở **http://localhost:8888** → JupyterLab → lấy token từ logs:
   ```bash
   docker-compose logs jupyter | grep token
   ```
3. Trong JupyterLab, tạo notebook mới và test kết nối:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestConnection") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

# Kiểm tra kết nối
print(f"Spark version: {spark.version}")
print(f"Number of executors: {spark.sparkContext.defaultParallelism}")

# Test đơn giản
df = spark.range(1000000)
print(f"Count: {df.count()}")

spark.stop()
```

> [!TIP]
> Chụp ảnh màn hình Spark Master UI (http://localhost:8080) hiển thị 3 Workers đã kết nối. Lưu vào `results/screenshots/` để Thiện dùng cho slide và báo cáo.

---

## 📥 BƯỚC 2: Tải Dữ liệu từ Kaggle

### 2.1. Cài Kaggle CLI

```bash
pip install kaggle
```

Tạo API token từ Kaggle: Vào https://www.kaggle.com/settings → "Create New Token" → lưu file `kaggle.json` vào `~/.kaggle/`

```bash
mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json
```

### 2.2. Tải bộ dữ liệu

```bash
cd data/raw/

kaggle competitions download -c favorita-grocery-sales-forecasting

# Giải nén
unzip favorita-grocery-sales-forecasting.zip -d .
```

### 2.3. Kiểm tra nhanh các file

```bash
# Xem dung lượng từng file
ls -lh data/raw/

# Xem cấu trúc vài dòng đầu
head -5 data/raw/train.csv
head -5 data/raw/stores.csv
head -5 data/raw/items.csv
head -5 data/raw/oil.csv
head -5 data/raw/holidays_events.csv
head -5 data/raw/transactions.csv

# Đếm số dòng (rất lớn - cần chờ)
wc -l data/raw/train.csv
```

**Các file dữ liệu cần có:**

| File | Mô tả | Dung lượng ước tính |
|------|--------|---------------------|
| `train.csv` | Dữ liệu giao dịch hàng ngày | ~5 GB, ~125M dòng |
| `stores.csv` | Thông tin 54 cửa hàng | Vài KB |
| `items.csv` | Thông tin 4,100 mã hàng | Vài KB |
| `transactions.csv` | Số giao dịch/ngày/cửa hàng | Vài MB |
| `oil.csv` | Giá dầu thô hàng ngày | Vài KB |
| `holidays_events.csv` | Ngày lễ/sự kiện | Vài KB |

---

## 🔍 BƯỚC 3: Phân tích Dữ liệu Khám phá (EDA)

Tạo notebook `src/etl/01_eda.ipynb` trong JupyterLab:

### 3.1. Khởi tạo Spark Session

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("EDA_CorporacionFavorita") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
```

### 3.2. Đọc dữ liệu

```python
# Đọc bảng chính
train = spark.read.csv(
    "/home/jovyan/data/raw/train.csv",
    header=True,
    inferSchema=True
)

# Đọc các bảng phụ
stores = spark.read.csv("/home/jovyan/data/raw/stores.csv", header=True, inferSchema=True)
items = spark.read.csv("/home/jovyan/data/raw/items.csv", header=True, inferSchema=True)
oil = spark.read.csv("/home/jovyan/data/raw/oil.csv", header=True, inferSchema=True)
holidays = spark.read.csv("/home/jovyan/data/raw/holidays_events.csv", header=True, inferSchema=True)
transactions = spark.read.csv("/home/jovyan/data/raw/transactions.csv", header=True, inferSchema=True)
```

### 3.3. Phân tích 5V

```python
# ===== VOLUME =====
print("=" * 60)
print("VOLUME ANALYSIS")
print("=" * 60)
print(f"Train rows:        {train.count():,}")
print(f"Train columns:     {len(train.columns)}")
print(f"Stores rows:       {stores.count()}")
print(f"Items rows:        {items.count()}")
print(f"Transactions rows: {transactions.count()}")
print(f"Oil rows:          {oil.count()}")
print(f"Holidays rows:     {holidays.count()}")

# ===== VARIETY =====
print("\n" + "=" * 60)
print("VARIETY ANALYSIS")
print("=" * 60)
# Số lượng family (nhóm hàng)
print(f"Unique product families: {items.select('family').distinct().count()}")
# Số cửa hàng theo type
stores.groupBy("type").count().show()
# Số thành phố
print(f"Unique cities: {stores.select('city').distinct().count()}")

# ===== VELOCITY =====
print("\n" + "=" * 60)
print("VELOCITY ANALYSIS")
print("=" * 60)
date_range = train.agg(
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date")
)
date_range.show()

# ===== VERACITY =====
print("\n" + "=" * 60)
print("VERACITY ANALYSIS")
print("=" * 60)
# Missing values trong train
print("Missing values in train:")
for col_name in train.columns:
    null_count = train.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        print(f"  {col_name}: {null_count:,} nulls ({null_count/train.count()*100:.2f}%)")

# Giá trị âm (hàng trả lại)
negative_sales = train.filter(F.col("unit_sales") < 0).count()
print(f"\nNegative unit_sales (returns): {negative_sales:,}")

# Missing values trong oil
oil_nulls = oil.filter(F.col("dcoilwtico").isNull()).count()
print(f"\nOil price missing values: {oil_nulls}")

# ===== VALUE =====
print("\n" + "=" * 60)
print("VALUE ANALYSIS")
print("=" * 60)
print("→ Accurate demand forecasting enables:")
print("  - Inventory optimization")
print("  - Perishable goods waste reduction")
print("  - Supply chain stockout prevention")
```

### 3.4. Phân tích phân bố cửa hàng (chuẩn bị cho chọn Subset)

```python
# Phân bố cửa hàng theo type và cluster
stores.groupBy("type", "cluster").count().orderBy("type", "cluster").show(50)

# Phân bố cửa hàng theo thành phố
stores.groupBy("city").count().orderBy(F.desc("count")).show(25)

# Schema các bảng
print("TRAIN Schema:")
train.printSchema()
print("STORES Schema:")
stores.printSchema()
print("ITEMS Schema:")
items.printSchema()
```

---

## 🎯 BƯỚC 4: Lọc Subset Đại diện & Join Dữ liệu

Tạo script `src/etl/02_subset_and_join.py`:

### 4.1. Chọn 10 cửa hàng đại diện (Stratified Store Selection)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("SubsetSelection_Join") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Đọc dữ liệu
stores = spark.read.csv("/home/jovyan/data/raw/stores.csv", header=True, inferSchema=True)

# Xem phân bố stores
stores.groupBy("type").count().show()
stores.groupBy("cluster").count().show()

# ----- CHỌN 10 CỬA HÀNG ĐẠI DIỆN -----
# Tiêu chí: trải đều qua type (A,B,C,D,E), mix thành phố lớn/nhỏ
# Ví dụ danh sách (bạn có thể điều chỉnh sau khi xem phân bố thực tế):

selected_stores = [
    3,   # Type D, Quito (thủ đô, thành phố lớn nhất)
    44,  # Type A, Quito
    45,  # Type E, Quito (cluster khác)
    47,  # Type B, Guayaquil (thành phố lớn thứ 2)
    49,  # Type A, Guayaquil
    20,  # Type C, thành phố trung bình
    25,  # Type B, thành phố nhỏ
    31,  # Type C, thành phố nhỏ
    36,  # Type D, thành phố trung bình
    52,  # Type E, thành phố nhỏ
]

print(f"Selected {len(selected_stores)} stores: {selected_stores}")

# Kiểm tra tính đại diện
stores.filter(F.col("store_nbr").isin(selected_stores)) \
    .select("store_nbr", "city", "state", "type", "cluster") \
    .orderBy("type") \
    .show()
```

> [!NOTE]
> Danh sách `selected_stores` ở trên là **ví dụ minh hoạ**. Bạn cần chạy phần EDA trước, xem phân bố thực tế của `type` và `cluster`, rồi chọn 10 cửa hàng sao cho **mỗi type (A–E) có ít nhất 1 cửa hàng**, và có **mix giữa thành phố lớn (Quito, Guayaquil) và thành phố nhỏ**.

### 4.2. Lọc theo thời gian + cửa hàng

```python
# Đọc train.csv
train = spark.read.csv(
    "/home/jovyan/data/raw/train.csv",
    header=True,
    inferSchema=True
)

# Cast date column
train = train.withColumn("date", F.to_date("date", "yyyy-MM-dd"))

# ----- LỌC SUBSET -----
# Điều kiện 1: Thời gian từ 01/01/2016 đến 15/08/2017
# Điều kiện 2: Chỉ lấy 10 cửa hàng đã chọn

subset = train.filter(
    (F.col("date") >= "2016-01-01") &
    (F.col("date") <= "2017-08-15") &
    (F.col("store_nbr").isin(selected_stores))
)

print(f"Subset row count: {subset.count():,}")
print(f"Date range: {subset.agg(F.min('date'), F.max('date')).collect()[0]}")
```

### 4.3. Làm sạch Dữ liệu (Data Cleaning)

```python
# ----- CLEANING -----

# 1. Xử lý giá trị âm (hàng trả lại → đưa về 0)
subset = subset.withColumn(
    "unit_sales",
    F.when(F.col("unit_sales") < 0, 0).otherwise(F.col("unit_sales"))
)

# 2. Xử lý null trong onpromotion
subset = subset.withColumn(
    "onpromotion",
    F.when(F.col("onpromotion").isNull(), False).otherwise(F.col("onpromotion"))
)

print("After cleaning:")
print(f"  Negative sales: {subset.filter(F.col('unit_sales') < 0).count()}")
print(f"  Null onpromotion: {subset.filter(F.col('onpromotion').isNull()).count()}")
```

### 4.4. Join 6 bảng dữ liệu

```python
# ----- ĐỌC CÁC BẢNG PHỤ -----
items = spark.read.csv("/home/jovyan/data/raw/items.csv", header=True, inferSchema=True)
oil = spark.read.csv("/home/jovyan/data/raw/oil.csv", header=True, inferSchema=True)
holidays = spark.read.csv("/home/jovyan/data/raw/holidays_events.csv", header=True, inferSchema=True)
transactions = spark.read.csv("/home/jovyan/data/raw/transactions.csv", header=True, inferSchema=True)

# ----- XỬ LÝ BẢNG OIL (Forward Fill giá dầu) -----
oil = oil.withColumn("date", F.to_date("date", "yyyy-MM-dd"))

# Forward Fill: điền giá dầu bị trống (cuối tuần) bằng giá ngày giao dịch gần nhất
oil_window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
oil = oil.withColumn(
    "oil_price",
    F.last(F.col("dcoilwtico"), ignorenulls=True).over(oil_window)
).drop("dcoilwtico")

# ----- XỬ LÝ BẢNG HOLIDAYS -----
holidays = holidays.withColumn("date", F.to_date("date", "yyyy-MM-dd"))

# Chỉ lấy ngày lễ KHÔNG bị chuyển dời
holidays_clean = holidays.filter(
    (F.col("transferred") == False) | (F.col("transferred").isNull())
).select(
    "date",
    F.lit(True).alias("is_holiday"),
    F.col("type").alias("holiday_type"),
    F.col("description").alias("holiday_description")
).dropDuplicates(["date"])  # Tránh duplicate khi join

# ----- XỬ LÝ BẢNG TRANSACTIONS -----
transactions = transactions.withColumn("date", F.to_date("date", "yyyy-MM-dd"))

# ----- JOIN TẤT CẢ -----
# Join 1: subset + stores (trên store_nbr)
df = subset.join(stores, on="store_nbr", how="inner")

# Join 2: + items (trên item_nbr)
df = df.join(items, on="item_nbr", how="inner")

# Join 3: + oil (trên date) — LEFT JOIN vì có thể có ngày không có giá dầu
df = df.join(oil, on="date", how="left")

# Join 4: + holidays (trên date) — LEFT JOIN
df = df.join(holidays_clean, on="date", how="left")

# Join 5: + transactions (trên date + store_nbr) — LEFT JOIN
df = df.join(transactions, on=["date", "store_nbr"], how="left")

# Điền is_holiday = False nếu null (ngày không phải lễ)
df = df.withColumn(
    "is_holiday",
    F.when(F.col("is_holiday").isNull(), False).otherwise(F.col("is_holiday"))
)

# Kiểm tra kết quả
print(f"Final joined dataframe: {df.count():,} rows, {len(df.columns)} columns")
df.printSchema()
df.show(5)
```

---

## 💾 BƯỚC 5: Xuất Dữ liệu ra Parquet

### 5.1. Xuất Sample Parquet (100k dòng cho Tiến)

```python
# ----- SAMPLE PARQUET (100K DÒNG) -----
# Lấy mẫu ngẫu nhiên ~100k dòng
sample_fraction = 100000 / df.count()
sample_df = df.sample(fraction=sample_fraction, seed=42)

sample_df.coalesce(1).write \
    .mode("overwrite") \
    .parquet("/home/jovyan/data/sample/favorita_sample_100k.parquet")

print(f"Sample exported: {sample_df.count():,} rows")
```

> [!IMPORTANT]
> Xuất Sample Parquet 100k dòng **NGAY ngày thứ 3 (18/11)** để Tiến có thể bắt đầu code Feature Engineering song song mà không phải chờ bạn hoàn thành toàn bộ pipeline.

### 5.2. Xuất Full Subset Parquet (15M dòng)

```python
# ----- FULL SUBSET PARQUET -----
# Repartition trước khi xuất để tối ưu (1 partition per ~1M rows)
num_partitions = max(1, int(df.count() / 1000000))

df.repartition(num_partitions).write \
    .mode("overwrite") \
    .parquet("/home/jovyan/data/processed/favorita_full_subset.parquet")

print(f"Full subset exported: {df.count():,} rows in {num_partitions} partitions")
```

### 5.3. Kiểm tra dữ liệu đã xuất

```python
# Đọc lại và kiểm tra
check = spark.read.parquet("/home/jovyan/data/processed/favorita_full_subset.parquet")
print(f"Parquet rows: {check.count():,}")
print(f"Parquet columns: {len(check.columns)}")
check.printSchema()
check.describe().show()
```

---

## 🧪 BƯỚC 6: Chạy Thí nghiệm 1 — CSV vs. Parquet Benchmark

Tạo notebook/script `src/etl/experiment1_storage_benchmark.py`:

### 6.1. Chuẩn bị: Lưu subset ở cả 2 định dạng

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Experiment1_StorageBenchmark") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

# Đọc full subset đã xử lý
df = spark.read.parquet("/home/jovyan/data/processed/favorita_full_subset.parquet")
row_count = df.count()
print(f"Dataset: {row_count:,} rows")

# Xuất ra CSV để so sánh
df.write.mode("overwrite").option("header", True).csv(
    "/home/jovyan/data/processed/favorita_full_subset_csv"
)
```

### 6.2. Benchmark: Đo thời gian đọc + truy vấn

```python
def benchmark_read_and_query(format_name, path, spark):
    """Benchmark đọc dữ liệu và chạy truy vấn Join + GroupBy"""
    results = {}
    
    # ----- ĐO THỜI GIAN ĐỌC -----
    start = time.time()
    if format_name == "CSV":
        data = spark.read.csv(path, header=True, inferSchema=True)
    else:
        data = spark.read.parquet(path)
    
    # Force evaluation (Spark lazy)
    data.count()
    read_time = time.time() - start
    results["read_time"] = read_time
    
    # ----- ĐO THỜI GIAN TRUY VẤN -----
    # Truy vấn: GroupBy store_nbr + family, tính tổng và trung bình unit_sales
    start = time.time()
    query_result = data.groupBy("store_nbr", "family") \
        .agg(
            F.sum("unit_sales").alias("total_sales"),
            F.avg("unit_sales").alias("avg_sales"),
            F.count("*").alias("record_count")
        ) \
        .orderBy(F.desc("total_sales"))
    
    # Force evaluation
    query_result.count()
    query_time = time.time() - start
    results["query_time"] = query_time
    
    results["total_time"] = read_time + query_time
    
    return results

# ----- CHẠY BENCHMARK -----
print("=" * 60)
print("EXPERIMENT 1: CSV vs. PARQUET STORAGE BENCHMARK")
print("=" * 60)

# CSV Benchmark
print("\n📄 Running CSV benchmark...")
csv_results = benchmark_read_and_query(
    "CSV", 
    "/home/jovyan/data/processed/favorita_full_subset_csv",
    spark
)

# Parquet Benchmark
print("📦 Running Parquet benchmark...")
parquet_results = benchmark_read_and_query(
    "Parquet",
    "/home/jovyan/data/processed/favorita_full_subset.parquet", 
    spark
)

# ----- KẾT QUẢ -----
print("\n" + "=" * 60)
print("RESULTS")
print("=" * 60)
print(f"{'Metric':<25} {'CSV':>12} {'Parquet':>12} {'Speedup':>10}")
print("-" * 60)
print(f"{'Read Time (s)':<25} {csv_results['read_time']:>12.2f} {parquet_results['read_time']:>12.2f} {csv_results['read_time']/parquet_results['read_time']:>9.1f}x")
print(f"{'Query Time (s)':<25} {csv_results['query_time']:>12.2f} {parquet_results['query_time']:>12.2f} {csv_results['query_time']/parquet_results['query_time']:>9.1f}x")
print(f"{'Total Time (s)':<25} {csv_results['total_time']:>12.2f} {parquet_results['total_time']:>12.2f} {csv_results['total_time']/parquet_results['total_time']:>9.1f}x")
```

### 6.3. Đo dung lượng ổ đĩa

```bash
# Chạy lệnh này trên terminal (không phải trong PySpark)
echo "=== CSV Size ==="
du -sh data/processed/favorita_full_subset_csv/

echo "=== Parquet Size ==="
du -sh data/processed/favorita_full_subset.parquet/

echo "=== Compression Ratio ==="
# Tính thủ công: Parquet_size / CSV_size * 100 = ?%
```

### 6.4. Ghi nhận kết quả

Tạo file `results/experiment1/benchmark_results.md`:

```markdown
# Experiment 1: CSV vs. Parquet Storage Benchmark

## Setup
- Dataset: ~15M rows (Corporación Favorita subset)
- Cluster: 1 Master + 2 Workers (each 2 cores, 3GB RAM)
- Query: GroupBy store_nbr + family → SUM, AVG unit_sales

## Results

| Metric              | CSV      | Parquet  | Speedup |
|---------------------|----------|----------|---------|
| File Size           | ???  MB  | ???  MB  | ?.?x    |
| Read Time (seconds) | ???      | ???      | ?.?x    |
| Query Time (seconds)| ???      | ???      | ?.?x    |
| Total Time (seconds)| ???      | ???      | ?.?x    |

## Analysis
- Parquet reduces storage by ~??% compared to CSV
- Parquet read performance is ~?.?x faster due to:
  - Columnar storage format (only reads needed columns)
  - Predicate pushdown (filters applied before data is read into memory)
  - Snappy compression (reduces I/O bandwidth)

## Screenshots
- [Spark UI - CSV Job](screenshots/csv_job.png)
- [Spark UI - Parquet Job](screenshots/parquet_job.png)
```

> [!TIP]
> Nhớ chụp ảnh màn hình Spark Web UI (http://localhost:8080 và http://localhost:4040) khi chạy từng benchmark, bao gồm:
> - DAG Visualization
> - Stage Duration
> - Shuffle Read / Write sizes
> 
> Đây là "bằng chứng" quan trọng cho báo cáo và slide!

---

## 📅 Lộ trình Thời gian cho Thành

### Giai đoạn 1: 16/11 – 20/11 (5 ngày)

| Ngày | Công việc | Sản phẩm |
|------|-----------|----------|
| 16-17/11 | Viết `docker-compose.yml`, khởi chạy cụm Spark, kiểm tra Web UI | Cụm Spark chạy ổn định |
| 18/11 | Tải data Kaggle, chạy EDA cơ bản | Notebook EDA |
| 19/11 | Viết script lọc Subset + Join + Cleaning, xuất **Sample 100k** | `sample_100k.parquet` → **gửi cho Tiến** |
| 20/11 | Hoàn thiện pipeline, xuất **Full Subset 15M** | `full_subset.parquet` |

> **🔴 Mốc Sync 20/11:** Chốt Docker + Parquet sạch 15M dòng

### Giai đoạn 2: 21/11 – 25/11 (5 ngày)

| Ngày | Công việc | Sản phẩm |
|------|-----------|----------|
| 21-22/11 | Chuẩn bị dữ liệu CSV để so sánh, code benchmark script | Script benchmark |
| 23-24/11 | Chạy **Thí nghiệm 1 (CSV vs. Parquet)**, ghi nhận số liệu | Bảng kết quả + screenshots |
| 25/11 | Chụp ảnh kiến trúc Spark UI, viết nháp phần *System & Data* bằng tiếng Anh | Nháp 1-2 trang gửi Thiện |

> **🔴 Mốc Sync 25/11:** Hoàn thành TN1 + có số liệu cho Thiện

### Giai đoạn 3: 26/11 – 30/11 (5 ngày)

| Ngày | Công việc |
|------|-----------|
| 26-28/11 | Viết giải thích tính đại diện của Subset (tại sao chọn 10 cửa hàng này) |
| 29-30/11 | Chuẩn bị phần trình bày tiếng Anh (~4.5 phút), soạn slides |

### Giai đoạn 4-5: 01/12 – 05/12

| Ngày | Công việc |
|------|-----------|
| 01-03/12 | Đọc chéo báo cáo, kiểm tra lỗi chính tả/ngữ pháp, hoàn thiện slide |
| 04/12 | Rehearsal thuyết trình (bấm giờ 15 phút) |
| 05/12 | Luyện Q&A tiếng Anh + Nộp bài |

---

## 🗣️ Các câu hỏi Q&A tiếng Anh Thành cần chuẩn bị

Phần của Thành trong thuyết trình (~4.5 phút) cần bao quát:

1. **"Why did you choose this subset strategy?"**
   - Giải thích 2 chiều lọc: thời gian (2016-2017 vì onpromotion đầy đủ) + không gian (10 cửa hàng đại diện 5 types)

2. **"Why Parquet instead of CSV?"**
   - Columnar storage, predicate pushdown, Snappy compression → giảm 70-80% size, đọc nhanh 3-5x

3. **"How did you handle missing values in oil prices?"**
   - Forward Fill dùng Window Function + `last(ignorenulls=True)`

4. **"Why Docker Compose instead of a real cluster?"**
   - Giả lập phân tán trên máy cá nhân, dễ scale worker bằng `--scale`, giới hạn CPU/RAM để benchmark

5. **"Explain the data leakage concern"**
   - Vì là time-series nên KHÔNG được random split, phải split theo mốc thời gian (train ≤ 31/07, test = 01-15/08)

---

## ⚠️ Các lỗi thường gặp & Cách xử lý

| Lỗi | Nguyên nhân | Cách sửa |
|-----|-------------|----------|
| `java.lang.OutOfMemoryError` | Worker hết RAM khi join/shuffle | Tăng `spark.sql.shuffle.partitions` lên 16-32, hoặc tăng RAM cho worker |
| Workers không kết nối Master | Network Docker chưa đúng | Kiểm tra `docker network ls`, đảm bảo tất cả container cùng network |
| JupyterLab không kết nối Spark | Sai `SPARK_MASTER` URL | Kiểm tra `spark://spark-master:7077` trong SparkSession config |
| CSV đọc chậm quá / timeout | File 5GB quá lớn | Đọc subset đã lọc, không đọc toàn bộ 125M dòng cùng lúc |
| `AnalysisException: cannot resolve column` | Tên cột sai sau join | Dùng `df.printSchema()` để kiểm tra tên cột chính xác |

---

## 🛠️ Lệnh Docker hữu ích

```bash
# Khởi động cụm
docker-compose up -d

# Dừng cụm (giữ lại data)
docker-compose down

# Dừng và xoá hết (bao gồm volumes)
docker-compose down -v

# Xem resource usage real-time
docker stats

# Scale workers (cho Thiện dùng ở TN4)
docker-compose up -d --scale spark-worker=1  # 1 worker
docker-compose up -d --scale spark-worker=2  # 2 workers
docker-compose up -d --scale spark-worker=3  # 3 workers

# Vào shell của container
docker exec -it spark-master bash
docker exec -it jupyter-pyspark bash

# Xem logs
docker-compose logs -f spark-master
docker-compose logs -f spark-worker-1
```

---

> [!CAUTION]
> **Lưu ý quan trọng:**
> - **KHÔNG commit file CSV/Parquet lớn lên GitHub.** Thêm vào `.gitignore`: `data/raw/`, `data/processed/`, `data/sample/`
> - Thống nhất dùng **1 file `docker-compose.yml` duy nhất** từ ngày 20/11. Mọi thay đổi phải thông qua GitHub pull request.
> - Ghi chú và nhận xét kết quả thí nghiệm **bằng tiếng Anh** ngay từ đầu để Thiện ghép nhanh vào báo cáo.
