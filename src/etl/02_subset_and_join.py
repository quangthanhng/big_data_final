"""
Bước 4 & 5: Lọc Subset Đại diện, Làm sạch, Join 6 Bảng & Xuất Parquet
Author: Thành — Data & Infrastructure Engineer
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

def main():
    print("=" * 60)
    print("BƯỚC 4 & 5: SUBSET FILTERING, CLEANING, JOIN & PARQUET EXPORT")
    print("=" * 60)

    start_total = time.time()

    # 1. Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("Subset_and_Join_Pipeline") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.host", "jupyter-pyspark") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "16") \
        .getOrCreate()

    print("✅ Spark Session Connected!")

    # 2. Danh sách 10 cửa hàng đại diện (Stratified Stores)
    selected_stores = [
        44,  # Type A, Cluster 5  — Quito (thủ đô)
        52,  # Type A, Cluster 11 — Manta (ven biển)
        11,  # Type B, Cluster 6  — Cayambe
        31,  # Type B, Cluster 10 — Babahoyo
        30,  # Type C, Cluster 3  — Guayaquil (đô thị lớn)
        14,  # Type C, Cluster 7  — Riobamba
        3,   # Type D, Cluster 8  — Quito
        25,  # Type D, Cluster 1  — Salinas (thị trấn ven biển)
        28,  # Type E, Cluster 10 — Guayaquil
        43   # Type E, Cluster 10 — Esmeraldas
    ]
    print(f"📌 Selected stores: {selected_stores}")

    # 3. Đọc, Lọc & Làm sạch train.csv
    print("\n⏳ Reading and filtering train.csv...")
    train_raw = spark.read.csv("/home/jovyan/data/raw/train.csv", header=True, inferSchema=True)
    train_raw = train_raw.withColumn("date", F.to_date("date", "yyyy-MM-dd"))

    train_subset = train_raw.filter(
        (F.col("date") >= "2016-01-01") &
        (F.col("date") <= "2017-08-15") &
        (F.col("store_nbr").isin(selected_stores))
    )

    train_clean = train_subset.withColumn(
        "unit_sales",
        F.when(F.col("unit_sales") < 0, 0.0).otherwise(F.col("unit_sales"))
    ).withColumn(
        "onpromotion",
        F.when(F.col("onpromotion").isNull(), False).otherwise(F.col("onpromotion"))
    )

    subset_count = train_clean.count()
    print(f"✅ Subset rows after filter & clean: {subset_count:,}")

    # 4. Đọc & Xử lý 5 bảng phụ trợ
    print("\n⏳ Processing auxiliary tables...")
    stores = spark.read.csv("/home/jovyan/data/raw/stores.csv", header=True, inferSchema=True) \
        .withColumnRenamed("type", "store_type")
    
    items = spark.read.csv("/home/jovyan/data/raw/items.csv", header=True, inferSchema=True)

    oil = spark.read.csv("/home/jovyan/data/raw/oil.csv", header=True, inferSchema=True) \
        .withColumn("date", F.to_date("date", "yyyy-MM-dd"))
    oil_window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    oil_clean = oil.withColumn(
        "oil_price",
        F.last(F.col("dcoilwtico"), ignorenulls=True).over(oil_window)
    ).drop("dcoilwtico")

    holidays = spark.read.csv("/home/jovyan/data/raw/holidays_events.csv", header=True, inferSchema=True) \
        .withColumn("date", F.to_date("date", "yyyy-MM-dd"))
    holidays_clean = holidays.filter(
        (F.col("transferred") == False) | (F.col("transferred").isNull())
    ).select(
        "date",
        F.lit(True).alias("is_holiday"),
        F.col("type").alias("holiday_type")
    ).dropDuplicates(["date"])

    transactions = spark.read.csv("/home/jovyan/data/raw/transactions.csv", header=True, inferSchema=True) \
        .withColumn("date", F.to_date("date", "yyyy-MM-dd"))

    # 5. Join 6 bảng
    print("\n⏳ Joining 6 tables...")
    df = train_clean.join(stores, on="store_nbr", how="inner")
    df = df.join(items, on="item_nbr", how="inner")
    df = df.join(oil_clean, on="date", how="left")
    df = df.join(holidays_clean, on="date", how="left")
    df = df.withColumn("is_holiday", F.when(F.col("is_holiday").isNull(), False).otherwise(True))
    df = df.join(transactions, on=["date", "store_nbr"], how="left")
    df = df.withColumn("transactions", F.when(F.col("transactions").isNull(), 0).otherwise(F.col("transactions")))

    # 6. Xuất Parquet
    print("\n⏳ Exporting Parquet files...")
    # 6.1 Sample 100k
    sample_fraction = min(1.0, 100000.0 / subset_count)
    sample_df = df.sample(fraction=sample_fraction, seed=42).limit(100000)
    sample_path = "/home/jovyan/data/sample/favorita_sample_100k.parquet"
    sample_df.coalesce(1).write.mode("overwrite").parquet(sample_path)
    print(f"✅ Exported Sample 100k: {sample_path}")

    # 6.2 Full Subset
    full_path = "/home/jovyan/data/processed/favorita_full_subset.parquet"
    df.repartition(10).write.mode("overwrite").parquet(full_path)
    print(f"✅ Exported Full Subset: {full_path}")

    # 7. Kiểm tra kết quả
    print("\n⏳ Verifying exported datasets...")
    check_sample = spark.read.parquet(sample_path)
    check_full = spark.read.parquet(full_path)
    print(f"👉 Sample verified rows:      {check_sample.count():,}")
    print(f"👉 Full subset verified rows: {check_full.count():,}")

    spark.stop()
    print(f"\n🎉 ALL STEPS COMPLETED in {time.time() - start_total:.2f} seconds!")

if __name__ == "__main__":
    main()

