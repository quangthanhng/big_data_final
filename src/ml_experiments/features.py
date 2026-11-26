"""
Bước 2 — Feature Engineering on Spark (Tiến — ML & Feature Engineer)

Input : data/processed/favorita_full_subset.parquet  (output of Thành's ETL)
Output: data/processed/favorita_features.parquet

Features
  * Calendar : day_of_week, day_of_month, month, is_weekend, is_payday
               (Ecuador pays wages on the 15th and on the last day of month)
  * Lags     : lag_7, lag_14, lag_28            (same store & item, k days ago)
  * Rolling  : rolling_mean_7, rolling_mean_28  (mean of the previous n days)
  * Target   : label = log1p(unit_sales)        (matches the NWRMSLE metric)

Key Big-Data detail — the data is SPARSE:
  train.csv only stores (date, store, item) rows that sold something. A plain
  F.lag(col, 7) would therefore return "7 ROWS ago", which can be weeks ago.
  We convert the date into an integer day index and use
  Window.rangeBetween(-k, -k), so "7 days ago" really means 7 calendar days;
  days without a row count as 0 sales. All lag/rolling columns share the same
  partitionBy(store_nbr, item_nbr).orderBy(day_idx) spec, so Spark computes
  them with ONE shuffle + ONE sort (check the DAG in the Spark UI, port 4040).

Run (inside jupyter-pyspark):
    python /home/jovyan/src/ml_experiments/features.py               # full 10M
    python /home/jovyan/src/ml_experiments/features.py --dev         # dev sample
"""

import argparse

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (DEV_SAMPLE_PATH, FEATURES_PATH, FULL_SUBSET_PATH,
                    START_DATE, get_spark, timer)

LAGS = [7, 14, 28]
ROLLING = [7, 28]
BURN_IN_DAYS = max(LAGS + ROLLING)  # first 28 days have incomplete history

KEY_COLS = ["date", "store_nbr", "item_nbr"]
LABEL_COL = "label"           # log1p(unit_sales)
WEIGHT_COL = "weight"         # 1.25 for perishable items, else 1.0 (NWRMSLE)

CATEGORICAL_COLS = ["store_type", "family", "city", "holiday_type", "store_nbr_cat"]
NUMERIC_COLS = (
    ["onpromotion", "perishable", "cluster", "oil_price", "is_holiday",
     "day_of_week", "day_of_month", "month", "is_weekend", "is_payday"]
    + [f"lag_{k}" for k in LAGS]
    + [f"rolling_mean_{n}" for n in ROLLING]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def sample_series(df, fraction, seed=42):
    """Keep a deterministic share of whole (store_nbr, item_nbr) series.

    Used for the dev sample and for Experiment 3 (data scalability): unlike
    df.sample(), it never breaks a time series, so lag features stay valid.
    """
    if fraction >= 1.0:
        return df
    bucket = F.pmod(F.xxhash64("store_nbr", "item_nbr", F.lit(seed)), F.lit(10000))
    return df.filter(bucket < int(round(fraction * 10000)))


def fill_oil_price(df):
    """oil.csv has no rows for weekends/holidays, so after Thành's left join
    ~30% of rows still have oil_price = NULL. Forward-fill on the (tiny)
    date -> price table, back-fill the very first days, then broadcast-join."""
    oil = df.groupBy("date").agg(F.max("oil_price").alias("oil_price"))
    w = Window.orderBy("date")  # ~590 rows -> a single partition is fine
    oil = oil.select(
        "date",
        F.coalesce(
            F.last("oil_price", ignorenulls=True).over(
                w.rowsBetween(Window.unboundedPreceding, 0)),
            F.first("oil_price", ignorenulls=True).over(
                w.rowsBetween(0, Window.unboundedFollowing)),
        ).alias("oil_price"),
    )
    return df.drop("oil_price").join(F.broadcast(oil), on="date", how="left")


def add_calendar_features(df):
    return (
        df.withColumn("day_of_week", F.dayofweek("date"))        # 1=Sun … 7=Sat
        .withColumn("day_of_month", F.dayofmonth("date"))
        .withColumn("month", F.month("date"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
        .withColumn(
            "is_payday",
            ((F.col("day_of_month") == 15) | (F.col("date") == F.last_day("date")))
            .cast("int"),
        )
    )


def add_lag_rolling_features(df):
    """Lags & rolling means on log1p(sales) using calendar-day frames."""
    w = Window.partitionBy("store_nbr", "item_nbr").orderBy("day_idx")
    cols = {}
    for k in LAGS:
        cols[f"lag_{k}"] = F.coalesce(
            F.sum(LABEL_COL).over(w.rangeBetween(-k, -k)), F.lit(0.0))
    for n in ROLLING:
        # previous n days, today excluded; missing days = 0 sales
        cols[f"rolling_mean_{n}"] = F.coalesce(
            F.sum(LABEL_COL).over(w.rangeBetween(-n, -1)), F.lit(0.0)) / n
    return df.withColumns(cols)


# ---------------------------------------------------------------------------
# Main transformation
# ---------------------------------------------------------------------------
def build_features(df):
    df = fill_oil_price(df)
    df = add_calendar_features(df)
    df = (
        df.withColumn("day_idx", F.datediff("date", F.lit(START_DATE)))
        .withColumn(LABEL_COL, F.log1p(F.greatest(F.col("unit_sales"), F.lit(0.0))))
        .withColumn(WEIGHT_COL, F.when(F.col("perishable") == 1, 1.25).otherwise(1.0))
        .withColumn("onpromotion", F.col("onpromotion").cast("int"))
        .withColumn("is_holiday", F.col("is_holiday").cast("int"))
        .withColumn("holiday_type", F.coalesce("holiday_type", F.lit("None")))
        .withColumn("store_nbr_cat", F.col("store_nbr").cast("string"))
    )
    df = add_lag_rolling_features(df)
    # Drop the burn-in period where lag_28 / rolling_mean_28 cannot be computed.
    df = df.filter(F.col("day_idx") >= BURN_IN_DAYS)

    # NOTE: `transactions` is NOT used as a feature: the number of receipts of
    # the same day is unknown at forecast time (target leakage).
    keep = (KEY_COLS + ["unit_sales", LABEL_COL, WEIGHT_COL, "day_idx"]
            + CATEGORICAL_COLS + NUMERIC_COLS)
    return df.select(*keep)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dev", action="store_true", help="use the dev sample")
    p.add_argument("--input", default=None)
    p.add_argument("--output", default=FEATURES_PATH)
    p.add_argument("--fraction", type=float, default=1.0,
                   help="share of (store, item) series to keep")
    args = p.parse_args()
    src = args.input or (DEV_SAMPLE_PATH if args.dev else FULL_SUBSET_PATH)
    out = args.output if not args.dev else args.output.replace(".parquet", "_dev.parquet")

    spark = get_spark("Feature_Engineering")
    timings = {}
    df = sample_series(spark.read.parquet(src), args.fraction)

    with timer("feature_engineering_write", timings):
        build_features(df).write.mode("overwrite").parquet(out)

    feats = spark.read.parquet(out)
    print(f"✅ Features: {feats.count():,} rows, {len(feats.columns)} columns -> {out}")
    feats.printSchema()
    feats.orderBy("store_nbr", "item_nbr", "date").show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
