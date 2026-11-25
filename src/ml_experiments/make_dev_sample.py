"""
Create a small DEV sample that keeps whole time series intact.

Why not use data/sample/favorita_sample_100k.parquet?
  That file is a *random row* sample: each (store, item) series keeps only a
  few scattered days, so lag_7 / rolling_mean_28 are almost always empty.
  Here we sample whole (store_nbr, item_nbr) series instead, so every kept
  series still has all of its days.

Usage (inside jupyter-pyspark):
    python /home/jovyan/src/ml_experiments/make_dev_sample.py --fraction 0.01
"""

import argparse

from common import DEV_SAMPLE_PATH, FULL_SUBSET_PATH, get_spark
from features import sample_series


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--fraction", type=float, default=0.01,
                   help="share of (store, item) series to keep (0.01 ≈ 100k rows)")
    p.add_argument("--input", default=FULL_SUBSET_PATH)
    p.add_argument("--output", default=DEV_SAMPLE_PATH)
    args = p.parse_args()

    spark = get_spark("Dev_Sample")
    df = spark.read.parquet(args.input)
    dev = sample_series(df, args.fraction)
    dev.coalesce(1).write.mode("overwrite").parquet(args.output)

    out = spark.read.parquet(args.output)
    n_series = out.select("store_nbr", "item_nbr").distinct().count()
    print(f"✅ Dev sample: {out.count():,} rows, {n_series:,} series -> {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
