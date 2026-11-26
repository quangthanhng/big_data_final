"""
End-to-end benchmark pipeline for Experiments 3 & 4 (handed over to Thiện).

    read Parquet  ->  Feature Engineering (Window shuffle)  ->  train model  ->  evaluate

Every run appends ONE line to results/pipeline_runs.csv with the data size,
the cluster resources the job got, and the time of each stage.

Experiment 3 — Data scalability (keep 2 workers, change data size):
    python run_pipeline.py --rows 2000000  --label exp3_2M
    python run_pipeline.py --rows 5000000  --label exp3_5M
    python run_pipeline.py --rows 10000000 --label exp3_10M   # ≈ full subset (10.07M)

Experiment 4 — Resource scaling (keep data, change number of workers):
    docker compose up -d --scale ... / start 1, 2, 3 workers, then
    python run_pipeline.py --label exp4_1w
    python run_pipeline.py --label exp4_2w
    python run_pipeline.py --label exp4_3w

`--rows` keeps whole (store, item) series (hash-based), so lag features stay
valid at every size. Shuffle Read/Write per stage: Spark UI -> port 4040
(while running) -> Stages tab.
"""

import argparse
import csv
import os
import time
from datetime import datetime

from pyspark import StorageLevel

from common import (FULL_SUBSET_PATH, RESULTS_DIR, cluster_info, get_spark,
                    save_json, timer)
from features import build_features, sample_series
from train_models import (MODEL_NAMES, add_model_args, default_params,
                          time_split, train_and_eval)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default=FULL_SUBSET_PATH)
    p.add_argument("--rows", type=int, default=None,
                   help="approx. number of input rows (default: all)")
    p.add_argument("--fraction", type=float, default=None,
                   help="share of series to keep (alternative to --rows)")
    p.add_argument("--model", default="rf", choices=list(MODEL_NAMES))
    p.add_argument("--label", default="run")
    p.add_argument("--shuffle-partitions", type=int, default=16)
    add_model_args(p)
    args = p.parse_args()

    spark = get_spark(f"Pipeline_{args.label}",
                      shuffle_partitions=args.shuffle_partitions)
    # Give executors a moment to register before reading cluster info.
    time.sleep(3)
    t = {}
    t_all = time.time()

    with timer("1_read", t):
        raw = spark.read.parquet(args.input)
        total = raw.count()

    fraction = args.fraction
    if fraction is None:
        fraction = 1.0 if not args.rows else min(1.0, args.rows / total)
    # seed differs from make_dev_sample (42) so sampling a sample still works
    raw = sample_series(raw, fraction, seed=7)

    with timer("2_feature_engineering", t):
        feats = build_features(raw).persist(StorageLevel.MEMORY_AND_DISK)
        n_rows = feats.count()
    train, test = time_split(feats)

    res, _ = train_and_eval(args.model, train, test, default_params(args))
    t["3_train"] = res["train_time_s"]
    t["4_predict_eval"] = res["predict_time_s"]
    t["total"] = round(time.time() - t_all, 2)
    feats.unpersist()

    info = cluster_info(spark)
    row = {
        "label": args.label,
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "model": res["model"],
        "fraction": round(fraction, 4),
        "input_rows_total": total,
        "feature_rows": n_rows,
        "executors": info["executors"],
        "executor_cores": info["executor_cores"],
        "total_cores": info["default_parallelism"],
        "executor_memory": info["executor_memory"],
        "read_s": t["1_read"],
        "feature_eng_s": t["2_feature_engineering"],
        "train_s": t["3_train"],
        "predict_eval_s": t["4_predict_eval"],
        "total_s": t["total"],
        "nwrmsle": res["nwrmsle"],
        "rmse": res["rmse"],
    }

    os.makedirs(RESULTS_DIR, exist_ok=True)
    csv_path = os.path.join(RESULTS_DIR, "pipeline_runs.csv")
    new = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row))
        if new:
            w.writeheader()
        w.writerow(row)
    save_json({**row, "cluster": info, "result": res}, f"pipeline_{args.label}.json")

    print("\n" + "=" * 60)
    for k, v in row.items():
        print(f"{k:<18}: {v}")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
