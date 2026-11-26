"""
Bước 3 — Model training with Spark MLlib + Experiment 2 (Accuracy vs. Cost)
Tiến — ML & Feature Engineer

Models (all trained on log1p(unit_sales), weighted like NWRMSLE):
  1. Linear Regression (Ridge, L2)      – fast linear baseline
  2. Random Forest Regressor            – non-linear, handles categoricals
  3. Gradient-Boosted Trees (GBT)       – strongest on tabular data, costliest
Plus a naive baseline (prediction = rolling_mean_7) that needs no training.

Split: time-based, train <= 2017-07-31, test = 2017-08-01 … 2017-08-15.

Metrics on the test set:
  * NWRMSLE – sqrt( Σ w·(log1p(ŷ) − log1p(y))² / Σ w ), w = 1.25 if perishable
  * RMSLE   – same without weights
  * RMSE / MAE – on the original unit_sales scale
  * train_time_s / predict_time_s – wall clock

Run (inside jupyter-pyspark), after features.py:
    python /home/jovyan/src/ml_experiments/train_models.py --dev      # quick test
    python /home/jovyan/src/ml_experiments/train_models.py            # Experiment 2
"""

import argparse
import csv
import os
from datetime import datetime

from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import (GBTRegressor, LinearRegression,
                                   RandomForestRegressor)
from pyspark.sql import functions as F

from common import (FEATURES_PATH, RESULTS_DIR, TEST_END, TEST_START,
                    TRAIN_END, cluster_info, get_spark, save_json, timer)
from features import CATEGORICAL_COLS, LABEL_COL, NUMERIC_COLS, WEIGHT_COL

MODEL_NAMES = {"lr": "LinearRegression", "rf": "RandomForest", "gbt": "GBT"}


# ---------------------------------------------------------------------------
# Split & pipelines
# ---------------------------------------------------------------------------
def time_split(feats):
    train = feats.filter(F.col("date") <= F.lit(TRAIN_END))
    test = feats.filter(F.col("date").between(F.lit(TEST_START), F.lit(TEST_END)))
    return train, test


def build_pipeline(model, params):
    idx_cols = [f"{c}_idx" for c in CATEGORICAL_COLS]
    stages = [
        StringIndexer(inputCol=c, outputCol=o, handleInvalid="keep")
        for c, o in zip(CATEGORICAL_COLS, idx_cols)
    ]
    if model == "lr":
        # Linear model needs one-hot encoding; trees can split on indices.
        ohe_cols = [f"{c}_ohe" for c in CATEGORICAL_COLS]
        stages.append(OneHotEncoder(inputCols=idx_cols, outputCols=ohe_cols,
                                    handleInvalid="keep"))
        feature_cols = ohe_cols + NUMERIC_COLS
        est = LinearRegression(
            labelCol=LABEL_COL, weightCol=WEIGHT_COL, featuresCol="features",
            regParam=params["lr_reg"], elasticNetParam=0.0,  # 0 = Ridge
            maxIter=params["lr_iter"], standardization=True)
    elif model == "rf":
        feature_cols = idx_cols + NUMERIC_COLS
        est = RandomForestRegressor(
            labelCol=LABEL_COL, weightCol=WEIGHT_COL, featuresCol="features",
            numTrees=params["rf_trees"], maxDepth=params["rf_depth"],
            maxBins=64, subsamplingRate=params["subsample"],
            featureSubsetStrategy="onethird", seed=42)
    elif model == "gbt":
        feature_cols = idx_cols + NUMERIC_COLS
        est = GBTRegressor(
            labelCol=LABEL_COL, weightCol=WEIGHT_COL, featuresCol="features",
            maxIter=params["gbt_iter"], maxDepth=params["gbt_depth"],
            stepSize=params["gbt_step"], maxBins=64,
            subsamplingRate=params["subsample"], seed=42)
    else:
        raise ValueError(model)

    stages.append(VectorAssembler(inputCols=feature_cols, outputCol="features",
                                  handleInvalid="keep"))
    stages.append(est)
    return Pipeline(stages=stages), feature_cols


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
def evaluate(pred, pred_col="prediction"):
    """All metrics in ONE Spark job (a single aggregation)."""
    p = F.greatest(F.col(pred_col), F.lit(0.0))      # log1p(ŷ) >= 0
    y = F.col(LABEL_COL)
    w = F.col(WEIGHT_COL)
    err_units = F.expm1(p) - F.col("unit_sales")
    row = pred.agg(
        F.sqrt(F.sum(w * (p - y) ** 2) / F.sum(w)).alias("nwrmsle"),
        F.sqrt(F.avg((p - y) ** 2)).alias("rmsle"),
        F.sqrt(F.avg(err_units ** 2)).alias("rmse"),
        F.avg(F.abs(err_units)).alias("mae"),
        F.count(F.lit(1)).alias("n_test"),
    ).first()
    return {k: (round(v, 5) if isinstance(v, float) else v)
            for k, v in row.asDict().items()}


def feature_importances(fitted, feature_cols, top=15):
    est = fitted.stages[-1]
    if not hasattr(est, "featureImportances"):
        return None
    imp = est.featureImportances.toArray()
    ranked = sorted(zip(feature_cols, imp), key=lambda t: -t[1])[:top]
    return [{"feature": f, "importance": round(float(v), 5)} for f, v in ranked]


def train_and_eval(model, train, test, params):
    """Fit one model, return (metrics dict, fitted pipeline)."""
    pipeline, feature_cols = build_pipeline(model, params)
    t = {}
    with timer(f"{model}_train", t):
        fitted = pipeline.fit(train)
    with timer(f"{model}_predict_eval", t):
        metrics = evaluate(fitted.transform(test))
    out = {"model": MODEL_NAMES[model], **metrics,
           "train_time_s": t[f"{model}_train"],
           "predict_time_s": t[f"{model}_predict_eval"]}
    if model == "lr":
        out["lr_coefficients_count"] = len(fitted.stages[-1].coefficients)
    imp = feature_importances(fitted, feature_cols)
    if imp:
        out["feature_importances"] = imp
    return out, fitted


def default_params(args):
    return {
        "lr_reg": args.lr_reg, "lr_iter": 50,
        "rf_trees": args.rf_trees, "rf_depth": args.rf_depth,
        "gbt_iter": args.gbt_iter, "gbt_depth": args.gbt_depth, "gbt_step": 0.1,
        "subsample": args.subsample,
    }


def add_model_args(p):
    p.add_argument("--lr-reg", type=float, default=0.01)
    p.add_argument("--rf-trees", type=int, default=30)
    p.add_argument("--rf-depth", type=int, default=10)
    p.add_argument("--gbt-iter", type=int, default=30)
    p.add_argument("--gbt-depth", type=int, default=6)
    p.add_argument("--subsample", type=float, default=0.7,
                   help="row subsampling rate per tree (RF/GBT)")


# ---------------------------------------------------------------------------
# Experiment 2
# ---------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dev", action="store_true", help="use dev feature file")
    p.add_argument("--features", default=FEATURES_PATH)
    p.add_argument("--models", default="lr,rf,gbt")
    p.add_argument("--tag", default=None, help="suffix for result files")
    add_model_args(p)
    args = p.parse_args()

    path = args.features.replace(".parquet", "_dev.parquet") if args.dev else args.features
    tag = args.tag or ("dev" if args.dev else "full")
    params = default_params(args)

    spark = get_spark("Experiment2_Model_Comparison")
    feats = spark.read.parquet(path)
    train, test = time_split(feats)

    # Cache once so every model pays the same (zero) I/O cost.
    timings = {}
    with timer("load_and_cache", timings):
        train = train.persist(StorageLevel.MEMORY_AND_DISK)
        test = test.persist(StorageLevel.MEMORY_AND_DISK)
        n_train, n_test = train.count(), test.count()
    print(f"📊 Train rows: {n_train:,} | Test rows: {n_test:,}")

    results = []
    # Naive baseline: tomorrow ≈ mean of the last 7 days (no training at all)
    base = {"model": "Baseline (rolling_mean_7)",
            **evaluate(test, pred_col="rolling_mean_7"),
            "train_time_s": 0.0, "predict_time_s": None}
    results.append(base)
    print(base)

    for m in [s.strip() for s in args.models.split(",") if s.strip()]:
        print(f"\n🚀 Training {MODEL_NAMES[m]} ...")
        res, _ = train_and_eval(m, train, test, params)
        results.append(res)
        print({k: v for k, v in res.items() if k != "feature_importances"})

    report = {
        "experiment": "2 - model accuracy vs. training cost",
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "features_path": path,
        "train_rows": n_train, "test_rows": n_test,
        "split": {"train_end": TRAIN_END, "test": [TEST_START, TEST_END]},
        "params": params,
        "cluster": cluster_info(spark),
        "timings": timings,
        "results": results,
    }
    save_json(report, f"experiment2_{tag}.json")

    # Flat CSV that Thiện can plot directly
    os.makedirs(RESULTS_DIR, exist_ok=True)
    csv_path = os.path.join(RESULTS_DIR, f"experiment2_{tag}.csv")
    cols = ["model", "nwrmsle", "rmsle", "rmse", "mae", "train_time_s",
            "predict_time_s", "n_test"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(results)
    print(f"💾 Saved {csv_path}")

    print("\n" + "=" * 72)
    print(f"{'Model':<28}{'NWRMSLE':>10}{'RMSLE':>10}{'RMSE':>10}{'Train(s)':>12}")
    for r in results:
        print(f"{r['model']:<28}{r['nwrmsle']:>10.4f}{r['rmsle']:>10.4f}"
              f"{r['rmse']:>10.2f}{r['train_time_s']:>12.1f}")
    print("=" * 72)
    spark.stop()


if __name__ == "__main__":
    main()
