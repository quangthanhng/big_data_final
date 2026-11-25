# ML stage — Feature Engineering & Models (Tiến)

Input: `data/processed/favorita_full_subset.parquet`, which is Thành's ETL output (10,075,199 rows, 10 stores, 2016-01-01 → 2017-08-15).
Every script runs **inside the `jupyter-pyspark` container** and connects to `spark://spark-master:7077`.

| File | Purpose |
|---|---|
| `common.py` | Spark session, paths, time-based split dates, timers |
| `make_dev_sample.py` | Small sample that keeps **whole** (store, item) series, for quick tests |
| `features.py` | Calendar + lag + rolling features, `log1p` target → `data/processed/favorita_features.parquet` |
| `train_models.py` | **Experiment 2**: Baseline vs. Linear Regression vs. Random Forest vs. GBT |
| `run_pipeline.py` | End-to-end benchmark (read → FE → train → eval) for **Experiments 3 & 4** (Thiện) |
| `results/` | JSON/CSV outputs: `experiment2_*.json/csv`, `pipeline_runs.csv` |

## How to run

Use **`spark-submit`**, not `python`. Inside the Jupyter image, plain `python` cannot import `pyspark`.

```bash
cd docker
docker compose up -d spark-master spark-worker-1 spark-worker-2 jupyter
docker exec -it jupyter-pyspark bash
cd /home/jovyan/src/ml_experiments

# quick dev loop (≈97k rows / 315 series, ~2 min in total)
spark-submit make_dev_sample.py --fraction 0.01
spark-submit features.py --dev
spark-submit train_models.py --dev

# Experiment 2 on the full subset
spark-submit features.py              # writes favorita_features.parquet
spark-submit train_models.py          # -> results/experiment2_full.{json,csv}

# Experiment 3 (2 workers, growing data)
spark-submit run_pipeline.py --rows 2000000  --label exp3_2M
spark-submit run_pipeline.py --rows 5000000  --label exp3_5M
spark-submit run_pipeline.py --rows 10000000 --label exp3_10M
# Experiment 4 (all data, 1 / 2 / 3 workers): start the workers, then
spark-submit run_pipeline.py --label exp4_2w
```

Machine with little RAM? Lower the memory without editing code:
`EXECUTOR_MEMORY=1500m DRIVER_MEMORY=1g spark-submit train_models.py`

Running from **Git Bash on Windows**? Run `export MSYS_NO_PATHCONV=1` first. Otherwise Git Bash rewrites
`/home/jovyan/...` into a Windows path, e.g.
`docker exec -w /home/jovyan/src/ml_experiments jupyter-pyspark spark-submit features.py --dev`.

**Windows/WSL permission fix:** the driver runs as `jovyan` (uid 1000) and the executors as `spark` (uid 185).
`common.py` therefore sets `spark.hadoop.fs.permissions.umask-mode=000`. Without it, writes fail with `Mkdirs failed`.

Useful flags: `--models lr,rf`, `--rf-trees 30 --rf-depth 10`, `--gbt-iter 30 --gbt-depth 6`,
`--subsample 0.7`, `--shuffle-partitions 16`.

## Design notes (for the report)

1. **Sparse data → calendar-day windows, not row lags.** `train.csv` only contains rows that had sales, so
   `F.lag(x, 7)` would return "7 *rows* ago", which can be weeks earlier. We map `date` to an integer
   `day_idx` and use `Window.partitionBy(store_nbr, item_nbr).orderBy(day_idx).rangeBetween(-k, -k)`.
   A missing day counts as 0 sales.
2. **One shuffle for all lag/rolling features.** All five window columns share the same partition and order
   spec, so Spark plans a single `Exchange hashpartitioning(store_nbr, item_nbr)` plus one sort
   (visible in the SQL/DAG tab of the Spark UI on port 4040). This is the main shuffle of the pipeline.
3. **Lags/rolling are computed on `log1p(unit_sales)`.** This keeps the features on the same scale as the
   target, which helps the linear model.
4. **Burn-in.** The first 28 days of 2016 are dropped because `lag_28` / `rolling_mean_28` have no full history there.
5. **Oil price fix.** `oil.csv` has no weekend rows, so about 30% of rows still had `oil_price = NULL` after the ETL join.
   We forward-fill (then back-fill) on the small date→price table and broadcast-join it back.
6. **Leakage guards.** The split is time-based (train ≤ 2017-07-31, test 2017-08-01 … 15).
   `transactions` is *not* used as a feature, because same-day receipt counts are unknown at forecast time.
7. **Metric.** NWRMSLE uses weight 1.25 for perishable items (Kaggle definition). The same weights are passed to
   MLlib through `weightCol`, so every model optimises the metric it is judged on.
8. **No Python UDFs.** The driver image (jupyter/pyspark-notebook) and the worker image (apache/spark) ship
   different Python versions. Everything therefore stays in the JVM (DataFrame API + MLlib).

## Known limitations

- **One-step-ahead evaluation.** For test days after 2017-08-07, `lag_7` and `rolling_mean_7` use *actual*
  sales from earlier test days. A true 15-day-ahead forecast would need lags ≥ 16 days or recursive prediction.
  The scores are therefore optimistic compared with the Kaggle leaderboard.
- **Only rows with recorded sales are evaluated** (no zero-sales rows), as in the provided subset.
- `is_holiday` from the ETL marks local/regional holidays for *all* stores (the join is by date only).
- The full subset has 10.07M rows, so the "15M" level of Experiment 3 is not reachable without adding stores.
