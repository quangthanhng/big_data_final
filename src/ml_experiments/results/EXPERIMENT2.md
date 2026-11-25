# Experiment 2 — Model Accuracy vs. Training Cost

Raw outputs: `experiment2_full.json` (incl. feature importances) and `experiment2_full.csv` (ready to plot).

## Setup
- **Data:** `favorita_features.parquet` has 9,653,492 rows (10 stores, 4,024 items). The first 28 days are dropped as lag burn-in.
- **Time-based split:** train = 2016-01-29 → 2017-07-31 (**9,354,342 rows**), test = 2017-08-01 → 2017-08-15 (**299,150 rows**).
- **Cluster:** 2 workers × 2 cores (4 cores in total), **executor memory 1.2 GB**, 16 shuffle partitions.
  The host has 7.2 GB RAM, so Docker/WSL was capped at ~4.8 GB. The team standard is 3 GB per executor.
- **Target:** `log1p(unit_sales)`. Every model is trained with `weightCol` (perishable = 1.25), the same weights NWRMSLE uses.
- **Hyper-parameters:** LR Ridge `regParam=0.01`; RF 30 trees, depth 10; GBT 30 iterations, depth 6, step 0.1.
  RF and GBT use `subsamplingRate=0.7` and `maxBins=64`.

## Results

| Model | NWRMSLE ↓ | RMSLE | RMSE (units) | MAE | Train time |
|---|---:|---:|---:|---:|---:|
| Baseline (`rolling_mean_7`) | 0.6301 | 0.6314 | **17.82** | 4.468 | 0 s |
| Linear Regression (Ridge) | 0.5481 | 0.5484 | 19.23 | 4.412 | 176 s (2.9 min) |
| Random Forest | 0.5229 | 0.5237 | 19.12 | **4.211** | 1,295 s (21.6 min) |
| GBT | **0.5206** | **0.5213** | 19.03 | 4.219 | 11,211 s (3.1 h) |

## Key observations
1. **Diminishing returns.**
   - Baseline → LR: −13.0% NWRMSLE.
   - LR → RF: −4.6% NWRMSLE for **7.3×** the training time.
   - RF → GBT: only −0.45% NWRMSLE for **8.7×** more time (64× the time of LR).
   - In practice, Random Forest is the best accuracy/cost compromise.
2. **Why GBT is so slow on Spark.**
   - RF grows all 30 trees **in parallel**: one distributed pass (`collectAsMap` job) per tree level serves the whole forest.
   - GBT is **sequential by nature**: tree *k* fits the residuals of trees 1…k−1. 30 iterations × ~6 levels therefore means
     about 180 distributed passes, each one with a shuffle of split statistics.
   - Job time rose from ~43 s to 60–90 s per level over the run. The 1.2 GB executors spill cached data to disk, and
     no checkpoint directory was set, so the RDD lineage keeps growing.
3. **Lag/rolling features dominate.**
   - RF importances: `rolling_mean_7` 0.45, `rolling_mean_28` 0.24, `lag_7` 0.14, `lag_14` 0.09, `lag_28` 0.05.
   - GBT importances: `rolling_mean_7` 0.60, then `rolling_mean_28` 0.12, `day_of_month` 0.04 (payday effect), `onpromotion` 0.02.
   - This justifies the costly Window-function stage: the main shuffle of the pipeline produces the features that matter most.
4. **Metric choice matters.** Every model beats the baseline on NWRMSLE/RMSLE/MAE but is *worse* on RMSE.
   - The models minimise squared error in **log space**. `expm1` of a log-space mean behaves like a geometric mean, so the
     models under-predict the few very high-volume items, which dominate RMSE.
   - This is expected and fine for the Kaggle objective (NWRMSLE), which deliberately treats relative errors on small and
     large items equally.

## Limitations
- One-step-ahead evaluation: for test days after 2017-08-07, `lag_7`/`rolling_mean_7` use actual sales from earlier test days.
- Only rows with recorded sales are scored (zero-sales days are not in the subset).
- Executor memory was below the team standard (1.2 GB vs 3 GB), so absolute times are pessimistic. Relative comparisons stay valid.

## Possible improvements
- `spark.sparkContext.setCheckpointDir(...)` so GBT can truncate lineage every 10 iterations (`checkpointInterval`).
- Tune GBT with fewer, deeper trees or a larger `stepSize`. Or train on recent months only, to cut its cost.
- Horizon-safe features (lags ≥ 16 days) for a true 15-day-ahead forecast.
