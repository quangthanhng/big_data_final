# Experiments 3 & 4 — Data Scalability & Resource Scaling Analysis

**Author:** Thiện — System Performance, Report & Pitch Lead  
**Pipeline:** `run_pipeline.py` (End-to-end: Read Parquet → Window Feature Engineering → Random Forest 30 trees, depth 10 → Time-based Eval)  
**Input Dataset:** `favorita_full_subset.parquet` (10,075,199 rows, 10 stores across Ecuador)  

---

## 1. Experiment 3: Data Scalability Benchmark

### Setup
- **Cluster configuration:** Fixed at 2 Spark Workers (4 cores total, 1.2 GB memory per executor, 16 shuffle partitions).
- **Data sizes tested:** 2,000,000 rows (20% sample), 5,000,000 rows (50% sample), and 10,000,000 rows (100% full subset).
- **Sampling method:** Series-preserving hash sampling (`sample_series`), ensuring continuous time-series history for window calculations.

### Results Table

| Target Rows | Feature Rows | Read I/O (s) | Feature Eng & Shuffle (s) | Model Training (s) | Predict & Eval (s) | Total Time (s) | Total Time (min) | Shuffle Data | NWRMSLE |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **2,000,000** | 1,916,320 | 15.24 s | 39.42 s | 272.51 s | 3.12 s | **330.29 s** | 5.50 min | 118.4 MB | 0.5362 |
| **5,000,000** | 4,790,185 | 37.81 s | 94.63 s | 664.20 s | 7.01 s | **803.65 s** | 13.39 min | 295.2 MB | 0.5284 |
| **10,000,000** | 9,653,492 | 74.04 s | 185.20 s | 1,295.33 s | 13.49 s | **1,568.06 s** | 26.13 min | 590.1 MB | 0.5229 |

### Technical Observations & System Insights
1. **Near-Linear Execution Scalability ($O(N)$):**
   - When scaling from 2M to 10M rows (a 5.03x increase in feature rows), total execution time grew from 330.29s to 1,568.06s (a **4.75x** increase).
   - The pipeline demonstrates exceptional sub-linear to linear algorithmic complexity due to Spark's partition-level parallelism and vectorized Parquet decoding.
2. **Shuffle Volume & Stage Distribution:**
   - Feature Engineering shuffle scales exactly linearly with record count (~61 bytes shuffled per feature row).
   - The dominant cost remains tree construction in Spark MLlib (accounting for **82.5%** of total runtime), while Feature Engineering accounts for **11.8%**, Read I/O for **4.7%**, and Evaluation for **0.9%**.
3. **Model Generalization:**
   - Increasing the dataset from 2M to 10M rows improved NWRMSLE from 0.5362 to 0.5229 (−2.48% relative error reduction), proving that training on the complete multi-store history provides superior variance reduction for Random Forest.

---

## 2. Experiment 4: Horizontal Resource Scaling Benchmark

### Setup
- **Workload:** Full dataset (~10M rows, 9,653,492 feature rows after burn-in).
- **Model:** Random Forest (30 trees, max depth 10, subsample 0.7).
- **Cluster variations:**
  - **1 Worker:** 2 cores, 1.2 GB executor memory.
  - **2 Workers:** 4 cores, 2.4 GB aggregate executor memory.
  - **3 Workers:** 6 cores, 3.6 GB aggregate executor memory.

### Results Table

| Workers | Total Cores | Total Memory | Total Time (s) | Total Time (min) | Speedup | Ideal Linear Speedup | Parallel Efficiency | Shuffle Transfer |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1 Worker** | 2 cores | 1.2 GB | **2,987.10 s** | 49.79 min | **1.00x** | 1.00x | 100.0% | Local block manager |
| **2 Workers** | 4 cores | 2.4 GB | **1,568.06 s** | 26.13 min | **1.90x** | 2.00x | **95.2%** | 590.1 MB inter-worker |
| **3 Workers** | 6 cores | 3.6 GB | **1,108.00 s** | 18.47 min | **2.70x** | 3.00x | **89.9%** | 590.1 MB all-to-all |

### Technical Observations & Amdahl's Law Analysis
1. **High Parallel Efficiency:**
   - Scaling from 1 worker (2 cores) to 2 workers (4 cores) achieves a **1.90x speedup** (95.2% parallel efficiency), cutting runtime from ~50 minutes down to ~26 minutes.
   - Scaling further to 3 workers (6 cores) achieves a **2.70x speedup** (89.9% parallel efficiency), reaching ~18.5 minutes.
2. **Amdahl's Law & Shuffle Bottleneck:**
   - The non-parallelizable portions (driver coordination, DAG scheduling, initial serialization, and stage barrier synchronization) account for ~5% of the baseline time.
   - In 3-worker mode, all-to-all shuffle network traffic across Docker bridge networking creates minor contention, explaining why efficiency dips slightly below 90%.
3. **Memory Pressure Relief:**
   - In the 1-worker setup, the 1.2 GB executor was severely memory constrained, causing occasional RDD spill to disk during tree node split histogram aggregation.
   - Adding workers increased aggregate cluster memory to 3.6 GB, eliminating disk spills and allowing full in-memory partition processing.
