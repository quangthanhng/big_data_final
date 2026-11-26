# Presentation Pitch Deck: Distributed Retail Demand Forecasting at Scale

**Target Time:** 15 Minutes (Strict academic / conference presentation)  
**Speaker Split:**
- **Thành (Slides 1–4, ~4.5 min):** Problem, 5V Analysis, Subset Strategy & Docker Infrastructure
- **Tiến (Slides 5–8, ~4.5 min):** Data Engineering, Window Feature Pipeline & MLlib Models (Exp 2)
- **Thiện (Slides 9–13, ~5.5 min):** Data Scalability (Exp 3), Resource Scaling (Exp 4), Business Impact & Conclusion

---

## Slide 1: Title & Team Introduction (0.5 min) — *Speaker: Thành*

### Slide Content:
- **Title:** Distributed Retail Demand Forecasting at Scale
- **Subtitle:** End-to-End Big Data Architecture with Apache Spark & Docker on the Corporación Favorita Dataset
- **Authors & Roles:**
  - **Nguyễn Quang Thành:** Data & Infrastructure Engineer
  - **Bùi Trần Tiến:** ML & Feature Engineer
  - **Phạm Ngọc Thiện:** System Performance, Report & Pitch Lead
- **Key Highlight:** Handling 125M+ transaction records using containerized distributed computing.

> **Speaker Notes (Thành):**  
> *"Good morning professors and fellow students. Today, our team will present our Big Data final project: End-to-End Distributed Retail Demand Forecasting with Apache Spark on Docker. I am Thành, and together with Tiến and Thiện, we designed, deployed, and benchmarked an enterprise-grade Big Data pipeline processing over 125 million retail records."*

---

## Slide 2: Problem Statement & Value Proposition (1.0 min) — *Speaker: Thành*

### Slide Content:
- **The Core Business Problem:**
  - Grocery retail operates on razor-thin margins and high inventory volatility.
  - **Under-forecasting:** Stockouts, lost revenue, customer churn.
  - **Over-forecasting:** Severe financial waste from spoiled perishable foods.
- **Why Single-Node Tools Fail:**
  - Single-threaded Pandas / Scikit-Learn cannot load 5+ GB CSV files into memory without Out-Of-Memory (OOM) crashes.
  - Feature calculations (multi-week lags & rolling averages) across millions of series demand distributed parallel computation.
- **Project Goal:**
  - Build a reproducible, distributed demand forecasting platform on Apache Spark, optimizing Normalized Weighted Root Mean Squared Logarithmic Error (NWRMSLE).

> **Speaker Notes (Thành):**  
> *"In modern retail supply chains, demand forecasting is critical. If a supermarket under-predicts demand, shelves go empty. If it over-predicts, fresh food spoils. However, solving this across an entire national retail network means crunching tens of millions of transactional records. Traditional tools like Pandas choke on datasets of this scale. Our mission was to deploy a distributed Big Data solution capable of processing, feature engineering, and training ML models efficiently at scale."*

---

## Slide 3: Dataset Characterization: 5V Big Data Analysis (1.5 min) — *Speaker: Thành*

### Slide Content:
- **Volume:** 125.49 million transactions in `train.csv` (4.7 GB) + 6 auxiliary relational tables = 5.2+ GB total.
- **Variety:** Heterogeneous multi-modal schemas:
  - Temporal sales records (time-series).
  - Categorical metadata (54 stores across 22 cities, 4,100 items across 33 families).
  - Macroeconomic data (`oil.csv` crude oil prices).
  - National & regional calendar events (`holidays_events.csv`).
- **Velocity:** High-frequency Point-of-Sale (POS) transactions logged daily across nationwide branches.
- **Veracity:**
  - 7,795 negative sales records (product returns).
  - 21.6M missing values in `onpromotion` prior to April 2014.
  - 43 days of missing weekend/holiday oil prices.
- **Value:** Directly drives automatic inventory replenishment, mitigating stockouts and perishable spoilage.

> **Speaker Notes (Thành):**  
> *"The Corporación Favorita dataset exhibits all classic 5V Big Data characteristics. We analyzed 125.5 million transactions. The data is highly varied—combining store geography, product hierarchies, holiday schedules, and macroeconomic crude oil prices. Veracity challenges were significant: handling missing promotion flags, negative sales from returns, and missing weekend oil prices. Successfully taming this data translates into direct commercial value."*

---

## Slide 4: Subset Strategy & Docker Architecture (1.5 min) — *Speaker: Thành*

### Slide Content:
- **Representative Stratified Subset Strategy:**
  - *Time Filter:* 2016-01-01 to 2017-08-15 (~1.5 years of pristine modern data, zero missing promotion values).
  - *Spatial Stratification:* 10 representative stores covering all 5 store types (A, B, C, D, E) and diverse clusters across Ecuador.
  - *Result:* **10,075,199 rows (~10M)**. Large enough to generate realistic distributed cluster pressure; small enough for rapid benchmarking.
- **Containerized Distributed Spark Architecture:**
  - **1 Spark Master Node:** Port 8080 (Web UI monitoring), port 7077 (RPC).
  - **3 Spark Worker Nodes:** Enforced hardware quotas (`2 CPUs`, `3 GB RAM` per container) via Docker Compose.
  - **1 JupyterLab Driver Node:** Interactive and `spark-submit` client.
  - **Shared Docker Volume:** Cross-container access to raw and Parquet data.

> **Speaker Notes (Thành):**  
> *"Running 125 million rows repeatedly on personal laptops is inefficient. Therefore, we designed a smart two-dimensional stratified subset: filtering to the most recent 1.5 years and selecting 10 representative stores spanning every store tier and geographic region. This yielded a solid 10-million row subset. We deployed a full distributed cluster locally using Docker Compose, configuring 1 Master and 3 Workers with strict CPU and RAM limits to simulate real enterprise cloud nodes. Now, Tiến will walk you through data engineering and modeling."*

---

## Slide 5: Distributed Data Engineering & Storage Benchmark (1.0 min) — *Speaker: Tiến*

### Slide Content:
- **ETL Relational Join Pipeline:**
  - Clean returns: clamp `unit_sales < 0` to 0.
  - Window forward-fill for oil prices across weekends.
  - Multi-table join across 6 tables using PySpark SQL.
- **Experiment 1 Results: CSV vs. Parquet (Snappy):**
  - **Disk Footprint:** Reduced from **1,420 MB to 385 MB (↓ 72.9%)**.
  - **Read I/O Time:** Accelerated from **298.5 s to 74.0 s (4.03× Faster)**.
  - *Why?* Columnar storage, snappy compression, dictionary encoding, and Spark predicate pushdown.

*(Visual: `docs/images/chart1_storage_io.svg`)*

> **Speaker Notes (Tiến):**  
> *"Thank you, Thành. In the data engineering phase, we joined 6 relational tables and cleaned anomalies. In Experiment 1, we benchmarked storage: converting uncompressed CSV to columnar Parquet with Snappy compression reduced disk consumption by nearly 73% and accelerated read I/O by over 4 times! This laid a high-performance foundation for our feature pipeline."*

---

## Slide 6: Time-Series Feature Engineering via Calendar Windows (1.5 min) — *Speaker: Tiến*

### Slide Content:
- **The Challenge: Sparse Point-of-Sale Data:**
  - Zero-sale days are absent from the dataset. Standard `F.lag(col, 7)` would look back 7 *rows*, which might be weeks ago!
- **The Solution: Calendar-Day Range Windows:**
  - Map `date` to continuous `day_idx`.
  - Use `Window.partitionBy(store, item).orderBy(day_idx).rangeBetween(-k, -k)`.
- **Engineered Feature Set:**
  - Lags: `lag_7`, `lag_14`, `lag_28` on $log(1 + \text{unit\_sales})$.
  - Rolling Stats: `rolling_mean_7`, `rolling_mean_28`.
  - Calendar: `day_of_week`, `day_of_month`, `month`, `is_weekend`, `is_payday` (15th and end of month payroll surge).
- **Big Data Optimization (1-Shuffle Strategy):**
  - All 5 window features share the exact same partition key `(store_nbr, item_nbr)` and order `day_idx`.
  - Spark plans a **single distributed HashPartitioning Exchange**, avoiding redundant re-shuffling of 10M records.

> **Speaker Notes (Tiến):**  
> *"A key technical triumph of our project was handling sparse time-series data. In grocery retail, inactive days are missing. Standard lag functions pull 7 rows ago, not 7 days ago! We solved this by mapping dates to integer day indices and using Spark calendar range windows. Even better, by harmonizing the partition keys across all rolling and lag features, Spark performed the entire window feature transformation in a single network shuffle exchange."*

---

## Slide 7: Model Training & Experiment 2 Trade-off Analysis (1.5 min) — *Speaker: Tiến*

### Slide Content:
- **Strict Leakage-Free Time Split:**
  - Train: 2016-01-29 to 2017-07-31 (**9.35M rows**).
  - Test: 2017-08-01 to 2017-08-15 (**299k rows**).
  - Metric: **NWRMSLE** with sample weighting (`perishable = 1.25`).
- **Models Benchmarked on Spark MLlib (4 Cores):**
  - **Baseline (`rolling_mean_7`):** NWRMSLE = 0.6301 | 0 s
  - **Linear Regression (Ridge):** NWRMSLE = 0.5481 | 2.9 min
  - **Random Forest (30 trees, depth 10):** NWRMSLE = **0.5229** | **21.6 min** (★ Production Choice)
  - **GBT (30 iterations, depth 6):** NWRMSLE = **0.5206** | **186.8 min (~3.1 hours)**

*(Visual: `docs/images/chart2_model_tradeoff.svg`)*

> **Speaker Notes (Tiến):**  
> *"In Experiment 2, we evaluated models on 9.35 million training rows. Random Forest achieved a massive 17% error reduction over baseline and 4.6% over Linear Regression in 21.6 minutes. While GBT was slightly more accurate by 0.45%, it took over 3 hours—almost 9 times longer! Why? Random Forest builds trees in parallel across worker nodes, whereas GBT is strictly sequential, requiring hundreds of iterative distributed shuffle stages. Thus, Random Forest is clearly our production champion. Now, Thiện will present our scalability benchmarks."*

---

## Slide 8: Feature Importance Insights (0.5 min) — *Speaker: Tiến*

### Slide Content:
- **Top Predictive Features in Random Forest:**
  1. `rolling_mean_7`: **45.4%** importance
  2. `rolling_mean_28`: **24.3%** importance
  3. `lag_7`: **14.3%** importance
  4. `lag_14`: **8.9%** importance
  5. `lag_28`: **4.6%** importance
  6. `is_payday`, `is_weekend`, `onpromotion`: Combined **2.5%**
- **Conclusion:**
  - Lag and rolling indicators account for **96.9%** of model split decisions.
  - The computational investment in Spark Window transformations was completely justified by predictive power.

> **Speaker Notes (Tiến):**  
> *"Feature importance analysis proved that our engineered lag and rolling window features account for nearly 97% of the model's split decisions, demonstrating that our distributed window engineering was the primary driver of forecast accuracy."*

---

## Slide 9: Experiment 3: Data Scalability Benchmark (1.5 min) — *Speaker: Thiện*

### Slide Content:
- **Setup:** Fixed cluster of 2 Workers (4 cores, 2.4 GB memory). Scaled from 2M → 5M → 10M rows.
- **Performance Breakdown:**
  - **2M Rows:** 330.3 s (5.5 min) | 118 MB shuffle | NWRMSLE: 0.5362
  - **5M Rows:** 803.7 s (13.4 min) | 295 MB shuffle | NWRMSLE: 0.5284
  - **10M Rows:** 1,568.1 s (26.1 min) | 590 MB shuffle | NWRMSLE: 0.5229
- **Key Findings:**
  - **Near-Linear $O(N)$ Complexity:** 5.03× data increase resulted in only 4.75× runtime increase.
  - **Stage Allocation:** RF Training dominates runtime (**82.5%**), Window Feature Engineering accounts for **11.8%**, Read I/O for **4.7%**.
  - **Accuracy Gains:** Training on full multi-store data reduced NWRMSLE by 2.48%.

*(Visual: `docs/images/chart3_data_scalability.svg`)*

> **Speaker Notes (Thiện):**  
> *"Thank you, Tiến. Moving to Experiment 3: Data Scalability. We evaluated the entire end-to-end pipeline across 2 million, 5 million, and 10 million rows on a fixed 2-worker cluster. The results demonstrate near-linear $O(N)$ scalability: scaling data by 5 times increased total runtime by only 4.75 times. Furthermore, training on the complete multi-store dataset reduced forecast error, proving that distributed scale directly improves predictive quality."*

---

## Slide 10: Experiment 4: Horizontal Resource Scaling (1.5 min) — *Speaker: Thiện*

### Slide Content:
- **Setup:** Fixed 10M rows (full subset). Scaled worker nodes from 1 to 3 workers (2 to 6 CPU cores).
- **Empirical Results:**
  - **1 Worker (2 Cores, 1.2 GB RAM):** 2,987.1 s (~49.8 min) | **1.00× Speedup** (100% Efficiency)
  - **2 Workers (4 Cores, 2.4 GB RAM):** 1,568.1 s (~26.1 min) | **1.90× Speedup** (**95.2% Efficiency**)
  - **3 Workers (6 Cores, 3.6 GB RAM):** 1,108.0 s (~18.5 min) | **2.70× Speedup** (**89.9% Efficiency**)
- **Amdahl's Law Analysis:**
  - Runtime dropped from ~50 minutes to ~18.5 minutes.
  - Minor efficiency dip (89.9%) at 3 workers is attributed to all-to-all shuffle network communication and DAG synchronization overhead.

*(Visual: `docs/images/chart4_resource_scaling.svg`)*

> **Speaker Notes (Thiện):**  
> *"In Experiment 4, we tested horizontal resource scaling by adding worker containers to our Docker cluster while holding data constant at 10 million rows. Moving from 1 to 2 workers cut runtime in half with 95.2% parallel efficiency. Scaling to 3 workers achieved a 2.70x speedup with 89.9% efficiency, slashing runtime from 50 minutes down to 18.5 minutes. As predicted by Amdahl's Law, the slight efficiency drop is caused by inter-worker network shuffle transfer and driver synchronization."*

---

## Slide 11: Real-World Business Impact & ROI (0.5 min) — *Speaker: Thiện*

### Slide Content:
- **From Baseline to Production Random Forest:**
  - NWRMSLE dropped from **0.6301 to 0.5229 (↓ 17.0% relative error)**.
- **Bottom-Line Financial Translation:**
  - In national supermarket retail ($1B+ annual turnover), a 1% reduction in forecast error typically saves $2M–$5M annually by cutting spoiled dairy and produce while eliminating stockouts.
- **Resource Rightsizing:**
  - Running optimized 2-worker or 3-worker Spark nodes provides predictable, cost-effective SLA completion in under 20 minutes.

> **Speaker Notes (Thiện):**  
> *"To put this into financial perspective: our distributed model achieved a 17% error reduction over standard retail baselines. In a national supermarket chain with billions in sales, cutting demand forecast errors by even 1% saves millions in perishable waste and lost sales. Our cluster architecture delivers this accuracy within a 20-minute daily operational batch window."*

---

## Slide 12: Architectural Limitations & Future Directions (0.5 min) — *Speaker: Thiện*

### Slide Content:
- **Current Limitations:**
  - One-step-ahead testing horizon: multi-step 15-day forecasting currently uses actual sales for earlier test days.
  - Zero-sales sparsity: unrecorded item-days are omitted rather than explicitly populated.
- **Production Roadmap:**
  - **Real-Time Streaming:** Ingest live POS register events via **Apache Kafka** feeding **Spark Structured Streaming**.
  - **Feature Store:** Decouple rolling window compute using Feast or Hopsworks.
  - **Multi-Horizon Recursive Forecasting:** Deploy recursive multi-day autoregressive prediction models.

> **Speaker Notes (Thiện):**  
> *"Looking forward, our production roadmap includes streaming ingestion using Apache Kafka and Spark Structured Streaming for intraday demand updates, deploying a distributed feature store, and expanding from 1-day ahead to 15-day recursive forecasting."*

---

## Slide 13: Conclusion & Key Takeaways (0.5 min) — *Speaker: Thiện*

### Slide Content:
- **Key Takeaways:**
  1. **Parquet vs. CSV:** 73% space reduction, 4.0× I/O speedup.
  2. **Feature Engineering:** Single-shuffle calendar window strategy unlocks 97% of predictive power.
  3. **Model Pragmatism:** Random Forest is the distributed sweet-spot (17% error drop, 8.7× faster than GBT).
  4. **Scalability:** Confirmed near-linear $O(N)$ data scaling and 90% horizontal cluster efficiency.
- **Repository:** `https://github.com/quangthanhng/big_data_final`

> **Speaker Notes (Thiện):**  
> *"In conclusion, this project demonstrated that combining columnar Parquet, single-shuffle window feature engineering, and distributed Random Forest on Spark provides an optimal, highly scalable solution for enterprise demand forecasting. We thank you for your attention and welcome any questions!"*

---

## Slide 14: Q&A Preparation: Anticipated Questions & Strong Answers

1. **Q: Why didn't you train on all 125 million rows?**  
   *Answer (Thành):* Full 125M rows contain 21M missing values in `onpromotion` before 2014 and ancient macroeconomic patterns. Our stratified 10-store subset (2016–2017) captures 10.07 million rows, covering every single store tier and climate cluster while maintaining clean, representative modern consumer behavior.
2. **Q: Why did GBT take 3 hours while Random Forest took 21 minutes?**  
   *Answer (Tiến):* Random Forest builds all trees in parallel across worker nodes. GBT is strictly sequential—tree $k$ fits residuals of tree $k-1$. For 30 iterations and depth 6, Spark must coordinate ~180 sequential distributed stages, each requiring barrier synchronization and shuffle aggregation.
3. **Q: Why did speedup drop from 1.90x to 2.70x at 3 workers?**  
   *Answer (Thiện):* Governed by Amdahl's Law. Driver overhead, task scheduling, and all-to-all shuffle network traffic across the Docker virtual bridge network impose communication latency that slightly reduces parallel efficiency from 95% to 90%.
