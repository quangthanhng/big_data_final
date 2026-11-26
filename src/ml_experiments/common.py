"""
Shared helpers for the ML stage (Tiến): Spark session, paths, timing.

All scripts in this folder are meant to run INSIDE the `jupyter-pyspark`
container, e.g.

    docker exec -it jupyter-pyspark python /home/jovyan/src/ml_experiments/train_models.py

Note: the driver (Jupyter image) and the workers (apache/spark image) ship
different Python versions, so this code deliberately avoids Python UDFs and
RDD lambdas — everything stays in the JVM (DataFrame API + MLlib).
"""

import json
import os
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Paths (as seen from inside the containers — see docker/docker-compose.yml)
# ---------------------------------------------------------------------------
DATA_DIR = os.environ.get("DATA_DIR", "/home/jovyan/data")
SRC_DIR = os.environ.get("SRC_DIR", "/home/jovyan/src")

FULL_SUBSET_PATH = f"{DATA_DIR}/processed/favorita_full_subset.parquet"
DEV_SAMPLE_PATH = f"{DATA_DIR}/sample/favorita_dev_sample.parquet"
FEATURES_PATH = f"{DATA_DIR}/processed/favorita_features.parquet"

# results/ is written under src/ because only data/ and src/ are mounted.
RESULTS_DIR = os.environ.get("RESULTS_DIR", f"{SRC_DIR}/ml_experiments/results")

# ---------------------------------------------------------------------------
# Time-based split (NO random split -> no future leakage)
# ---------------------------------------------------------------------------
START_DATE = "2016-01-01"
TRAIN_END = "2017-07-31"   # train: <= 31/07/2017
TEST_START = "2017-08-01"  # test : 01/08/2017 – 15/08/2017
TEST_END = "2017-08-15"


def get_spark(app_name, master=None, executor_memory=None, executor_cores=2,
              shuffle_partitions=16, driver_memory=None):
    """Create (or reuse) a SparkSession connected to the Docker cluster.

    `master` defaults to $SPARK_MASTER (set in docker-compose) and falls back
    to local[*] so the code can also be smoke-tested without the cluster.
    Memory can be lowered on small machines without editing code:
        EXECUTOR_MEMORY=1g DRIVER_MEMORY=1g spark-submit train_models.py
    """
    master = master or os.environ.get("SPARK_MASTER", "local[*]")
    executor_memory = executor_memory or os.environ.get("EXECUTOR_MEMORY", "2g")
    driver_memory = driver_memory or os.environ.get("DRIVER_MEMORY", "2g")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.cores", str(executor_cores))
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        # Driver (jovyan, uid 1000) and executors (spark, uid 185) are different
        # users on the shared bind mount. Without this, folders created by the
        # driver are 755 and executors fail with "Mkdirs failed" (seen on
        # Windows/WSL hosts). 000 -> new dirs/files are writable by everyone.
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
    )
    if master.startswith("spark://"):
        builder = (
            builder.config("spark.driver.host", "jupyter-pyspark")
            .config("spark.driver.bindAddress", "0.0.0.0")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


@contextmanager
def timer(label, store=None):
    """Measure wall-clock time of a block. Spark is lazy, so make sure the
    block ends with an ACTION (count / write / fit / evaluate)."""
    t0 = time.time()
    yield
    elapsed = time.time() - t0
    print(f"⏱  {label}: {elapsed:.2f}s")
    if store is not None:
        store[label] = round(elapsed, 2)


def save_json(obj, filename):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    print(f"💾 Saved {path}")
    return path


def cluster_info(spark):
    """Small summary of the resources the job actually got (for the report)."""
    sc = spark.sparkContext
    conf = sc.getConf()
    # getExecutorMemoryStatus includes the driver -> subtract 1
    n_exec = max(sc._jsc.sc().getExecutorMemoryStatus().size() - 1, 0)
    return {
        "master": sc.master,
        "executors": n_exec,
        "executor_cores": conf.get("spark.executor.cores", "?"),
        "executor_memory": conf.get("spark.executor.memory", "?"),
        "default_parallelism": sc.defaultParallelism,
        "shuffle_partitions": conf.get("spark.sql.shuffle.partitions", "?"),
    }
