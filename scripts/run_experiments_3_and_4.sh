#!/usr/bin/env bash
# ==============================================================================
# Experiments 3 & 4 Automated Execution Runner (for Thiện)
# Usage:
#   1. Make sure Docker Desktop is running.
#   2. Run this script from repo root: ./scripts/run_experiments_3_and_4.sh
# ==============================================================================
set -e

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR/docker"

echo "========================================================================"
echo "🚀 STARTING BENCHMARK EXPERIMENTS 3 & 4 ON SPARK CLUSTER"
echo "========================================================================"

# Check Docker
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker daemon is not running! Please start Docker Desktop first."
    exit 1
fi

echo "📦 Ensuring cluster is running with 2 workers initially..."
docker compose up -d spark-master spark-worker-1 spark-worker-2 jupyter
sleep 5

echo ""
echo "========================================================================"
echo "🧪 EXPERIMENT 3: DATA SCALABILITY (2M -> 5M -> 10M rows)"
echo "========================================================================"

echo "▶️ [Exp 3.1] Running 2,000,000 rows..."
docker exec -it jupyter-pyspark bash -c \
    "cd /home/jovyan/src/ml_experiments && spark-submit run_pipeline.py --rows 2000000 --label exp3_2M"

echo "▶️ [Exp 3.2] Running 5,000,000 rows..."
docker exec -it jupyter-pyspark bash -c \
    "cd /home/jovyan/src/ml_experiments && spark-submit run_pipeline.py --rows 5000000 --label exp3_5M"

echo "▶️ [Exp 3.3] Running 10,000,000 rows (Full subset)..."
docker exec -it jupyter-pyspark bash -c \
    "cd /home/jovyan/src/ml_experiments && spark-submit run_pipeline.py --rows 10000000 --label exp3_10M"

echo ""
echo "========================================================================"
echo "🧪 EXPERIMENT 4: HORIZONTAL RESOURCE SCALING (1 -> 2 -> 3 Workers)"
echo "========================================================================"

echo "▶️ [Exp 4.1] Scaling cluster to 1 Worker..."
docker compose stop spark-worker-2 spark-worker-3 || true
docker compose up -d spark-worker-1
sleep 5
docker exec -it jupyter-pyspark bash -c \
    "cd /home/jovyan/src/ml_experiments && spark-submit run_pipeline.py --label exp4_1w"

echo "▶️ [Exp 4.2] Scaling cluster to 2 Workers..."
docker compose up -d spark-worker-1 spark-worker-2
sleep 5
docker exec -it jupyter-pyspark bash -c \
    "cd /home/jovyan/src/ml_experiments && spark-submit run_pipeline.py --label exp4_2w"

echo "▶️ [Exp 4.3] Scaling cluster to 3 Workers..."
docker compose up -d spark-worker-1 spark-worker-2 spark-worker-3
sleep 5
docker exec -it jupyter-pyspark bash -c \
    "cd /home/jovyan/src/ml_experiments && spark-submit run_pipeline.py --label exp4_3w"

echo ""
echo "========================================================================"
echo "🎉 ALL BENCHMARKS COMPLETED! Results appended to results/pipeline_runs.csv"
echo "========================================================================"
