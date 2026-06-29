#!/bin/bash
# =====================================================
# Spark 作业统一运行脚本
# =====================================================
# 在 spark-master 容器内执行：
#   bash /opt/jobs/spark-submit.sh <作业名>
#
# 作业名：
#   clean     - 数据清洗
#   ml-train  - PySpark MLlib 训练
# =====================================================

set -e

JOB_NAME="${1:-clean}"
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
JOBS_DIR="/opt/jobs"

case "$JOB_NAME" in
  clean)
    echo "=== Spark Clean: 去重/类型转换/派生字段 ==="
    spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --driver-memory 1g \
      --executor-memory 1g \
      "${JOBS_DIR}/spark/clean.py"
    ;;

  ml-train)
    echo "=== Spark MLlib Train: 回归/聚类/分类 ==="
    spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --driver-memory 1g \
      --executor-memory 1g \
      --num-executors 1 \
      "${JOBS_DIR}/ml/train.py"
    ;;

  *)
    echo "Usage: $0 {clean|ml-train}"
    exit 1
    ;;
esac