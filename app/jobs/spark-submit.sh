#!/bin/bash
# Spark 任务统一入口 - smart-scenic-bigdata
# 用法（在 spark-master 容器内）:
#   bash /opt/jobs/spark-submit.sh <task>     # task: clean | ml-train | fpgrowth

set -e

TASK="$1"
if [ -z "$TASK" ]; then
    echo "Usage: $0 {clean|ml-train|fpgrowth}"
    exit 1
fi

SPARK_HOME=/opt/spark
HDFS_CLEANED=hdfs://hadoop-namenode:9000/scenic/cleaned

case "$TASK" in
    clean)
        # Spark 清洗: /scenic/sqoop -> /scenic/cleaned
        if [ ! -f /opt/jobs/spark/clean.py ]; then
            echo "[FAIL] /opt/jobs/spark/clean.py not found"
            exit 1
        fi
        $SPARK_HOME/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/jobs/spark/clean.py
        ;;

    ml-train)
        # PySpark MLlib 训练
        if [ ! -f /opt/jobs/ml/train.py ]; then
            echo "[FAIL] /opt/jobs/ml/train.py not found"
            exit 1
        fi
        $SPARK_HOME/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/jobs/ml/train.py
        ;;

    fpgrowth)
        if [ ! -f /opt/jobs/ml/fpgrowth.py ]; then
            echo "[FAIL] /opt/jobs/ml/fpgrowth.py not found"
            exit 1
        fi
        $SPARK_HOME/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/jobs/ml/fpgrowth.py
        ;;

    *)
        echo "Unknown task: $TASK"
        exit 1
        ;;
esac
