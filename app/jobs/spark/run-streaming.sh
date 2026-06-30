#!/bin/bash
# ============================================================
# Spark Structured Streaming 启动脚本
# 智能景区大数据平台 - 实时事件流（Kafka → Spark → HDFS/HBase）
# ============================================================
# 作业要求：
#   6.3 数据处理与存储 - Kafka 实时流处理
#   6.5 可视化与系统整合 - 动态数据更新
#
# 执行（在 spark-master 容器内）：
#   bash /opt/jobs/spark/run-streaming.sh
#
# 依赖：
#   - spark-sql-kafka-0-10_2.12:3.4.1（通过 --packages 自动下载）
#   - Kafka topic scenic_events 已创建（kafka_producer.py 启动时会自动建）
#   - HBase scenic_realtime 表已存在（hbase-ddl.sh 启动时会建）
# ============================================================
set -e

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka-1:9092}"
KAFKA_TOPIC_EVENTS="${KAFKA_TOPIC_EVENTS:-scenic_events}"
HBASE_CONTAINER="${HBASE_CONTAINER:-hbase-master}"
SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"

# 任务脚本路径
JOB_SCRIPT="${JOB_SCRIPT:-/opt/jobs/spark/streaming_visit.py}"

# Kafka 包（首次启动需要下载，~5 MB）
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"

echo "=== Spark Structured Streaming 启动 ==="
echo "SPARK_MASTER:    ${SPARK_MASTER}"
echo "KAFKA_BOOTSTRAP: ${KAFKA_BOOTSTRAP}"
echo "TOPIC:           ${KAFKA_TOPIC_EVENTS}"
echo "HBASE_CONTAINER: ${HBASE_CONTAINER}"
echo "JOB_SCRIPT:      ${JOB_SCRIPT}"
echo "KAFKA_PACKAGE:   ${KAFKA_PACKAGE}"
echo ""

# 1) 确保 Kafka topic 存在
echo "[1/3] 确保 Kafka topic ${KAFKA_TOPIC_EVENTS} 存在 ..."
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "${KAFKA_TOPIC_EVENTS}" \
    --partitions 2 --replication-factor 1 2>&1 | head -3 || true

# 2) 确保 HBase scenic_realtime 表存在
echo "[2/3] 确保 HBase ${HBASE_CONTAINER} scenic_realtime 表存在 ..."
docker exec "${HBASE_CONTAINER}" hbase shell -e "exists 'scenic_realtime' || create 'scenic_realtime', 'cf'" 2>&1 | head -3 || true

# 3) spark-submit
echo "[3/3] 提交 Spark Streaming 任务 ..."
exec "${SPARK_HOME}/bin/spark-submit" \
    --master "${SPARK_MASTER}" \
    --deploy-mode client \
    --name "SmartScenic-Streaming" \
    --packages "${KAFKA_PACKAGE}" \
    --conf "spark.sql.streaming.checkpointLocation=/scenic/realtime/_checkpoints/main" \
    --conf "spark.sql.shuffle.partitions=4" \
    "${JOB_SCRIPT}"
