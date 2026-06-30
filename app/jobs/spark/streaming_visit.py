"""
Spark Structured Streaming - 智能景区实时事件处理
====================================================

作业要求：
  6.3 数据处理与存储 - Kafka 实时流
  6.5 可视化与系统整合 - 数据的动态更新功能，根据时间段或景点选择展示不同的景区数据

数据流：
  Kafka topic scenic_events（实时入园/出园/消费事件）
       ↓  Spark Structured Streaming
       ↓  1) 原始明细 → HDFS /scenic/realtime/events/ (Parquet, 按 dt/hour 分区)
       ↓  2) 窗口聚合（5 分钟窗口）→ HDFS /scenic/realtime/agg_attraction_5min/ (Parquet)
       ↓  3) 最新事件 → HBase scenic_realtime (foreachBatch + 写 HBase put 脚本)

启动方式（spark-master 容器内）：
  spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
      /opt/jobs/spark/streaming_visit.py

环境要求：
  - Kafka topic scenic_events 已创建
  - HDFS /scenic/realtime/ 可写
  - HBase master 容器可达（hbase shell 可执行）
"""
from __future__ import annotations

import os
import sys
import time
import subprocess
import shutil
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, TimestampType,
)

# ============== 配置 ==============
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
TOPIC = os.getenv("KAFKA_TOPIC_EVENTS", "scenic_events")
HDFS_REALTIME_BASE = "hdfs://hadoop-namenode:9000/scenic/realtime"
HDFS_OUT_EVENTS = f"{HDFS_REALTIME_BASE}/events"
HDFS_OUT_AGG = f"{HDFS_REALTIME_BASE}/agg_attraction_5min"
HDFS_TMP_HBASE = f"{HDFS_REALTIME_BASE}/_hbase_put_tmp"

# HBase 容器（写 HBase 用 docker exec）
HBASE_CONTAINER = os.getenv("HBASE_CONTAINER", "hbase-master")
HBASE_TABLE = "scenic_realtime"

CHECKPOINT_BASE = f"{HDFS_REALTIME_BASE}/_checkpoints"

WINDOW_DURATION = "5 minutes"
WATERMARK = "2 minutes"
TRIGGER_INTERVAL = "30 seconds"


# ============== Kafka 事件 schema ==============
event_schema = StructType([
    StructField("type",         StringType(),  True),
    StructField("visitor_id",   StringType(),  True),
    StructField("attraction_id", StringType(), True),
    StructField("event_type",   StringType(),  True),  # enter / exit / consume
    StructField("ts",           LongType(),    True),  # 毫秒时间戳
])


# ============== 写 HBase 的辅助函数 ==============
def _write_hbase_batch(spark, batch_df, batch_id: int) -> None:
    """把一个 micro-batch 的数据通过 'hbase shell put' 灌进 HBase scenic_realtime。

    由于 happybase 协议不兼容（详见 AGENTS.md 5.3），我们采用 P3 同款方案：
      1. 把 batch 数据写成 HDFS 文本（每行一条 put）
      2. docker exec hbase shell 批量导入
    """
    if batch_df.rdd.isEmpty():
        return

    local_dir = f"/tmp/hbase_realtime_batch_{batch_id}"
    if os.path.exists(local_dir):
        shutil.rmtree(local_dir)
    os.makedirs(local_dir, exist_ok=True)

    # 1) Spark 写出 HBase put 文本
    def to_put(row):
        rk = f"E{int(time.time() * 1000)}_{row.visitor_id}_{row.event_type}"
        return (
            f'put "{HBASE_TABLE}", "{rk}", "cf:visitor_id", "{row.visitor_id}"\n'
            f'put "{HBASE_TABLE}", "{rk}", "cf:attraction_id", "{row.attraction_id}"\n'
            f'put "{HBASE_TABLE}", "{rk}", "cf:event_type", "{row.event_type}"\n'
            f'put "{HBASE_TABLE}", "{rk}", "cf:ts", "{row.ts}"\n'
        )

    batch_df.rdd.map(to_put).coalesce(1).saveAsTextFile(f"file://{local_dir}/puts")

    # 2) 合并 part 文件
    puts_file = f"{local_dir}/puts_merged.txt"
    with open(puts_file, "w") as out:
        for f in os.listdir(f"{local_dir}/puts"):
            with open(f"{local_dir}/puts/{f}", "r", encoding="utf-8") as fp:
                out.write(fp.read())

    # 3) docker exec hbase shell
    try:
        # 读 file 给 hbase shell stdin
        result = subprocess.run(
            ["docker", "exec", "-i", HBASE_CONTAINER, "hbase", "shell"],
            stdin=open(puts_file, "rb"),
            capture_output=True,
            timeout=60,
        )
        if result.returncode == 0:
            print(f"  [batch {batch_id}] HBase put OK ({os.path.getsize(puts_file)} bytes)")
        else:
            print(f"  [batch {batch_id}] HBase put WARN: {result.stderr.decode('utf-8', 'ignore')[:200]}")
    except FileNotFoundError:
        # 没 docker 命令 → 降级写到 HDFS，由外部 hbase 工具读
        batch_df.write.mode("append").text(f"{HDFS_TMP_HBASE}/batch_{batch_id}")
        print(f"  [batch {batch_id}] no docker, fallback to HDFS {HDFS_TMP_HBASE}/batch_{batch_id}")
    except Exception as e:
        print(f"  [batch {batch_id}] HBase put ERR: {e}")

    # 4) 清理本地
    shutil.rmtree(local_dir, ignore_errors=True)


# ============== 主程序 ==============
def main() -> None:
    print("=== Smart Scenic Spark Streaming ===", flush=True)
    print(f"Kafka bootstrap: {KAFKA_BOOTSTRAP}", flush=True)
    print(f"Topic:           {TOPIC}", flush=True)
    print(f"HDFS out:        {HDFS_REALTIME_BASE}", flush=True)
    print(f"Window:          {WINDOW_DURATION} (watermark {WATERMARK})", flush=True)
    print(f"Trigger:         {TRIGGER_INTERVAL}", flush=True)

    # ============== Spark Session（带 Kafka 包） ==============
    spark = (
        SparkSession.builder
        .appName("SmartScenic-Streaming")
        .config("spark.sql.streaming.checkpointLocation", f"{CHECKPOINT_BASE}/main")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ============== 1. Kafka source ==============
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    print(f"[1/4] Kafka source subscribed to {TOPIC}", flush=True)

    # 2. 解析 JSON payload
    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str", "CAST(timestamp AS TIMESTAMP) AS kafka_ts")
        .select(
            F.from_json(F.col("json_str"), event_schema).alias("e"),
            F.col("kafka_ts"),
        )
        .select(
            F.col("e.visitor_id").alias("visitor_id"),
            F.col("e.attraction_id").alias("attraction_id"),
            F.col("e.event_type").alias("event_type"),
            F.col("e.ts").alias("ts_ms"),
            F.col("kafka_ts"),
        )
        .filter(F.col("visitor_id").isNotNull() & F.col("event_type").isNotNull())
        .withColumn("ts", F.from_unixtime(F.col("ts_ms") / 1000.0).cast(TimestampType()))
        .withColumn("dt", F.to_date("ts"))
        .withColumn("hour", F.hour("ts"))
    )
    print("[2/4] Parsed schema:", flush=True)
    parsed.printSchema()

    # ============== 3. 明细流：写 HDFS Parquet（按 dt/hour 分区） ==============
    events_query = (
        parsed.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", HDFS_OUT_EVENTS)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/events")
        .partitionBy("dt", "hour")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    print(f"[3/4] Events sink started -> {HDFS_OUT_EVENTS}/dt=YYYY-MM-DD/hour=HH/", flush=True)

    # ============== 4. 窗口聚合：每 5 分钟按 attraction 统计 enter/exit/consume ==============
    agg = (
        parsed
        .withWatermark("ts", WATERMARK)
        .groupBy(
            F.window(F.col("ts"), WINDOW_DURATION, "1 minute"),
            F.col("attraction_id"),
            F.col("event_type"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("visitor_id").alias("unique_visitors"),
            F.max("ts").alias("last_event_ts"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("attraction_id"),
            F.col("event_type"),
            F.col("event_count"),
            F.col("unique_visitors"),
            F.col("last_event_ts"),
        )
    )

    agg_query = (
        agg.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", HDFS_OUT_AGG)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/agg")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    print(f"[4/4] Agg sink started -> {HDFS_OUT_AGG}/ (5-min window by attraction)", flush=True)

    # ============== 5. foreachBatch：每批把最新事件灌进 HBase ==============
    hbase_query = (
        parsed
        .select("visitor_id", "attraction_id", "event_type", "ts_ms")
        .writeStream
        .outputMode("append")
        .foreachBatch(lambda df, batch_id: _write_hbase_batch(spark, df, batch_id))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/hbase")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    print(f"[5/5] HBase sink started -> {HBASE_TABLE} (every {TRIGGER_INTERVAL})", flush=True)

    print("\n=== Streaming Started ===", flush=True)
    print("Awaiting termination... (Ctrl-C to stop)", flush=True)

    # 等待所有 query
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping...", flush=True)
        for q in spark.streams.active:
            q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
