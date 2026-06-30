"""
Spark 作业 - 把 cleaned 的游玩记录数据写入 HBase scenic_visit_record
====================================================================

作业要求：
  在 HBase 中存储实时游玩记录数据，并进行快速查询
  创建表结构，包括列族和列，如：时间、游客ID、景点ID、游玩时长

数据流：
  MySQL  -> Sqoop -> HDFS /scenic/sqoop/t_visit_record
        -> Spark clean.py -> HDFS /scenic/cleaned/t_visit_record (Parquet)
        -> 本脚本 -> HBase scenic_visit_record (cf: visit_time/attraction_id/duration_hours)
                                       scenic_visitor_profile (stats: total_visits/total_duration/...)
                                       scenic_attraction_heat (stats: total_visitors/...)

执行方式（在 spark-master 容器内）：
  spark-submit --master spark://spark-master:7077 /opt/jobs/spark/write_visit_to_hbase.py

实现说明：
  由于 happybase Python 客户端与 HBase 2.x Thrift 协议不兼容（详见 AGENTS.md 5.3），
  本脚本采用"Spark 输出 HBase ImportTsv 格式 + 容器内 hbase shell 批量导入"两步走：
  1. Spark 把游玩记录 join 游客/景点后写出 HDFS /tmp/hbase_import/visit/ 目录的 HFile-like 文本
  2. 写一个 shell 脚本 /opt/jobs/hbase/import_visit.sh 调用 hbase shell 批量 put
  3. 同时聚合：按 visitor 写入 scenic_visitor_profile，按 attraction 写入 scenic_attraction_heat
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType

# ============== Spark Session ==============
spark = SparkSession.builder \
    .appName("SmartScenic-WriteVisitToHBase") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============== 输入路径 ==============
HDFS_CLEAN = "hdfs://hadoop-namenode:9000/scenic/cleaned"
HDFS_OUT_HBASE = "hdfs://hadoop-namenode:9000/tmp/hbase_import/visit"

# ============== 1. 读清洗后的游玩记录 ==============
print("[1/4] read cleaned t_visit_record ...", flush=True)
df_vr = spark.read.parquet(f"{HDFS_CLEAN}/t_visit_record")
total = df_vr.count()
print(f"    rows: {total}", flush=True)

# 标准化字段类型
df_vr = df_vr.select(
    F.col("record_id").cast(StringType()).alias("record_id"),
    F.col("visit_time").cast(StringType()).alias("visit_time"),
    F.col("visitor_id").cast(StringType()).alias("visitor_id"),
    F.col("attraction_id").cast(StringType()).alias("attraction_id"),
    F.col("duration_hours").cast(DoubleType()).alias("duration_hours"),
    F.col("visit_date").cast(StringType()).alias("visit_date"),
).filter(F.col("visitor_id").isNotNull() & F.col("attraction_id").isNotNull())


# ============== 2. 输出 HBase put 脚本（scenic_visit_record） ==============
print("[2/4] generate scenic_visit_record put script ...", flush=True)


def to_put_visit(row):
    """生成一条 HBase put 命令：scenic_visit_record 表
    row_key: V{visitor_id}_{visit_time}  按游客+时间前缀查
    """
    rk = f"V{row.visitor_id}_{row.visit_time}".replace(" ", "_").replace(":", "")
    cf = "cf"
    return (
        f'put "scenic_visit_record", "{rk}", "{cf}:visit_time", "{row.visit_time}"\n'
        f'put "scenic_visit_record", "{rk}", "{cf}:attraction_id", "{row.attraction_id}"\n'
        f'put "scenic_visit_record", "{rk}", "{cf}:duration_hours", "{row.duration_hours}"\n'
        f'put "scenic_visit_record", "{rk}", "{cf}:visitor_id", "{row.visitor_id}"\n'
    )


# 用 rdd + map 写出，避免 dataframe 复杂 UDF
puts_visit = df_vr.rdd.map(to_put_visit)
puts_visit.saveAsTextFile(f"{HDFS_OUT_HBASE}/scenic_visit_record")


# ============== 3. 游客画像 scenic_visitor_profile ==============
print("[3/4] generate scenic_visitor_profile put script ...", flush=True)
df_visitor_stats = (
    df_vr.groupBy("visitor_id")
    .agg(
        F.count("*").alias("total_visits"),
        F.round(F.sum("duration_hours"), 2).alias("total_duration"),
        F.first("attraction_id").alias("last_attraction"),
        F.max("visit_time").alias("last_visit_time"),
    )
)


def to_put_profile(row):
    rk = f"V{int(row.visitor_id):08d}"
    return (
        f'put "scenic_visitor_profile", "{rk}", "stats:total_visits", "{row.total_visits}"\n'
        f'put "scenic_visitor_profile", "{rk}", "stats:total_duration", "{row.total_duration}"\n'
        f'put "scenic_visitor_profile", "{rk}", "stats:last_attraction", "{row.last_attraction}"\n'
        f'put "scenic_visitor_profile", "{rk}", "stats:last_visit_time", "{row.last_visit_time}"\n'
    )


puts_profile = df_visitor_stats.rdd.map(to_put_profile)
puts_profile.saveAsTextFile(f"{HDFS_OUT_HBASE}/scenic_visitor_profile")


# ============== 4. 景点热度 scenic_attraction_heat ==============
print("[4/4] generate scenic_attraction_heat put script ...", flush=True)
df_attr_stats = (
    df_vr.groupBy("attraction_id")
    .agg(
        F.countDistinct("visitor_id").alias("total_visitors"),
        F.round(F.sum("duration_hours"), 2).alias("total_duration"),
        F.max("visit_time").alias("last_visit_time"),
    )
)


def to_put_attr(row):
    rk = f"A{int(row.attraction_id):04d}"
    return (
        f'put "scenic_attraction_heat", "{rk}", "stats:total_visitors", "{row.total_visitors}"\n'
        f'put "scenic_attraction_heat", "{rk}", "stats:total_duration", "{row.total_duration}"\n'
        f'put "scenic_attraction_heat", "{rk}", "stats:last_visit_time", "{row.last_visit_time}"\n'
    )


puts_attr = df_attr_stats.rdd.map(to_put_attr)
puts_attr.saveAsTextFile(f"{HDFS_OUT_HBASE}/scenic_attraction_heat")


print("\n=== Spark Write-To-HBase Puts Generated ===", flush=True)
print(f"输出目录：{HDFS_OUT_HBASE}/", flush=True)
print(f"  scenic_visit_record    : {total} 行", flush=True)
print(f"  scenic_visitor_profile : {df_visitor_stats.count()} 行", flush=True)
print(f"  scenic_attraction_heat : {df_attr_stats.count()} 行", flush=True)
print("\n下一步：", flush=True)
print("  docker exec hbase-master hbase shell /opt/jobs/hbase/import_visit.sh", flush=True)

spark.stop()
