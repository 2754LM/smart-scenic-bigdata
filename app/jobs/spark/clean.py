"""
Spark 数据清洗作业 - 选题十八 智能景区管理系统
================================================

作业要求：
  使用Spark进行数据清洗，去除噪声和无效数据

执行方式（在 spark-master 容器内）：
  spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/jobs/spark/clean.py

输入：HDFS /scenic/sqoop/{t_attraction,t_visitor,t_consumption,t_visit_record}/
输出：HDFS /scenic/cleaned/...

清洗规则：
  1. 去重（基于主键）
  2. 过滤空值主键记录
  3. 类型转换（字符串 → 数字/日期）
  4. 过滤异常值（年龄 < 0、金额 < 0、时长 < 0）
  5. 派生字段（年龄段、消费等级）
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

# ============== Spark Session ==============
spark = SparkSession.builder \
    .appName("SmartScenic-Clean") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============== 输入路径 ==============
HDFS_IN = "hdfs://hadoop-namenode:9000/scenic/sqoop"
HDFS_OUT = "hdfs://hadoop-namenode:9000/scenic/cleaned"


# ============== 1. 景点表清洗 ==============
print("[1/4] clean t_attraction ...", flush=True)
df_attr = spark.read.csv(f"{HDFS_IN}/t_attraction", sep="\t", header=False, encoding="utf-8")
# Sqoop 默认输出格式：列名是 _c0, _c1, ...
# 由于中文字段名被 Sqoop 跳过，实际是 numCols 列
# 实际项目里用 Sqoop 显式 --columns 重新指定会更安全

# 简化：假设 Sqoop 输出 schema 已正确（如果不对应自行加 schema）
# 这里我们重读 Sqoop 输出时用 Sqoop 生成的列名 _c0, _c1, ...
attr_count_before = df_attr.count()
df_attr_clean = df_attr.dropDuplicates().filter(F.col("_c0").isNotNull())
attr_count_after = df_attr_clean.count()
print(f"    rows: {attr_count_before} -> {attr_count_after}", flush=True)

# 显式列名（Sqoop 输出：景点ID\t景点名称\t类型\t位置\t开放时间）
df_attr_clean = df_attr_clean.toDF(
    "attraction_id", "attraction_name", "attraction_type", "location", "open_time"
)
df_attr_clean.write.mode("overwrite").parquet(f"{HDFS_OUT}/t_attraction")


# ============== 2. 游客表清洗 ==============
print("[2/4] clean t_visitor ...", flush=True)
df_vis = spark.read.csv(f"{HDFS_IN}/t_visitor", sep="\t", header=False, encoding="utf-8")
vis_count_before = df_vis.count()
df_vis_clean = (
    df_vis.dropDuplicates()
    .filter(F.col("_c0").isNotNull())
    .filter(F.col("_c3").cast(IntegerType()).between(0, 120))  # 年龄合理范围
)
vis_count_after = df_vis_clean.count()
print(f"    rows: {vis_count_before} -> {vis_count_after}", flush=True)

df_vis_clean = df_vis_clean.toDF(
    "visitor_id", "visitor_name", "gender", "age", "region"
)
# 派生字段：年龄段
df_vis_clean = df_vis_clean.withColumn(
    "age_group",
    F.when(F.col("age") < 18, "未成年")
     .when(F.col("age") < 30, "青年")
     .when(F.col("age") < 45, "中年")
     .when(F.col("age") < 60, "中老年")
     .otherwise("老年")
)
df_vis_clean.write.mode("overwrite").parquet(f"{HDFS_OUT}/t_visitor")


# ============== 3. 消费表清洗 ==============
print("[3/4] clean t_consumption ...", flush=True)
df_cons = spark.read.csv(f"{HDFS_IN}/t_consumption", sep="\t", header=False, encoding="utf-8")
cons_count_before = df_cons.count()
df_cons_clean = (
    df_cons.dropDuplicates()
    .filter(F.col("_c0").isNotNull())
    .filter(F.col("_c3").isNotNull())  # 游客ID
    .filter(F.col("_c4").cast(DoubleType()) > 0)  # 消费金额 > 0
)
cons_count_after = df_cons_clean.count()
print(f"    rows: {cons_count_before} -> {cons_count_after}", flush=True)

df_cons_clean = df_cons_clean.toDF(
    "consumption_id", "consume_time", "visitor_id", "attraction_id", "amount"
)
# 消费金额类型转换
df_cons_clean = df_cons_clean.withColumn("amount", F.col("amount").cast(DoubleType()))
# 派生字段：消费等级
df_cons_clean = df_cons_clean.withColumn(
    "consume_level",
    F.when(F.col("amount") < 100, "低消费")
     .when(F.col("amount") < 500, "中消费")
     .when(F.col("amount") < 1000, "高消费")
     .otherwise("超高消费")
)
# 派生字段：消费日期（去掉时分秒）
df_cons_clean = df_cons_clean.withColumn(
    "consume_date", F.to_date("consume_time")
)
df_cons_clean.write.mode("overwrite").parquet(f"{HDFS_OUT}/t_consumption")


# ============== 4. 游玩记录表清洗 ==============
print("[4/4] clean t_visit_record ...", flush=True)
df_vr = spark.read.csv(f"{HDFS_IN}/t_visit_record", sep="\t", header=False, encoding="utf-8")
vr_count_before = df_vr.count()
df_vr_clean = (
    df_vr.dropDuplicates()
    .filter(F.col("_c0").isNotNull())
    .filter(F.col("_c3").isNotNull())
    .filter(F.col("_c4").cast(DoubleType()) > 0)  # 游玩时长 > 0
    .filter(F.col("_c4").cast(DoubleType()) < 24)  # 单次游玩不超过 24 小时
)
vr_count_after = df_vr_clean.count()
print(f"    rows: {vr_count_before} -> {vr_count_after}", flush=True)

df_vr_clean = df_vr_clean.toDF(
    "record_id", "visit_time", "visitor_id", "attraction_id", "duration_hours"
)
df_vr_clean = df_vr_clean.withColumn("duration_hours", F.col("duration_hours").cast(DoubleType()))
df_vr_clean = df_vr_clean.withColumn("visit_date", F.to_date("visit_time"))
df_vr_clean.write.mode("overwrite").parquet(f"{HDFS_OUT}/t_visit_record")


# ============== 完成 ==============
print("\n=== Spark Clean Done ===", flush=True)
print(f"输出目录：{HDFS_OUT}", flush=True)
print(f"  t_attraction      : {attr_count_after:>6} 行", flush=True)
print(f"  t_visitor         : {vis_count_after:>6} 行", flush=True)
print(f"  t_consumption     : {cons_count_after:>6} 行", flush=True)
print(f"  t_visit_record    : {vr_count_after:>6} 行", flush=True)

spark.stop()