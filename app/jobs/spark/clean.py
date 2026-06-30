"""
Spark 数据清洗作业 - 选题十八 智能景区管理系统
================================================
作业要求：  使用Spark进行数据清洗，去除噪声和无效数据

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
  4. 过滤异常值（年龄 < 0、金额 < 0、时长 < 0 / > 24）
  5. 派生字段（年龄段、消费等级、visit_date）

重要：Sqoop --table 把 MySQL 表列顺序原样输出.
  MySQL 的列顺序 (从 mysql-init/01-init-business.sql):
  - t_attraction:   景点ID, 景点名称, 类型, 位置, 开放时间
  - t_visitor:      游客ID, 姓名, 性别, 年龄, 地区
  - t_consumption:  消费ID, 时间, 游客ID, 景点ID, 消费金额
  - t_visit_record: 记录ID, 时间, 游客ID, 景点ID, 游玩时长

  Sqoop 不读 CSV 表头, 所以 _cX 列顺序与 MySQL 一致.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

# ============== Spark Session ==============
spark = SparkSession.builder \
    .appName("SmartScenic-Clean") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============== 输入输出路径 ==============
HDFS_IN  = "hdfs://hadoop-namenode:9000/scenic/sqoop"
HDFS_OUT = "hdfs://hadoop-namenode:9000/scenic/cleaned"


# ============== 1. 景点表清洗 ==============
# Sqoop 列顺序: _c0=景点ID, _c1=景点名称, _c2=类型, _c3=位置, _c4=开放时间
print("[1/4] clean t_attraction ...", flush=True)
df_attr = spark.read.csv(f"{HDFS_IN}/t_attraction", sep=",", header=False, encoding="utf-8")
attr_count_before = df_attr.count()

df_attr_clean = (
    df_attr.dropDuplicates()
    .filter(F.col("_c0").isNotNull())
)
attr_count_after = df_attr_clean.count()
print(f"    rows: {attr_count_before} -> {attr_count_after}", flush=True)

df_attr_clean = df_attr_clean.select(
    F.col("_c0").alias("attraction_id"),
    F.col("_c1").alias("attraction_name"),
    F.col("_c2").alias("attraction_type"),
    F.col("_c3").alias("location"),
    F.col("_c4").alias("open_time"),
)
df_attr_clean.write.mode("overwrite").parquet(f"{HDFS_OUT}/t_attraction")


# ============== 2. 游客表清洗 ==============
# Sqoop 列顺序: _c0=游客ID, _c1=姓名, _c2=性别, _c3=年龄, _c4=地区
print("[2/4] clean t_visitor ...", flush=True)
df_vis = spark.read.csv(f"{HDFS_IN}/t_visitor", sep=",", header=False, encoding="utf-8")
vis_count_before = df_vis.count()

df_vis_clean = (
    df_vis.dropDuplicates()
    .filter(F.col("_c0").isNotNull())
    .filter(F.col("_c3").cast(IntegerType()).between(0, 120))
)
vis_count_after = df_vis_clean.count()
print(f"    rows: {vis_count_before} -> {vis_count_after}", flush=True)

df_vis_clean = df_vis_clean.select(
    F.col("_c0").alias("visitor_id"),
    F.col("_c1").alias("visitor_name"),
    F.col("_c2").alias("gender"),
    F.col("_c3").cast(IntegerType()).alias("age"),
    F.col("_c4").alias("region"),
)
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
# Sqoop 列顺序: _c0=消费ID, _c1=时间, _c2=游客ID, _c3=景点ID, _c4=消费金额
print("[3/4] clean t_consumption ...", flush=True)
df_cons = spark.read.csv(f"{HDFS_IN}/t_consumption", sep=",", header=False, encoding="utf-8")
cons_count_before = df_cons.count()

df_cons_clean = (
    df_cons.dropDuplicates()
    .filter(F.col("_c0").isNotNull())
    .filter(F.col("_c2").isNotNull())  # 游客ID FK
    .filter(F.col("_c4").cast(DoubleType()) > 0)  # 金额 > 0
)
cons_count_after = df_cons_clean.count()
print(f"    rows: {cons_count_before} -> {cons_count_after}", flush=True)

df_cons_clean = df_cons_clean.select(
    F.col("_c0").cast("bigint").alias("consumption_id"),
    F.col("_c1").alias("consume_time"),
    F.col("_c2").alias("visitor_id"),
    F.col("_c3").alias("attraction_id"),
    F.col("_c4").cast(DoubleType()).alias("amount"),
)
df_cons_clean = df_cons_clean.withColumn(
    "consume_level",
    F.when(F.col("amount") < 100, "低消费")
     .when(F.col("amount") < 500, "中消费")
     .when(F.col("amount") < 1000, "高消费")
     .otherwise("超高消费")
)
df_cons_clean = df_cons_clean.withColumn(
    "consume_date", F.to_date("consume_time")
)
df_cons_clean.write.mode("overwrite").parquet(f"{HDFS_OUT}/t_consumption")


# ============== 4. 游玩记录表清洗 ==============
# Sqoop 列顺序: _c0=记录ID, _c1=时间, _c2=游客ID, _c3=景点ID, _c4=游玩时长
print("[4/4] clean t_visit_record ...", flush=True)
df_vr = spark.read.csv(f"{HDFS_IN}/t_visit_record", sep=",", header=False, encoding="utf-8")
vr_count_before = df_vr.count()

df_vr_clean = (
    df_vr.dropDuplicates()
    .filter(F.col("_c0").isNotNull())
    .filter(F.col("_c2").isNotNull())  # 游客ID FK
    .filter(F.col("_c4").cast(DoubleType()) > 0)  # 时长 > 0
    .filter(F.col("_c4").cast(DoubleType()) < 24)  # 时长 < 24
)
vr_count_after = df_vr_clean.count()
print(f"    rows: {vr_count_before} -> {vr_count_after}", flush=True)

df_vr_clean = df_vr_clean.select(
    F.col("_c0").cast("bigint").alias("record_id"),
    F.col("_c1").alias("visit_time"),
    F.col("_c2").alias("visitor_id"),
    F.col("_c3").alias("attraction_id"),
    F.col("_c4").cast(DoubleType()).alias("duration_hours"),
)
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
