"""
Spark FPGrowth 关联规则 - 智能景区
====================================
基于消费数据挖掘「景点 → 景点」「类型 → 类型」的关联规则。

执行方式（spark-master 内）：
  spark-submit --master spark://spark-master:7077 /opt/jobs/ml/fpgrowth.py

输入：HDFS /scenic/cleaned/{t_attraction, t_consumption}
输出：HDFS /scenic/models/fpgrowth_rules.json  +  /shared/models/
"""
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.fpm import FPGrowth

spark = SparkSession.builder.appName("SmartScenic-FPGrowth").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

HDFS_CLEAN = "hdfs://hadoop-namenode:9000/scenic/cleaned"
HDFS_OUT   = "hdfs://hadoop-namenode:9000/scenic/models/fpgrowth_rules"
SHARED_OUT = "/shared/models/fpgrowth_rules.json"

print("[1/3] load cleaned data ...", flush=True)
df_attr = spark.read.parquet(f"{HDFS_CLEAN}/t_attraction") \
    .select("attraction_id", "attraction_name", "attraction_type")
df_cons = spark.read.parquet(f"{HDFS_CLEAN}/t_consumption") \
    .select("visitor_id", "consume_time", "attraction_id")

print(f"    attractions={df_attr.count()}  consumption={df_cons.count()}", flush=True)


print("[2/3] FPGrowth: 同一游客多次到访的景点关联 ...", flush=True)
df_items = df_cons.select("visitor_id", "attraction_id") \
    .dropDuplicates(["visitor_id", "attraction_id"]) \
    .groupBy("visitor_id") \
    .agg(F.collect_list("attraction_id").alias("items")) \
    .filter(F.size("items") >= 2)

fp = FPGrowth(itemsCol="items", minSupport=0.02, minConfidence=0.3)
model = fp.fit(df_items)
rules_df = model.associationRules.orderBy(F.desc("lift"))
rules_df.show(10, truncate=False)

print(f"[3/3] save rules ... {rules_df.count()} total", flush=True)

# === 关联结果转为 JSON，含中文景点名 ===
attr_map = {r["attraction_id"]: r["attraction_name"] for r in df_attr.collect()}

def items_to_names(items):
    return [{"景点ID": i, "景点名称": attr_map.get(i, str(i))} for i in items]

rules_json = []
for r in rules_df.collect():
    rules_json.append({
        "antecedent": items_to_names(list(r["antecedent"])),
        "consequent": items_to_names(list(r["consequent"])),
        "confidence": float(r["confidence"]),
        "lift":       float(r["lift"]),
        "support":    float(r["support"]),
    })

# 保存到 local tmp + 直接写 shared volume（避免被 spark 清理）
import os, shutil
TMP_JSON = "/tmp/fpgrowth_rules.json"
SHARED_JSON = "/shared/models/fpgrowth_rules.json"
print(f"    writing to {TMP_JSON} ({len(rules_json)} rules)", flush=True)
with open(TMP_JSON, "w", encoding="utf-8") as f:
    json.dump(rules_json, f, ensure_ascii=False, indent=2)
print(f"    wrote {os.path.getsize(TMP_JSON)} bytes", flush=True)

# 立刻直接写一份到 shared volume（不依赖 /tmp 后续存在）
try:
    os.makedirs("/shared/models", exist_ok=True)
    with open(SHARED_JSON, "w", encoding="utf-8") as f:
        json.dump(rules_json, f, ensure_ascii=False, indent=2)
    print(f"    shared -> {SHARED_JSON}", flush=True)
except Exception as e:
    print(f"    shared SKIP: {e}", flush=True)

# 复制到 HDFS 和 local shared
sc = spark.sparkContext
hadoop = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
FileUtil = sc._jvm.org.apache.hadoop.fs.FileUtil

# HDFS
hdfs_dst = sc._jvm.org.apache.hadoop.fs.Path("/scenic/models/fpgrowth_rules.json")
FileUtil.copy(
    sc._jvm.org.apache.hadoop.fs.FileSystem.getLocal(hadoop.getConf()),
    sc._jvm.org.apache.hadoop.fs.Path(TMP_JSON),
    hadoop, hdfs_dst, True, sc._jsc.hadoopConfiguration()
)
print(f"    HDFS   -> /scenic/models/fpgrowth_rules.json", flush=True)

# local shared volume（容错：如果目录不存在则跳过）
try:
    parent = os.path.dirname(SHARED_OUT) or "/shared/models"
    os.makedirs(parent, exist_ok=True)
    shutil.copy(TMP_JSON, SHARED_OUT)
    print(f"    local  -> {SHARED_OUT}", flush=True)
except Exception as e:
    print(f"    local SKIP: {e}", flush=True)

print(f"\n=== FPGrowth Done: {len(rules_json)} rules ===", flush=True)
spark.stop()