"""
PySpark MLlib 训练脚本 - 选题十八 智能景区管理系统
====================================================

作业要求：
  - 回归分析：使用回归模型（线性/Lasso/Ridge）预测游客数量和消费金额
  - 聚类分析：K-means / DBSCAN 识别不同游客群体
  - 分类分析：决策树/随机森林 分类不同游客行为

执行方式（在 spark-master 容器内）：
  spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/jobs/ml/train.py

输入：HDFS /scenic/cleaned/...
输出：
  HDFS  /scenic/models/                 (主存储)
  /shared/models/  (挂载卷，后端直接加载)

双轨模式：
  训练用 PySpark MLlib（在容器内）→ 模型保存 HDFS + shared volume
  预测用 PySpark 加载（在后端）→ 实时 transform() 预测
"""
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier
from pyspark.ml.evaluation import (
    RegressionEvaluator, ClusteringEvaluator, MulticlassClassificationEvaluator
)
from pyspark.sql import functions as F

# ============== Spark Session ==============
spark = SparkSession.builder \
    .appName("SmartScenic-ML-Train") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_CLEAN = "hdfs://hadoop-namenode:9000/scenic/cleaned"
HDFS_MODEL = "hdfs://hadoop-namenode:9000/scenic/models"
SHARED_MODEL = "/shared/models"  # 挂载卷，后端加载用


# ============== 1. 准备训练数据 ==============
print("[1/6] prepare training data ...", flush=True)

df_visitor = spark.read.parquet(f"{HDFS_CLEAN}/t_visitor") \
    .select("visitor_id", "age", "age_group")

df_cons = spark.read.parquet(f"{HDFS_CLEAN}/t_consumption") \
    .groupBy("visitor_id") \
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("consumption_id").alias("purchase_count"),
        F.avg("amount").alias("avg_amount")
    )

df_visit = spark.read.parquet(f"{HDFS_CLEAN}/t_visit_record") \
    .groupBy("visitor_id") \
    .agg(
        F.count("record_id").alias("visit_count"),
        F.avg("duration_hours").alias("avg_duration"),
        F.countDistinct("attraction_id").alias("unique_attractions")
    )

df_features = (
    df_visitor
    .join(df_cons,   "visitor_id", "left")
    .join(df_visit,  "visitor_id", "left")
    .fillna(0)
)
df_features = df_features.withColumn(
    "high_value_label",
    F.when(F.col("total_amount") > 500, 1.0).otherwise(0.0)
)
print(f"    total rows: {df_features.count()}", flush=True)

# 特征组装 + 标准化
feature_cols = ["age", "purchase_count", "avg_amount", "visit_count",
                "avg_duration", "unique_attractions"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                        withMean=True, withStd=True)
pipeline_prep = Pipeline(stages=[assembler, scaler])
df_prep = pipeline_prep.fit(df_features).transform(df_features)

train_data, test_data = df_prep.randomSplit([0.8, 0.2], seed=42)
print(f"    train: {train_data.count()}, test: {test_data.count()}", flush=True)


# ============== 辅助函数：保存模型到 HDFS + shared volume ==============
import shutil
import os
import subprocess

def save_model(trained_model, name: str):
    """保存到 HDFS 主存储 + /shared/models/ 给后端直接加载"""
    hdfs_path  = f"{HDFS_MODEL}/{name}"

    # 1. 保存到 HDFS
    trained_model.write().overwrite().save(hdfs_path)
    print(f"      HDFS  -> {hdfs_path}", flush=True)

    # 2. 复制到 local shared volume（用 hdfs dfs -get 绕过 pyspark 跨文件系统问题）
    local_path = f"{SHARED_MODEL}/{name}"
    if os.path.exists(local_path):
        shutil.rmtree(local_path)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    os.makedirs(local_path, exist_ok=True)

    # hdfs dfs -get <hdfs_dir> <local_dir>  会把 hdfs_dir 整个放到 local_dir 下
    # 先 get 到 /tmp/<name>，再 mv 到 /shared/models/<name>
    tmp_root = "/tmp/_model_dl"
    os.makedirs(tmp_root, exist_ok=True)
    tmp_target = os.path.join(tmp_root, name)
    if os.path.exists(tmp_target):
        shutil.rmtree(tmp_target)
    subprocess.run(
        ["hdfs", "dfs", "-get", hdfs_path, tmp_target],
        check=True, capture_output=True
    )
    # hdfs dfs -get 出来是目录 tmp_target/
    # 我们要把它移到 local_path
    if os.path.isdir(tmp_target):
        # tmp_target 本身就是目录
        for item in os.listdir(tmp_target):
            shutil.move(os.path.join(tmp_target, item), local_path)
        shutil.rmtree(tmp_target, ignore_errors=True)
    print(f"      local -> {local_path}", flush=True)


# ============== 2. 回归模型 ==============
print("\n[2/6] regression training ...", flush=True)

regression_results = []
for name, model in [
    ("linear",  LinearRegression(featuresCol="features", labelCol="total_amount", regParam=0.0, elasticNetParam=0.0)),
    ("lasso",   LinearRegression(featuresCol="features", labelCol="total_amount", regParam=0.1, elasticNetParam=1.0)),
    ("ridge",   LinearRegression(featuresCol="features", labelCol="total_amount", regParam=0.1, elasticNetParam=0.0)),
    ("rf",      RandomForestRegressor(featuresCol="features", labelCol="total_amount", numTrees=20)),
]:
    trained = model.fit(train_data)
    pred = trained.transform(test_data)
    rmse = RegressionEvaluator(labelCol="total_amount", metricName="rmse").evaluate(pred)
    r2   = RegressionEvaluator(labelCol="total_amount", metricName="r2").evaluate(pred)
    print(f"    {name:8s}  RMSE={rmse:8.2f}  R²={r2:6.4f}", flush=True)
    save_model(trained, f"regression_{name}")
    regression_results.append((f"regression_{name}", "regression", rmse, r2))


# ============== 3. 聚类模型 ==============
print("\n[3/6] KMeans clustering ...", flush=True)
kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=4, seed=42)
kmeans_model = kmeans.fit(df_prep)
kmeans_pred  = kmeans_model.transform(df_prep)
silhouette   = ClusteringEvaluator(predictionCol="cluster", metricName="silhouette").evaluate(kmeans_pred)
print(f"    silhouette score: {silhouette:6.4f}", flush=True)
save_model(kmeans_model, "clustering_kmeans")
print(f"    cluster centers:", flush=True)
for i, c in enumerate(kmeans_model.clusterCenters()):
    print(f"      cluster {i}: {[round(float(x),2) for x in c]}", flush=True)


# ============== 4. 分类模型 ==============
print("\n[4/6] classification training ...", flush=True)

df_clf = df_prep.filter(F.col("high_value_label").isin([0.0, 1.0]))
train_c, test_c = df_clf.randomSplit([0.8, 0.2], seed=42)

classification_results = []
for name, model in [
    ("dt", DecisionTreeClassifier(featuresCol="features", labelCol="high_value_label", maxDepth=10)),
    ("rf", RandomForestClassifier(featuresCol="features", labelCol="high_value_label", numTrees=20, maxDepth=10)),
]:
    trained = model.fit(train_c)
    pred    = trained.transform(test_c)
    acc     = MulticlassClassificationEvaluator(labelCol="high_value_label", metricName="accuracy").evaluate(pred)
    f1      = MulticlassClassificationEvaluator(labelCol="high_value_label", metricName="f1").evaluate(pred)
    print(f"    {name:4s}  accuracy={acc:6.4f}  f1={f1:6.4f}", flush=True)
    save_model(trained, f"classification_{name}")
    classification_results.append((f"classification_{name}", "classification", 0.0, acc))


# ============== 5. 模型对比报告 ==============
print("\n[5/6] model comparison report ...", flush=True)
report_rows = regression_results + [
    ("clustering_kmeans", "clustering", 0.0, silhouette),
] + classification_results

report = spark.createDataFrame(
    report_rows,
    ["model", "task", "rmse", "score"]
)
report.write.mode("overwrite").parquet(f"{HDFS_MODEL}/_comparison_report")
print(f"    saved -> {HDFS_MODEL}/_comparison_report", flush=True)


# ============== 6. 完成 ==============
print("\n=== PySpark MLlib Training Done ===", flush=True)
print(f"HDFS models  : {HDFS_MODEL}", flush=True)
print(f"Shared volume: {SHARED_MODEL}  <- backend 加载用", flush=True)
print("\nBackend 加载示例:", flush=True)
print("  PipelineModel.load('/shared/models/regression_linear')", flush=True)

spark.stop()