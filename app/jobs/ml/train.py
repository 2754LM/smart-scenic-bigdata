"""
PySpark MLlib 训练 + 导出 sklearn 兼容系数 - 智能景区
=====================================================

作业要求：
  - 回归：Linear/Lasso/Ridge/随机森林
  - 聚类：KMeans
  - 分类：DT/RF
  - 关联规则：FPGrowth（独立脚本 fpgrowth.py）

执行（spark-master 内）：
  spark-submit --master spark://spark-master:7077 /opt/jobs/ml/train.py

输出：
  HDFS  /scenic/models/                  - 完整 Spark MLlib 模型（生态保留）
  Local /shared/models/                  - 复制一份给后端
  Local /shared/models/sklearn/          - **JSON 系数给后端直接预测**（无需 PySpark）
"""
import json
import os
import shutil

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel, RandomForestRegressor
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel, DecisionTreeClassifier, DecisionTreeClassificationModel
from pyspark.ml.evaluation import RegressionEvaluator, ClusteringEvaluator, MulticlassClassificationEvaluator

spark = SparkSession.builder \
    .appName("SmartScenic-ML-Train") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

HDFS_CLEAN = "hdfs://hadoop-namenode:9000/scenic/cleaned"
HDFS_MODEL = "hdfs://hadoop-namenode:9000/scenic/models"
SHARED_MODEL = "/shared/models"
SKLEARN_EXPORT_DIR = "/shared/models/sklearn"

FEATURE_COLS = ["age", "purchase_count", "avg_amount", "visit_count",
                "avg_duration", "unique_attractions"]


# ============== 1. 准备训练数据 ==============
print("[1/6] prepare training data ...", flush=True)

df_visitor = spark.read.parquet(f"{HDFS_CLEAN}/t_visitor") \
    .select("visitor_id", "age")
df_cons = spark.read.parquet(f"{HDFS_CLEAN}/t_consumption") \
    .groupBy("visitor_id") \
    .agg(F.sum("amount").alias("total_amount"),
         F.count("consumption_id").alias("purchase_count"),
         F.avg("amount").alias("avg_amount"))
df_visit = spark.read.parquet(f"{HDFS_CLEAN}/t_visit_record") \
    .groupBy("visitor_id") \
    .agg(F.count("record_id").alias("visit_count"),
         F.avg("duration_hours").alias("avg_duration"),
         F.countDistinct("attraction_id").alias("unique_attractions"))

df_features = (df_visitor.join(df_cons, "visitor_id", "left")
                         .join(df_visit, "visitor_id", "left")
                         .fillna(0))
df_features = df_features.withColumn(
    "high_value_label", F.when(F.col("total_amount") > 500, 1.0).otherwise(0.0))
print(f"    total rows: {df_features.count()}", flush=True)

assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
pipeline_prep = Pipeline(stages=[assembler, scaler])
prep_model = pipeline_prep.fit(df_features)
df_prep = prep_model.transform(df_features)

train_data, test_data = df_prep.randomSplit([0.8, 0.2], seed=42)
print(f"    train: {train_data.count()}, test: {test_data.count()}", flush=True)


# ============== 辅助函数 ==============
def save_spark_model(trained_model, name: str):
    hdfs_path = f"{HDFS_MODEL}/{name}"
    trained_model.write().overwrite().save(hdfs_path)

    sc = spark.sparkContext
    hadoop = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    FileUtil = sc._jvm.org.apache.hadoop.fs.FileUtil
    src_path = sc._jvm.org.apache.hadoop.fs.Path(hdfs_path)
    dst_path = sc._jvm.org.apache.hadoop.fs.Path(f"{SHARED_MODEL}/{name}")

    if os.path.exists(f"{SHARED_MODEL}/{name}"):
        shutil.rmtree(f"{SHARED_MODEL}/{name}")
    FileUtil.copy(hadoop, src_path, hadoop, dst_path, False, sc._jsc.hadoopConfiguration())


def save_json(obj: dict, name: str):
    """保存 JSON 到 SKLEARN_EXPORT_DIR"""
    os.makedirs(SKLEARN_EXPORT_DIR, exist_ok=True)
    path = f"{SKLEARN_EXPORT_DIR}/{name}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    print(f"    sklearn JSON -> {path}", flush=True)


# ============== 2. 回归模型 + 导出系数 ==============
print("\n[2/6] regression training ...", flush=True)

# 保存 prep_model 的 scaler 参数（后端做同样标准化）
scaler_model = prep_model.stages[1]
scaler_mean = scaler_model.mean.toArray().tolist()
scaler_std = scaler_model.std.toArray().tolist()

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
    r2 = RegressionEvaluator(labelCol="total_amount", metricName="r2").evaluate(pred)
    print(f"    {name:8s}  RMSE={rmse:8.2f}  R²={r2:6.4f}", flush=True)

    full_pipeline = Pipeline(stages=[assembler, scaler, trained])
    save_spark_model(full_pipeline, f"regression_{name}")

    # 导出 sklearn 系数（仅 Linear 类）
    if isinstance(trained, LinearRegressionModel):
        coefs = trained.coefficients.toArray().tolist()
        intercept = float(trained.intercept)
        save_json({
            "task": "consumption_amount",
            "model": name,
            "type": "linear",
            "feature_cols": FEATURE_COLS,
            "coefficients": coefs,
            "intercept": intercept,
            "scaler_mean": scaler_mean,
            "scaler_std": scaler_std,
            "metrics": {"rmse": float(rmse), "r2": float(r2)},
        }, f"regression_{name}")

    regression_results.append({"model": name, "rmse": float(rmse), "r2": float(r2)})


# ============== 3. 聚类模型 ==============
print("\n[3/6] KMeans clustering ...", flush=True)
kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=4, seed=42)
kmeans_model = kmeans.fit(df_prep)
kmeans_pred = kmeans_model.transform(df_prep)
silhouette = ClusteringEvaluator(predictionCol="cluster", metricName="silhouette").evaluate(kmeans_pred)
print(f"    silhouette score: {silhouette:6.4f}", flush=True)

full_km_pipeline = Pipeline(stages=[assembler, scaler, kmeans_model])
save_spark_model(full_km_pipeline, "clustering_kmeans")

# 导出 KMeans 中心点
centers = [c.tolist() for c in kmeans_model.clusterCenters()]
save_json({
    "task": "cluster",
    "model": "kmeans",
    "type": "kmeans",
    "feature_cols": FEATURE_COLS,
    "n_clusters": 4,
    "cluster_centers": centers,
    "scaler_mean": scaler_mean,
    "scaler_std": scaler_std,
    "metrics": {"silhouette": float(silhouette)},
}, "clustering_kmeans")

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
    pred = trained.transform(test_c)
    acc = MulticlassClassificationEvaluator(labelCol="high_value_label", metricName="accuracy").evaluate(pred)
    f1 = MulticlassClassificationEvaluator(labelCol="high_value_label", metricName="f1").evaluate(pred)
    print(f"    {name:4s}  accuracy={acc:6.4f}  f1={f1:6.4f}", flush=True)

    full_clf_pipeline = Pipeline(stages=[assembler, scaler, trained])
    save_spark_model(full_clf_pipeline, f"classification_{name}")

    # 导出 RF 的特征重要性（DT 太复杂）
    if isinstance(trained, RandomForestClassificationModel):
        importances = trained.featureImportances.toArray().tolist()
        save_json({
            "task": "high_value_visitor",
            "model": name,
            "type": "random_forest",
            "feature_cols": FEATURE_COLS,
            "feature_importances": importances,
            "scaler_mean": scaler_mean,
            "scaler_std": scaler_std,
            "metrics": {"accuracy": float(acc), "f1": float(f1)},
            "note": "sklearn RandomForest with same params (n_estimators=20, max_depth=10) approximates this",
        }, f"classification_{name}")

    classification_results.append({"model": name, "accuracy": float(acc), "f1": float(f1)})


# ============== 5. 对比报告 ==============
print("\n[5/6] save comparison report ...", flush=True)
report = {
    "regression":   regression_results,
    "clustering":   [{"model": "kmeans", "silhouette": float(silhouette)}],
    "classification": classification_results,
    "feature_cols": FEATURE_COLS,
    "training_data_size": int(df_features.count()),
}
with open("/shared/models/_comparison_report.json", "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)
print(f"    -> /shared/models/_comparison_report.json", flush=True)


# ============== 6. 完成 ==============
print("\n=== PySpark MLlib Training Done ===", flush=True)
print(f"HDFS models     : {HDFS_MODEL}", flush=True)
print(f"Shared models   : {SHARED_MODEL}", flush=True)
print(f"Sklearn JSON    : {SKLEARN_EXPORT_DIR}", flush=True)
print("\nBackend 用法:", flush=True)
print("  - 直接读 JSON 系数做预测（线性模型，毫秒级）", flush=True)
print("  - 用 sklearn.ensemble 重建 RF（特征重要性已知）", flush=True)

spark.stop()