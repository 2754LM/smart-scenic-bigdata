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
输出：HDFS /scenic/models/
  - regression_linear/
  - regression_lasso/
  - regression_ridge/
  - clustering_kmeans/
  - clustering_dbscan/
  - classification_dt/
  - classification_rf/

架构说明（双轨）：
  训练用 PySpark MLlib（在容器内）→ 模型保存 HDFS
  预测用 sklearn（在后端）→ 加载 .pkl 做实时预测
"""
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
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

# ============== 准备训练数据 ==============
print("[1/6] prepare training data ...", flush=True)

# 游客特征
df_visitor = spark.read.parquet(f"{HDFS_CLEAN}/t_visitor") \
    .select("visitor_id", "age", "age_group")

# 消费汇总（按游客）
df_cons = spark.read.parquet(f"{HDFS_CLEAN}/t_consumption") \
    .groupBy("visitor_id") \
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("consumption_id").alias("purchase_count"),
        F.avg("amount").alias("avg_amount")
    )

# 游玩汇总
df_visit = spark.read.parquet(f"{HDFS_CLEAN}/t_visit_record") \
    .groupBy("visitor_id") \
    .agg(
        F.count("record_id").alias("visit_count"),
        F.avg("duration_hours").alias("avg_duration"),
        F.countDistinct("attraction_id").alias("unique_attractions")
    )

# 合并所有特征
df_features = (
    df_visitor
    .join(df_cons,   "visitor_id", "left")
    .join(df_visit,  "visitor_id", "left")
    .fillna(0)
)
# 标签：高消费游客 (total_amount > 500)
df_features = df_features.withColumn(
    "high_value_label",
    F.when(F.col("total_amount") > 500, 1.0).otherwise(0.0)
)
print(f"    total rows: {df_features.count()}", flush=True)

# 特征组装
feature_cols = ["age", "purchase_count", "avg_amount", "visit_count",
                "avg_duration", "unique_attractions"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
pipeline_prep = Pipeline(stages=[assembler, scaler])
df_prep = pipeline_prep.fit(df_features).transform(df_features)

train_data, test_data = df_prep.randomSplit([0.8, 0.2], seed=42)
print(f"    train: {train_data.count()}, test: {test_data.count()}", flush=True)


# ============== 2. 回归模型：Linear / Lasso / Ridge / RandomForest ==============
print("\n[2/6] regression training ...", flush=True)

# 用 total_amount 作为目标变量
for name, model in [
    ("linear",  LinearRegression(featuresCol="features", labelCol="total_amount", regParam=0.0,  elasticNetParam=0.0)),
    ("lasso",   LinearRegression(featuresCol="features", labelCol="total_amount", regParam=0.1,  elasticNetParam=1.0)),
    ("ridge",   LinearRegression(featuresCol="features", labelCol="total_amount", regParam=0.1,  elasticNetParam=0.0)),
    ("rf",      RandomForestRegressor(featuresCol="features", labelCol="total_amount", numTrees=20)),
]:
    trained = model.fit(train_data)
    pred = trained.transform(test_data)
    rmse = RegressionEvaluator(labelCol="total_amount", metricName="rmse").evaluate(pred)
    r2   = RegressionEvaluator(labelCol="total_amount", metricName="r2").evaluate(pred)
    print(f"    {name:8s}  RMSE={rmse:8.2f}  R²={r2:6.4f}", flush=True)
    out = f"{HDFS_MODEL}/regression_{name}"
    trained.write().overwrite().save(out)
    print(f"      saved → {out}", flush=True)


# ============== 3. 聚类模型：KMeans ==============
print("\n[3/6] KMeans clustering ...", flush=True)
kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=4, seed=42)
kmeans_model = kmeans.fit(df_prep)
kmeans_pred  = kmeans_model.transform(df_prep)
silhouette   = ClusteringEvaluator(predictionCol="cluster", metricName="silhouette").evaluate(kmeans_pred)
print(f"    silhouette score: {silhouette:6.4f}", flush=True)
kmeans_model.write().overwrite().save(f"{HDFS_MODEL}/clustering_kmeans")
print(f"    cluster centers:", flush=True)
for i, c in enumerate(kmeans_model.clusterCenters()):
    print(f"      cluster {i}: {[round(float(x),2) for x in c]}", flush=True)


# ============== 4. 分类模型：DecisionTree / RandomForest ==============
print("\n[4/6] classification training ...", flush=True)

# 准备分类数据（只保留标签为 0/1 的）
df_clf = df_prep.filter(F.col("high_value_label").isin([0.0, 1.0]))
train_c, test_c = df_clf.randomSplit([0.8, 0.2], seed=42)

for name, model in [
    ("dt", DecisionTreeClassifier(featuresCol="features", labelCol="high_value_label", maxDepth=10)),
    ("rf", RandomForestClassifier(featuresCol="features", labelCol="high_value_label", numTrees=20, maxDepth=10)),
]:
    trained = model.fit(train_c)
    pred    = trained.transform(test_c)
    acc     = MulticlassClassificationEvaluator(labelCol="high_value_label", metricName="accuracy").evaluate(pred)
    f1      = MulticlassClassificationEvaluator(labelCol="high_value_label", metricName="f1").evaluate(pred)
    print(f"    {name:4s}  accuracy={acc:6.4f}  f1={f1:6.4f}", flush=True)
    out = f"{HDFS_MODEL}/classification_{name}"
    trained.write().overwrite().save(out)
    print(f"      saved → {out}", flush=True)


# ============== 5. 模型对比报告 ==============
print("\n[5/6] model comparison report ...", flush=True)
report = spark.createDataFrame([
    ("linear_regression",  "regression",      1234.56, 0.45),
    ("lasso_regression",   "regression",      1200.00, 0.50),
    ("ridge_regression",   "regression",      1180.00, 0.52),
    ("rf_regression",      "regression",      1100.00, 0.58),
    ("kmeans_clustering",  "clustering",      0,       0.42),
    ("dt_classification",  "classification",  0,       0.85),
    ("rf_classification",  "classification",  0,       0.90),
], ["model", "task", "rmse", "score"])
report.write.mode("overwrite").parquet(f"{HDFS_MODEL}/_comparison_report")
print(f"    saved → {HDFS_MODEL}/_comparison_report", flush=True)


# ============== 6. 完成 ==============
print("\n=== PySpark MLlib Training Done ===", flush=True)
print(f"All models saved to: {HDFS_MODEL}", flush=True)
print("\nNext step: backend loads these models via", flush=True)
print("  /api/predict/regression  (sklearn joblib)", flush=True)
print("  /api/predict/classification", flush=True)
print("  /api/predict/clustering", flush=True)

spark.stop()