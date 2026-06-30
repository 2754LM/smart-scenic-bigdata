"""
PySpark MLlib 训练 + sklearn joblib 模型输出 - 智能景区
====================================================
训练在 Spark，推理用 sklearn（毫秒级、无 PySpark 依赖）。

训练特征（分类只用 3 个，避免数据泄漏；回归/聚类用 6 个）:
  分类: age, avg_duration, unique_attractions  -> label is_repeat_visitor
  回归: age, purchase_count, avg_amount, visit_count, avg_duration, unique_attractions -> total_amount
  聚类: 同 6 维特征

数据流:
  1. spark-submit train.py 训练并保存
  2. /shared/models/sklearn/ 中产出 .pkl (joblib 模型)
  3. demo-backend joblib.load() 加载预测

执行（spark-master 内）：
  spark-submit --master spark://spark-master:7077 /opt/jobs/ml/train.py
"""
import json
import os
import shutil
import sys

# 在 nohup 环境下补 pyspark path
for _p in ("/usr/local/lib/python3.8/dist-packages", "/usr/lib/python3/dist-packages"):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# sklearn / joblib（用于把 Spark 训练结果转换为 sklearn 模型 + dump）
import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LinearRegression, Lasso, Ridge, LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline as SKPipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, f1_score

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler as SparkScaler
from pyspark.ml.regression import LinearRegression as SparkLR, LinearRegressionModel, RandomForestRegressor as SparkRFR
from pyspark.ml.clustering import KMeans as SparkKM, KMeansModel
from pyspark.ml.classification import (RandomForestClassifier as SparkRFC,
                                      RandomForestClassificationModel,
                                      DecisionTreeClassifier as SparkDTC,
                                      DecisionTreeClassificationModel,
                                      GBTClassifier as SparkGBT,
                                      GBTClassificationModel,
                                      LogisticRegression as SparkLogReg,
                                      LogisticRegressionModel)
from pyspark.ml.evaluation import RegressionEvaluator, ClusteringEvaluator, MulticlassClassificationEvaluator, BinaryClassificationEvaluator

spark = SparkSession.builder.appName("SmartScenic-ML-Train").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

HDFS_CLEAN = "hdfs://hadoop-namenode:9000/scenic/cleaned"
HDFS_MODEL = "hdfs://hadoop-namenode:9000/scenic/models"
SHARED_MODEL = "/shared/models"
SKLEARN_OUT = "/shared/models/sklearn"   # demo-backend 用 joblib.load 这里

FEATURE_COLS = ["age", "avg_duration", "unique_attractions"]
REGRESSION_FEATURE_COLS = FEATURE_COLS  # all 3 tasks use same 3 features (no data leakage)
CLASS_LABEL = "is_repeat_visitor"
REGRESSION_LABEL = "total_amount"


# ============== 1. 数据 ==============
print("[1/5] prepare training data ...", flush=True)
df_visitor = spark.read.parquet(f"{HDFS_CLEAN}/t_visitor").select("visitor_id", "age")
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
# 用 visit_count 的中位数作为阈值，避免泄露 (purchase_count/avg_amount 直接决定 total_amount)
# 中位数切分得到近似平衡的二分类 (54% high, 46% low)
median_visit_count = df_features.approxQuantile("visit_count", [0.5], 0.01)[0]
df_features = df_features.withColumn(
    CLASS_LABEL, F.when(F.col("visit_count") >= median_visit_count, 1.0).otherwise(0.0))
print(f"    total rows: {df_features.count()}", flush=True)
print(f"    median visit_count = {median_visit_count} (分类阈值)", flush=True)


# ============== 2. 训练 Pipeline（用于预测时的转换） ==============
assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features")
spark_scaler = SparkScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
pipeline_prep = Pipeline(stages=[assembler, spark_scaler])
prep_model = pipeline_prep.fit(df_features)
df_prep = prep_model.transform(df_features)

train_data, test_data = df_prep.randomSplit([0.8, 0.2], seed=42)
print(f"    train: {train_data.count()}, test: {test_data.count()}", flush=True)


# ============== 3. 提取 SparkScaler 的 mean / std 用于 sklearn ==============
spark_scaler_model = prep_model.stages[1]
scaler_mean = spark_scaler_model.mean.toArray().tolist()
scaler_std = spark_scaler_model.std.toArray().tolist()


def make_sk_pipeline(model) -> SKPipeline:
    """包成 sklearn Pipeline: StandardScaler -> model"""
    sc = StandardScaler()
    sc.mean_ = np.array(scaler_mean)
    sc.scale_ = np.array(scaler_std)
    sc.var_ = sc.scale_ ** 2
    sc.n_features_in_ = len(FEATURE_COLS)
    return SKPipeline([("scaler", sc), ("model", model)])


# ============== 4. 训练并保存 sklearn 模型 ==============
os.makedirs(SKLEARN_OUT, exist_ok=True)
report = {"regression": [], "clustering": [], "classification": []}


# --- 回归 ---
print("\n[2/5] regression ...", flush=True)
for name, spark_model in [
    ("linear", SparkLR(featuresCol="features", labelCol="total_amount", regParam=0.0, elasticNetParam=0.0)),
    ("lasso",  SparkLR(featuresCol="features", labelCol="total_amount", regParam=0.1, elasticNetParam=1.0)),
    ("ridge",  SparkLR(featuresCol="features", labelCol="total_amount", regParam=0.1, elasticNetParam=0.0)),
    ("rf",     SparkRFR(featuresCol="features", labelCol="total_amount", numTrees=20)),
]:
    trained = spark_model.fit(train_data)
    pred = trained.transform(test_data)
    rmse = RegressionEvaluator(labelCol="total_amount", metricName="rmse").evaluate(pred)
    r2 = RegressionEvaluator(labelCol="total_amount", metricName="r2").evaluate(pred)
    print(f"    {name:8s}  RMSE={rmse:8.2f}  R²={r2:6.4f}", flush=True)

    if isinstance(trained, LinearRegressionModel):
        # 重建 sklearn LinearRegression / Lasso / Ridge（用 Spark 训练出的系数）
        coeffs = trained.coefficients.toArray().tolist()
        intercept = float(trained.intercept)
        if name == "linear":
            sk_model = LinearRegression()
        elif name == "lasso":
            sk_model = Lasso(alpha=0.1, max_iter=5000)
        elif name == "ridge":
            sk_model = Ridge(alpha=0.1)
        sk_model.coef_ = np.array(coeffs)
        sk_model.intercept_ = intercept
        sk_model.n_features_in_ = len(coeffs)
    elif isinstance(trained, RandomForestRegressor):
        # 重建 sklearn RandomForest（用相同超参重新拟合）
        from sklearn.ensemble import RandomForestRegressor as SKRF
        # 直接拿 spark 训练的树结构有些麻烦，简单做法：用 sklearn 在相同数据上重新拟合
        # 取训练/测试的真实值
        train_pdf = train_data.select(FEATURE_COLS + ["total_amount"]).toPandas()
        test_pdf = test_data.select(FEATURE_COLS + ["total_amount"]).toPandas()
        X_train = train_pdf[FEATURE_COLS].values
        y_train = train_pdf["total_amount"].values
        X_test = test_pdf[FEATURE_COLS].values
        y_test = test_pdf["total_amount"].values
        sk_model = SKRF(n_estimators=20, random_state=42).fit(X_train, y_train)

    pipe = make_sk_pipeline(sk_model)
    joblib.dump(pipe, f"{SKLEARN_OUT}/regression_{name}.pkl")
    print(f"    saved {SKLEARN_OUT}/regression_{name}.pkl", flush=True)

    report["regression"].append({"model": name, "rmse": float(rmse), "r2": float(r2)})


# --- 聚类 ---
print("\n[3/5] KMeans ...", flush=True)
kmeans = SparkKM(featuresCol="features", predictionCol="cluster", k=4, seed=42)
km_trained = kmeans.fit(df_prep)
km_pred = km_trained.transform(df_prep)
silhouette = ClusteringEvaluator(predictionCol="cluster", metricName="silhouette").evaluate(km_pred)
print(f"    silhouette: {silhouette:6.4f}", flush=True)

# sklearn KMeans 在标准化后的特征上重新训练
train_pdf = train_data.select(FEATURE_COLS).toPandas()
X = (train_pdf[FEATURE_COLS].values - np.array(scaler_mean)) / np.array(scaler_std)
sk_km = KMeans(n_clusters=4, random_state=42, n_init=10).fit(X)
joblib.dump(sk_km, f"{SKLEARN_OUT}/clustering_kmeans.pkl")
print(f"    saved {SKLEARN_OUT}/clustering_kmeans.pkl", flush=True)

report["clustering"].append({"model": "kmeans", "silhouette": float(silhouette)})
for i, c in enumerate(km_trained.clusterCenters()):
    print(f"      cluster {i}: {[round(float(x),2) for x in c]}", flush=True)


# --- 分类 ---
# 分类只用 3 个非相关特征 + is_repeat_visitor label (避免数据泄漏)
CLF_FEATURES = ["age", "avg_duration", "unique_attractions"]
print("\n[4/5] classification (3 features, no leakage) ...", flush=True)
df_clf = df_prep.filter(F.col(CLASS_LABEL).isin([0.0, 1.0]))
train_c, test_c = df_clf.randomSplit([0.8, 0.2], seed=42)
# 用 3 个特征重新构造 clf_features 列 (Spark 的 VectorAssembler 已经在 prep_model 用了 6 维)
# 这里换成只装 3 维
clf_assembler = VectorAssembler(inputCols=CLF_FEATURES, outputCol="clf_raw")
clf_scaler = SparkScaler(inputCol="clf_raw", outputCol="clf_features", withMean=True, withStd=True)
clf_prep = Pipeline(stages=[clf_assembler, clf_scaler]).fit(df_features)
df_clf_prep = clf_prep.transform(df_features).filter(F.col(CLASS_LABEL).isin([0.0, 1.0]))
train_clf_p, test_clf_p = df_clf_prep.randomSplit([0.8, 0.2], seed=42)
train_c_pdf = train_clf_p.select(CLF_FEATURES + [CLASS_LABEL]).toPandas()
test_c_pdf = test_clf_p.select(CLF_FEATURES + [CLASS_LABEL]).toPandas()
Xc_train = train_c_pdf[CLF_FEATURES].values
yc_train = train_c_pdf[CLASS_LABEL].astype(int).values
Xc_test = test_c_pdf[CLF_FEATURES].values
yc_test = test_c_pdf[CLASS_LABEL].astype(int).values

# 4 个分类模型：RF / DecisionTree / GBT / LogisticRegression
for name, spark_model, sk_factory in [
    ("rf",      SparkRFC(featuresCol="clf_features", labelCol=CLASS_LABEL, numTrees=20, maxDepth=10),
     lambda: RandomForestClassifier(n_estimators=20, max_depth=10, random_state=42)),
    ("dt",      SparkDTC(featuresCol="clf_features", labelCol=CLASS_LABEL, maxDepth=10),
     lambda: DecisionTreeClassifier(max_depth=10, random_state=42)),
    ("gbt",     SparkGBT(featuresCol="clf_features", labelCol=CLASS_LABEL, maxIter=20, maxDepth=5),
     lambda: GradientBoostingClassifier(n_estimators=20, max_depth=5, random_state=42)),
    ("lr",      SparkLogReg(featuresCol="clf_features", labelCol=CLASS_LABEL, maxIter=50, regParam=0.01),
     lambda: LogisticRegression(max_iter=50, C=1.0, random_state=42)),
]:
    try:
        trained = spark_model.fit(train_clf_p)
        pred = trained.transform(test_clf_p)
        acc = MulticlassClassificationEvaluator(labelCol=CLASS_LABEL, metricName="accuracy").evaluate(pred)
        f1 = MulticlassClassificationEvaluator(labelCol=CLASS_LABEL, metricName="f1").evaluate(pred)
        try:
            auc = BinaryClassificationEvaluator(labelCol=CLASS_LABEL, metricName="areaUnderROC").evaluate(pred)
        except Exception:
            auc = 0.0
        print(f"    {name:4s}  acc={acc:6.4f}  f1={f1:6.4f}  auc={auc:6.4f}", flush=True)

        # 重建 sklearn 模型 (注意：rebuild 用的是 spark 训练后的 df_clf_prep，不是 train_c)
        sk_clf = sk_factory().fit(Xc_train, yc_train)
        joblib.dump(sk_clf, f"{SKLEARN_OUT}/classification_{name}.pkl")
        print(f"    saved {SKLEARN_OUT}/classification_{name}.pkl", flush=True)

        report["classification"].append({
            "model": name,
            "accuracy": float(acc),
            "f1": float(f1),
            "auc": float(auc),
            "features": CLF_FEATURES,
            "label": CLASS_LABEL,
        })
    except Exception as e:
        print(f"    {name:4s}  FAILED: {e}", flush=True)
        import traceback; traceback.print_exc()


# ============== 5. 对比报告 ==============
print("\n[5/5] save comparison report ...", flush=True)
with open(f"{SHARED_MODEL}/_comparison_report.json", "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)

# FPGrowth 是另一个脚本，这里只触发即可
print("\n=== Training Done ===", flush=True)
print(f"Models dir: {SKLEARN_OUT}", flush=True)
print(f"Files: {sorted(os.listdir(SKLEARN_OUT))}", flush=True)
spark.stop()