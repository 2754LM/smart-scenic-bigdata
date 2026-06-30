"""
PySpark MLlib 单条预测服务 - 智能景区
=======================================
读取 stdin JSON 特征 → 用 /shared/models/ 下的模型做预测 → 输出 JSON。

用法（spark-master 内）：
  echo '{"task":"consumption_amount","features":{"age":30,...}}' | \
    /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/jobs/ml/predict.py

stdout 输出 JSON：{"prediction":..., "model":..., "probability":...}
"""
import json
import sys
import time
import traceback

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

spark = SparkSession.builder \
    .appName("SmartScenic-Predict") \
    .master("local[2]") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

MODELS_DIR = "/shared/models"
FEATURE_COLS = ["age", "purchase_count", "avg_amount", "visit_count", "avg_duration", "unique_attractions"]


def predict_regression(model_name: str, feature_dict: dict) -> float:
    model = PipelineModel.load(f"{MODELS_DIR}/{model_name}")
    row = {c: float(feature_dict.get(c, 0)) for c in FEATURE_COLS}
    pdf = spark.createDataFrame([row])
    out = model.transform(pdf).collect()[0]
    return float(out["prediction"])


def predict_classification(model_name: str, feature_dict: dict) -> tuple:
    model = PipelineModel.load(f"{MODELS_DIR}/{model_name}")
    row = {c: float(feature_dict.get(c, 0)) for c in FEATURE_COLS}
    pdf = spark.createDataFrame([row])
    out = model.transform(pdf).collect()[0]
    return float(out["probability"]), float(out["prediction"])


def main():
    try:
        req = json.loads(sys.stdin.read())
        task = req.get("task", "consumption_amount")
        features = req.get("features", {})

        started = time.time()
        result = None
        if task in ("consumption_amount", "daily_visitor"):
            for m in ("regression_rf", "regression_ridge", "regression_linear"):
                try:
                    val = predict_regression(m, features)
                    result = {"prediction": round(val, 4), "model": m, "engine": "pyspark",
                              "elapsed_ms": int((time.time() - started) * 1000)}
                    break
                except FileNotFoundError:
                    continue
            if result is None:
                raise RuntimeError("no regression model available")

        elif task == "high_value_visitor":
            for m in ("classification_rf", "classification_dt"):
                try:
                    proba, label = predict_classification(m, features)
                    result = {"prediction": round(proba, 4), "label": "高消费" if label > 0.5 else "普通",
                              "probability": round(proba, 4), "model": m, "engine": "pyspark",
                              "elapsed_ms": int((time.time() - started) * 1000)}
                    break
                except FileNotFoundError:
                    continue
            if result is None:
                raise RuntimeError("no classification model available")

        elif task == "cluster":
            from pyspark.ml.clustering import KMeansModel
            km = KMeansModel.load(f"{MODELS_DIR}/clustering_kmeans")
            row = {c: float(features.get(c, 0)) for c in FEATURE_COLS}
            pdf = spark.createDataFrame([row])
            out = km.transform(pdf).collect()[0]
            result = {"cluster": int(out["prediction"]), "model": "clustering_kmeans", "engine": "pyspark",
                      "elapsed_ms": int((time.time() - started) * 1000)}
        else:
            raise ValueError(f"unknown task: {task}")

        # 写到文件 + stdout（双保险，spark-submit 重定向可能丢 stdout）
        out_json = json.dumps(result, ensure_ascii=False)
        sys.stdout.write(out_json + "\n")
        sys.stdout.flush()
        with open("/tmp/predict_result.json", "w", encoding="utf-8") as f:
            f.write(out_json)
    except Exception as e:
        err = {"error": str(e), "trace": traceback.format_exc()}
        sys.stdout.write(json.dumps(err) + "\n")
        sys.stdout.flush()
        with open("/tmp/predict_result.json", "w", encoding="utf-8") as f:
            f.write(json.dumps(err, ensure_ascii=False))
    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()