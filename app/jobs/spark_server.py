"""
Long-running Spark REST Server - Smart Scenic BigData
=====================================================
- Loads PySpark MLlib models at startup (in-memory)
- Flask REST API for predictions
- FPGrowth on demand

Run inside spark-master container:
  cd /tmp/spark_jobs && nohup python3 spark_server.py > /tmp/srv.log 2>&1 &
"""
import os
import sys

# Ensure pyspark is importable in nohup env
for _p in ("/usr/local/lib/python3.8/dist-packages", "/usr/lib/python3/dist-packages"):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import json
import logging
from datetime import datetime
from typing import Any, Dict, List

# Force PYSPARK_PYTHON to same interpreter
os.environ.setdefault("PYSPARK_PYTHON", "python3")
os.environ.setdefault("JAVA_HOME", "/opt/java/openjdk")

from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("spark-server")

app = Flask(__name__)

# ============== Init Spark + Models ==============
spark = SparkSession.builder \
    .appName("SmartScenic-SparkServer") \
    .master("local[2]") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
log.info("SparkSession ready")

MODELS_DIR = "/shared/models"
HDFS_CLEAN = "hdfs://hadoop-namenode:9000/scenic/cleaned"
FEATURE_COLS = ["age", "purchase_count", "avg_amount", "visit_count", "avg_duration", "unique_attractions"]

_models: Dict[str, Any] = {}


def load_models():
    global _models
    if _models:
        return _models
    for name in ("regression_linear", "regression_lasso", "regression_ridge",
                 "regression_rf", "clustering_kmeans",
                 "classification_dt", "classification_rf"):
        path = f"{MODELS_DIR}/{name}"
        if os.path.exists(path):
            try:
                _models[name] = PipelineModel.load(path)
                log.info("  loaded %s", name)
            except Exception as e:
                log.warning("  failed %s: %s", name, e)
    log.info("total %d models loaded", len(_models))
    return _models


def predict_single(task: str, features: Dict[str, float]) -> Dict[str, Any]:
    if task == "consumption_amount":
        candidates = ("regression_rf", "regression_ridge", "regression_linear")
    elif task == "high_value_visitor":
        candidates = ("classification_rf", "classification_dt")
    elif task == "cluster":
        candidates = ("clustering_kmeans",)
    elif task == "daily_visitor":
        candidates = ("regression_ridge",)
    else:
        return {"error": f"unknown task: {task}"}

    for m in candidates:
        if m in _models:
            try:
                row = {c: float(features.get(c, 0.0)) for c in FEATURE_COLS}
                pdf = spark.createDataFrame([row])
                out = _models[m].transform(pdf).collect()[0]
                d = out.asDict()
                if task in ("consumption_amount", "daily_visitor"):
                    val = float(d.get("prediction", 0))
                    return {"prediction": round(val, 2), "model": m, "task": task, "engine": "pyspark"}
                elif task == "high_value_visitor":
                    val = float(d.get("prediction", 0))
                    return {"prediction": round(val, 4),
                            "label": "high_value" if val > 0.5 else "normal",
                            "probability": round(val, 4), "model": m, "task": task, "engine": "pyspark"}
                elif task == "cluster":
                    val = int(d.get("prediction", 0))
                    profiles = [
                        {"label": "low_freq_low_spend", "tip": "Push coupons to drive first visit"},
                        {"label": "high_freq_mid_spend", "tip": "Recommend popular attractions"},
                        {"label": "mid_freq_high_spend", "tip": "Suggest VIP/year-pass"},
                        {"label": "high_freq_high_spend", "tip": "Personal butler service"},
                    ]
                    return {"cluster": val,
                            "label": profiles[val]["label"] if val < len(profiles) else f"cluster_{val}",
                            "tip": profiles[val]["tip"] if val < len(profiles) else "",
                            "model": m, "task": task, "engine": "pyspark"}
            except Exception as e:
                log.warning("predict failed for %s: %s", m, e)
                continue
    return {"error": "no model available"}


def compare_daily(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Daily actual vs predicted (split train/test by time, then predict on test using ridge model)."""
    # 1. daily aggregates from cleaned data
    cons = spark.read.parquet(f"{HDFS_CLEAN}/t_consumption") \
        .withColumn("date", F.to_date("consume_time")) \
        .groupBy("date") \
        .agg(F.sum("amount").alias("actual_amount"),
             F.count("consumption_id").alias("purchase_count"))
    visits = spark.read.parquet(f"{HDFS_CLEAN}/t_visit_record") \
        .withColumn("date", F.to_date("visit_time")) \
        .groupBy("date") \
        .agg(F.count("record_id").alias("actual_visits"),
             F.avg("duration_hours").alias("avg_duration"),
             F.countDistinct("visitor_id").alias("unique_visitors"))

    daily = cons.join(visits, "date", "outer").fillna(0).orderBy("date")
    daily = daily.withColumn("month", F.month("date")) \
                 .withColumn("weekday", F.dayofweek("date")) \
                 .withColumn("dayofyear", F.dayofyear("date")) \
                 .withColumn("is_weekend", F.when(F.col("weekday").isin([1, 7]), 1).otherwise(0))

    # train/test split by date: train < split_date, test >= split_date
    split_date = "2023-09-01"

    # build features for prediction (using actuals as features -> in-sample prediction)
    feat = daily
    for c in FEATURE_COLS:
        if c not in feat.columns:
            feat = feat.withColumn(c, F.lit(0.0).cast("double"))

    if "regression_ridge" in _models:
        pred = _models["regression_ridge"].transform(feat).select(
            "date", "actual_amount", "actual_visits", "prediction"
        ).withColumnRenamed("prediction", "predicted_amount") \
         .withColumn("is_test", F.when(F.col("date") >= F.lit(split_date), 1).otherwise(0))
    else:
        pred = feat.withColumn("predicted_amount", F.lit(0.0)).withColumn("is_test", F.lit(0))

    pred = pred.filter((F.col("date") >= start_date) & (F.col("date") <= end_date))
    rows = pred.orderBy("date").collect()
    return [
        {
            "date": str(r["date"]),
            "actual_amount": float(r["actual_amount"] or 0),
            "actual_visits": int(r["actual_visits"] or 0),
            "predicted_amount": float(r["predicted_amount"] or 0),
            "is_test": int(r["is_test"]),
        }
        for r in rows
    ]


def run_fpgrowth(min_support=0.02, min_confidence=0.3, top_n=30) -> List[Dict[str, Any]]:
    from pyspark.ml.fpm import FPGrowth

    df_attr = spark.read.parquet(f"{HDFS_CLEAN}/t_attraction") \
        .select("attraction_id", "attraction_name")
    df_cons = spark.read.parquet(f"{HDFS_CLEAN}/t_consumption") \
        .select("visitor_id", "attraction_id") \
        .dropDuplicates(["visitor_id", "attraction_id"]) \
        .groupBy("visitor_id") \
        .agg(F.collect_list("attraction_id").alias("items")) \
        .filter(F.size("items") >= 2)

    fp = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence)
    model = fp.fit(df_cons)
    rules = model.associationRules.orderBy(F.desc("lift")).limit(top_n).collect()

    attr_map = {r["attraction_id"]: r["attraction_name"] for r in df_attr.collect()}

    def to_names(items):
        return [{"id": int(i), "name": attr_map.get(i, str(i))} for i in items]

    return [
        {
            "antecedent": to_names(list(r["antecedent"])),
            "consequent": to_names(list(r["consequent"])),
            "confidence": float(r["confidence"]),
            "lift": float(r["lift"]),
            "support": float(r["support"]),
        }
        for r in rules
    ]


# ============== API Routes ==============
@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "spark": "ready",
        "models": list(_models.keys()),
        "ts": datetime.now().isoformat(),
    })


@app.route("/models")
def models():
    return jsonify({
        "models": [
            {"name": n, "task": "regression"}
            for n in ("regression_linear", "regression_lasso", "regression_ridge", "regression_rf") if n in _models
        ] + [
            {"name": n, "task": "classification"}
            for n in ("classification_dt", "classification_rf") if n in _models
        ] + [
            {"name": "clustering_kmeans", "task": "cluster"} if "clustering_kmeans" in _models else None
        ],
        "feature_cols": FEATURE_COLS,
    })


@app.route("/predict/<task>", methods=["POST"])
def predict(task):
    body = request.get_json(force=True)
    features = body.get("features", {})
    if not isinstance(features, dict):
        return jsonify({"error": "features must be dict"}), 400
    try:
        result = predict_single(task, {k: float(v) for k, v in features.items()})
        return jsonify(result)
    except Exception as e:
        log.exception("predict failed")
        return jsonify({"error": str(e)}), 500


@app.route("/compare_daily")
def compare_daily_api():
    start = request.args.get("start", "2023-01-01")
    end = request.args.get("end", "2023-12-31")
    try:
        results = compare_daily(start, end)
        return jsonify({"results": results, "model": "regression_ridge",
                        "train_before": "2023-09-01", "test_from": "2023-09-01",
                        "start": start, "end": end})
    except Exception as e:
        log.exception("compare_daily failed")
        return jsonify({"error": str(e)}), 500


@app.route("/fpgrowth")
def fpgrowth_api():
    try:
        min_support = float(request.args.get("min_support", 0.02))
        min_confidence = float(request.args.get("min_confidence", 0.3))
        top_n = int(request.args.get("top_n", 30))
        results = run_fpgrowth(min_support, min_confidence, top_n)
        return jsonify({"results": results, "count": len(results)})
    except Exception as e:
        log.exception("fpgrowth failed")
        return jsonify({"error": str(e)}), 500


# ============== Main ==============
if __name__ == "__main__":
    load_models()
    log.info("starting Flask on 0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, threaded=True, debug=False)