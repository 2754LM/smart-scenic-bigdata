"""
Model service - 加载 spark-master 训练的 sklearn joblib 模型做预测。

数据流:
  Spark train.py → /shared/models/sklearn/*.pkl (joblib)
  → demo-backend joblib.load → predict() 毫秒级

无 PySpark / Java 依赖。
"""
from __future__ import annotations

import logging
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import joblib
import numpy as np

import config

log = logging.getLogger("smart-scenic.model")

MODELS_DIR = Path(config.PYSPARK_MODELS_DIR) / "sklearn"

_models: Dict[str, Any] = {}
_metrics: Dict[str, Any] = {}
_loaded = False


def _load_models() -> None:
    global _loaded
    if _loaded:
        return
    _loaded = True
    if not MODELS_DIR.exists():
        log.warning("models dir not found: %s", MODELS_DIR)
        return
    for p in MODELS_DIR.glob("*.pkl"):
        try:
            _models[p.stem] = joblib.load(p)
            log.info("loaded %s", p.stem)
        except Exception as e:
            log.warning("failed to load %s: %s", p.name, e)

    # load metrics
    rj = Path(config.PYSPARK_MODELS_DIR) / "_comparison_report.json"
    if rj.exists():
        import json
        with open(rj, "r", encoding="utf-8") as f:
            _metrics.update(json.load(f))


_load_models()


# ----------------------------------------------------------------------
# 特征转换：把前端字段映射到 Spark 训练时的特征
# ----------------------------------------------------------------------
# 所有任务统一使用 3 个特征 (age, avg_duration, unique_attractions) —
# 回归 (预测消费)、聚类 (群体)、分类 (回头客) 都靠这 3 个 features.
# 这是为了彻底避免数据泄漏 — 否则 high-value 标签可以直接从 total_amount 推出.
# 见 app/jobs/ml/train.py 顶部注释.
FEATURE_COLS = ["age", "avg_duration", "unique_attractions"]
REGRESSION_FEATURES = FEATURE_COLS
CLASSIFICATION_FEATURES = FEATURE_COLS
CLUSTERING_FEATURES = FEATURE_COLS


def _features_to_spark(task: str, f: Dict[str, Any]) -> np.ndarray:
    """
    把前端的任意字段映射到对应任务的特征:
      - regression: 3 features [age, avg_duration, unique_attractions]
      - classification: 3 features (same)
      - clustering: 3 features (same)
    所有任务统一 3 个 feature, 避免数据泄漏 (high_value 可被 total_amount 推出).
    返回 numpy array shape (1, 3)
    """
    if task in ("high_value_visitor",):
        keys = CLASSIFICATION_FEATURES
    else:
        keys = REGRESSION_FEATURES

    vals = []
    for k in keys:
        v = f.get(k)
        if v is None:
            # 兼容中文 key
            alias = {"年龄": "age",
                     "平均时长": "avg_duration",
                     "景点数": "unique_attractions"}.get(k)
            v = f.get(alias, 0) if alias else 0
        try:
            vals.append(float(v))
        except (TypeError, ValueError):
            vals.append(0.0)
    return np.array([vals])


def predict(task: str, features: Dict[str, Any]) -> Dict[str, Any]:
    X = _features_to_spark(task, features)
    started = datetime.now()

    if task == "consumption_amount":
        candidates = ("regression_ridge", "regression_linear", "regression_lasso", "regression_rf")
    elif task == "daily_visitor":
        # 客流量预测：使用 ridge 回归模型 + 日聚合
        candidates = ("regression_ridge", "regression_rf")
    elif task == "high_value_visitor":
        # 4 个分类模型 (rf/dt/gbt/lr) - 任务: 是否高频回头客
        # 训练时只用了 3 个特征 (age, avg_duration, unique_attractions)
        # 这里我们把所有 6 个特征喂进去, 但模型内部只用前 3 个
        candidates = ("classification_rf", "classification_gbt", "classification_dt", "classification_lr")
    elif task == "cluster":
        candidates = ("clustering_kmeans",)
    else:
        raise ValueError(f"unsupported task: {task}")

    for m in candidates:
        if m in _models:
            try:
                pred = _models[m].predict(X)
                val = float(pred[0])
                if task == "high_value_visitor":
                    proba = float(_models[m].predict_proba(X)[0][1])
                    return {
                        "type": task,
                        "prediction": round(proba, 4),
                        "probability": round(proba, 4),
                        "label": "high_value" if proba > 0.5 else "normal",
                        "model": m,
                        "engine": "sklearn",
                        "elapsed_ms": int((datetime.now() - started).total_seconds() * 1000),
                        "timestamp": datetime.now().isoformat(),
                    }
                elif task == "cluster":
                    cluster_id = int(val)
                    profiles = [
                        {"label": "low_freq_low_spend", "tip": "Push coupons to drive first visit"},
                        {"label": "high_freq_mid_spend", "tip": "Recommend popular attractions"},
                        {"label": "mid_freq_high_spend", "tip": "Suggest VIP/year-pass"},
                        {"label": "high_freq_high_spend", "tip": "Personal butler service"},
                    ]
                    return {
                        "type": task,
                        "cluster": cluster_id,
                        "label": profiles[cluster_id]["label"] if cluster_id < len(profiles) else f"cluster_{cluster_id}",
                        "tip": profiles[cluster_id]["tip"] if cluster_id < len(profiles) else "",
                        "model": m,
                        "engine": "sklearn",
                        "elapsed_ms": int((datetime.now() - started).total_seconds() * 1000),
                        "timestamp": datetime.now().isoformat(),
                    }
                else:
                    return {
                        "type": task,
                        "prediction": round(max(val, 0.0), 2),
                        "model": m,
                        "engine": "sklearn",
                        "elapsed_ms": int((datetime.now() - started).total_seconds() * 1000),
                        "timestamp": datetime.now().isoformat(),
                    }
            except Exception as e:
                log.warning("predict failed for %s: %s", m, e)
                continue
    raise RuntimeError("no model available for task: " + task)


def regression_predict(features: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Run all 4 regression models and return predictions for given features."""
    _load_models()
    if features is None:
        features = {"age": 35, "avg_duration": 3.5, "unique_attractions": 8}
    X = _features_to_spark("consumption_amount", features)

    model_map = {
        "linear": "regression_linear",
        "lasso": "regression_lasso",
        "ridge": "regression_ridge",
        "rf": "regression_rf",
    }
    models_out: Dict[str, Any] = {}
    best_pred = 0.0
    for name, model_key in model_map.items():
        if model_key in _models:
            try:
                pred = _models[model_key].predict(X)
                val = round(max(float(pred[0]), 0.0), 2)
                models_out[name] = {
                    "prediction": val,
                    "features_used": list(REGRESSION_FEATURES),
                }
                if name == "ridge" or best_pred == 0.0:
                    best_pred = val
            except Exception as e:
                log.warning("regression_predict failed for %s: %s", model_key, e)
    return {
        "consumption_amount": best_pred,
        "models": models_out,
        "input_features": features,
    }


def regression_report() -> List[Dict[str, Any]]:
    return _metrics.get("regression", [])


def classification_report() -> List[Dict[str, Any]]:
    return _metrics.get("classification", [])


def clustering_report() -> List[Dict[str, Any]]:
    """计算每个聚类中心的统计信息（人数/平均年龄/平均消费/平均游玩次数/平均时长）。
    流程: 从 MySQL 取游客聚合数据 → sklearn KMeans 预测 → 分组统计
    """
    if "clustering_kmeans" not in _models:
        return []
    from services.mysql_service import query
    import numpy as np

    # 游客聚合
    rows = query(
        "SELECT v.游客ID AS visitor_id, v.年龄 AS age, "
        "  COALESCE(SUM(c.消费金额), 0) AS total_consume, "
        "  COALESCE(COUNT(c.消费ID), 0) AS consume_count, "
        "  COALESCE(COUNT(vr.记录ID), 0) AS visit_count, "
        "  COALESCE(SUM(vr.游玩时长), 0) AS total_duration, "
        "  COALESCE(COUNT(DISTINCT vr.景点ID), 0) AS unique_attractions "
        "FROM t_visitor v "
        "LEFT JOIN t_consumption c ON v.游客ID = c.游客ID "
        "LEFT JOIN t_visit_record vr ON v.游客ID = vr.游客ID "
        "GROUP BY v.游客ID, v.年龄 LIMIT 10000"
    )
    if not rows:
        return []

    feature_order = ["age", "avg_duration", "unique_attractions"]



    # 构造 3-feature 矩阵 (年龄 / 平均游玩时长 / 去过的景点数)
    X = np.zeros((len(rows), 3))
    for i, r in enumerate(rows):
        X[i, 0] = r.get("age") or 30                            # age
        X[i, 1] = (r.get("total_duration") / max(r.get("visit_count") or 1, 1))  # avg_duration
        X[i, 2] = r.get("unique_attractions") or 0             # unique_attractions

    km = _models["clustering_kmeans"]
    # KMeans was trained on StandardScaler-transformed features (see train.py).
    # Use the scaler from the regression pipeline (same mean/std) to transform.
    X_raw = X.copy()
    if "regression_ridge" in _models:
        scaler = _models["regression_ridge"].named_steps.get("scaler")
        if scaler is not None:
            X = scaler.transform(X)
    labels = km.predict(X)

    stats = []
    for c in range(4):
        mask = (labels == c)
        if mask.sum() == 0:
            continue
        n = int(mask.sum())
        stats.append({
            "cluster": c,
            "n": n,
            "avg_age": round(float(X_raw[mask, 0].mean()), 1),
            "avg_duration_h": round(float(X_raw[mask, 1].mean()), 2),
            "approx_unique_attractions": int(round(float(X_raw[mask, 2].mean()))),
        })
    return stats


def compare_models() -> Dict[str, Any]:
    return {
        "regression": _metrics.get("regression", []),
        "classification": _metrics.get("classification", []),
        "clustering": _metrics.get("clustering", []),
    }


def confusion_matrix() -> List[Dict[str, Any]]:
    """Compute 2x2 confusion matrix for each classification model using MySQL data."""
    import numpy as np
    from services.mysql_service import query as mysql_query

    rows = mysql_query(
        "SELECT v.年龄 AS age, "
        "  AVG(vr.游玩时长) AS avg_duration, "
        "  COUNT(DISTINCT vr.景点ID) AS unique_attractions, "
        "  COUNT(vr.记录ID) AS visit_count "
        "FROM t_visitor v "
        "LEFT JOIN t_visit_record vr ON v.游客ID = vr.游客ID "
        "GROUP BY v.游客ID, v.年龄"
    )
    if not rows:
        return []

    import pandas as pd
    df = pd.DataFrame(rows)
    for col in ["age", "avg_duration", "unique_attractions", "visit_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    median_vc = df["visit_count"].median()
    df["label"] = (df["visit_count"] >= median_vc).astype(int)

    features = ["age", "avg_duration", "unique_attractions"]
    X_raw = df[features].values.astype(float)

    scaler = _models.get("_scaler")
    if scaler is not None:
        X = scaler.transform(X_raw)
    else:
        X = X_raw

    y_true = df["label"].values
    results = []
    for name in ["rf", "dt", "gbt", "lr"]:
        key = f"classification_{name}"
        model = _models.get(key)
        if model is None:
            continue
        y_pred = model.predict(X)
        tp = int(((y_pred == 1) & (y_true == 1)).sum())
        fp = int(((y_pred == 1) & (y_true == 0)).sum())
        fn = int(((y_pred == 0) & (y_true == 1)).sum())
        tn = int(((y_pred == 0) & (y_true == 0)).sum())
        results.append({
            "model": name.upper(),
            "tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "total": tp + fp + fn + tn,
        })
    return results


def model_status() -> Dict[str, Any]:
    return {
        "models_dir": str(MODELS_DIR),
        "models_loaded": sorted(_models.keys()),
        "metrics_loaded": bool(_metrics),
    }