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
# 特征转换：把前端字段映射到 Spark 训练时的 6 个特征
# ----------------------------------------------------------------------
def _features_to_spark(task: str, f: Dict[str, Any]) -> np.ndarray:
    """
    把前端的任意字段映射到 Spark 训练的 6 个特征:
      [age, purchase_count, avg_amount, visit_count, avg_duration, unique_attractions]
    返回 numpy array shape (1, 6)
    """
    # 直接用 6 个英文字段（前端应该已经映射好）
    keys = ["age", "purchase_count", "avg_amount", "visit_count", "avg_duration", "unique_attractions"]
    vals = []
    for k in keys:
        v = f.get(k)
        if v is None:
            # 兼容中文 key
            alias = {"年龄": "age", "年龄_": "age",
                     "购买次数": "purchase_count",
                     "平均消费": "avg_amount",
                     "游玩次数": "visit_count",
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
        # 4 个分类模型 (rf/dt/gbt/lr)，按 accuracy 排序优先级
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
        "  COALESCE(SUM(vr.游玩时长), 0) AS total_duration "
        "FROM t_visitor v "
        "LEFT JOIN t_consumption c ON v.游客ID = c.游客ID "
        "LEFT JOIN t_visit_record vr ON v.游客ID = vr.游客ID "
        "GROUP BY v.游客ID, v.年龄 LIMIT 10000"
    )
    if not rows:
        return []

    feature_order = ["age", "purchase_count", "avg_amount", "visit_count", "avg_duration", "unique_attractions"]
    # 构造特征矩阵（用每日平均值近似）
    X = np.zeros((len(rows), 6))
    for i, r in enumerate(rows):
        X[i, 0] = r.get("age") or 30
        X[i, 1] = r.get("consume_count") or 0
        X[i, 2] = (r.get("total_consume") / max(r.get("consume_count") or 1, 1))
        X[i, 3] = r.get("visit_count") or 0
        X[i, 4] = (r.get("total_duration") / max(r.get("visit_count") or 1, 1))
        X[i, 5] = 3  # 近似

    km = _models["clustering_kmeans"]
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
            "avg_age": round(float(X[mask, 0].mean()), 1),
            "avg_total_consume": round(float(X[mask, 2].mean() * X[mask, 1].mean()), 0),  # avg_amount * purchase_count
            "avg_per_consume": round(float(X[mask, 2].mean()), 1),
            "avg_visit_count": round(float(X[mask, 3].mean()), 1),
            "avg_duration_h": round(float(X[mask, 4].mean()), 2),
        })
    return stats


def compare_models() -> Dict[str, Any]:
    return {
        "regression": _metrics.get("regression", []),
        "classification": _metrics.get("classification", []),
        "clustering": _metrics.get("clustering", []),
    }


def model_status() -> Dict[str, Any]:
    return {
        "models_dir": str(MODELS_DIR),
        "models_loaded": sorted(_models.keys()),
        "metrics_loaded": bool(_metrics),
    }