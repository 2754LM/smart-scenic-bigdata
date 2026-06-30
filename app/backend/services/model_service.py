"""
Model service: 加载 Spark 训练后的 sklearn JSON 系数做预测。

数据流：
  PySpark train.py (spark-master) → Spark MLlib 模型
                              → JSON 系数 (/shared/models/sklearn/*.json)
                              → backend (demo-backend) 加载 JSON 用 sklearn 做预测

不需要 PySpark / Java 在 demo-backend 内。
"""
from __future__ import annotations

import json
import logging
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
from sklearn.cluster import KMeans
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import Lasso, LinearRegression, Ridge
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, r2_score
from sklearn.preprocessing import StandardScaler

import config

log = logging.getLogger("smart-scenic.model")

SKLEARN_DIR = Path(config.PYSPARK_MODELS_DIR) / "sklearn"

# 内存缓存
_json_models: Dict[str, Dict[str, Any]] = {}
_report: Dict[str, Any] = {}


def _load_json(name: str) -> Optional[Dict[str, Any]]:
    """加载 JSON 模型（缓存）"""
    if name in _json_models:
        return _json_models[name]
    path = SKLEARN_DIR / f"{name}.json"
    if not path.exists():
        log.warning("model json not found: %s", path)
        return None
    with open(path, "r", encoding="utf-8") as f:
        d = json.load(f)
    _json_models[name] = d
    return d


def _standardize(values: List[float], mean: List[float], std: List[float]) -> List[float]:
    return [(v - m) / (s if s > 1e-12 else 1e-12) for v, m, s in zip(values, mean, std)]


def _all_loaded() -> bool:
    if _report.get("loaded"):
        return True
    rj = SKLEARN_DIR.parent / "_comparison_report.json"
    if not rj.exists():
        return False
    with open(rj, "r", encoding="utf-8") as f:
        _report.update(json.load(f))
    _report["loaded"] = True
    log.info("model comparison report loaded: %d models", len(_report.get("regression", [])))
    return True


def predict(task: str, features: Dict[str, Any]) -> Dict[str, Any]:
    _all_loaded()
    feat = _normalize_features(task, features)

    if task == "consumption_amount":
        return _predict_regression("ridge", feat)  # ridge 性能最好 + 最稳
    if task == "daily_visitor":
        return _predict_regression("linear", feat)
    if task == "high_value_visitor":
        return _predict_classification(feat)
    if task == "cluster":
        return _predict_cluster(feat)
    raise ValueError(f"unsupported task: {task}")


def _normalize_features(task: str, features: Dict[str, Any]) -> Dict[str, float]:
    """统一特征 key 到训练时的英文字段"""
    aliases = {
        "age": "age", "年龄": "age",
        "purchase_count": "purchase_count",
        "avg_amount": "avg_amount", "avg_consume": "avg_amount",
        "visit_count": "visit_count",
        "avg_duration": "avg_duration",
        "unique_attractions": "unique_attractions",
    }
    return {aliases.get(k, k): float(v) for k, v in features.items()}


def _predict_regression(model_name: str, features: Dict[str, float]) -> Dict[str, Any]:
    m = _load_json(f"regression_{model_name}") or _load_json("regression_linear")
    if m is None:
        return _fallback_regression(features)
    cols = m["feature_cols"]
    mean, std = m["scaler_mean"], m["scaler_std"]
    raw = [features.get(c, 0.0) for c in cols]
    scaled = _standardize(raw, mean, std)
    val = sum(c * s for c, s in zip(m["coefficients"], scaled)) + m["intercept"]
    val = max(val, 0.0)
    return {
        "type": "consumption_amount",
        "prediction": round(val, 2),
        "model": f"pyspark_{m['model']}",
        "engine": "pyspark-sklearn",
        "metrics": m.get("metrics", {}),
        "timestamp": datetime.now().isoformat(),
    }


def _predict_classification(features: Dict[str, float]) -> Dict[str, Any]:
    m = _load_json("classification_rf")
    if m is None:
        return _fallback_classification(features)
    cols = m["feature_cols"]
    mean, std = m["scaler_mean"], m["scaler_std"]
    raw = [features.get(c, 0.0) for c in cols]
    scaled = _standardize(raw, mean, std)
    # 简单的基于特征重要性的加权投票（近似 RF）
    importances = m["feature_importances"]
    weighted_score = sum(i * s for i, s in zip(importances, scaled))
    # 归一化到 0-1 概率
    proba = 1 / (1 + math.exp(-weighted_score / 3.0))  # sigmoid
    proba = max(0.0, min(1.0, proba))
    return {
        "type": "high_value_visitor",
        "prediction": round(proba, 4),
        "probability": round(proba, 4),
        "label": "高消费" if proba > 0.5 else "普通",
        "model": f"pyspark_{m['model']}",
        "engine": "pyspark-sklearn",
        "metrics": m.get("metrics", {}),
        "timestamp": datetime.now().isoformat(),
    }


def _predict_cluster(features: Dict[str, float]) -> Dict[str, Any]:
    m = _load_json("clustering_kmeans")
    if m is None:
        return _fallback_cluster(features)
    cols = m["feature_cols"]
    mean, std = m["scaler_mean"], m["scaler_std"]
    raw = [features.get(c, 0.0) for c in cols]
    scaled = _standardize(raw, mean, std)
    centers = m["cluster_centers"]
    dists = [sum((a - b) ** 2 for a, b in zip(scaled, c)) ** 0.5 for c in centers]
    cluster = int(np.argmin(dists))
    profiles = [
        {"label": "低频低消费游客", "tip": "推送优惠券刺激首次到访"},
        {"label": "高频中消费游客", "tip": "推荐热门景点增加复购"},
        {"label": "中频高消费游客", "tip": "推荐 VIP 服务与年卡"},
        {"label": "高频高消费游客", "tip": "重点维护，提供专属管家服务"},
    ]
    return {
        "type": "cluster",
        "cluster": cluster,
        "label": profiles[cluster]["label"] if cluster < len(profiles) else f"聚类{cluster}",
        "tip": profiles[cluster]["tip"] if cluster < len(profiles) else "",
        "model": f"pyspark_{m['model']}",
        "engine": "pyspark-sklearn",
        "metrics": m.get("metrics", {}),
        "timestamp": datetime.now().isoformat(),
    }


# ----------------------------------------------------------------------
# Fallback: 简单 sklearn 模型（如果 JSON 不存在）
# ----------------------------------------------------------------------
def _fallback_regression(features: Dict[str, float]) -> Dict[str, Any]:
    val = (
        features.get("age", 30) * 5
        + features.get("purchase_count", 0) * 100
        + features.get("avg_amount", 0) * 0.8
        + features.get("visit_count", 0) * 50
    )
    return {
        "type": "consumption_amount",
        "prediction": round(max(val, 100), 2),
        "model": "fallback_linear",
        "engine": "sklearn_fallback",
        "timestamp": datetime.now().isoformat(),
    }


def _fallback_classification(features: Dict[str, float]) -> Dict[str, Any]:
    score = features.get("total_amount", 0) / 1000
    proba = max(0.0, min(1.0, score))
    return {
        "type": "high_value_visitor",
        "prediction": round(proba, 4),
        "probability": round(proba, 4),
        "label": "高消费" if proba > 0.5 else "普通",
        "model": "fallback_threshold",
        "engine": "sklearn_fallback",
        "timestamp": datetime.now().isoformat(),
    }


def _fallback_cluster(features: Dict[str, float]) -> Dict[str, Any]:
    return {
        "type": "cluster",
        "cluster": 0,
        "label": "未识别",
        "model": "fallback",
        "engine": "sklearn_fallback",
        "timestamp": datetime.now().isoformat(),
    }


# ----------------------------------------------------------------------
# Reports（来自 train.py 导出的 JSON）
# ----------------------------------------------------------------------
def regression_report() -> List[Dict[str, Any]]:
    _all_loaded()
    return _report.get("regression", [])


def classification_report() -> List[Dict[str, Any]]:
    _all_loaded()
    return _report.get("classification", [])


def clustering_report() -> List[Dict[str, Any]]:
    _all_loaded()
    return _report.get("clustering", [])


def compare_models() -> Dict[str, Any]:
    _all_loaded()
    return {
        "regression": _report.get("regression", []),
        "classification": _report.get("classification", []),
        "clustering": _report.get("clustering", []),
    }


def model_status() -> Dict[str, Any]:
    """返回模型状态（前端诊断用）"""
    return {
        "sklearn_json_dir": str(SKLEARN_DIR),
        "models_loaded": list(_json_models.keys()),
        "report_loaded": _report.get("loaded", False),
    }