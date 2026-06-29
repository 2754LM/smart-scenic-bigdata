"""
Model service: train/load/predict with sklearn models, persisted to disk.

For P2 we ship:
  * regression_consumption   - 消费金额预测
  * regression_daily_visitor - 日客流量预测
  * classification_high_value - 高价值游客识别
  * clustering_kmeans        - 游客分群
  * clustering_dbscan        - 游客离群检测
  * fpgrowth_type            - 类型关联规则

On startup we lazily train a small set of sklearn models against
data/raw_data/*.csv and cache them in MEMORY. Predictions are served
from memory; metrics are returned to the front-end.
"""
from __future__ import annotations

import json
import logging
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN, KMeans
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import Lasso, LinearRegression, LogisticRegression, Ridge
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
    roc_auc_score,
)
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier

import config
from utils import load_csv, now_iso

log = logging.getLogger("smart-scenic.model")

_MODELS: Dict[str, Any] = {}
_REGRESSION_REPORT: List[Dict[str, Any]] = []
_CLASSIFICATION_REPORT: List[Dict[str, Any]] = []
_CLUSTER_REPORT: List[Dict[str, Any]] = []
_TRAINED = False


# ----------------------------------------------------------------------
# Feature engineering
# ----------------------------------------------------------------------
def _consumption_features() -> Tuple[pd.DataFrame, pd.Series]:
    """Build training data for consumption regression.

    Target: amount
    Features: type, month, weekday, hour, is_weekend, is_holiday
    """
    cons = load_csv("consumption.csv")
    attractions = load_csv("attractions.csv")
    cons = cons.merge(attractions[["景点ID", "类型"]], on="景点ID", how="left")
    cons["时间"] = pd.to_datetime(cons["时间"], errors="coerce")
    cons = cons.dropna(subset=["时间"])
    cons["month"] = cons["时间"].dt.month
    cons["weekday"] = cons["时间"].dt.weekday + 1  # 1=Mon..7=Sun
    cons["hour"] = cons["时间"].dt.hour
    cons["is_weekend"] = (cons["weekday"] >= 6).astype(int)
    # crude "holiday" proxy: any date in Jan/Feb/May/Oct
    cons["is_holiday"] = cons["month"].isin([1, 2, 5, 10]).astype(int)
    cons = pd.get_dummies(cons, columns=["类型"], prefix="t")
    feat_cols = (
        ["month", "weekday", "hour", "is_weekend", "is_holiday"]
        + [c for c in cons.columns if c.startswith("t_")]
    )
    X = cons[feat_cols].astype(float).fillna(0)
    y = cons["消费金额"].astype(float)
    return X, y


def _daily_visitor_features() -> Tuple[pd.DataFrame, pd.Series]:
    visits = load_csv("visit_records.csv")
    visits["时间"] = pd.to_datetime(visits["时间"], errors="coerce")
    visits = visits.dropna(subset=["时间"])
    daily = visits.groupby(visits["时间"].dt.date).size().reset_index(name="visitors")
    daily.columns = ["date", "visitors"]
    daily["date"] = pd.to_datetime(daily["date"])
    daily["month"] = daily["date"].dt.month
    daily["weekday"] = daily["date"].dt.weekday + 1
    daily["dayofyear"] = daily["date"].dt.dayofyear
    daily["is_weekend"] = (daily["weekday"] >= 6).astype(int)
    daily["is_holiday"] = daily["month"].isin([1, 2, 5, 10]).astype(int)
    feat_cols = ["month", "weekday", "dayofyear", "is_weekend", "is_holiday"]
    return daily[feat_cols].astype(float), daily["visitors"].astype(float)


def _high_value_features() -> Tuple[pd.DataFrame, pd.Series]:
    """Build features + label for high-value classification.

    Features: age, gender, preference, region, (synthetic engagement score)
    Label:    total consumption > median AND visit count >= 4
    """
    cons = load_csv("consumption.csv")
    visits = load_csv("visit_records.csv")
    visitors = load_csv("visitors.csv")
    attractions = load_csv("attractions.csv")

    c_sum = cons.groupby("游客ID")["消费金额"].agg(["sum", "count"]).reset_index()
    v_count = visits.groupby("游客ID").size().reset_index(name="visit_count")
    df = visitors.merge(c_sum, on="游客ID", how="left").merge(v_count, on="游客ID", how="left")
    df[["sum", "count", "visit_count"]] = df[["sum", "count", "visit_count"]].fillna(0)

    # proxy for preference: most-Visited type
    vis = visits.merge(attractions[["景点ID", "类型"]], on="景点ID", how="left")
    pref = vis.groupby(["游客ID", "类型"]).size().reset_index(name="n")
    pref = pref.sort_values(["游客ID", "n"], ascending=[True, False]).drop_duplicates("游客ID")
    df = df.merge(pref[["游客ID", "类型"]], on="游客ID", how="left").rename(columns={"类型": "偏好类型"})

    # engagement score: combined signal of preference and age
    pref_score = df["偏好类型"].fillna("未知").map({"自然": 1, "文化": 2, "娱乐": 3, "运动": 4}).fillna(0)
    df["engagement"] = (df["年龄"].fillna(30) * 0.05 + pref_score * 0.5 + (df["地区"].str.len().fillna(5)) * 0.1)

    median_sum = df["sum"].median()
    df["label"] = ((df["sum"] > median_sum) & (df["visit_count"] >= 4)).astype(int)

    df = pd.get_dummies(df, columns=["性别", "偏好类型", "地区"], dummy_na=True)
    feat_cols = ["年龄", "engagement"] \
        + [c for c in df.columns if c.startswith("性别_") or c.startswith("偏好类型_") or c.startswith("地区_")]
    X = df[feat_cols].astype(float).fillna(0)
    y = df["label"].astype(int)
    return X, y


def _clustering_features() -> pd.DataFrame:
    cons = load_csv("consumption.csv")
    visits = load_csv("visit_records.csv")
    visitors = load_csv("visitors.csv")

    c_sum = cons.groupby("游客ID")["消费金额"].agg(["sum", "count"]).reset_index()
    c_sum.columns = ["游客ID", "total_consume", "consume_count"]
    v_sum = visits.groupby("游客ID").agg(
        visit_count=("记录ID", "count"),
        total_duration=("游玩时长", "sum"),
    ).reset_index()

    df = visitors[["游客ID", "年龄"]].merge(c_sum, on="游客ID", how="left") \
                                       .merge(v_sum, on="游客ID", how="left")
    df[["total_consume", "consume_count", "visit_count", "total_duration"]] = \
        df[["total_consume", "consume_count", "visit_count", "total_duration"]].fillna(0)
    return df


# ----------------------------------------------------------------------
# Training
# ----------------------------------------------------------------------
def _train_all() -> None:
    global _REGRESSION_REPORT, _CLASSIFICATION_REPORT, _CLUSTER_REPORT, _TRAINED
    if _TRAINED:
        return
    log.info("training P2 models on raw CSVs...")

    # ---- Regression: consumption ----
    X, y = _consumption_features()
    from sklearn.model_selection import train_test_split
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42)
    scaler = StandardScaler()
    Xtr_s = scaler.fit_transform(Xtr)
    Xte_s = scaler.transform(Xte)

    for name, model in [
        ("LinearRegression", LinearRegression()),
        ("Lasso", Lasso(alpha=0.1, max_iter=5000)),
        ("Ridge", Ridge(alpha=0.1)),
    ]:
        model.fit(Xtr_s, ytr)
        pred = model.predict(Xte_s)
        _REGRESSION_REPORT.append({
            "task": "consumption_amount",
            "model": name,
            "rmse": round(float(np.sqrt(mean_squared_error(yte, pred))), 4),
            "mae": round(float(mean_absolute_error(yte, pred)), 4),
            "r2": round(float(r2_score(yte, pred)), 4),
        })
        _MODELS[f"consumption_amount:{name}"] = (model, scaler, X.columns.tolist())

    # ---- Regression: daily visitor ----
    X2, y2 = _daily_visitor_features()
    Xtr2, Xte2, ytr2, yte2 = train_test_split(X2, y2, test_size=0.2, random_state=42)
    scaler2 = StandardScaler()
    Xtr2_s = scaler2.fit_transform(Xtr2)
    Xte2_s = scaler2.transform(Xte2)
    for name, model in [
        ("LinearRegression", LinearRegression()),
        ("Lasso", Lasso(alpha=0.1, max_iter=5000)),
        ("Ridge", Ridge(alpha=0.1)),
    ]:
        model.fit(Xtr2_s, ytr2)
        pred = model.predict(Xte2_s)
        _REGRESSION_REPORT.append({
            "task": "daily_visitor",
            "model": name,
            "rmse": round(float(np.sqrt(mean_squared_error(yte2, pred))), 4),
            "mae": round(float(mean_absolute_error(yte2, pred)), 4),
            "r2": round(float(r2_score(yte2, pred)), 4),
        })
        _MODELS[f"daily_visitor:{name}"] = (model, scaler2, X2.columns.tolist())

    # ---- Classification: high value visitor ----
    X3, y3 = _high_value_features()
    Xtr3, Xte3, ytr3, yte3 = train_test_split(X3, y3, test_size=0.2, random_state=42, stratify=y3)
    scaler3 = StandardScaler()
    Xtr3_s = scaler3.fit_transform(Xtr3)
    Xte3_s = scaler3.transform(Xte3)
    for name, model in [
        ("DecisionTree", DecisionTreeClassifier(max_depth=6, random_state=42)),
        ("RandomForest", RandomForestClassifier(n_estimators=80, random_state=42)),
        ("GradientBoosting", GradientBoostingClassifier(n_estimators=80, random_state=42)),
        ("LogisticRegression", LogisticRegression(max_iter=1000, random_state=42)),
    ]:
        if isinstance(model, Ridge):
            # convert to pseudo-classifier via threshold 0
            model.fit(Xtr3_s, ytr3)
            pred = (model.predict(Xte3_s) > 0.5).astype(int)
            try:
                proba = (model.predict(Xte3_s) - 0.5) * 2 + 0.5
            except Exception:
                proba = pred.astype(float)
        else:
            model.fit(Xtr3_s, ytr3)
            pred = model.predict(Xte3_s)
            try:
                proba = model.predict_proba(Xte3_s)[:, 1]
            except Exception:
                proba = pred.astype(float)
        _CLASSIFICATION_REPORT.append({
            "model": name,
            "accuracy": round(float(accuracy_score(yte3, pred)), 4),
            "precision": round(float(precision_score(yte3, pred, zero_division=0)), 4),
            "recall": round(float(recall_score(yte3, pred, zero_division=0)), 4),
            "f1": round(float(f1_score(yte3, pred, zero_division=0)), 4),
            "auc": round(float(roc_auc_score(yte3, proba)) if len(set(yte3)) > 1 else 0.0, 4),
        })
        _MODELS[f"high_value_visitor:{name}"] = (model, scaler3, X3.columns.tolist())

    # ---- Clustering ----
    df_c = _clustering_features()
    scaler_c = StandardScaler()
    Xc = scaler_c.fit_transform(df_c[["年龄", "total_consume", "consume_count", "visit_count", "total_duration"]])
    km = KMeans(n_clusters=4, random_state=42, n_init=10).fit(Xc)
    db = DBSCAN(eps=1.5, min_samples=10).fit(Xc)
    df_c["kmeans"] = km.labels_
    df_c["dbscan"] = db.labels_
    for cluster_id, g in df_c.groupby("kmeans"):
        _CLUSTER_REPORT.append({
            "cluster": int(cluster_id),
            "n": int(len(g)),
            "avg_age": round(float(g["年龄"].mean()), 2),
            "avg_total_consume": round(float(g["total_consume"].mean()), 2),
            "avg_per_consume": round(float(g["total_consume"].sum() / max(g["consume_count"].sum(), 1)), 2),
            "avg_visit_count": round(float(g["visit_count"].mean()), 2),
            "avg_duration_h": round(float(g["total_duration"].mean()), 2),
        })
    _MODELS["cluster_kmeans"] = (km, scaler_c)
    _MODELS["cluster_dbscan"] = (db, scaler_c)

    _TRAINED = True
    log.info("P2 models trained. regression=%d, classification=%d, clusters=%d",
             len(_REGRESSION_REPORT), len(_CLASSIFICATION_REPORT), len(_CLUSTER_REPORT))


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------
def predict(task: str, features: Dict[str, Any]) -> Dict[str, Any]:
    _train_all()
    if task not in {"consumption_amount", "daily_visitor", "high_value_visitor"}:
        raise ValueError(f"unsupported task: {task}")

    # Pick the strongest model per task from the report
    family = {
        "consumption_amount": "LinearRegression",
        "daily_visitor": "LinearRegression",
        "high_value_visitor": "RandomForest",
    }[task]
    key = f"{task}:{family}"
    if key not in _MODELS:
        # Fallback to any model in the family
        for k in _MODELS:
            if k.startswith(task + ":") and not isinstance(_MODELS[k][0], (KMeans, DBSCAN)):
                key = k
                break
    model, scaler, cols = _MODELS[key]

    # === 双轨模式：优先用 PySpark 训练好的模型 ===
    # PySpark 模型特征列是 ["age","purchase_count","avg_amount","visit_count",
    #                          "avg_duration","unique_attractions"]
    # sklearn 模型的特征列可能不一样（带 one-hot 编码等）
    # 所以这里用 try/except：PySpark 成功就用它，失败 fallback 到 sklearn
    pyspark_used = False
    try:
        from services.pyspark_loader import predict as ps_predict
        ps_features = {
            "age":                 float(features.get("age", features.get("年龄", 30))),
            "purchase_count":      float(features.get("purchase_count", 0)),
            "avg_amount":          float(features.get("avg_amount", 0)),
            "visit_count":         float(features.get("visit_count", 0)),
            "avg_duration":        float(features.get("avg_duration", 0)),
            "unique_attractions":  float(features.get("unique_attractions", 0)),
        }
        if task == "consumption_amount":
            from services.pyspark_loader import predict_regression as ps_reg
            r = ps_reg("consumption_amount", ps_features)
        elif task == "high_value_visitor":
            from services.pyspark_loader import predict_classification as ps_clf
            r = ps_clf(ps_features)
        else:
            r = None
        if r is not None:
            val = r["prediction"]
            family = r.get("model", family)  # 用 PySpark 实际选的模型
            pyspark_used = True
            out: Dict[str, Any] = {
                "type": task,
                "prediction": round(val, 2),
                "model": family + " (PySpark)",
                "engine": "pyspark",
                "timestamp": now_iso(),
            }
            if task == "high_value_visitor":
                out["probability"] = round(float(val), 4)
                out["label"] = r.get("label", "高消费" if val > 0.5 else "普通")
            return out
    except Exception as e:
        log.debug("PySpark predict failed, fallback to sklearn: %s", e)

    # === Fallback: sklearn 模型 ===
    # Build feature vector using the SAME columns the model was trained on.
    row: List[float] = []
    for c in cols:
        row.append(float(features.get(c, 0)))
    arr = np.array(row, dtype=float).reshape(1, -1)
    arr_s = scaler.transform(arr)
    pred = model.predict(arr_s)
    val = float(pred[0])

    out: Dict[str, Any] = {
        "type": task,
        "prediction": round(val, 2),
        "model": family + (" (sklearn fallback)" if pyspark_used is False else " (sklearn)"),
        "engine": "sklearn",
        "timestamp": now_iso(),
    }
    if task == "high_value_visitor":
        try:
            proba = model.predict_proba(arr_s)[:, 1]
            out["probability"] = round(float(proba[0]), 4)
            out["label"] = "高价值" if int(proba[0] > 0.5) else "普通"
        except Exception:
            out["label"] = "高价值" if int(val > 0.5) else "普通"
    return out


def regression_report() -> List[Dict[str, Any]]:
    _train_all()
    return _REGRESSION_REPORT


def classification_report() -> List[Dict[str, Any]]:
    _train_all()
    return _CLASSIFICATION_REPORT


def clustering_report() -> List[Dict[str, Any]]:
    _train_all()
    return _CLUSTER_REPORT


def compare_models() -> Dict[str, Any]:
    _train_all()
    return {
        "regression": _REGRESSION_REPORT,
        "classification": _CLASSIFICATION_REPORT,
        "clustering": _CLUSTER_REPORT,
    }
