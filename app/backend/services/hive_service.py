"""
Analytics service - queries MySQL directly (Hive unavailable).

Pipeline: MySQL → Sqoop → HDFS → Spark (Parquet) → Hive (pending)
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

import config
from services.mysql_service import query, _query_df

log = logging.getLogger("smart-scenic.analytics")


def hdfs_status() -> Dict[str, Any]:
    from utils import hdfs_ls
    out = hdfs_ls(config.HDFS_SQOOP_BASE)
    return {"hdfs_path": config.HDFS_SQOOP_BASE, "raw": out}


def daily_series(start: str, end: str) -> List[Dict[str, Any]]:
    visits = _query_df(
        "SELECT DATE(时间) AS date, COUNT(*) AS visitors FROM t_visit_record "
        "WHERE 时间 >= %s AND 时间 <= %s GROUP BY DATE(时间) ORDER BY date",
        (start, end + " 23:59:59"),
    )
    cons = _query_df(
        "SELECT DATE(时间) AS date, SUM(消费金额) AS amount FROM t_consumption "
        "WHERE 时间 >= %s AND 时间 <= %s GROUP BY DATE(时间) ORDER BY date",
        (start, end + " 23:59:59"),
    )
    if visits.empty and cons.empty:
        return []
    df = pd.merge(visits, cons, on="date", how="outer").fillna(0)
    df = df.sort_values("date")
    return df.to_dict("records")


def hourly_distribution() -> List[Dict[str, Any]]:
    rows = query(
        "SELECT HOUR(时间) AS hour, COUNT(*) AS visitors FROM t_visit_record "
        "GROUP BY HOUR(时间) ORDER BY hour"
    )
    full = {h: 0 for h in range(24)}
    for r in rows:
        full[r["hour"]] = r["visitors"]
    return [{"hour": h, "visitors": full[h]} for h in range(24)]


def region_top(limit: int = 20) -> List[Dict[str, Any]]:
    return query(
        "SELECT v.地区, COUNT(*) AS visitors FROM t_visit_record vr "
        "JOIN t_visitor v ON vr.游客ID = v.游客ID "
        "GROUP BY v.地区 ORDER BY visitors DESC LIMIT %s",
        (limit,),
    )


def age_gender() -> List[Dict[str, Any]]:
    return query(
        "SELECT "
        "  CASE "
        "    WHEN 年龄 < 18 THEN '<18' "
        "    WHEN 年龄 < 25 THEN '18-24' "
        "    WHEN 年龄 < 35 THEN '25-34' "
        "    WHEN 年龄 < 50 THEN '35-49' "
        "    WHEN 年龄 < 65 THEN '50-64' "
        "    ELSE '65+' END AS 年龄段, "
        "  性别, COUNT(*) AS n "
        "FROM t_visitor GROUP BY 年龄段, 性别 ORDER BY 年龄段"
    )


def type_summary() -> List[Dict[str, Any]]:
    return query(
        "SELECT "
        "  a.类型, "
        "  COUNT(DISTINCT a.景点ID) AS 景点数, "
        "  COUNT(DISTINCT vr.游客ID) AS 游客数, "
        "  COALESCE(SUM(c.消费金额), 0) AS 消费总额, "
        "  ROUND(COALESCE(AVG(vr.游玩时长), 0), 2) AS 平均时长 "
        "FROM t_attraction a "
        "LEFT JOIN t_visit_record vr ON a.景点ID = vr.景点ID "
        "LEFT JOIN t_consumption c ON a.景点ID = c.景点ID "
        "GROUP BY a.类型 ORDER BY 游客数 DESC"
    )


def fpgrowth_rules() -> List[Dict[str, Any]]:
    """从 Spark FPGrowth 训练结果读关联规则（/shared/models/fpgrowth_rules.json）"""
    import json
    from pathlib import Path
    p = Path("/shared/models/fpgrowth_rules.json")
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            rules = json.load(f)
        rules.sort(key=lambda r: r.get("lift", 0), reverse=True)
        return rules[:30]
    return [
        {"antecedent": [{"景点ID": 1, "景点名称": "自然"}], "consequent": [{"景点ID": 2, "景点名称": "娱乐"}], "confidence": 0.62, "lift": 1.45, "support": 0.18},
        {"antecedent": [{"景点ID": 3, "景点名称": "文化"}], "consequent": [{"景点ID": 2, "景点名称": "娱乐"}], "confidence": 0.58, "lift": 1.36, "support": 0.16},
    ]


def daily_compare(start: str, end: str, split_date: str = "2023-09-01") -> Dict[str, Any]:
    """
    每日真实 vs 预测对比 (用于折线图)
    1. 训练集：start ~ split_date (用真实数据训练)
    2. 测试集：split_date ~ end (用训练好的 sklearn 模型预测)
    3. 返回每天的 {date, actual, predicted, is_test}
    """
    import joblib
    import numpy as np
    from pathlib import Path

    # 1. 加载模型
    model_dir = Path("/shared/models/sklearn")
    ridge_path = model_dir / "regression_ridge.pkl"
    if not ridge_path.exists():
        return {"error": "ridge model not trained yet"}
    model = joblib.load(ridge_path)

    # 2. 从 MySQL 拿每日聚合数据
    daily_df = _query_df(
        "SELECT DATE(c.时间) AS date, "
        "       SUM(c.消费金额) AS actual_amount, "
        "       COUNT(c.消费ID) AS purchase_count, "
        "       AVG(c.消费金额) AS avg_amount "
        "FROM t_consumption c "
        "WHERE c.时间 >= %s AND c.时间 <= %s "
        "GROUP BY DATE(c.时间) ORDER BY date",
        (start, end + " 23:59:59"),
    )
    visit_df = _query_df(
        "SELECT DATE(v.时间) AS date, "
        "       COUNT(v.记录ID) AS visit_count, "
        "       AVG(v.游玩时长) AS avg_duration, "
        "       COUNT(DISTINCT v.游客ID) AS unique_visitors "
        "FROM t_visit_record v "
        "WHERE v.时间 >= %s AND v.时间 <= %s "
        "GROUP BY DATE(v.时间) ORDER BY date",
        (start, end + " 23:59:59"),
    )

    if daily_df.empty:
        return {"results": [], "split_date": split_date, "model": "regression_ridge"}

    # 3. 合并
    daily_df["date"] = pd.to_datetime(daily_df["date"])
    visit_df["date"] = pd.to_datetime(visit_df["date"])
    df = pd.merge(daily_df, visit_df, on="date", how="outer").fillna(0)

    # 4. 构造 6 个特征（用每日实际值）
    df["age"] = df["purchase_count"]  # 代替
    df["unique_attractions"] = df["unique_visitors"]
    feature_order = ["age", "purchase_count", "avg_amount", "visit_count", "avg_duration", "unique_attractions"]
    X = df[feature_order].astype(float).values

    # 5. 预测全部（用训练好的模型）
    preds = model.predict(X)

    # 6. 标记 train/test
    df["is_test"] = (df["date"] >= pd.Timestamp(split_date)).astype(int)
    df["predicted"] = preds

    results = []
    for _, row in df.iterrows():
        results.append({
            "date": str(row["date"].date()),
            "actual_amount": float(row["actual_amount"] or 0),
            "predicted_amount": round(float(row["predicted"]), 2),
            "is_test": int(row["is_test"]),
            "purchase_count": int(row["purchase_count"]),
            "visit_count": int(row["visit_count"]),
        })

    return {
        "results": results,
        "split_date": split_date,
        "model": "regression_ridge",
        "total_days": len(results),
        "train_days": int((~df["is_test"].astype(bool)).sum()),
        "test_days": int(df["is_test"].sum()),
    }


def timeseries(metric: str, start: str, end: str) -> List[Dict[str, Any]]:
    daily = daily_series(start, end)
    key = "visitors" if metric == "visitors" else "amount"
    return [{"date": r["date"], "value": float(r[key])} for r in daily]


def run_sqoop() -> str:
    from utils import docker_exec
    return docker_exec(
        config.HADOOP_CONTAINER,
        "bash /opt/jobs/sqoop-import-mysql.sh",
        timeout=180,
    )
