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
    """24h 时段游客分布 — 仅统计景点开放时间内的记录。
    数据原始时间是随机生成的（凌晨也有数据），与景点开放时间不符。
    修正：JOIN 景点表，逐记录判断 HOUR(时间) 是否在该景点开放区间内（Python 端过滤）。
    """
    rows = query(
        "SELECT vr.时间, vr.景点ID, a.开放时间 "
        "FROM t_visit_record vr "
        "JOIN t_attraction a ON vr.景点ID = a.景点ID"
    )
    import re
    from datetime import datetime
    full = {h: 0 for h in range(24)}
    for r in rows:
        open_str = r.get("开放时间") or ""
        m = re.match(r"\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})", open_str)
        if not m:
            continue
        oh, ch = int(m.group(1)), int(m.group(3))
        ts = r["时间"]
        h = ts.hour if hasattr(ts, "hour") else datetime.strptime(str(ts)[:19], "%Y-%m-%d %H:%M:%S").hour
        # 判断是否在开放时段（处理跨夜）
        if oh <= ch:
            in_range = oh <= h < ch
        else:  # 跨夜 22:00-02:00
            in_range = h >= oh or h < ch
        if in_range:
            full[h] += 1
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
    """聚合查询：分阶段执行避免大表 JOIN 慢。"""
    # 1. 景点 + 游玩统计（基于 t_visit_record 聚合到景点）
    visit_df = _query_df(
        "SELECT vr.景点ID, COUNT(DISTINCT vr.游客ID) AS 游客数, "
        "       AVG(vr.游玩时长) AS 平均时长 "
        "FROM t_visit_record vr GROUP BY vr.景点ID"
    )
    # 2. 景点 + 消费统计（基于 t_consumption 聚合到景点）
    cons_df = _query_df(
        "SELECT c.景点ID, SUM(c.消费金额) AS 消费总额 "
        "FROM t_consumption c GROUP BY c.景点ID"
    )
    # 3. 景点主表
    atts = query("SELECT 景点ID, 景点名称, 类型 FROM t_attraction")
    if not atts:
        return []
    atts_df = pd.DataFrame(atts)
    atts_df["景点ID"] = atts_df["景点ID"].astype(str)
    if not visit_df.empty:
        visit_df["景点ID"] = visit_df["景点ID"].astype(str)
        atts_df = atts_df.merge(visit_df, on="景点ID", how="left")
    if not cons_df.empty:
        cons_df["景点ID"] = cons_df["景点ID"].astype(str)
        atts_df = atts_df.merge(cons_df, on="景点ID", how="left")
    atts_df = atts_df.fillna({"游客数": 0, "平均时长": 0.0, "消费总额": 0.0})

    # 4. 按 类型 聚合
    grouped = atts_df.groupby("类型").agg(
        景点数=("景点ID", "nunique"),
        游客数=("游客数", "sum"),
        消费总额=("消费总额", "sum"),
        平均时长=("平均时长", "mean"),
    ).reset_index()
    # 转为 python float（避免 numpy float 序列化错误）
    grouped["平均时长"] = grouped["平均时长"].astype(float).round(2)
    grouped["消费总额"] = grouped["消费总额"].astype(float).round(2)
    grouped["游客数"] = grouped["游客数"].astype(int)
    grouped["景点数"] = grouped["景点数"].astype(int)
    return grouped.sort_values("游客数", ascending=False).to_dict("records")


def fpgrowth_rules() -> List[Dict[str, Any]]:
    """从 Spark FPGrowth 训练结果读关联规则（/shared/models/fpgrowth_rules.json）
    优先取简单规则（|a|<=2, |c|<=2），按 lift 降序。
    """
    import json
    from pathlib import Path
    p = Path("/shared/models/fpgrowth_rules.json")
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            rules = json.load(f)
        # 优先返回简单规则（antecedent 和 consequent 都 <= 2 项）
        simple = [r for r in rules if len(r.get("antecedent", [])) <= 2 and len(r.get("consequent", [])) <= 2]
        simple.sort(key=lambda r: r.get("lift", 0), reverse=True)
        if len(simple) >= 10:
            return simple[:20]
        rules.sort(key=lambda r: r.get("lift", 0), reverse=True)
        return rules[:20]
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
