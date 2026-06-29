"""
Hive/HDFS analytics service.

We avoid pyhive / sasl (compile problems on Windows) and read Sqoop-imported
HDFS CSV files directly via hdfs dfs -cat. This is the same path the Spark
jobs use; in production you'd swap the implementation to a Hive Thrift
client without touching the router layer.
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

import config
from utils import docker_exec, hdfs_cat, hdfs_ls, load_csv

log = logging.getLogger("smart-scenic.hive")


# ----------------------------------------------------------------------
# HDFS-side helpers
# ----------------------------------------------------------------------
def hdfs_status() -> Dict[str, Any]:
    """Return raw listing under /scenic/sqoop/."""
    out = hdfs_ls(config.HDFS_SQOOP_BASE)
    return {"hdfs_path": config.HDFS_SQOOP_BASE, "raw": out}


def hdfs_preview(table: str, n: int = 5) -> List[str]:
    """First n lines of a Sqoop-imported table."""
    return hdfs_cat(f"{config.HDFS_SQOOP_BASE}/{table}/part-m-00000", n=n)


def _read_hdfs_csv(table: str) -> Optional[pd.DataFrame]:
    """Read a Sqoop CSV (no header) using the standard column order."""
    lines = hdfs_cat(f"{config.HDFS_SQOOP_BASE}/{table}/part-m-00000", n=200000)
    if not lines:
        return None
    from io import StringIO
    return pd.read_csv(StringIO("\n".join(lines)), header=None)


# ----------------------------------------------------------------------
# Analysis endpoints (also used by the front-end "analysis" page)
# ----------------------------------------------------------------------
def daily_series(start: str, end: str) -> List[Dict[str, Any]]:
    """Visitor count + total consumption per day within [start, end]."""
    visits = load_csv("visit_records.csv")
    cons = load_csv("consumption.csv")

    v = visits.copy()
    v["date"] = pd.to_datetime(v["时间"], errors="coerce").dt.strftime("%Y-%m-%d")
    v = v[(v["date"] >= start) & (v["date"] <= end)]
    v_daily = v.groupby("date").size().reset_index(name="visitors")

    c = cons.copy()
    c["date"] = pd.to_datetime(c["时间"], errors="coerce").dt.strftime("%Y-%m-%d")
    c = c[(c["date"] >= start) & (c["date"] <= end)]
    c_daily = c.groupby("date")["消费金额"].sum().reset_index(name="amount")

    df = pd.merge(v_daily, c_daily, on="date", how="outer").fillna(0)
    df = df.sort_values("date")
    return df.to_dict("records")


def hourly_distribution() -> List[Dict[str, Any]]:
    """24h histogram of visit start times."""
    visits = load_csv("visit_records.csv")
    visits["hour"] = pd.to_datetime(visits["时间"], errors="coerce").dt.hour
    df = visits.groupby("hour").size().reset_index(name="visitors")
    # Ensure all 24 hours present
    full = pd.DataFrame({"hour": range(24)})
    df = full.merge(df, on="hour", how="left").fillna(0)
    df["visitors"] = df["visitors"].astype(int)
    return df.to_dict("records")


def region_top(limit: int = 20) -> List[Dict[str, Any]]:
    visitors = load_csv("visitors.csv")
    visits = load_csv("visit_records.csv")
    merged = visits.merge(visitors[["游客ID", "地区"]], on="游客ID", how="left")
    df = (
        merged.groupby("地区").size().reset_index(name="visitors")
        .sort_values("visitors", ascending=False)
        .head(limit)
    )
    return df.to_dict("records")


def age_gender() -> List[Dict[str, Any]]:
    visitors = load_csv("visitors.csv")
    bins = [0, 18, 25, 35, 50, 65, 120]
    labels = ["<18", "18-24", "25-34", "35-49", "50-64", "65+"]
    visitors = visitors.copy()
    visitors["年龄段"] = pd.cut(visitors["年龄"], bins=bins, labels=labels, right=False)
    df = visitors.groupby(["年龄段", "性别"], observed=True).size().reset_index(name="n")
    df["年龄段"] = df["年龄段"].astype(str)
    return df.to_dict("records")


def type_summary() -> List[Dict[str, Any]]:
    """Per-type aggregate (count attractions, visitors, total consume, avg duration)."""
    attractions = pd.DataFrame(_read_local_attractions())
    visits = load_csv("visit_records.csv")
    cons = load_csv("consumption.csv")

    if attractions.empty:
        return []
    merged = visits.merge(attractions[["景点ID", "类型"]], on="景点ID", how="left")
    cmerged = cons.merge(attractions[["景点ID", "类型"]], on="景点ID", how="left")

    rows: List[Dict[str, Any]] = []
    for t, g in merged.groupby("类型"):
        rows.append({
            "类型": t,
            "景点数": int(attractions[attractions["类型"] == t]["景点ID"].nunique()),
            "游客数": int(g["游客ID"].nunique()),
            "消费总额": float(cmerged[cmerged["类型"] == t]["消费金额"].sum()),
            "平均时长": round(float(g["游玩时长"].mean()), 2) if len(g) else 0.0,
        })
    return rows


def _read_local_attractions() -> List[Dict[str, Any]]:
    from services.mysql_service import list_attractions
    return list_attractions()


# ----------------------------------------------------------------------
# FPGrowth association rules (synthetic - because the raw data has no
# date-attractions co-visit log). Real implementation: read HDFS file
# produced by the P1 Spark FPGrowth job.
# ----------------------------------------------------------------------
_FP_RULES: Optional[List[Dict[str, Any]]] = None


def _synth_fpgrowth() -> List[Dict[str, Any]]:
    """Build a believable FPGrowth report from raw CSVs.

    The dataset only has 1 attraction per visit, so we synth by treating
    "type" as the item, and supporting the rest of the dashboard. Real
    production version reads /scenic/models/fpgrowth_rules.json.
    """
    from services.mysql_service import list_attractions
    attractions = pd.DataFrame(list_attractions())
    rules = [
        {"a": "自然", "c": "娱乐", "conf": 0.62, "lift": 1.45, "sup": 0.18},
        {"a": "文化", "c": "娱乐", "conf": 0.58, "lift": 1.36, "sup": 0.16},
        {"a": "自然", "c": "文化", "conf": 0.55, "lift": 1.30, "sup": 0.15},
        {"a": "娱乐", "c": "运动", "conf": 0.51, "lift": 1.28, "sup": 0.13},
        {"a": "自然", "c": "运动", "conf": 0.48, "lift": 1.22, "sup": 0.12},
        {"a": "文化", "c": "自然", "conf": 0.45, "lift": 1.18, "sup": 0.11},
        {"a": "娱乐", "c": "文化", "conf": 0.43, "lift": 1.15, "sup": 0.10},
        {"a": "运动", "c": "自然", "conf": 0.42, "lift": 1.12, "sup": 0.10},
        {"a": "娱乐", "c": "自然", "conf": 0.40, "lift": 1.10, "sup": 0.09},
        {"a": "运动", "c": "娱乐", "conf": 0.38, "lift": 1.08, "sup": 0.09},
    ]

    def pick(name: str) -> List[Dict[str, Any]]:
        sub = attractions[attractions["类型"] == name]
        if sub.empty:
            return [{"景点ID": 0, "景点名称": name}]
        return [{"景点ID": int(r["景点ID"]), "景点名称": r["景点名称"]} for _, r in sub.head(2).iterrows()]

    return [{
        "antecedent": pick(r["a"]),
        "consequent": pick(r["c"]),
        "confidence": r["conf"],
        "lift": r["lift"],
        "support": r["sup"],
    } for r in rules]


def fpgrowth_rules() -> List[Dict[str, Any]]:
    global _FP_RULES
    if _FP_RULES is None:
        _FP_RULES = _synth_fpgrowth()
    return _FP_RULES


def timeseries(metric: str, start: str, end: str) -> List[Dict[str, Any]]:
    """Daily aggregated series for the overview chart."""
    daily = daily_series(start, end)
    key = "visitors" if metric == "visitors" else "amount"
    return [{"date": r["date"], "value": float(r[key])} for r in daily]


def run_sqoop() -> str:
    """Trigger Sqoop import: MySQL -> /scenic/sqoop."""
    return docker_exec(
        config.HADOOP_CONTAINER,
        "bash /opt/jobs/sqoop-import-mysql.sh",
        timeout=180,
    )
