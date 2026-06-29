"""
MySQL data access layer.

Business tables: t_attraction, t_visitor, t_consumption, t_visit_record.
The seed dataset is tiny (10/20/32/20 rows) so we also merge in the
raw_data/*.csv files (10K+ rows) to give the front-end something to plot.
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import pymysql

import config
from utils import load_csv

log = logging.getLogger("smart-scenic.mysql")

# Module-level cache for the small MySQL tables.
_CACHE_LOCK = threading.Lock()
_CACHE: Dict[str, pd.DataFrame] = {}


def _connect():
    return pymysql.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DB,
        charset="utf8mb4",
    )


def query(sql: str, args: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """Run a SELECT and return rows as dicts."""
    try:
        conn = _connect()
    except Exception as e:
        log.warning("mysql connect failed: %s", e)
        return []
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, args or ())
            return cur.fetchall()
    finally:
        conn.close()


def table_exists(name: str) -> bool:
    rows = query(
        "SELECT COUNT(*) AS n FROM information_schema.tables "
        "WHERE table_schema=%s AND table_name=%s",
        (config.MYSQL_DB, name),
    )
    return bool(rows and rows[0].get("n", 0))


def _load_table(name: str, force: bool = False) -> pd.DataFrame:
    with _CACHE_LOCK:
        if name in _CACHE and not force:
            return _CACHE[name]
        rows = query(f"SELECT * FROM {name}")
        df = pd.DataFrame(rows) if rows else pd.DataFrame()
        _CACHE[name] = df
        return df


# ----------------------------------------------------------------------
# Public API used by routers
# ----------------------------------------------------------------------
def list_attractions() -> List[Dict[str, Any]]:
    """All attractions, prefer MySQL, fall back to raw_data CSV."""
    df = _load_table("t_attraction")
    if not df.empty:
        # Map MySQL columns to the Chinese keys the front-end expects.
        # (MySQL stores Chinese names directly since P0 schema)
        return df.to_dict("records")
    # Fallback to local CSV
    df = load_csv("attractions.csv")
    return df.to_dict("records")


def list_visitors(
    gender: Optional[str] = None,
    min_age: Optional[int] = None,
    max_age: Optional[int] = None,
    page: int = 1,
    page_size: int = 30,
) -> Dict[str, Any]:
    df = load_csv("visitors.csv")
    if gender:
        df = df[df["性别"] == gender]
    if min_age is not None:
        df = df[df["年龄"] >= min_age]
    if max_age is not None:
        df = df[df["年龄"] <= max_age]
    total = len(df)
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": df.iloc[start:end].to_dict("records"),
    }


def visitor_aggregate(visitor_id: int) -> Optional[Dict[str, Any]]:
    visitors = load_csv("visitors.csv")
    if visitor_id not in visitors["游客ID"].values:
        return None
    cons = load_csv("consumption.csv")
    visits = load_csv("visit_records.csv")
    own_cons = cons[cons["游客ID"] == visitor_id]
    own_visits = visits[visits["游客ID"] == visitor_id]
    return {
        "游客ID": visitor_id,
        "总消费": float(own_cons["消费金额"].sum()),
        "消费笔数": int(len(own_cons)),
        "游玩次数": int(len(own_visits)),
        "总游玩时长": float(own_visits["游玩时长"].sum()),
        "平均满意度": None,  # satisfaction is in MySQL t_visit, not raw CSV
    }


def list_consumption(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    visitor_id: Optional[int] = None,
    attraction_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 30,
) -> Dict[str, Any]:
    df = load_csv("consumption.csv")
    if start_date:
        df = df[df["时间"] >= start_date]
    if end_date:
        df = df[df["时间"] <= end_date + " 23:59:59"]
    if visitor_id is not None:
        df = df[df["游客ID"] == visitor_id]
    if attraction_id is not None:
        df = df[df["景点ID"] == attraction_id]
    total = len(df)
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": df.iloc[start:end].to_dict("records"),
    }


def list_visits(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    visitor_id: Optional[int] = None,
    attraction_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 30,
) -> Dict[str, Any]:
    df = load_csv("visit_records.csv")
    if start_date:
        df = df[df["时间"] >= start_date]
    if end_date:
        df = df[df["时间"] <= end_date + " 23:59:59"]
    if visitor_id is not None:
        df = df[df["游客ID"] == visitor_id]
    if attraction_id is not None:
        df = df[df["景点ID"] == attraction_id]
    total = len(df)
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": df.iloc[start:end].to_dict("records"),
    }


# ----------------------------------------------------------------------
# Aggregate metrics for the overview dashboard
# ----------------------------------------------------------------------
def overview_kpi() -> Dict[str, Any]:
    visitors = load_csv("visitors.csv")
    cons = load_csv("consumption.csv")
    visits = load_csv("visit_records.csv")
    attractions = list_attractions()
    total_consume = float(cons["消费金额"].sum())
    total_visits = int(len(visits))
    total_consume_count = int(len(cons))
    avg_consume = total_consume / total_consume_count if total_consume_count else 0.0
    avg_duration = float(visits["游玩时长"].mean()) if total_visits else 0.0

    # daily avg
    visits_ts = pd.to_datetime(visits["时间"], errors="coerce").dt.date
    days = visits_ts.dropna().unique()
    daily_avg = total_visits / len(days) if len(days) else 0.0

    return {
        "游客总数": int(len(visitors)),
        "景点总数": int(len(attractions)),
        "消费总额": round(total_consume, 2),
        "游玩次数": total_visits,
        "平均消费": round(avg_consume, 2),
        "平均游玩时长": round(avg_duration, 2),
        "消费笔数": total_consume_count,
        "日均游客": round(daily_avg, 1),
    }


def attraction_rank(limit: int = 10) -> List[Dict[str, Any]]:
    visits = load_csv("visit_records.csv")
    ranks = (
        visits.groupby("景点ID").size().reset_index(name="游客数")
        .sort_values("游客数", ascending=False)
        .head(limit)
    )
    ranks["景点ID"] = ranks["景点ID"].astype(str)  # CSV loads as int64, merge key must match
    attractions = pd.DataFrame(list_attractions())
    merged = ranks.merge(attractions, on="景点ID", how="left")
    merged["景点名称"] = merged["景点名称"].fillna(merged["景点ID"].astype(str))
    return merged[["景点ID", "景点名称", "游客数"]].to_dict("records")


def overview_health() -> Dict[str, Any]:
    """Check connectivity to MySQL, HBase, Hive."""
    from services.admin_service import _run_in_container
    comps: Dict[str, bool] = {}
    # MySQL
    try:
        conn = _connect()
        conn.close()
        comps["mysql"] = True
    except Exception:
        comps["mysql"] = False
    # HBase via docker exec (use socket API for compatibility)
    try:
        r = _run_in_container(config.HBASE_CONTAINER, "echo", "status", timeout=5)
        comps["hbase"] = r["exit_code"] == 0
    except Exception:
        comps["hbase"] = False
    # Hive via docker exec
    try:
        r = _run_in_container("hive-server-1", "echo", "ok", timeout=5)
        comps["hive"] = r["exit_code"] == 0
    except Exception:
        comps["hive"] = False
    return {
        "status": "ok" if comps.get("mysql") else "degraded",
        "ts": datetime.now().timestamp(),
        "components": comps,
        "hive": comps.get("hive", False),
        "hbase": comps.get("hbase", False),
    }
