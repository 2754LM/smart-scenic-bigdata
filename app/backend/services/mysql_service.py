"""
MySQL data access layer - queries MySQL tables directly.

Pipeline: CSV → MySQL → Sqoop → HDFS → Spark → Hive → Frontend
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import pymysql

import config

log = logging.getLogger("smart-scenic.mysql")


def _connect():
    return pymysql.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def query(sql: str, args: Optional[tuple] = None) -> List[Dict[str, Any]]:
    try:
        conn = _connect()
    except Exception as e:
        log.warning("mysql connect failed: %s", e)
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args or ())
            return cur.fetchall()
    finally:
        conn.close()


def _query_df(sql: str, args: Optional[tuple] = None) -> pd.DataFrame:
    rows = query(sql, args)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def table_exists(name: str) -> bool:
    rows = query(
        "SELECT COUNT(*) AS n FROM information_schema.tables "
        "WHERE table_schema=%s AND table_name=%s",
        (config.MYSQL_DB, name),
    )
    return bool(rows and rows[0].get("n", 0))


# ----------------------------------------------------------------------
# CRUD operations
# ----------------------------------------------------------------------

def list_attractions() -> List[Dict[str, Any]]:
    """All attractions from MySQL t_attraction."""
    return query("SELECT * FROM t_attraction")


def list_visitors(
    gender: Optional[str] = None,
    min_age: Optional[int] = None,
    max_age: Optional[int] = None,
    page: int = 1,
    page_size: int = 30,
) -> Dict[str, Any]:
    where = []
    params = []
    if gender:
        where.append("性别 = %s")
        params.append(gender)
    if min_age is not None:
        where.append("年龄 >= %s")
        params.append(min_age)
    if max_age is not None:
        where.append("年龄 <= %s")
        params.append(max_age)
    where_clause = ("WHERE " + " AND ".join(where)) if where else ""

    count_sql = f"SELECT COUNT(*) AS total FROM t_visitor {where_clause}"
    total = query(count_sql, tuple(params))[0]["total"]

    offset = (page - 1) * page_size
    data_sql = f"SELECT * FROM t_visitor {where_clause} LIMIT %s OFFSET %s"
    items = query(data_sql, tuple(params + [page_size, offset]))

    return {"total": total, "page": page, "page_size": page_size, "items": items}


def visitor_aggregate(visitor_id: int) -> Optional[Dict[str, Any]]:
    v = query("SELECT * FROM t_visitor WHERE 游客ID = %s", (str(visitor_id),))
    if not v:
        return None

    total_consume = query(
        "SELECT SUM(消费金额) AS s FROM t_consumption WHERE 游客ID = %s", (str(visitor_id),)
    )[0]["s"] or 0
    consume_count = query(
        "SELECT COUNT(*) AS n FROM t_consumption WHERE 游客ID = %s", (str(visitor_id),)
    )[0]["n"]
    visit_count = query(
        "SELECT COUNT(*) AS n FROM t_visit_record WHERE 游客ID = %s", (str(visitor_id),)
    )[0]["n"]
    total_duration = query(
        "SELECT SUM(游玩时长) AS s FROM t_visit_record WHERE 游客ID = %s", (str(visitor_id),)
    )[0]["s"] or 0

    return {
        "游客ID": visitor_id,
        "总消费": float(total_consume),
        "消费笔数": int(consume_count),
        "游玩次数": int(visit_count),
        "总游玩时长": float(total_duration),
    }


def list_consumption(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    visitor_id: Optional[int] = None,
    attraction_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 30,
) -> Dict[str, Any]:
    where = []
    params = []
    if start_date:
        where.append("时间 >= %s")
        params.append(start_date)
    if end_date:
        where.append("时间 <= %s")
        params.append(end_date + " 23:59:59")
    if visitor_id is not None:
        where.append("游客ID = %s")
        params.append(str(visitor_id))
    if attraction_id is not None:
        where.append("景点ID = %s")
        params.append(str(attraction_id))
    where_clause = ("WHERE " + " AND ".join(where)) if where else ""

    count_sql = f"SELECT COUNT(*) AS total FROM t_consumption {where_clause}"
    total = query(count_sql, tuple(params))[0]["total"]

    offset = (page - 1) * page_size
    data_sql = f"SELECT * FROM t_consumption {where_clause} LIMIT %s OFFSET %s"
    items = query(data_sql, tuple(params + [page_size, offset]))

    return {"total": total, "page": page, "page_size": page_size, "items": items}


def list_visits(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    visitor_id: Optional[int] = None,
    attraction_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 30,
) -> Dict[str, Any]:
    where = []
    params = []
    if start_date:
        where.append("时间 >= %s")
        params.append(start_date)
    if end_date:
        where.append("时间 <= %s")
        params.append(end_date + " 23:59:59")
    if visitor_id is not None:
        where.append("游客ID = %s")
        params.append(str(visitor_id))
    if attraction_id is not None:
        where.append("景点ID = %s")
        params.append(str(attraction_id))
    where_clause = ("WHERE " + " AND ".join(where)) if where else ""

    count_sql = f"SELECT COUNT(*) AS total FROM t_visit_record {where_clause}"
    total = query(count_sql, tuple(params))[0]["total"]

    offset = (page - 1) * page_size
    data_sql = f"SELECT * FROM t_visit_record {where_clause} LIMIT %s OFFSET %s"
    items = query(data_sql, tuple(params + [page_size, offset]))

    return {"total": total, "page": page, "page_size": page_size, "items": items}


# ----------------------------------------------------------------------
# Aggregate metrics for overview dashboard
# ----------------------------------------------------------------------

def overview_kpi() -> Dict[str, Any]:
    n_visitors = query("SELECT COUNT(*) AS n FROM t_visitor")[0]["n"]
    n_attractions = query("SELECT COUNT(*) AS n FROM t_attraction")[0]["n"]
    consume_stats = query(
        "SELECT SUM(消费金额) AS total_amount, COUNT(*) AS total_count FROM t_consumption"
    )[0]
    visit_stats = query(
        "SELECT COUNT(*) AS total_visits, AVG(游玩时长) AS avg_duration FROM t_visit_record"
    )[0]

    total_consume = float(consume_stats["total_amount"] or 0)
    total_cnt = int(consume_stats["total_count"] or 0)
    avg_consume = total_consume / total_cnt if total_cnt else 0.0

    total_visits = int(visit_stats["total_visits"] or 0)
    avg_duration = float(visit_stats["avg_duration"] or 0)

    # daily avg
    daily = query(
        "SELECT COUNT(DISTINCT DATE(时间)) AS days FROM t_visit_record"
    )[0]
    days = daily["days"] or 1
    daily_avg = total_visits / days if days else 0.0

    return {
        "游客总数": int(n_visitors),
        "景点总数": int(n_attractions),
        "消费总额": round(total_consume, 2),
        "游玩次数": total_visits,
        "平均消费": round(avg_consume, 2),
        "平均游玩时长": round(avg_duration, 2),
        "消费笔数": total_cnt,
        "日均游客": round(daily_avg, 1),
    }


def attraction_rank(limit: int = 10) -> List[Dict[str, Any]]:
    ranks = query(
        "SELECT 景点ID, COUNT(*) AS 游客数 FROM t_visit_record "
        "GROUP BY 景点ID ORDER BY 游客数 DESC LIMIT %s",
        (limit,),
    )
    attr_ids = [r["景点ID"] for r in ranks]
    if not attr_ids:
        return []
    placeholders = ",".join(["%s"] * len(attr_ids))
    attrs = query(
        f"SELECT 景点ID, 景点名称 FROM t_attraction WHERE 景点ID IN ({placeholders})",
        tuple(attr_ids),
    )
    attr_map = {a["景点ID"]: a["景点名称"] for a in attrs}
    return [
        {
            "景点ID": r["景点ID"],
            "景点名称": attr_map.get(r["景点ID"], str(r["景点ID"])),
            "游客数": r["游客数"],
        }
        for r in ranks
    ]


def overview_health() -> Dict[str, Any]:
    from services.admin_service import _run_in_container
    comps: Dict[str, bool] = {}
    try:
        conn = _connect()
        conn.close()
        comps["mysql"] = True
    except Exception:
        comps["mysql"] = False
    try:
        r = _run_in_container(config.HBASE_CONTAINER, "echo", "status", timeout=5)
        comps["hbase"] = r["exit_code"] == 0
    except Exception:
        comps["hbase"] = False
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
