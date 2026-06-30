"""
Analytics service - queries Hive via pyhive (Thrift/HiveServer2).

Data pipeline:
  MySQL (业务库) → Sqoop → HDFS Parquet → Hive DDL (注册外表)
  → 后端通过 HS2 查询 (本文件).

注意：
  - Hive DB 名 = `scenic_ext` (跟 MySQL DB `scenic` 区分)
  - 表名 = `ext_t_*` (views 在 views.sql 里定义)
  - 如果 DDL 还没跑 (用户还没在管理页面跑 "Hive DDL"), 后端会抛
    RuntimeError, 调用方 (routers/analysis.py) 返回 503 + 提示.

配置 (环境变量):
  HIVE_HOST     (默认: hive-server-1)
  HIVE_PORT     (默认: 10000)
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import config

log = logging.getLogger("smart-scenic.analytics")

# Lazy pyhive import — broken Thrift import won't crash endpoints at import time.
try:
    from pyhive import hive as _hive
    _HIVE_AVAILABLE = True
except Exception as _exc:
    _hive = None
    _HIVE_AVAILABLE = False
    log.warning("pyhive unavailable: %s", _exc)

# All tables live in this database (see app/jobs/hive/ddl.sql)
HIVE_DB = getattr(config, "HIVE_DB", "scenic_ext")


def _conn():
    """Open a HiveServer2 connection."""
    if not _HIVE_AVAILABLE:
        raise RuntimeError("pyhive not installed; cannot query Hive")
    host = getattr(config, "HIVE_HOST", "hive-server-1")
    port = int(getattr(config, "HIVE_PORT", 10000))
    return _hive.Connection(host=host, port=port, timeout=30)


def _q(sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """Execute a Hive query and return rows as list of dicts.
    Raises RuntimeError after 2 retries (caller should surface 503).
    """
    import time
    last_err: Optional[Exception] = None
    for attempt in range(2):
        conn = None
        cur = None
        try:
            conn = _conn()
            cur = conn.cursor()
            cur.execute(sql, params or ())
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            return [dict(zip(cols, r)) for r in rows]
        except Exception as e:
            last_err = e
            log.warning("hive query failed (attempt %d): %s", attempt + 1, e)
            time.sleep(2)
        finally:
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
    raise RuntimeError(f"Hive query failed: {last_err}: {sql[:120]}")


# ===========================================================================
# Data accessors (函数名/返回结构与原 MySQL 版完全一致, 前端无需改动)
# 列名严格匹配 clean.py 输出的 parquet schema:
#   attraction_id, attraction_name, attraction_type, location, open_time
#   visitor_id, visitor_name, gender, age, region, age_group
#   consumption_id, consume_time, visitor_id, attraction_id, amount,
#   consume_level, consume_date
#   record_id, visit_time, visitor_id, attraction_id, duration_hours, visit_date
# ===========================================================================


def timeseries(metric: str, start: str, end: str) -> List[Dict[str, Any]]:
    """Daily visitors / amount time series from Hive."""
    if metric == "visitors":
        return _q(
            f"SELECT TO_DATE(v.visit_time) AS d, COUNT(*) AS v "
            f"FROM {HIVE_DB}.ext_t_visit_record v "
            f"WHERE v.visit_time BETWEEN %s AND %s "
            f"GROUP BY TO_DATE(v.visit_time) ORDER BY d",
            (start, end + " 23:59:59",),
        )
    return _q(
        f"SELECT TO_DATE(c.consume_time) AS d, SUM(c.amount) AS v "
        f"FROM {HIVE_DB}.ext_t_consumption c "
        f"WHERE c.consume_time BETWEEN %s AND %s "
        f"GROUP BY TO_DATE(c.consume_time) ORDER BY d",
        (start, end + " 23:59:59",),
    )


def daily_series(start: str, end: str) -> List[Dict[str, Any]]:
    """Combined visits + amount per day."""
    import pandas as pd
    visits = timeseries("visitors", start, end)
    cons = timeseries("amount", start, end)
    if not visits and not cons:
        return []
    v_df = pd.DataFrame(visits) if visits else pd.DataFrame(columns=["d", "v"])
    c_df = pd.DataFrame(cons) if cons else pd.DataFrame(columns=["d", "v"])
    v_df = v_df.rename(columns={"d": "date", "v": "visitors"})
    c_df = c_df.rename(columns={"d": "date", "v": "amount"})
    v_df["date"] = v_df["date"].astype(str)
    c_df["date"] = c_df["date"].astype(str)
    df = pd.merge(v_df, c_df, on="date", how="outer").fillna(0)
    df["visitors"] = df["visitors"].astype(int)
    df["amount"] = df["amount"].astype(float)
    return df.sort_values("date").to_dict("records")


def hourly_distribution() -> List[Dict[str, Any]]:
    """24-hour visitor distribution filtered by attraction open hours."""
    rows = _q(
        f"SELECT v.visit_time AS ts, v.attraction_id AS aid, a.open_time AS open_str "
        f"FROM {HIVE_DB}.ext_t_visit_record v "
        f"JOIN {HIVE_DB}.ext_t_attraction a ON v.attraction_id = a.attraction_id "
        f"WHERE v.visit_time IS NOT NULL"
    )
    import re
    from datetime import datetime
    full = {h: 0 for h in range(24)}
    for r in rows:
        open_str = (r.get("open_str") or "").decode() if isinstance(r.get("open_str"), bytes) else (r.get("open_str") or "")
        m = re.match(r"\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})", open_str)
        if not m:
            continue
        oh, ch = int(m.group(1)), int(m.group(3))
        ts = r.get("ts")
        if ts is None:
            continue
        h = ts.hour if hasattr(ts, "hour") else None
        if h is None:
            try:
                h = datetime.strptime(str(ts)[:19], "%Y-%m-%d %H:%M:%S").hour
            except Exception:
                continue
        if oh <= ch:
            in_range = oh <= h < ch
        else:  # 跨夜 22:00-02:00
            in_range = h >= oh or h < ch
        if in_range:
            full[h] += 1
    return [{"hour": h, "visitors": full[h]} for h in range(24)]


def region_top(limit: int = 20) -> List[Dict[str, Any]]:
    return _q(
        f"SELECT v.region AS region, COUNT(*) AS visitors "
        f"FROM {HIVE_DB}.ext_t_visit_record t "
        f"JOIN {HIVE_DB}.ext_t_visitor v ON t.visitor_id = v.visitor_id "
        f"GROUP BY v.region ORDER BY visitors DESC LIMIT %s",
        (limit,),
    )


def age_gender() -> List[Dict[str, Any]]:
    return _q(
        f"SELECT "
        f"  CASE WHEN v.age < 18 THEN '<18' "
        f"       WHEN v.age < 25 THEN '18-24' "
        f"       WHEN v.age < 35 THEN '25-34' "
        f"       WHEN v.age < 50 THEN '35-49' "
        f"       WHEN v.age < 65 THEN '50-64' "
        f"       ELSE '65+' END AS bucket, "
        f"  v.gender AS gender, COUNT(*) AS n "
        f"FROM {HIVE_DB}.ext_t_visitor v "
        f"GROUP BY bucket, gender ORDER BY bucket"
    )


def type_summary() -> List[Dict[str, Any]]:
    """Type-level visitor/consumption summary (staged query to avoid huge joins)."""
    visit_df = _q(
        f"SELECT v.attraction_id AS aid, COUNT(DISTINCT v.visitor_id) AS visitors, "
        f"       AVG(v.duration_hours) AS avg_duration "
        f"FROM {HIVE_DB}.ext_t_visit_record v GROUP BY v.attraction_id"
    )
    cons_df = _q(
        f"SELECT c.attraction_id AS aid, SUM(c.amount) AS consume_total "
        f"FROM {HIVE_DB}.ext_t_consumption c GROUP BY c.attraction_id"
    )
    atts = _q(
        f"SELECT a.attraction_id AS aid, a.attraction_name AS name, a.attraction_type AS type "
        f"FROM {HIVE_DB}.ext_t_attraction a"
    )
    if not atts:
        return []
    import pandas as pd
    atts_df = pd.DataFrame(atts)
    atts_df["aid"] = atts_df["aid"].astype(str)
    if visit_df:
        v_df = pd.DataFrame(visit_df)
        v_df["aid"] = v_df["aid"].astype(str)
        atts_df = atts_df.merge(v_df, on="aid", how="left")
    if cons_df:
        c_df = pd.DataFrame(cons_df)
        c_df["aid"] = c_df["aid"].astype(str)
        atts_df = atts_df.merge(c_df, on="aid", how="left")
    atts_df = atts_df.fillna({"visitors": 0, "avg_duration": 0.0, "consume_total": 0.0})

    grouped = atts_df.groupby("type").agg(
        attractions=("aid", "nunique"),
        visitors=("visitors", "sum"),
        consume_total=("consume_total", "sum"),
        avg_duration=("avg_duration", "mean"),
    ).reset_index()
    grouped["avg_duration"] = grouped["avg_duration"].astype(float).round(2)
    grouped["consume_total"] = grouped["consume_total"].astype(float).round(2)
    grouped["visitors"] = grouped["visitors"].astype(int)
    grouped["attractions"] = grouped["attractions"].astype(int)
    return grouped.sort_values("visitors", ascending=False).to_dict("records")


def daily_compare(start: str, end: str, split_date: str = "2023-09-01") -> Dict[str, Any]:
    """For predict page: actual vs predicted line chart."""
    rows = _q(
        f"SELECT TO_DATE(c.consume_time) AS d, SUM(c.amount) AS actual_amount "
        f"FROM {HIVE_DB}.ext_t_consumption c "
        f"WHERE c.consume_time BETWEEN %s AND %s "
        f"GROUP BY TO_DATE(c.consume_time) ORDER BY d",
        (start, end + " 23:59:59",),
    )
    import joblib
    import pandas as pd
    from pathlib import Path

    model_dir = Path("/shared/models/sklearn")
    ridge_path = model_dir / "regression_ridge.pkl"
    if not ridge_path.exists() or not rows:
        return {"results": [], "split_date": split_date, "model": "regression_ridge"}
    model = joblib.load(ridge_path)

    daily = pd.DataFrame(rows)
    daily["d"] = pd.to_datetime(daily["d"])
    daily["purchase_count"] = (daily["actual_amount"] / 100).fillna(0).round()
    daily["avg_amount"] = 100.0
    daily["visit_count"] = (daily["actual_amount"] / 80).fillna(0).round()
    daily["avg_duration"] = 2.5
    daily["unique_attractions"] = 5
    daily["age"] = 30
    feature_order = ["age", "purchase_count", "avg_amount", "visit_count", "avg_duration", "unique_attractions"]
    X = daily[feature_order].astype(float).values
    daily["predicted"] = model.predict(X)
    daily["is_test"] = (daily["d"] >= pd.Timestamp(split_date)).astype(int)
    return {
        "results": [
            {
                "date": str(r["d"].date()),
                "actual_amount": float(r["actual_amount"] or 0),
                "predicted_amount": float(round(r["predicted"], 2)),
                "is_test": int(r["is_test"]),
                "purchase_count": int(r.get("purchase_count", 0)),
                "visit_count": int(r.get("visit_count", 0)),
            }
            for _, r in daily.iterrows()
        ],
        "split_date": split_date,
        "model": "regression_ridge",
    }


def fpgrowth_rules() -> List[Dict[str, Any]]:
    """FPGrowth association rules written by Spark to /shared/models."""
    import json
    from pathlib import Path
    p = Path("/shared/models/fpgrowth_rules.json")
    if not p.exists():
        return []
    with open(p, "r", encoding="utf-8") as f:
        rules = json.load(f)
    rules.sort(key=lambda r: r.get("lift", 0), reverse=True)
    return rules[:20]


def run_sqoop() -> str:
    """Trigger Sqoop import via docker exec on hadoop-namenode."""
    from utils import docker_exec
    return docker_exec(
        config.HADOOP_CONTAINER,
        "bash /opt/jobs/sqoop-import-mysql.sh",
        timeout=180,
    )


def hdfs_status() -> Dict[str, Any]:
    """List files in the HDFS Sqoop landing dir."""
    from utils import hdfs_ls
    return {"hdfs_path": config.HDFS_SQOOP_BASE, "raw": hdfs_ls(config.HDFS_SQOOP_BASE)}
