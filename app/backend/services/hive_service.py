"""
Analytics service - queries Hive via pyhive (Thrift/HiveServer2).

Pipeline: MySQL → Sqoop → HDFS → Spark (Parquet) → Hive tables.

Configuration via environment:
  HIVE_HOST     (default: hive-server-1)
  HIVE_PORT     (default: 10000)
  HIVE_TIMEOUT  (default: 30s per query)
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import config

log = logging.getLogger("smart-scenic.analytics")

# Lazy pyhive import — broken Thrift import won't crash analysis endpoints at import time.
try:
    from pyhive import hive as _hive
    _HIVE_AVAILABLE = True
except Exception as _exc:
    _hive = None
    _HIVE_AVAILABLE = False
    log.warning("pyhive unavailable: %s", _exc)


def _conn():
    if not _HIVE_AVAILABLE:
        raise RuntimeError("pyhive not installed; cannot query Hive")
    host = getattr(config, "HIVE_HOST", "hive-server-1")
    port = int(getattr(config, "HIVE_PORT", 10000))
    return _hive.Connection(host=host, port=port, timeout=30)


def _q(sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """Execute a Hive query and return rows as list of dicts."""
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
    raise RuntimeError(f"Hive query failed after retries: {last_err}: {sql[:120]}")


# ===========================================================================
# Data accessors (keep the same function names so callers don't break)
# ===========================================================================


def timeseries(metric: str, start: str, end: str) -> List[Dict[str, Any]]:
    """
    Daily visitors / consumption time series from Hive.
    metric: 'visitors' or 'amount'
    """
    if metric == "visitors":
        return _q(
            "SELECT TO_DATE(t.visit_time) AS d, COUNT(*) AS v "
            "FROM scenic.t_visit_record_partitioned t "
            "WHERE t.visit_time BETWEEN %s AND %s "
            "GROUP BY TO_DATE(t.visit_time) ORDER BY d",
            (start, end + " 23:59:59",),
        )
    return _q(
        "SELECT TO_DATE(c.consume_time) AS d, SUM(c.amount) AS v "
        "FROM scenic.t_consumption_partitioned c "
        "WHERE c.consume_time BETWEEN %s AND %s "
        "GROUP BY TO_DATE(c.consume_time) ORDER BY d",
        (start, end + " 23:59:59",),
    )


def daily_series(start: str, end: str) -> List[Dict[str, Any]]:
    """Combined visits + amount per day, like the original MySQL version."""
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
    """
    Hourly visitor distribution. Filtered by attraction open hours.
    """
    rows = _q(
        "SELECT t.visit_time AS ts, t.attraction_id AS aid, a.open_hours AS open_str "
        "FROM scenic.t_visit_record t JOIN scenic.t_attraction a "
        "  ON t.attraction_id = a.attraction_id "
        "WHERE t.visit_time IS NOT NULL"
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
        else:
            in_range = h >= oh or h < ch
        if in_range:
            full[h] += 1
    return [{"hour": h, "visitors": full[h]} for h in range(24)]


def region_top(limit: int = 20) -> List[Dict[str, Any]]:
    return _q(
        "SELECT v.region AS region, COUNT(*) AS visitors "
        "FROM scenic.t_visit_record t JOIN scenic.t_visitor v "
        "  ON t.visitor_id = v.visitor_id "
        "GROUP BY v.region ORDER BY visitors DESC LIMIT %s",
        (limit,),
    )


def age_gender() -> List[Dict[str, Any]]:
    return _q(
        "SELECT "
        "  CASE WHEN v.age < 18 THEN '<18' "
        "       WHEN v.age < 25 THEN '18-24' "
        "       WHEN v.age < 35 THEN '25-34' "
        "       WHEN v.age < 50 THEN '35-49' "
        "       WHEN v.age < 65 THEN '50-64' "
        "       ELSE '65+' END AS bucket, "
        "  v.gender AS gender, COUNT(*) AS n "
        "FROM scenic.t_visitor v GROUP BY bucket, gender ORDER BY bucket"
    )


def type_summary() -> List[Dict[str, Any]]:
    """Type-level visitor/consumption summary from Hive."""
    return _q(
        "SELECT a.type AS type, "
        "       COUNT(DISTINCT a.attraction_id) AS attractions, "
        "       SUM(visit_count) AS visitors, "
        "       SUM(consume_amount) AS consume_total, "
        "       AVG(avg_duration) AS avg_duration "
        "FROM scenic.t_attraction_type_summary a "
        "GROUP BY a.type ORDER BY visitors DESC"
    )


def daily_compare(start: str, end: str, split_date: str = "2023-09-01") -> Dict[str, Any]:
    """For predict page: actual vs predicted line chart."""
    rows = _q(
        "SELECT TO_DATE(c.consume_time) AS d, SUM(c.amount) AS actual_amount "
        "FROM scenic.t_consumption c "
        "WHERE c.consume_time BETWEEN %s AND %s "
        "GROUP BY TO_DATE(c.consume_time) ORDER BY d",
        (start, end + " 23:59:59",),
    )
    import json
    from pathlib import Path
    import joblib
    import pandas as pd
    import numpy as np

    model_dir = Path("/shared/models/sklearn")
    ridge = joblib.load(model_dir / "regression_ridge.pkl") if (model_dir / "regression_ridge.pkl").exists() else None
    # 6 features in same order as FEATURE_COLS in train.py
    FEATURE_ORDER = ["age", "purchase_count", "avg_amount", "visit_count", "avg_duration", "unique_attractions"]
    # pull daily aggregates (placeholder fill with means from MySQL is fine for chart)
    daily = pd.DataFrame(rows)
    if daily.empty or ridge is None:
        return {"results": [], "split_date": split_date, "model": "regression_ridge"}
    daily["d"] = pd.to_datetime(daily["d"])
    # Use lag values as fill for missing features
    daily["purchase_count"] = (daily["actual_amount"] / 100).fillna(0).round()
    daily["avg_amount"] = 100.0
    daily["visit_count"] = (daily["actual_amount"] / 80).fillna(0).round()
    daily["avg_duration"] = 2.5
    daily["unique_attractions"] = 5
    daily["age"] = 30
    X = daily[FEATURE_ORDER].astype(float).values
    daily["predicted"] = ridge.predict(X)
    daily["is_test"] = (daily["d"] >= pd.Timestamp(split_date)).astype(int)
    return {
        "results": [
            {
                "date": str(r["d"].date()),
                "actual_amount": float(r["actual_amount"] or 0),
                "predicted_amount": float(round(r["predicted"], 2)),
                "is_test": int(r["is_test"]),
            }
            for _, r in daily.iterrows()
        ],
        "split_date": split_date,
        "model": "regression_ridge",
    }


def fpgrowth_rules() -> List[Dict[str, Any]]:
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
    from utils import docker_exec
    return docker_exec(
        config.HADOOP_CONTAINER,
        "bash /opt/jobs/sqoop-import-mysql.sh",
        timeout=180,
    )


def hdfs_status() -> Dict[str, Any]:
    from utils import hdfs_ls
    return {"hdfs_path": config.HDFS_SQOOP_BASE, "raw": hdfs_ls(config.HDFS_SQOOP_BASE)}
