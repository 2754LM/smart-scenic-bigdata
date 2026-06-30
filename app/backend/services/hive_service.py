"""
Analytics service - queries Hive via beeline through docker exec.

Data pipeline:
  MySQL (业务库) → Sqoop → HDFS Parquet → Hive DDL (注册外表)
  → 后端通过 beeline -e SQL 查询 HS2 (本文件).

Why not pyhive? pyhive + thrift-sasl requires libsasl2-dev in demo-backend
image; to keep the image slim we use beeline-over-EXEC instead. This means:
  - One docker exec per query (each starts a new JVM) — slow but OK for admin UI
  - No SAP/SASL/HiveClient2 dependencies in demo-backend
  - Uses existing _run_in_container path

Configuration via environment:
  HIVE_HOST     (default: hive-server-1)
  HIVE_PORT     (default: 10000)
"""
from __future__ import annotations

import csv
import io
import json
import logging
import re
from typing import Any, Dict, List, Optional

import config

log = logging.getLogger("smart-scenic.analytics")

# All tables live in this database (see app/jobs/hive/ddl.sql)
HIVE_DB = getattr(config, "HIVE_DB", "scenic_ext")


def _beeline(sql: str, timeout: int = 90) -> List[Dict[str, Any]]:
    """Run SQL through beeline -e (one JVM spawn per query, ~30-60s typical).

    Returns list of dict rows. Beeline output looks like:
        header_line1\theader_line2\theader_line3
        value1\tvalue2\tvalue3
        ...
    (with --outputformat=tsv2).
    """
    import services.admin_service as ad
    from services.docker_client import _request as dc_request
    sql_escaped = sql.replace("'", "'\\''")
    cmd = f"/opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/{HIVE_DB}' -n hive -p hive --silent=true --outputformat=tsv2 -e '{sql_escaped}' 2>/dev/null"
    r = ad._run_in_container("hive-server-1", "bash", "-c", cmd, timeout=timeout)
    stdout = r.get("stdout", "") or ""
    rc = r.get("exit_code", -1)
    if rc not in (0, None) or "FAILED: Execution Error" in stdout or "Error:" in stdout:
        # Detached exec stdout isn't captured. Fall back to container logs (stdout tail).
        log.warning("beeline rc=%s — fetching container logs (exec stdout detached)", rc)
        log_resp = dc_request("GET", "/containers/hive-server-1/logs", {"stdout": True, "stderr": True, "tail": 500})
        if log_resp and not isinstance(log_resp, dict):
            return _parse_beeline_tsv(str(log_resp))
        # Real error visible on stdout: raise
        if "FAILED" in stdout or "Error" in stdout:
            log.error("beeline error: %s", stdout[:500])
            raise RuntimeError(f"Hive query failed: {stdout[:300]}")
        # No stdout and no container log: treat as empty
        return []
    return _parse_beeline_tsv(stdout)


def _parse_beeline_tsv(out: str) -> List[Dict[str, Any]]:
    """Parse beeline --outputformat=tsv2 output (header + rows, tab-separated)."""
    if not out:
        return []
    rows = [r for r in out.splitlines() if r.strip()]
    if not rows:
        return []
    header = rows[0].split("\t")
    out_rows = []
    for line in rows[1:]:
        # Skip 'N rows selected' footer
        if "row(s) selected" in line.lower() or line.startswith("Time taken"):
            continue
        # Skip separator lines like --+--+--
        if re.match(r"^[-+\s]+$", line):
            continue
        cells = line.split("\t")
        if len(cells) != len(header):
            continue
        out_rows.append({h: c.strip() for h, c in zip(header, cells)})
    return out_rows


# ===========================================================================
# Data accessors — same signatures/return shapes as before, so callers unchanged
# 列名匹配 clean.py 输出的 parquet schema.
# ===========================================================================


def timeseries(metric: str, start: str, end: str) -> List[Dict[str, Any]]:
    """Daily visitors / amount time series from Hive."""
    if metric == "visitors":
        return _beeline(
            f"SELECT TO_DATE(v.visit_time) AS d, COUNT(*) AS n "
            f"FROM {HIVE_DB}.ext_t_visit_record v "
            f"WHERE v.visit_time BETWEEN '{start}' AND '{end} 23:59:59' "
            f"GROUP BY TO_DATE(v.visit_time) ORDER BY d"
        )
    return _beeline(
        f"SELECT TO_DATE(c.consume_time) AS d, SUM(c.amount) AS n "
        f"FROM {HIVE_DB}.ext_t_consumption c "
        f"WHERE c.consume_time BETWEEN '{start}' AND '{end} 23:59:59' "
        f"GROUP BY TO_DATE(c.consume_time) ORDER BY d"
    )


def daily_series(start: str, end: str) -> List[Dict[str, Any]]:
    """Combined visits + amount per day."""
    import pandas as pd
    visits = timeseries("visitors", start, end)
    cons = timeseries("amount", start, end)
    if not visits and not cons:
        return []
    v_df = pd.DataFrame(visits) if visits else pd.DataFrame(columns=["d", "n"])
    c_df = pd.DataFrame(cons) if cons else pd.DataFrame(columns=["d", "n"])
    v_df = v_df.rename(columns={"d": "date", "n": "visitors"})
    c_df = c_df.rename(columns={"d": "date", "n": "amount"})
    v_df["date"] = v_df["date"].astype(str)
    c_df["date"] = c_df["date"].astype(str)
    df = pd.merge(v_df, c_df, on="date", how="outer").fillna(0)
    df["visitors"] = df["visitors"].astype(int)
    df["amount"] = df["amount"].astype(float)
    return df.sort_values("date").to_dict("records")


def hourly_distribution() -> List[Dict[str, Any]]:
    """24-hour visitor distribution filtered by attraction open hours."""
    rows = _beeline(
        f"SELECT v.visit_time AS ts, v.attraction_id AS aid, a.open_time AS open_str "
        f"FROM {HIVE_DB}.ext_t_visit_record v "
        f"JOIN {HIVE_DB}.ext_t_attraction a ON v.attraction_id = a.attraction_id "
        f"WHERE v.visit_time IS NOT NULL"
    )
    full = {h: 0 for h in range(24)}
    for r in rows:
        open_str = r.get("open_str", "")
        m = re.match(r"\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})", str(open_str))
        if not m:
            continue
        oh, ch = int(m.group(1)), int(m.group(3))
        ts = r.get("ts", "")
        # Hive returns ts as "YYYY-MM-DD HH:MM:SS" string
        m2 = re.search(r"\s(\d{2}):", str(ts))
        if not m2:
            continue
        h = int(m2.group(1))
        if oh <= ch:
            in_range = oh <= h < ch
        else:
            in_range = h >= oh or h < ch
        if in_range:
            full[h] += 1
    return [{"hour": h, "visitors": full[h]} for h in range(24)]


def region_top(limit: int = 20) -> List[Dict[str, Any]]:
    return _beeline(
        f"SELECT v.region AS region, COUNT(*) AS visitors "
        f"FROM {HIVE_DB}.ext_t_visit_record t "
        f"JOIN {HIVE_DB}.ext_t_visitor v ON t.visitor_id = v.visitor_id "
        f"GROUP BY v.region ORDER BY visitors DESC LIMIT {limit}"
    )


def age_gender() -> List[Dict[str, Any]]:
    return _beeline(
        "SELECT "
        "  CASE WHEN v.age < 18 THEN '<18' "
        "       WHEN v.age < 25 THEN '18-24' "
        "       WHEN v.age < 35 THEN '25-34' "
        "       WHEN v.age < 50 THEN '35-49' "
        "       WHEN v.age < 65 THEN '50-64' "
        "       ELSE '65+' END AS bucket, "
        "  v.gender AS gender, COUNT(*) AS n "
        f"FROM {HIVE_DB}.ext_t_visitor v "
        "GROUP BY bucket, gender ORDER BY bucket"
    )


def type_summary() -> List[Dict[str, Any]]:
    """Type-level visitor/consumption summary."""
    rows = _beeline(
        f"SELECT a.type AS type, "
        f"       COUNT(DISTINCT a.attraction_id) AS attractions, "
        f"       COALESCE(SUM(c.amount), 0) AS consume_total, "
        f"       COALESCE(COUNT(DISTINCT v.visitor_id), 0) AS visitors, "
        f"       COALESCE(AVG(v.duration_hours), 0) AS avg_duration "
        f"FROM {HIVE_DB}.ext_t_attraction a "
        f"LEFT JOIN {HIVE_DB}.ext_t_consumption c ON a.attraction_id = c.attraction_id "
        f"LEFT JOIN {HIVE_DB}.ext_t_visit_record v ON a.attraction_id = v.attraction_id "
        f"GROUP BY a.type ORDER BY visitors DESC"
    )
    # Convert numeric strings to floats
    for r in rows:
        for k in ("attractions", "consume_total", "visitors", "avg_duration"):
            if k in r:
                try:
                    r[k] = float(r[k])
                except (ValueError, TypeError):
                    r[k] = 0
    return rows


def daily_compare(start: str, end: str, split_date: str = "2023-09-01") -> Dict[str, Any]:
    """For predict page: actual vs predicted line chart."""
    rows = _beeline(
        f"SELECT TO_DATE(c.consume_time) AS d, SUM(c.amount) AS actual_amount "
        f"FROM {HIVE_DB}.ext_t_consumption c "
        f"WHERE c.consume_time BETWEEN '{start}' AND '{end} 23:59:59' "
        f"GROUP BY TO_DATE(c.consume_time) ORDER BY d"
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
    if "d" not in daily.columns:
        return {"results": [], "split_date": split_date, "model": "regression_ridge"}
    daily["d"] = pd.to_datetime(daily["d"], errors="coerce")
    daily = daily.dropna(subset=["d"])
    if daily.empty:
        return {"results": [], "split_date": split_date, "model": "regression_ridge"}
    daily["purchase_count"] = (pd.to_numeric(daily["actual_amount"], errors="coerce").fillna(0) / 100).round()
    daily["avg_amount"] = 100.0
    daily["visit_count"] = (pd.to_numeric(daily["actual_amount"], errors="coerce").fillna(0) / 80).round()
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
    """FPGrowth rules from /shared/models/fpgrowth_rules.json."""
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
    import services.admin_service as ad
    cmd = ["bash", "/opt/jobs/sqoop-import-mysql.sh"]
    r = ad._run_in_container(config.HADOOP_CONTAINER, *cmd, timeout=600)
    return r.get("stdout", "")


def hdfs_status() -> Dict[str, Any]:
    """List files in the HDFS Sqoop landing dir."""
    import services.admin_service as ad
    r = ad._run_in_container(config.HADOOP_CONTAINER, "hdfs", "dfs", "-ls", "/scenic/sqoop/", timeout=30)
    return {"hdfs_path": "/scenic/sqoop/", "raw": r.get("stdout", "")}
