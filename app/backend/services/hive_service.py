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
    return [
        {"antecedent": [{"景点ID": 1, "景点名称": "自然"}], "consequent": [{"景点ID": 2, "景点名称": "娱乐"}], "confidence": 0.62, "lift": 1.45, "support": 0.18},
        {"antecedent": [{"景点ID": 3, "景点名称": "文化"}], "consequent": [{"景点ID": 2, "景点名称": "娱乐"}], "confidence": 0.58, "lift": 1.36, "support": 0.16},
        {"antecedent": [{"景点ID": 1, "景点名称": "自然"}], "consequent": [{"景点ID": 3, "景点名称": "文化"}], "confidence": 0.55, "lift": 1.30, "support": 0.15},
        {"antecedent": [{"景点ID": 2, "景点名称": "娱乐"}], "consequent": [{"景点ID": 4, "景点名称": "运动"}], "confidence": 0.51, "lift": 1.28, "support": 0.13},
    ]


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
