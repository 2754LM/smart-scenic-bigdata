"""
/api/overview/* - 总览大屏数据：KPI、时序、景点排名、健康检查。
数据源：
  - KPI / 景点排名 / 健康检查 → MySQL（实时汇总）
  - 时序 → Hive（数仓聚合查询）
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

import services.mysql_service as mysql_svc
import services.hive_service as hive_svc

router = APIRouter(prefix="/api/overview", tags=["overview"])


@router.get("/kpi")
def kpi():
    return {"source": "mysql", "data": mysql_svc.overview_kpi()}


@router.get("/timeseries")
def timeseries(metric: str = "visitors", start: str = "2023-01-01", end: str = "2023-12-31"):
    try:
        data = hive_svc.timeseries(metric, start, end)
    except RuntimeError as e:
        raise HTTPException(503, f"Hive unavailable: {e}")
    return {"source": "hive", "metric": metric, "count": len(data), "data": data}


@router.get("/attraction-rank")
def attraction_rank(limit: int = 10):
    data = mysql_svc.attraction_rank(limit)
    return {"source": "mysql", "count": len(data), "data": data}


@router.get("/health")
def health():
    return mysql_svc.overview_health()
