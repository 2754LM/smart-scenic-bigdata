"""
Overview (总览) APIs.
- GET /api/overview/kpi            总览 KPI
- GET /api/overview/timeseries     时序数据
- GET /api/overview/attraction-rank 景点热度 Top
- GET /api/overview/visitor-trend  游客趋势
"""
from fastapi import APIRouter, Query, HTTPException
from typing import Optional

from services.mysql_service import get_mysql
from services.hive_service import get_hive

router = APIRouter(prefix="/api/overview", tags=["总览"])


@router.get("/kpi")
def kpi():
    """总览 KPI 卡片."""
    mysql = get_mysql()
    data = mysql.overall_kpi()
    return {"code": 0, "data": data}


@router.get("/timeseries")
def timeseries(
    metric: str = Query("consumption", description="consumption | visit | visitors"),
    start: str = Query("2023-01-01"),
    end:   str = Query("2023-12-31"),
):
    mysql = get_mysql()
    rows = mysql.time_series(metric, start, end)
    return {"code": 0, "data": rows, "metric": metric}


@router.get("/attraction-rank")
def attraction_rank():
    mysql = get_mysql()
    rows = mysql.attraction_summary()
    # attach rank
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    return {"code": 0, "data": rows}


@router.get("/health")
def health():
    return {
        "code": 0,
        "data": {
            "mysql": get_mysql().health(),
            "hive":  get_hive().health(),
        }
    }
