"""
/api/analysis/* - 数据分析端点（hive/CSV 路径）。
"""
from __future__ import annotations

from fastapi import APIRouter, Query

import services.hive_service as hive_svc

router = APIRouter(prefix="/api/analysis", tags=["analysis"])


@router.get("/daily")
def daily(start: str = "2023-01-01", end: str = "2023-12-31"):
    data = hive_svc.daily_series(start, end)
    return {"source": "csv", "start": start, "end": end, "count": len(data), "data": data}


@router.get("/hourly")
def hourly():
    data = hive_svc.hourly_distribution()
    return {"source": "csv", "count": len(data), "data": data}


@router.get("/region")
def region(limit: int = 20):
    data = hive_svc.region_top(limit)
    return {"source": "csv", "count": len(data), "data": data}


@router.get("/age-group")
def age_group():
    data = hive_svc.age_gender()
    return {"source": "csv", "count": len(data), "data": data}


@router.get("/type-summary")
def type_summary():
    data = hive_svc.type_summary()
    return {"source": "csv", "count": len(data), "data": data}


@router.get("/fpgrowth")
def fpgrowth():
    data = hive_svc.fpgrowth_rules()
    return {"source": "syn", "count": len(data), "data": data}


@router.get("/daily-compare")
def daily_compare(start: str = "2023-01-01", end: str = "2023-12-31",
                 split_date: str = "2023-09-01"):
    """每日真实 vs 预测对比（用于折线图）
    测试集 = split_date 之后；训练集 = split_date 之前
    """
    data = hive_svc.daily_compare(start, end, split_date)
    return {"source": "sklearn", "start": start, "end": end, **data}
