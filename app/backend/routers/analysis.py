"""
/api/analysis/* - 数据分析端点 (Hive via pyhive).

如果 Hive 表还没建 (用户在管理页面跑了 "Hive DDL" 之后才能用),
返回 503 + 提示信息. 不要静默 fallback (用户明确要求"真正跑起来").
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

import services.hive_service as hive_svc

router = APIRouter(prefix="/api/analysis", tags=["analysis"])


def _wrap(data):
    return {"count": len(data) if isinstance(data, list) else 0, "data": data}


@router.get("/daily")
def daily(start: str = "2023-01-01", end: str = "2023-12-31"):
    try:
        data = hive_svc.daily_series(start, end)
    except RuntimeError as e:
        raise HTTPException(503, f"Hive unavailable: {e}. Run 'Hive DDL' in manage.html first.")
    return {"source": "auto", "start": start, "end": end, **_wrap(data)}


@router.get("/hourly")
def hourly():
    try:
        data = hive_svc.hourly_distribution()
    except RuntimeError as e:
        raise HTTPException(503, f"Hive unavailable: {e}")
    return {"source": "hive", **_wrap(data)}


@router.get("/region")
def region(limit: int = 20):
    try:
        data = hive_svc.region_top(limit)
    except RuntimeError as e:
        raise HTTPException(503, f"Hive unavailable: {e}")
    return {"source": "hive", **_wrap(data)}


@router.get("/age-group")
def age_group():
    try:
        data = hive_svc.age_gender()
    except RuntimeError as e:
        raise HTTPException(503, f"Hive unavailable: {e}")
    return {"source": "hive", **_wrap(data)}


@router.get("/type-summary")
def type_summary():
    try:
        data = hive_svc.type_summary()
    except RuntimeError as e:
        raise HTTPException(503, f"Hive unavailable: {e}")
    return {"source": "hive", **_wrap(data)}


@router.get("/fpgrowth")
def fpgrowth():
    try:
        data = hive_svc.fpgrowth_rules()
    except RuntimeError as e:
        raise HTTPException(503, f"Hive unavailable: {e}")
    return {"source": "fpgrowth", **_wrap(data)}


@router.get("/apriori")
def apriori():
    try:
        data = hive_svc.apriori_rules()
    except RuntimeError as e:
        raise HTTPException(503, f"Hive unavailable: {e}")
    return {"source": "apriori", **_wrap(data)}


@router.get("/daily-compare")
def daily_compare(start: str = "2023-01-01", end: str = "2023-12-31",
                  split_date: str = "2023-09-01"):
    """每日真实 vs 预测对比（用于折线图）
    测试集 = split_date 之后；训练集 = split_date 之前
    """
    try:
        data = hive_svc.daily_compare(start, end, split_date)
    except RuntimeError as e:
        raise HTTPException(503, f"Hive unavailable: {e}")
    return {"source": "hive+sklearn", "start": start, "end": end, **data}
