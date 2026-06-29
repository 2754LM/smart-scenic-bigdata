"""
Analysis APIs (Spark SQL / Hive).
- GET /api/analysis/daily          日客流量时序
- GET /api/analysis/hourly         时段分布
- GET /api/analysis/region         地区分布
- GET /api/analysis/age-group      年龄×性别分布
- GET /api/analysis/type-summary   景点类型汇总
- GET /api/analysis/fpgrowth       关联规则 (从 HDFS 报告读)
"""
from fastapi import APIRouter, Query
from typing import Optional

from services.hive_service import get_hive
from services.hdfs_service import get_hdfs
from services.model_service import get_model

router = APIRouter(prefix="/api/analysis", tags=["数据分析"])


@router.get("/daily")
def daily_visitors(start: str = "2023-01-01", end: str = "2023-12-31"):
    hive = get_hive()
    return {"code": 0, "data": hive.daily_visitors(start, end)}


@router.get("/hourly")
def hourly():
    hive = get_hive()
    return {"code": 0, "data": hive.hourly_distribution()}


@router.get("/region")
def region(limit: int = 20):
    hive = get_hive()
    return {"code": 0, "data": hive.region_distribution(limit)}


@router.get("/age-group")
def age_group():
    hive = get_hive()
    return {"code": 0, "data": hive.age_group_distribution()}


@router.get("/type-summary")
def type_summary():
    hive = get_hive()
    return {"code": 0, "data": hive.type_summary()}


@router.get("/fpgrowth")
def fpgrowth():
    """Top association rules from HDFS report (P1.2 output)."""
    model = get_model()
    rep = model.get_report("fp_growth")
    if not rep:
        return {"code": 0, "data": [], "message": "FPGrowth report not available. Run P1.2 first."}
    return {"code": 0, "data": rep.get("top_rules", [])}
