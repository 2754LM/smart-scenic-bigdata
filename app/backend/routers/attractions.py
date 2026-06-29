"""
/api/attractions/* - 景点信息与汇总。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

import services.mysql_service as mysql_svc
import services.hive_service as hive_svc
from utils import load_csv

router = APIRouter(prefix="/api/attractions", tags=["attractions"])


@router.get("")
def list_attractions():
    data = mysql_svc.list_attractions()
    return {"source": "mysql", "count": len(data), "data": data}


@router.get("/{attraction_id}")
def get_attraction(attraction_id: int):
    data = mysql_svc.list_attractions()
    hit = next((a for a in data if a["景点ID"] == attraction_id), None)
    if not hit:
        raise HTTPException(404, f"attraction {attraction_id} not found")
    return {"source": "mysql", "data": hit}


@router.get("/{attraction_id}/summary")
def attraction_summary(attraction_id: int):
    """Per-attraction aggregate: total visitors, consumption, avg duration."""
    visits = load_csv("visit_records.csv")
    cons = load_csv("consumption.csv")
    v = visits[visits["景点ID"] == attraction_id]
    c = cons[cons["景点ID"] == attraction_id]
    return {
        "source": "csv",
        "attraction_id": attraction_id,
        "data": {
            "游客数": int(v["游客ID"].nunique()),
            "游玩次数": int(len(v)),
            "消费总额": round(float(c["消费金额"].sum()), 2),
            "平均游玩时长": round(float(v["游玩时长"].mean()) if len(v) else 0.0, 2),
        },
    }
