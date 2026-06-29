"""
/api/realtime/* - HBase 实时画像查询。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

import services.hbase_service as hbase_svc

router = APIRouter(prefix="/api/realtime", tags=["realtime"])


@router.get("/visit-recent")
def visit_recent(limit: int = Query(20, ge=1, le=200)):
    """Latest realtime visit events from HBase scenic_realtime."""
    data = hbase_svc.recent_visits(limit)
    return {"source": "hbase", "count": len(data), "data": data}


@router.get("/visitor/{visitor_id}")
def visitor_profile(visitor_id: int):
    data = hbase_svc.visitor_profile(visitor_id)
    if data is None:
        raise HTTPException(404, f"visitor {visitor_id} not found in HBase")
    return {"source": "hbase", "data": data}


@router.get("/attraction/{attraction_id}")
def attraction_stat(attraction_id: int):
    data = hbase_svc.attraction_stat(attraction_id)
    if data is None:
        raise HTTPException(404, f"attraction {attraction_id} not found in HBase")
    return {"source": "hbase", "data": data}
