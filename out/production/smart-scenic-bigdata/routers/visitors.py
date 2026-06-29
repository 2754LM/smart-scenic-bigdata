"""
/api/visitors/* - 游客查询。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

import services.mysql_service as mysql_svc

router = APIRouter(prefix="/api/visitors", tags=["visitors"])


@router.get("")
def list_visitors(
    gender: str = Query(None, description="男 / 女"),
    min_age: int = Query(None),
    max_age: int = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=200),
):
    data = mysql_svc.list_visitors(gender, min_age, max_age, page, page_size)
    return {"source": "csv", **data}


@router.get("/{visitor_id}")
def get_visitor(visitor_id: int):
    from utils import load_csv
    visitors = load_csv("visitors.csv")
    row = visitors[visitors["游客ID"] == visitor_id]
    if row.empty:
        raise HTTPException(404, f"visitor {visitor_id} not found")
    return {"source": "csv", "data": row.iloc[0].to_dict()}


@router.get("/{visitor_id}/aggregate")
def visitor_aggregate(visitor_id: int):
    data = mysql_svc.visitor_aggregate(visitor_id)
    if not data:
        raise HTTPException(404, f"visitor {visitor_id} not found")
    return {"source": "csv", "data": data}
