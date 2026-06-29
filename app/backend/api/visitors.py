"""
Visitor APIs.
- GET /api/visitors               list (with pagination + filter)
- GET /api/visitors/{id}          detail
- GET /api/visitors/{id}/agg      per-visitor aggregate (消费/游玩)
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from services.mysql_service import get_mysql
from services.hbase_service import get_hbase

router = APIRouter(prefix="/api/visitors", tags=["游客"])


@router.get("")
def list_visitors(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    gender: Optional[str] = None,
    min_age: Optional[int] = None,
    max_age: Optional[int] = None,
):
    mysql = get_mysql()
    return {"code": 0, "data": mysql.list_visitors(page, page_size, gender, min_age, max_age)}


@router.get("/{id}")
def get_visitor(id: int):
    mysql = get_mysql()
    v = mysql.get_visitor(id)
    if not v:
        raise HTTPException(404, "visitor not found")
    return {"code": 0, "data": v}


@router.get("/{id}/aggregate")
def visitor_aggregate(id: int):
    mysql = get_mysql()
    hbase = get_hbase()
    v = mysql.get_visitor(id)
    if not v:
        raise HTTPException(404, "visitor not found")
    agg = mysql.visitor_aggregates(id)
    hbase_profile = hbase.get_visitor_profile(id)
    return {
        "code": 0,
        "data": {**v, "aggregate": agg, "hbase": hbase_profile}
    }
