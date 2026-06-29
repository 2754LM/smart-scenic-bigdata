"""
/api/consumption/* - 消费记录 + 游玩记录。
"""
from __future__ import annotations

from fastapi import APIRouter, Query

import services.mysql_service as mysql_svc

router = APIRouter(prefix="/api/consumption", tags=["consumption"])


@router.get("")
def list_consumption(
    start_date: str = Query(None),
    end_date: str = Query(None),
    visitor_id: int = Query(None),
    attraction_id: int = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=200),
):
    data = mysql_svc.list_consumption(start_date, end_date, visitor_id, attraction_id, page, page_size)
    return {"source": "csv", **data}


@router.get("/visits")
def list_visits(
    start_date: str = Query(None),
    end_date: str = Query(None),
    visitor_id: int = Query(None),
    attraction_id: int = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=200),
):
    data = mysql_svc.list_visits(start_date, end_date, visitor_id, attraction_id, page, page_size)
    return {"source": "csv", **data}
