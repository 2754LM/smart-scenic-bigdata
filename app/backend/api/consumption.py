"""
Consumption APIs.
- GET /api/consumption             list (filter by time/visitor/attraction)
- GET /api/consumption/visits      visit records
"""
from fastapi import APIRouter, Query
from typing import Optional

from services.mysql_service import get_mysql

router = APIRouter(prefix="/api/consumption", tags=["消费与游玩"])


@router.get("")
def list_consumption(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    start_date: Optional[str] = None,
    end_date:   Optional[str] = None,
    visitor_id:    Optional[int] = None,
    attraction_id: Optional[int] = None,
):
    mysql = get_mysql()
    return {"code": 0, "data": mysql.list_consumption(page, page_size, start_date, end_date,
                                                       visitor_id, attraction_id)}


@router.get("/visits")
def list_visits(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    start_date: Optional[str] = None,
    end_date:   Optional[str] = None,
    visitor_id:    Optional[int] = None,
    attraction_id: Optional[int] = None,
):
    mysql = get_mysql()
    return {"code": 0, "data": mysql.list_visit_records(page, page_size, start_date, end_date,
                                                         visitor_id, attraction_id)}
