"""
Attraction APIs.
- GET /api/attractions                 list all
- GET /api/attractions/{id}            detail
- GET /api/attractions/{id}/summary    per-attraction analytics
"""
from fastapi import APIRouter, HTTPException
from typing import List

from services.mysql_service import get_mysql
from services.hbase_service import get_hbase

router = APIRouter(prefix="/api/attractions", tags=["景点"])


@router.get("")
def list_attractions():
    mysql = get_mysql()
    return {"code": 0, "data": mysql.list_attractions()}


@router.get("/{id}")
def get_attraction(id: int):
    mysql = get_mysql()
    data = mysql.get_attraction(id)
    if not data:
        raise HTTPException(404, "attraction not found")
    return {"code": 0, "data": data}


@router.get("/{id}/summary")
def attraction_summary(id: int):
    """Per-attraction analytics (MySQL + HBase profile)."""
    mysql = get_mysql()
    hbase = get_hbase()
    attraction = mysql.get_attraction(id)
    if not attraction:
        raise HTTPException(404, "attraction not found")

    # Sum stats from MySQL
    summary_list = mysql.attraction_summary()
    my_summary = next((s for s in summary_list if s["景点ID"] == id), None)

    hbase_data = hbase.get_attraction_stat(id)

    return {
        "code": 0,
        "data": {
            "attraction": attraction,
            "summary": my_summary,
            "hbase": hbase_data,
        }
    }
