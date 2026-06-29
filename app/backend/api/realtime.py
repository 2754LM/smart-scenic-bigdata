"""
Realtime APIs (HBase + Kafka downstream).
- GET /api/realtime/visit-recent  最近游玩记录
- GET /api/realtime/visitor/{id}  游客画像 (HBase)
- GET /api/realtime/attraction/{id} 景点统计 (HBase)
"""
from fastapi import APIRouter

from services.hbase_service import get_hbase

router = APIRouter(prefix="/api/realtime", tags=["实时数据(HBase)"])


@router.get("/visit-recent")
def visit_recent(limit: int = 20):
    hbase = get_hbase()
    return {"code": 0, "data": hbase.scan_visit_recent(limit)}


@router.get("/visitor/{id}")
def visitor_profile(id: int):
    hbase = get_hbase()
    return {"code": 0, "data": hbase.get_visitor_profile(id)}


@router.get("/attraction/{id}")
def attraction_stat(id: int):
    hbase = get_hbase()
    return {"code": 0, "data": hbase.get_attraction_stat(id)}
