"""
/api/realtime/* - 实时数据 API

提供：
  - 读：HBase 实时画像查询
  - 写：Kafka 发布接口（评论/事件）→ 后台消费者落 HBase
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

import services.hbase_service as hbase_svc
import services.kafka_producer as kproducer
import services.kafka_consumer as kconsumer
from datetime import datetime

router = APIRouter(prefix="/api/realtime", tags=["realtime"])


# ============== 读接口（HBase 直读） ==============
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


# ============== 写接口（Kafka 发布） ==============
class ReviewIn(BaseModel):
    visitor_id:   str   = Field(..., description="游客ID")
    attraction_id: str  = Field(..., description="景点ID")
    rating:       int   = Field(..., ge=1, le=5, description="评分 1-5")
    comment:      str   = Field("", description="评论内容")


class EventIn(BaseModel):
    visitor_id:    str = Field(..., description="游客ID")
    attraction_id: str = Field(..., description="景点ID")
    event_type:    str = Field(..., description="事件类型: enter/exit/consume")


@router.post("/publish/review")
def publish_review(review: ReviewIn):
    """发布评论到 Kafka scenic_reviews topic
    后台 consumer 接收后写入 HBase scenic_reviews
    """
    r = kproducer.publish_review(
        visitor_id=review.visitor_id,
        attraction_id=review.attraction_id,
        rating=review.rating,
        comment=review.comment,
    )
    if not r.get("ok"):
        # Kafka 不可用 → 降级直接写 HBase（双写兜底）
        hbase_svc.put_review(
            visitor_id=review.visitor_id,
            attraction_id=review.attraction_id,
            rating=review.rating,
            comment=review.comment,
        )
        return {
            "status": "ok",
            "via": "hbase_fallback",
            "reason": r.get("error"),
        }
    return {
        "status": "ok",
        "via": "kafka",
        "kafka_meta": r,
    }


@router.post("/publish/event")
def publish_event(event: EventIn):
    """发布实时事件到 Kafka scenic_events topic
    后台 consumer 接收后写入 HBase scenic_realtime
    """
    if event.event_type not in {"enter", "exit", "consume"}:
        raise HTTPException(400, f"event_type must be one of enter/exit/consume")
    r = kproducer.publish_event(
        visitor_id=event.visitor_id,
        attraction_id=event.attraction_id,
        event_type=event.event_type,
    )
    if not r.get("ok"):
        hbase_svc.put_realtime_event(
            visitor_id=event.visitor_id,
            attraction_id=event.attraction_id,
            event_type=event.event_type,
        )
        return {
            "status": "ok",
            "via": "hbase_fallback",
            "reason": r.get("error"),
        }
    return {
        "status": "ok",
        "via": "kafka",
        "kafka_meta": r,
    }


# ============== 引擎状态 ==============
@router.get("/kafka/status")
def kafka_status():
    """查看 Kafka producer + consumer 状态"""
    return {
        "producer": kproducer.get_status(),
        "consumer": kconsumer.get_status(),
        "now":      datetime.utcnow().isoformat() + "Z",
    }