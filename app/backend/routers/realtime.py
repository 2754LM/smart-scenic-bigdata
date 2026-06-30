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


class TaskTriggerIn(BaseModel):
    """模拟实时任务触发器 - 一键生成 N 条事件流"""
    task_type: str = Field("random_events", description="任务类型: random_events|consume_burst|review_flood")
    count: int = Field(50, ge=1, le=500, description="生成事件数量")
    attraction_id: int | None = Field(None, description="指定景点 ID")


@router.post("/task/trigger")
def task_trigger(task: TaskTriggerIn):
    """
    触发实时任务：批量生成模拟事件 → Kafka → 后台 Consumer → HBase
    用于演示"创造任务后链路走 Kafka"的实时数据流。
    """
    import random

    produced = []
    attraction_ids = list(range(1, 11))  # 10 个景点

    for i in range(task.count):
        attr_id = task.attraction_id or random.choice(attraction_ids)
        visitor_id = str(random.randint(1, 10000))

        if task.task_type == "consume_burst":
            # 入园 + 消费 强事件
            kproducer.publish_event(visitor_id, str(attr_id), "enter")
            kproducer.publish_event(visitor_id, str(attr_id), "consume")
            r = kproducer.publish_event(visitor_id, str(attr_id), "exit")
        elif task.task_type == "review_flood":
            r = kproducer.publish_review(visitor_id, str(attr_id), random.randint(3, 5), f"auto-review-{i}")
        else:
            # random_events: 混合入园/出园/评论
            event_choice = random.choice(["enter", "exit", "review"])
            if event_choice == "review":
                r = kproducer.publish_review(visitor_id, str(attr_id), random.randint(1, 5), f"auto-{i}")
            elif event_choice == "enter":
                r = kproducer.publish_event(visitor_id, str(attr_id), "enter")
            else:
                r = kproducer.publish_event(visitor_id, str(attr_id), "exit")
        produced.append(r)

    return {
        "status": "ok",
        "task_type": task.task_type,
        "events_published": len(produced),
        "kafka_status": "events are being consumed in background → HBase",
        "tip": "wait 3-5 seconds then check HBase via /api/realtime/visit-recent",
    }