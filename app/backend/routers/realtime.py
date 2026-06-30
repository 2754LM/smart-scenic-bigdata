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
    """查询游客 HBase 实时画像；HBase 里只有触发过 Kafka 任务的游客，
    没触发过的从 MySQL 取历史聚合（不是故障兜底，是数据真实分布）."""
    data = hbase_svc.visitor_profile(visitor_id)
    if data is None:
        # Fallback: 从 MySQL 取历史数据，标记 source 为 mysql
        from services.mysql_service import visitor_aggregate
        v = visitor_aggregate(visitor_id)
        if v is None:
            raise HTTPException(404, f"visitor {visitor_id} not found")
        total_consume = v.get("总消费", 0)
        consume_count = v.get("消费笔数", 0)
        avg_consume = total_consume / max(consume_count, 1)
        total_duration = v.get("总游玩时长", 0)
        visit_count = v.get("游玩次数", 0)
        avg_duration = total_duration / max(visit_count, 1)
        return {
            "source": "mysql",
            "note": "游客未触发过 Kafka 任务（HBase 中无），返回 MySQL 历史聚合数据",
            "data": {
                "visitor_id": str(visitor_id),
                "total_visits": visit_count,
                "last_attraction": "",
                "last_visit_time": "",
                "recent_actions": [],
                "from_mysql": True,
                "消费总额": float(total_consume),
                "消费笔数": consume_count,
                "平均消费": round(avg_consume, 2),
                "平均游玩时长": round(avg_duration, 2),
            },
        }
    return {"source": "hbase", "data": data}


@router.get("/attraction/{attraction_id}")
def attraction_stat(attraction_id: int):
    """查询景点 HBase 实时统计；HBase 里只有触发过 Kafka 任务的景点,
    没触发过的从 MySQL 取历史聚合（不是故障兜底，是数据真实分布）."""
    data = hbase_svc.attraction_stat(attraction_id)
    if data is None:
        from services.mysql_service import query
        rows = query("SELECT 景点ID, 景点名称, 类型 FROM t_attraction WHERE 景点ID = %s", (str(attraction_id),))
        if not rows:
            raise HTTPException(404, f"attraction {attraction_id} not found")
        # MySQL 聚合
        from services.mysql_service import _query_df
        df = _query_df("SELECT COUNT(DISTINCT 游客ID) AS visitors, COUNT(*) AS visits, AVG(游玩时长) AS avg_duration FROM t_visit_record WHERE 景点ID = %s", (str(attraction_id),))
        if df.empty:
            return {"source": "mysql", "data": {"scenic_id": str(attraction_id), "name": rows[0].get("景点名称"), "type": rows[0].get("类型"), "visitor_count": 0, "visit_count": 0, "avg_duration": 0}}
        r = df.iloc[0]
        return {
            "source": "mysql",
            "note": "景点未触发过 Kafka 任务（HBase 中无），返回 MySQL 历史聚合",
            "data": {
                "scenic_id": str(attraction_id),
                "name": rows[0].get("景点名称"),
                "type": rows[0].get("类型"),
                "visitor_count": int(r["visitors"] or 0),
                "visit_count": int(r["visits"] or 0),
                "avg_duration": round(float(r["avg_duration"] or 0), 2),
            },
        }
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
    后台 consumer 接收后写入 HBase scenic_reviews.
    Kafka 不可用直接 503 (不要降级直写 HBase 绕过 Kafka).
    """
    r = kproducer.publish_review(
        visitor_id=review.visitor_id,
        attraction_id=review.attraction_id,
        rating=review.rating,
        comment=review.comment,
    )
    if not r.get("ok"):
        raise HTTPException(503, f"Kafka publish failed: {r.get('error')}")
    return {
        "status": "ok",
        "via": "kafka",
        "kafka_meta": r,
    }


@router.post("/publish/event")
def publish_event(event: EventIn):
    """发布实时事件到 Kafka scenic_events topic
    后台 consumer 接收后写入 HBase scenic_realtime.
    Kafka 不可用直接 503 (不要降级直写 HBase 绕过 Kafka).
    """
    if event.event_type not in {"enter", "exit", "consume"}:
        raise HTTPException(400, f"event_type must be one of enter/exit/consume")
    r = kproducer.publish_event(
        visitor_id=event.visitor_id,
        attraction_id=event.attraction_id,
        event_type=event.event_type,
    )
    if not r.get("ok"):
        raise HTTPException(503, f"Kafka publish failed: {r.get('error')}")
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


@router.post("/hbase/clear")
def hbase_clear():
    """清空 HBase scenic_realtime 表（用于演示重置）"""
    import services.hbase_service as hbase_svc
    deleted = hbase_svc.clear_realtime_table()
    return {"status": "ok", "deleted": deleted, "note": "HBase scenic_realtime 已清空"}


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