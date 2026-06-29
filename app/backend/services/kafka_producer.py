"""
Kafka Producer (Singleton)
=========================

用于发布实时事件到 Kafka topics：
  - scenic_reviews  (游客评论/评分)
  - scenic_events   (入场/离场/消费事件)

后端模块在 publish_review() / publish_event() 发送消息，
由 kafka_consumer.py 后台线程消费并写入 HBase。

环境变量 KAFKA_BOOTSTRAP 决定连接地址：
  - 容器内：kafka-1:9092 (PLAINTEXT, 容器互联)
  - 容器外：localhost:19095 (EXTERNAL, host 客户端)
"""
from __future__ import annotations

import json
import logging
import threading
from typing import Any, Dict, Optional

import config

log = logging.getLogger("smart-scenic.kafka-producer")

_producer: Optional[Any] = None
_lock = threading.Lock()
_enabled = False
_enable_error: Optional[str] = None


def _get_producer():
    """惰性单例：第一次调用时创建 KafkaProducer"""
    global _producer, _enabled, _enable_error
    if _producer is not None:
        return _producer
    with _lock:
        if _producer is not None:
            return _producer
        try:
            from kafka import KafkaProducer
            log.info("connecting KafkaProducer to %s ...", config.KAFKA_BOOTSTRAP)
            _producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: (k.encode("utf-8") if k else None),
                acks=1,                          # leader 写完即返回
                retries=3,
                request_timeout_ms=5000,
                max_block_ms=5000,
            )
            _enabled = True
            log.info("KafkaProducer ready, bootstrap=%s", config.KAFKA_BOOTSTRAP)
        except Exception as e:
            _enabled = False
            _enable_error = str(e)
            log.warning("KafkaProducer not available: %s", e)
    return _producer


def is_enabled() -> bool:
    """检查 producer 是否就绪"""
    _get_producer()
    return _enabled


def get_status() -> Dict[str, Any]:
    return {
        "enabled": _enabled,
        "bootstrap": config.KAFKA_BOOTSTRAP,
        "topic_review": config.KAFKA_TOPIC_REVIEW,
        "topic_events": config.KAFKA_TOPIC_EVENTS,
        "error": _enable_error,
    }


def _send(topic: str, key: str, value: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
    """发送一条消息到指定 topic，同步等待 ack"""
    prod = _get_producer()
    if prod is None:
        return {"ok": False, "error": _enable_error or "kafka not available"}
    try:
        fut = prod.send(topic, key=key, value=value)
        meta = fut.get(timeout=timeout)
        log.info("kafka send: topic=%s key=%s partition=%s offset=%s",
                 meta.topic, key, meta.partition, meta.offset)
        return {
            "ok": True,
            "topic": meta.topic,
            "partition": meta.partition,
            "offset": meta.offset,
        }
    except Exception as e:
        log.warning("kafka send failed: topic=%s key=%s err=%s", topic, key, e)
        return {"ok": False, "error": str(e)}


def publish_review(visitor_id: str, attraction_id: str, rating: int, comment: str) -> Dict[str, Any]:
    """发布评论到 scenic_reviews topic
    Consumer 会写入 HBase scenic_reviews 表
    """
    payload = {
        "type": "review",
        "visitor_id":  str(visitor_id),
        "attraction_id": str(attraction_id),
        "rating":     int(rating),
        "comment":    str(comment),
        "ts":         _now_ms(),
    }
    return _send(config.KAFKA_TOPIC_REVIEW, key=str(visitor_id), value=payload)


def publish_event(visitor_id: str, attraction_id: str, event_type: str) -> Dict[str, Any]:
    """发布实时事件（入场/离场/消费）到 scenic_events topic
    Consumer 会写入 HBase scenic_realtime 表
    event_type: 'enter' / 'exit' / 'consume'
    """
    payload = {
        "type": "event",
        "visitor_id":  str(visitor_id),
        "attraction_id": str(attraction_id),
        "event_type":  str(event_type),
        "ts":          _now_ms(),
    }
    return _send(config.KAFKA_TOPIC_EVENTS, key=str(attraction_id), value=payload)


def _now_ms() -> int:
    import time
    return int(time.time() * 1000)