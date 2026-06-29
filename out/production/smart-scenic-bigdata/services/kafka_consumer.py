"""
Kafka Consumer (Background Thread)
==================================

订阅 Kafka topics，把消息落 HBase：
  - scenic_reviews  → HBase scenic_reviews
  - scenic_events   → HBase scenic_realtime

启动方式：main.py on_startup() 调 start()
停止方式：main.py on_shutdown() 调 stop()
"""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Dict, Optional

import config

log = logging.getLogger("smart-scenic.kafka-consumer")

_consumer: Optional[Any] = None
_thread: Optional[threading.Thread] = None
_stop = threading.Event()
_running = False
_stats = {
    "started_at": None,
    "messages_consumed": 0,
    "messages_failed": 0,
    "last_msg_at": None,
    "last_error": None,
    "topics": [],
}


def _handle_message(topic: str, value: Dict[str, Any]) -> None:
    """处理一条 Kafka 消息，写入 HBase"""
    if topic == config.KAFKA_TOPIC_REVIEW:
        _write_review(value)
    elif topic == config.KAFKA_TOPIC_EVENTS:
        _write_event(value)
    else:
        log.warning("unknown topic: %s", topic)


def _write_review(v: Dict[str, Any]) -> None:
    """写评论到 HBase scenic_reviews"""
    import services.hbase_service as hbase_svc
    hbase_svc.put_review(
        visitor_id=str(v.get("visitor_id", "")),
        attraction_id=str(v.get("attraction_id", "")),
        rating=int(v.get("rating", 0)),
        comment=str(v.get("comment", "")),
        ts=int(v.get("ts", 0)),
    )


def _write_event(v: Dict[str, Any]) -> None:
    """写实时事件到 HBase scenic_realtime"""
    import services.hbase_service as hbase_svc
    hbase_svc.put_realtime_event(
        visitor_id=str(v.get("visitor_id", "")),
        attraction_id=str(v.get("attraction_id", "")),
        event_type=str(v.get("event_type", "")),
        ts=int(v.get("ts", 0)),
    )


def _consumer_loop() -> None:
    """后台线程主循环"""
    global _consumer, _running

    try:
        from kafka import KafkaConsumer
    except ImportError as e:
        log.error("kafka-python not installed: %s", e)
        _stats["last_error"] = "kafka-python not installed"
        return

    topics = [config.KAFKA_TOPIC_REVIEW, config.KAFKA_TOPIC_EVENTS]
    _stats["topics"] = topics

    while not _stop.is_set():
        try:
            log.info("kafka consumer connecting to %s, topics=%s", config.KAFKA_BOOTSTRAP, topics)
            _consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=config.KAFKA_BOOTSTRAP,
                group_id="smart-scenic-backend",
                auto_offset_reset="latest",    # 只看新消息，不重放过往
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,       # 1 秒没消息抛 StopIteration，循环检查 _stop
            )
            log.info("kafka consumer ready, group=smart-scenic-backend")

            _running = True
            _stats["started_at"] = int(time.time())

            for msg in _consumer:
                if _stop.is_set():
                    break
                try:
                    value = msg.value
                    if not isinstance(value, dict):
                        value = json.loads(value) if isinstance(value, (str, bytes)) else {}
                    _handle_message(msg.topic, value)
                    _stats["messages_consumed"] += 1
                    _stats["last_msg_at"] = int(time.time() * 1000)
                except Exception as e:
                    _stats["messages_failed"] += 1
                    _stats["last_error"] = str(e)
                    log.warning("consume msg failed: topic=%s err=%s", msg.topic, e)

            _consumer.close()
            _consumer = None

        except Exception as e:
            _stats["last_error"] = str(e)
            log.warning("kafka consumer error (retry in 5s): %s", e)
            if _stop.wait(5):
                break
    _running = False
    log.info("kafka consumer stopped")


def start() -> bool:
    """启动后台消费线程（应用启动时调用）"""
    global _thread
    if _thread is not None and _thread.is_alive():
        log.info("kafka consumer already running")
        return True
    _stop.clear()
    _thread = threading.Thread(target=_consumer_loop, name="kafka-consumer", daemon=True)
    _thread.start()
    return True


def stop(timeout: float = 5.0) -> None:
    """停止后台消费线程（应用关停时调用）"""
    _stop.set()
    if _thread is not None:
        _thread.join(timeout=timeout)
        _thread = None


def is_running() -> bool:
    return _running


def get_status() -> Dict[str, Any]:
    return {
        "running": _running,
        "thread_alive": _thread is not None and _thread.is_alive(),
        "bootstrap": config.KAFKA_BOOTSTRAP,
        "stats": _stats,
    }