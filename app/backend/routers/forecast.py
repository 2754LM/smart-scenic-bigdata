"""时序预测 API + WebSocket 端点

REST 端点:
  GET  /api/forecast/state      滚动窗口状态 (前端轮询用)
  GET  /api/forecast/predict    触发一次预测
  POST /api/forecast/start      启动 Kafka consumer
  POST /api/forecast/stop       停止 Kafka consumer
  POST /api/forecast/seed       重新从 CSV seed 滚动窗口 (调试用)

WebSocket 端点:
  WS   /ws/forecast             每 30s push 一次新预测 (类似参考图)
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect

from app.backend.services.forecast_service import get_forecast_service

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/forecast", tags=["forecast"])


@router.get("/state")
def get_state() -> Dict[str, Any]:
    """获取滚动窗口 + consumer 状态"""
    return get_forecast_service().get_state()


@router.get("/predict")
def predict() -> Dict[str, Any]:
    """触发一次预测 - 返回 {history, forecast, timestamps, now_idx}"""
    return get_forecast_service().predict()


@router.post("/start")
def start_consumer() -> Dict[str, Any]:
    """启动 Kafka consumer (或模拟器 fallback)"""
    svc = get_forecast_service()
    ok = svc.start_consumer()
    return {"ok": ok, "kafka_running": svc._consumer_running, "message": "consumer started"}


@router.post("/stop")
def stop_consumer() -> Dict[str, Any]:
    """停止 Kafka consumer"""
    svc = get_forecast_service()
    svc.stop_consumer()
    return {"ok": True, "kafka_running": False, "message": "consumer stopped"}


@router.post("/seed")
def seed_history(csv_path: str = "data/raw_data/consumption.csv") -> Dict[str, Any]:
    """从历史 CSV 重新 seed 滚动窗口 (调试用)"""
    svc = get_forecast_service()
    n = svc.seed_history_from_csv(csv_path=csv_path)
    return {"ok": True, "seeded": n}


# ============== WebSocket ==============
@router.websocket("/ws")
async def forecast_ws(websocket: WebSocket):
    """WebSocket 端点 - 每 30s push 一次新预测 (类似参考图效果)

    消息格式:
    {
      "ts": "2025-06-30T10:00:00",   # 推送时间
      "state": {...滚动窗口状态...},
      "predict": {...预测结果...}
    }
    """
    await websocket.accept()
    log.info("[WS] forecast client connected")
    try:
        # 首次 push
        svc = get_forecast_service()
        await websocket.send_text(json.dumps({
            "ts": svc._current_window_start.isoformat() if svc._current_window_start else None,
            "state": svc.get_state(),
            "predict": svc.predict(),
        }, ensure_ascii=False))

        # 循环 push (30s/次, 跟 WINDOW_MINUTES 一致)
        while True:
            await asyncio.sleep(30)
            payload = {
                "ts": svc._current_window_start.isoformat() if svc._current_window_start else None,
                "state": svc.get_state(),
                "predict": svc.predict(),
            }
            await websocket.send_text(json.dumps(payload, ensure_ascii=False))
    except WebSocketDisconnect:
        log.info("[WS] forecast client disconnected")
    except Exception as e:
        log.warning(f"[WS] forecast error: {e}")
        try:
            await websocket.close()
        except Exception:
            pass
