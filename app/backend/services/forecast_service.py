"""智能景区大数据平台 - 时序预测服务（VM 端加载 .pt 模型）

职责:
  1. 加载本地 .pt 模型 (torch.load)
  2. 维护滚动窗口 (deque, 最近 24 个 30min step)
  3. Kafka consumer (后台线程) - 消费 scenic_events, 累加到当前窗口
  4. 预测 - 用最近 24 步做特征, 预测未来 8 步
  5. 提供给 router: history / forecast / status

不依赖: docker exec / happybase (跟 predict_service 一样, 走 .pt 文件预测)
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# 默认参数 (跟 train-forecast.py 一致)
HISTORY_STEPS = 24
FORECAST_STEPS = 8
WINDOW_MINUTES = 30


# 延迟 import torch
def _import_torch():
    try:
        import torch
        import torch.nn as nn
    except ImportError:
        log.error("需要 PyTorch:  pip install torch --index-url https://download.pytorch.org/whl/cpu")
        raise
    return torch, nn


# ============== 模型定义 (跟 train-forecast.py 保持一致) ==============
class ForecastMLP(nn.Module):
    def __init__(self, in_dim=HISTORY_STEPS * 2, out_dim=FORECAST_STEPS,
                 hidden=64, layers=2, dropout=0.2):
        super().__init__()
        blocks = []
        prev = in_dim
        for i in range(layers):
            blocks.append(nn.Linear(prev, hidden))
            blocks.append(nn.ReLU())
            blocks.append(nn.Dropout(dropout))
            prev = hidden
        blocks.append(nn.Linear(prev, out_dim))
        self.net = nn.Sequential(*blocks)

    def forward(self, x):
        return self.net(x)


# ============== 主服务类 ==============
class ForecastService:
    """时序预测服务 (单例)"""

    def __init__(self, model_path: str = "data/models/forecast.pt",
                 meta_path: str = "data/models/forecast_meta.json",
                 device: str = "cpu"):
        self.model_path = Path(model_path)
        self.meta_path = Path(meta_path)
        self.device_str = device
        self._lock = threading.Lock()
        self._model = None
        self._meta: Dict[str, Any] = {}
        self._torch = None
        self._nn = None
        self._loaded = False

        # 滚动窗口: deque of (window_start_ts, visitor_count, total_consume)
        self._window: deque = deque(maxlen=HISTORY_STEPS)
        # 当前正在累积的 30min 窗口
        self._current_window_start: Optional[datetime] = None
        self._current_visitors: set = set()
        self._current_consume: float = 0.0

        # Kafka consumer 状态
        self._consumer_thread: Optional[threading.Thread] = None
        self._consumer_running = False
        self._kafka_consumer = None

    # ---------- 加载模型 ----------
    def try_load(self) -> bool:
        """尝试加载模型，失败不抛异常（让 P2 端点仍能工作）"""
        try:
            if not self.model_path.exists() or not self.meta_path.exists():
                log.warning(f"模型文件不存在: {self.model_path} / {self.meta_path}")
                return False
            torch, nn = _import_torch()
            self._torch, self._nn = torch, nn
            with open(self.meta_path, "r", encoding="utf-8") as f:
                self._meta = json.load(f)
            m = self._meta
            self._model = ForecastMLP(
                in_dim=m["in_dim"], out_dim=m["out_dim"],
                hidden=m["hidden"], layers=m["layers"], dropout=m["dropout"],
            )
            self._model.load_state_dict(torch.load(self.model_path, map_location=self.device_str))
            self._model.eval()
            self._loaded = True
            log.info(f"✓ forecast 模型已加载: {self.model_path} (R²={m.get('metrics', {}).get('r2', 'N/A')})")
            return True
        except Exception as e:
            log.warning(f"forecast 模型加载失败: {e}")
            self._loaded = False
            return False

    @property
    def loaded(self) -> bool:
        return self._loaded

    # ---------- 滚动窗口管理 ----------
    def _round_down(self, ts: datetime) -> datetime:
        """把时间戳向下取整到 30min 边界"""
        minute = (ts.minute // WINDOW_MINUTES) * WINDOW_MINUTES
        return ts.replace(minute=minute, second=0, microsecond=0)

    def _get_current_window_start(self, now: Optional[datetime] = None) -> datetime:
        now = now or datetime.now()
        if self._current_window_start is None:
            self._current_window_start = self._round_down(now)
        return self._current_window_start

    def _maybe_rotate_window(self, now: Optional[datetime] = None) -> bool:
        """如果跨过 30min 边界, 把当前窗口 roll 到 _window"""
        now = now or datetime.now()
        cur_start = self._get_current_window_start(now)
        if now < cur_start + timedelta(minutes=WINDOW_MINUTES):
            return False
        # 关闭当前窗口
        with self._lock:
            self._window.append({
                "ts": cur_start.isoformat(),
                "visitor_count": len(self._current_visitors),
                "total_consume": round(self._current_consume, 2),
            })
            self._current_visitors = set()
            self._current_consume = 0.0
            self._current_window_start = cur_start + timedelta(minutes=WINDOW_MINUTES)
        return True

    def feed_event(self, visitor_id: str, attraction_id: str, amount: float = 0.0):
        """喂入一条 Kafka 事件 (供后端 kafka_consumer 调用)"""
        now = datetime.now()
        self._maybe_rotate_window(now)
        with self._lock:
            self._current_visitors.add(visitor_id)
            self._current_consume += amount

    def seed_history_from_csv(self, csv_path: str = "data/raw_data/consumption.csv",
                              n_steps: int = HISTORY_STEPS) -> int:
        """从历史 CSV 预填滚动窗口 (供演示 / 冷启动)"""
        path = Path(csv_path)
        if not path.exists():
            log.warning(f"seed CSV 不存在: {path}")
            return 0
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")
            df["时间"] = pd.to_datetime(df["时间"])
            df["window_start"] = df["时间"].dt.floor(f"{WINDOW_MINUTES}min")
            agg = df.groupby("window_start").agg(
                visitor_count=("游客ID", "nunique"),
                total_consume=("消费金额", "sum"),
            ).reset_index()
            # 取最近 n_steps 个窗口
            agg = agg.tail(n_steps)
            with self._lock:
                self._window.clear()
                for _, row in agg.iterrows():
                    self._window.append({
                        "ts": row["window_start"].isoformat(),
                        "visitor_count": int(row["visitor_count"]),
                        "total_consume": round(float(row["total_consume"]), 2),
                    })
            log.info(f"✓ seed 滚动窗口: {len(self._window)} 个 30min 窗口 (来源 {path})")
            return len(self._window)
        except Exception as e:
            log.warning(f"seed 失败: {e}")
            return 0

    def get_state(self) -> Dict[str, Any]:
        """获取当前状态 (供前端轮询)"""
        with self._lock:
            window_snapshot = list(self._window)
            current = {
                "ts": self._current_window_start.isoformat() if self._current_window_start else None,
                "visitor_count": len(self._current_visitors),
                "total_consume": round(self._current_consume, 2),
            }
        return {
            "loaded": self._loaded,
            "model": str(self.model_path) if self._loaded else None,
            "metrics": self._meta.get("metrics", {}) if self._loaded else {},
            "history": window_snapshot,
            "current": current,
            "history_len": len(window_snapshot),
            "history_required": HISTORY_STEPS,
            "forecast_steps": FORECAST_STEPS,
            "window_minutes": WINDOW_MINUTES,
            "kafka_running": self._consumer_running,
        }

    # ---------- 预测 ----------
    def predict(self) -> Dict[str, Any]:
        """执行一次预测 - 用当前 _window 做特征, 预测未来 8 步"""
        if not self._loaded:
            return {"ok": False, "error": "模型未加载", "forecast": [], "timestamps": []}
        with self._lock:
            if len(self._window) < HISTORY_STEPS:
                return {
                    "ok": False,
                    "error": f"历史窗口不足 ({len(self._window)}/{HISTORY_STEPS})",
                    "forecast": [],
                    "timestamps": [],
                }
            window_snapshot = list(self._window)

        # 构造特征 (24 * 2 = 48 维)
        visitors = np.array([w["visitor_count"] for w in window_snapshot], dtype=np.float32)
        consumes = np.array([w["total_consume"] for w in window_snapshot], dtype=np.float32)
        x_raw = np.concatenate([visitors, consumes])

        # 标准化
        scaler = self._meta.get("scaler", {})
        x_mean = np.array(scaler.get("x_mean", [0.0] * (HISTORY_STEPS * 2)), dtype=np.float32)
        x_std = np.array(scaler.get("x_std", [1.0] * (HISTORY_STEPS * 2)), dtype=np.float32)
        y_mean = np.array(scaler.get("y_mean", [0.0] * FORECAST_STEPS), dtype=np.float32)
        y_std = np.array(scaler.get("y_std", [1.0] * FORECAST_STEPS), dtype=np.float32)
        x_n = (x_raw - x_mean) / x_std

        # 推理
        self._torch = self._torch or _import_torch()[0]
        with self._torch.no_grad():
            pred_n = self._model(self._torch.from_numpy(x_n).float().unsqueeze(0)).numpy()[0]
        pred = pred_n * y_std + y_mean  # 反标准化
        pred = np.maximum(pred, 0).tolist()  # 客流量不能为负

        # 构造时间戳
        last_ts = datetime.fromisoformat(window_snapshot[-1]["ts"])
        timestamps = [
            (last_ts + timedelta(minutes=WINDOW_MINUTES * (i + 1))).isoformat()
            for i in range(FORECAST_STEPS)
        ]

        return {
            "ok": True,
            "forecast": [round(p, 2) for p in pred],
            "timestamps": timestamps,
            "history": window_snapshot,
            "last_history_ts": window_snapshot[-1]["ts"],
            "current": {
                "ts": self._current_window_start.isoformat() if self._current_window_start else None,
                "visitor_count": len(self._current_visitors),
                "total_consume": round(self._current_consume, 2),
            },
        }

    # ---------- Kafka consumer 启停 ----------
    def start_consumer(self, bootstrap_servers: str = "localhost:19095",
                        topic: str = "scenic_events",
                        group_id: str = "forecast-service") -> bool:
        """启动 Kafka consumer 后台线程"""
        if self._consumer_running:
            return True
        try:
            from kafka import KafkaConsumer
        except ImportError:
            log.warning("kafka-python 未装, 启动模拟器替代")
            return self._start_simulator()

        def _run():
            log.info(f"[Kafka] connecting to {bootstrap_servers} topic={topic}")
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=bootstrap_servers,
                    group_id=group_id,
                    auto_offset_reset="latest",
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    consumer_timeout_ms=2000,
                )
                self._kafka_consumer = consumer
                self._consumer_running = True
                log.info(f"[Kafka] ✓ consumer started, group={group_id}")
                for msg in consumer:
                    if not self._consumer_running:
                        break
                    e = msg.value or {}
                    vid = str(e.get("visitor_id", ""))
                    aid = str(e.get("attraction_id", ""))
                    amount = float(e.get("amount", 0.0))
                    self.feed_event(vid, aid, amount)
            except Exception as ex:
                log.warning(f"[Kafka] consumer 出错: {ex}, 启动模拟器")
                self._consumer_running = False
                self._start_simulator()

        self._consumer_thread = threading.Thread(target=_run, daemon=True, name="forecast-kafka")
        self._consumer_thread.start()
        time.sleep(0.5)  # 等 consumer 连接
        return self._consumer_running

    def _start_simulator(self) -> bool:
        """Kafka 不可用时, 用模拟器 (每 10-30 秒产生 1 个事件)"""
        if self._consumer_running:
            return True
        def _run():
            import random
            self._consumer_running = True
            log.info("[Simulator] ✓ started (Kafka 不可用, 改用模拟事件)")
            while self._consumer_running:
                time.sleep(random.uniform(8, 25))
                if not self._consumer_running:
                    break
                self.feed_event(
                    f"V{random.randint(1, 200):04d}",
                    f"A{random.randint(1, 10):03d}",
                    round(random.uniform(50, 500), 2),
                )
        self._consumer_thread = threading.Thread(target=_run, daemon=True, name="forecast-sim")
        self._consumer_thread.start()
        return True

    def stop_consumer(self):
        self._consumer_running = False
        if self._kafka_consumer:
            try:
                self._kafka_consumer.close()
            except Exception:
                pass
            self._kafka_consumer = None
        log.info("[Kafka/Simulator] consumer stopped")


# ============== 单例 ==============
_singleton: Optional[ForecastService] = None


def get_forecast_service() -> ForecastService:
    global _singleton
    if _singleton is None:
        _singleton = ForecastService()
        _singleton.try_load()
        # 冷启动 seed
        _singleton.seed_history_from_csv()
    return _singleton
