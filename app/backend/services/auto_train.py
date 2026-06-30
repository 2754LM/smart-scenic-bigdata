"""
Smart Auto-Train: 启动时自动触发 PySpark 训练

如果 /shared/models/ 没有模型，异步调 spark-submit 在 spark-master 容器里训练。
用户不用手动跑 spark-submit！

策略：
  1. 检测 /shared/models/ 是否已有模型
  2. 有 → 加载（pyspark_loader.load_all）
  3. 没有 → 起后台线程异步训练（不阻塞 startup）
     - 训练完成后再加载
     - 失败 → fallback sklearn
"""
from __future__ import annotations

import logging
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional

import config

log = logging.getLogger("smart-scenic.auto-train")


def _has_models() -> bool:
    """检查 /shared/models/ 是否有已训练好的模型"""
    d = Path(config.PYSPARK_MODELS_DIR)
    if not d.exists():
        return False
    return any(p.is_dir() and (p / "metadata").exists() for p in d.iterdir())


def _train_blocking(timeout: int = 600) -> bool:
    """阻塞式触发训练：通过 Docker socket API 在 spark-master 容器内跑
    spark-submit ml-train. 等待完成（或超时），返回是否成功。
    """
    log.info("auto-train: triggering spark-submit in spark-master container...")
    try:
        from services.docker_client import exec_capture
        result = exec_capture(
            config.SPARK_CONTAINER,
            ["bash", "/opt/jobs/spark-submit.sh", "ml-train"],
            timeout=timeout,
        )
        if result.get("exit_code") == 0:
            log.info("auto-train: training completed")
            return True
        else:
            log.error("auto-train: training failed (rc=%s)\nstdout=%s\nstderr=%s",
                      result.get("exit_code"),
                      (result.get("stdout") or "")[-500:],
                      (result.get("stderr") or "")[-500:])
            return False
    except subprocess.TimeoutExpired:
        log.error("auto-train: training timeout after %s seconds", timeout)
        return False
    except FileNotFoundError:
        log.error("auto-train: docker not found, skip")
        return False
    except Exception as e:
        log.error("auto-train: %s", e)
        return False


def _train_async_then_load() -> None:
    """后台线程：等几秒让服务先起，再训练 + 加载"""
    # 给服务几秒钟先起来
    time.sleep(3)

    if _has_models():
        log.info("auto-train: models already exist, skip training")
        from services import pyspark_loader
        pyspark_loader.load_all()
        return

    log.info("auto-train: no models found, start training in background...")
    ok = _train_blocking(timeout=600)
    if ok:
        from services import pyspark_loader
        pyspark_loader.load_all()
    else:
        log.warning("auto-train: training failed, will use sklearn fallback")


def auto_train_if_needed() -> str:
    """返回：'has_models' / 'training_async' / 'disabled'"""
    if not config.USE_PYSPARK_MODELS:
        return "disabled"

    if _has_models():
        return "has_models"

    # 没模型 → 后台异步训练（不阻塞 startup）
    t = threading.Thread(target=_train_async_then_load, name="auto-train", daemon=True)
    t.start()
    return "training_async"