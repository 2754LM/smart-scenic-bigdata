"""
Smart Scenic BigData Platform - P2 Backend (modular).

App layout:
  routers/    - URL path -> service call
  services/   - business / data access / ML model logic
  config.py   - env / connection settings
  schemas.py  - Pydantic models (shared)
  utils.py    - shared helpers (docker exec, CSV cache, ...)
"""
import logging
import time

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

import config
import services.hbase_service as hbase_svc
from routers import admin, analysis, attractions, consumption, overview, predict, predict_tourism, realtime, visitors

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
log = logging.getLogger("smart-scenic.backend")

# ----------------------------------------------------------------------
# App
# ----------------------------------------------------------------------
app = FastAPI(
    title=config.APP_TITLE,
    version=config.APP_VERSION,
    description=(
        "P2 backend: modular FastAPI exposing overview / attractions / visitors / "
        "consumption / analysis / predict / realtime endpoints over MySQL + "
        "HDFS/Hive + HBase + sklearn."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(overview.router)
app.include_router(attractions.router)
app.include_router(visitors.router)
app.include_router(consumption.router)
app.include_router(analysis.router)
app.include_router(predict.router)
app.include_router(predict_tourism.router)
app.include_router(realtime.router)
app.include_router(admin.router)


@app.on_event("startup")
def on_startup() -> None:
    """Best-effort init of HBase tables + seed so front-end has data."""
    # 1. 幂等建表 (scenic_realtime, scenic_reviews)
    try:
        result = hbase_svc.init_tables()
        log.info("HBase init tables: %s", result)
    except Exception as e:
        log.warning("HBase init tables skipped: %s", e)
    # 2. 若 scenic_realtime 为空，注入 demo 行
    try:
        if hbase_svc.seed_if_empty():
            log.info("HBase scenic_realtime table seeded with demo rows")
    except Exception as e:
        log.warning("HBase seed skipped: %s", e)

    # === 双轨 ML 模式：智能加载（如果没模型，自动触发训练） ===
    try:
        from services import pyspark_loader, auto_train
        status = auto_train.auto_train_if_needed()
        if status == "has_models":
            # 已有模型，直接加载
            if pyspark_loader.load_all():
                log.info("PySpark models loaded (dual-track mode active)")
            else:
                log.warning("PySpark models exist but failed to load, using sklearn fallback")
        elif status == "training_async":
            log.info("No PySpark models found. Auto-training in background (~5-10 min)...")
            log.info("Backend will use sklearn fallback until training finishes.")
            log.info("Check /api/predict/_engine for status.")
        else:
            log.info("PySpark dual-track mode disabled, using sklearn only")
    except Exception as e:
        log.warning("PySpark auto-train init failed: %s", e)

    # === Kafka 后台 consumer：消费 → 写 HBase ===
    try:
        from services import kafka_consumer
        kafka_consumer.start()
        log.info("Kafka consumer thread started")
    except Exception as e:
        log.warning("Kafka consumer start failed: %s", e)


@app.on_event("shutdown")
def on_shutdown() -> None:
    """关停 Kafka consumer 线程"""
    try:
        from services import kafka_consumer
        kafka_consumer.stop()
        log.info("Kafka consumer thread stopped")
    except Exception as e:
        log.warning("Kafka consumer stop failed: %s", e)


@app.get("/api/predict/_engine")
def predict_engine():
    """返回当前 predict 引擎状态（pyspark vs sklearn）"""
    try:
        from services import pyspark_loader, auto_train
        return {
            "pyspark_loader": pyspark_loader.get_status(),
            "auto_train":     {
                "models_exist": auto_train._has_models(),
                "use_pyspark":  config.USE_PYSPARK_MODELS,
            },
        }
    except ImportError:
        return {"pyspark_loaded": False, "error": "pyspark_loader not available"}


@app.get("/")
def root() -> HTMLResponse:
    return HTMLResponse(
        f"<h1>{config.APP_TITLE}</h1>"
        f"<p>Version {config.APP_VERSION}</p>"
        "<ul>"
        "<li><a href='/docs'>/docs</a> - Swagger UI</li>"
        "<li><a href='/redoc'>/redoc</a> - ReDoc</li>"
        "<li><a href='http://localhost:8080'>frontend</a> - 4-page dashboard</li>"
        "</ul>"
    )


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "ts": time.time(),
        "service": config.APP_TITLE,
        "version": config.APP_VERSION,
    }


if __name__ == "__main__":
    uvicorn.run(app, host=config.APP_HOST, port=config.APP_PORT)
