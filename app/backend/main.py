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
from routers import analysis, attractions, consumption, overview, predict, realtime, visitors

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
app.include_router(realtime.router)


@app.on_event("startup")
def on_startup() -> None:
    """Best-effort seed of HBase realtime table so front-end has data."""
    try:
        if hbase_svc.seed_if_empty():
            log.info("HBase scenic_realtime table seeded with demo rows")
    except Exception as e:
        log.warning("HBase seed skipped: %s", e)

    # === 双轨 ML 模式：尝试加载 PySpark 训练好的模型 ===
    try:
        from services import pyspark_loader
        if pyspark_loader.load_all():
            log.info("PySpark models loaded (dual-track mode active)")
        else:
            log.info("PySpark models not available, using sklearn fallback")
    except Exception as e:
        log.warning("PySpark model loading skipped: %s", e)


@app.get("/api/predict/_engine")
def predict_engine():
    """返回当前 predict 引擎状态（pyspark vs sklearn）"""
    try:
        from services import pyspark_loader
        return pyspark_loader.get_status()
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
