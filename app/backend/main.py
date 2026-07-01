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
from fastapi.staticfiles import StaticFiles

import config
import services.hbase_service as hbase_svc
from routers import admin, analysis, attractions, consumption, overview, predict, predict_tourism, visitors

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

    # === ML 模型加载 ===
    # 当前默认走 sklearn joblib (毫秒级, 无 PySpark 依赖).
    # pyspark 双轨模式可通过 USE_PYSPARK_MODELS=true 开启,
    # 但需要在 demo-backend 镜像里装 PySpark + JDK (见 docker/demo-backend/Dockerfile).
    try:
        from services import auto_train
        status = auto_train.auto_train_if_needed()
        if status == "training_async":
            log.info("Models not trained yet. Auto-training in background (~5-10 min)...")
            log.info("Backend will use sklearn joblib (in /shared/models/sklearn/*.pkl) once done.")
    except Exception as e:
        log.warning("ML auto-train init failed: %s", e)


@app.on_event("shutdown")
def on_shutdown() -> None:
    """FastAPI shutdown hook. No background threads to stop."""


@app.get("/api/predict/_engine")
def predict_engine():
    """返回当前 predict 引擎状态. 当前固定 sklearn joblib."""
    from services import auto_train, model_service
    return {
        "engine": "sklearn",
        "models_exist": auto_train._has_models(),
        "models_dir": config.PYSPARK_MODELS_DIR,
        "models_loaded": sorted(model_service._models.keys()),
    }


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


# Serve frontend HTML pages. MUST be registered LAST (after all API routers
# and explicit routes above) so it acts as a catch-all and never shadows
# /api/* requests. html=True serves index.html at "/" (already handled by
# the root() route above) and *.html files by name.
app.mount("/", StaticFiles(directory="/app/frontend", html=True), name="frontend")


if __name__ == "__main__":
    uvicorn.run(app, host=config.APP_HOST, port=config.APP_PORT)
