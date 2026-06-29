"""
FastAPI application entry.
- Mounts all API routers
- CORS enabled
- Auto docs at /docs
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from config import get_settings
from api.overview import router as overview_router
from api.attractions import router as attractions_router
from api.visitors import router as visitors_router
from api.consumption import router as consumption_router
from api.analysis import router as analysis_router
from api.predict import router as predict_router
from api.realtime import router as realtime_router


def create_app() -> FastAPI:
    s = get_settings()
    app = FastAPI(
        title=s.APP_NAME,
        version=s.APP_VERSION,
        description="Smart Scenic BigData Platform - Backend API (P2)",
        debug=s.DEBUG,
    )
    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=s.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routers
    app.include_router(overview_router)
    app.include_router(attractions_router)
    app.include_router(visitors_router)
    app.include_router(consumption_router)
    app.include_router(analysis_router)
    app.include_router(predict_router)
    app.include_router(realtime_router)

    @app.get("/")
    def root():
        return {
            "code": 0,
            "data": {
                "app": s.APP_NAME,
                "version": s.APP_VERSION,
                "docs": "/docs",
                "modules": [
                    "总览 /api/overview",
                    "景点 /api/attractions",
                    "游客 /api/visitors",
                    "消费 /api/consumption",
                    "分析 /api/analysis",
                    "预测 /api/predict",
                    "实时 /api/realtime",
                ],
            }
        }

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    s = get_settings()
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=s.DEBUG)
