"""
Prediction APIs.
- POST /api/predict             predict by type
- GET  /api/predict/regression  get regression comparison
- GET  /api/predict/classification  get classification comparison
- GET  /api/predict/clustering  get clustering stats
- GET  /api/predict/compare     full comparison report
"""
from fastapi import APIRouter, HTTPException
from datetime import datetime
from typing import Dict, Any

from services.model_service import get_model
from models.schemas import PredictRequest, PredictResponse

router = APIRouter(prefix="/api/predict", tags=["机器学习预测"])


@router.post("")
def predict(req: PredictRequest):
    model = get_model()
    result = model.predict(req.type, req.features)
    if "error" in result:
        raise HTTPException(400, result["error"])
    result["timestamp"] = datetime.now().isoformat()
    return {"code": 0, "data": result}


@router.get("/regression")
def regression():
    model = get_model()
    rep = model.get_report("regression")
    if not rep:
        return {"code": 0, "data": None, "message": "Regression report not available"}
    return {"code": 0, "data": rep}


@router.get("/classification")
def classification():
    model = get_model()
    rep = model.get_report("classification")
    if not rep:
        return {"code": 0, "data": None, "message": "Classification report not available"}
    return {"code": 0, "data": rep}


@router.get("/clustering")
def clustering():
    model = get_model()
    rep = model.get_report("clustering")
    if not rep:
        return {"code": 0, "data": None, "message": "Clustering report not available"}
    return {"code": 0, "data": rep}


@router.get("/compare")
def compare():
    model = get_model()
    return {"code": 0, "data": model.get_all_reports()}
