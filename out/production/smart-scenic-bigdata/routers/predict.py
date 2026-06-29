"""
/api/predict/* - 模型预测（回归/分类/聚类/对比）。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Dict

import services.model_service as model_svc

router = APIRouter(prefix="/api/predict", tags=["predict"])


class PredictRequest(BaseModel):
    type: str
    features: Dict[str, Any] = {}


@router.post("")
def do_predict(req: PredictRequest):
    try:
        data = model_svc.predict(req.type, req.features)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"source": "sklearn", "data": data}


@router.get("/regression")
def regression():
    data = model_svc.regression_report()
    return {"source": "sklearn", "data": {"results": data}}


@router.get("/classification")
def classification():
    data = model_svc.classification_report()
    return {"source": "sklearn", "data": {"results": data}}


@router.get("/clustering")
def clustering():
    data = model_svc.clustering_report()
    return {"source": "sklearn", "data": {"cluster_stats": data}}


@router.get("/compare")
def compare():
    return {"source": "sklearn", "data": model_svc.compare_models()}
