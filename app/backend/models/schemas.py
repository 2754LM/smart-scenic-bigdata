"""
Pydantic schemas for request/response.
"""
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from pydantic import BaseModel, Field


# ---------- Attraction ----------
class Attraction(BaseModel):
    景点ID: int
    景点名称: str
    类型: str
    位置: str
    开放时间: str


# ---------- Visitor ----------
class Visitor(BaseModel):
    游客ID: int
    姓名: str
    性别: str
    年龄: int
    地区: str
    年龄段: Optional[str] = None


# ---------- Consumption ----------
class Consumption(BaseModel):
    消费ID: int
    时间: datetime
    游客ID: int
    景点ID: int
    消费金额: float


# ---------- Visit Record ----------
class VisitRecord(BaseModel):
    记录ID: int
    时间: datetime
    游客ID: int
    景点ID: int
    游玩时长: float


# ---------- KPI (overview) ----------
class KPI(BaseModel):
    游客总数: int
    景点总数: int
    消费总额: float
    游玩总次数: int
    平均消费: float
    平均游玩时长: float
    月新增游客: int
    月消费笔数: int


# ---------- Time-series ----------
class TimeSeriesPoint(BaseModel):
    date: str
    value: float
    label: Optional[str] = None


# ---------- Rank ----------
class RankItem(BaseModel):
    景点ID: int
    景点名称: str
    类型: str
    游客数: int
    消费总额: float
    平均游玩时长: float
    rank: int


# ---------- FPGrowth rule ----------
class AssociationRule(BaseModel):
    antecedent: List[str]
    consequent: List[str]
    confidence: float
    lift: float
    support: float


# ---------- Model comparison ----------
class ModelMetric(BaseModel):
    model: str
    accuracy: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1: Optional[float] = None
    auc: Optional[float] = None
    rmse: Optional[float] = None
    mae: Optional[float] = None
    r2: Optional[float] = None
    silhouette: Optional[float] = None


# ---------- Prediction ----------
class PredictRequest(BaseModel):
    type: str = Field(..., description="consumption_amount | daily_visitor | high_value_visitor")
    features: Dict[str, Any]


class PredictResponse(BaseModel):
    type: str
    model: str
    prediction: float
    probability: Optional[float] = None
    timestamp: str


# ---------- Cluster ----------
class ClusterStat(BaseModel):
    cluster: int
    n: int
    avg_age: float
    avg_total_consume: float
    avg_per_consume: float
    avg_visit_count: float
    avg_duration_h: float


# ---------- Generic ----------
class GenericResponse(BaseModel):
    code: int = 0
    message: str = "ok"
    data: Optional[Any] = None
