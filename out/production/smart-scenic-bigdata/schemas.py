"""
Pydantic schemas shared by routers.
Field names use the **Chinese** keys the frontend expects (景点ID, 游客ID, etc.)
so the API can be consumed directly without remapping.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ============================================================
# Generic envelope
# ============================================================
class APIResponse(BaseModel):
    source: Optional[str] = None
    count: Optional[int] = None
    data: Any = None
    message: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


# ============================================================
# Attractions
# ============================================================
class Attraction(BaseModel):
    景点ID: int
    景点名称: str
    类型: str
    位置: str
    开放时间: Optional[str] = None


class AttractionList(BaseModel):
    count: int
    items: List[Attraction]


# ============================================================
# Visitors
# ============================================================
class Visitor(BaseModel):
    游客ID: int
    姓名: str
    性别: str
    年龄: int
    地区: str


class VisitorAggregate(BaseModel):
    游客ID: int
    总消费: float
    消费笔数: int
    游玩次数: int
    总游玩时长: float
    平均满意度: float


# ============================================================
# Consumption / Visit
# ============================================================
class Consumption(BaseModel):
    消费ID: int
    时间: str
    游客ID: int
    景点ID: int
    消费金额: float


class Visit(BaseModel):
    记录ID: int
    时间: str
    游客ID: int
    景点ID: int
    游玩时长: float


class PaginatedConsumption(BaseModel):
    total: int
    page: int
    page_size: int
    items: List[Consumption]


class PaginatedVisit(BaseModel):
    total: int
    page: int
    page_size: int
    items: List[Visit]


# ============================================================
# Analysis
# ============================================================
class DailyStat(BaseModel):
    date: str
    visitors: int
    amount: float


class HourlyStat(BaseModel):
    hour: int
    visitors: int


class RegionStat(BaseModel):
    地区: str
    visitors: int


class AgeGenderStat(BaseModel):
    年龄段: str
    性别: str
    n: int


class TypeSummary(BaseModel):
    类型: str
    景点数: int
    游客数: int
    消费总额: float
    平均时长: float


class FpGrowthItem(BaseModel):
    antecedent: List[Dict[str, Any]]  # [{景点ID, 景点名称}]
    consequent: List[Dict[str, Any]]
    confidence: float
    lift: float
    support: float


# ============================================================
# Overview
# ============================================================
class KPIResponse(BaseModel):
    游客总数: int
    景点总数: int
    消费总额: float
    游玩次数: int
    平均消费: float
    平均游玩时长: float
    消费笔数: int
    日均游客: float


class TimeSeriesPoint(BaseModel):
    date: str
    value: float


class AttractionRank(BaseModel):
    景点ID: int
    景点名称: str
    游客数: int


# ============================================================
# Prediction
# ============================================================
class PredictRequest(BaseModel):
    type: str  # consumption_amount | daily_visitor | high_value_visitor
    features: Dict[str, Any] = Field(default_factory=dict)


class PredictResponse(BaseModel):
    type: str
    prediction: Any
    model: str
    probability: Optional[float] = None
    label: Optional[str] = None
    timestamp: str = ""


class RegressionReport(BaseModel):
    task: str
    model: str
    rmse: float
    mae: float
    r2: float


class ClassificationReport(BaseModel):
    model: str
    accuracy: float
    precision: float
    recall: float
    f1: float
    auc: float


class ClusterStat(BaseModel):
    cluster: int
    n: int
    avg_age: float
    avg_total_consume: float
    avg_per_consume: float
    avg_visit_count: float
    avg_duration_h: float


# ============================================================
# Realtime (HBase)
# ============================================================
class VisitRecentItem(BaseModel):
    row_key: str
    visitor_id: str
    scenic_id: str
    action: str
    ts: str


class VisitorProfile(BaseModel):
    visitor_id: str
    total_visits: int
    last_attraction: Optional[str] = None
    last_visit_time: Optional[str] = None
    recent_actions: List[Dict[str, Any]] = []


class AttractionStat(BaseModel):
    scenic_id: str
    visitor_count: int
    last_24h_visits: int
    last_visit_time: Optional[str] = None


# ============================================================
# Health
# ============================================================
class HealthStatus(BaseModel):
    status: str
    ts: float
    components: Dict[str, bool] = Field(default_factory=dict)
    uptime_s: float = 0.0
    hive: bool = False
    hbase: bool = False
