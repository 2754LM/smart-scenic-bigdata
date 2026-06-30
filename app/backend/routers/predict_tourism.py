"""
/api/predict-tourism/* - 场景化预测：与景区业务深度结合的 ML 应用
==================================================
- /attraction-forecast   景点明日客流量预测
- /attraction-recommend  景点智能推荐（基于历史游玩序列）
- /route-recommend       游玩路线推荐（基于关联规则 + 类型偏好）
- /visitor-profile       游客画像（消费 + 偏好 + 群体归类）
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

import services.mysql_service as mysql_svc
import services.model_service as model_svc

router = APIRouter(prefix="/api/predict-tourism", tags=["predict-tourism"])


# ----------------------------------------------------------------------
# 1. 景点明日客流量预测
# ----------------------------------------------------------------------
@router.get("/attraction-forecast")
def attraction_forecast():
    """
    预测每个景点的"明日客流量"。
    方法：
      1. 取每个景点的最近 30 天日均游客
      2. 用昨日实际值 × (近 30 天日均 / 整体日均) 作为基础预测
      3. 加入星期因子（周末 1.2x，工作日 0.85x）
      4. 加上 ±10% 随机扰动模拟模型不确定性
    同时返回"昨日实际"+"前 7 天趋势"用作对比
    """
    # 1. 景点列表
    atts = mysql_svc.query("SELECT 景点ID, 景点名称, 类型 FROM t_attraction ORDER BY 景点ID")
    if not atts:
        return {"error": "no attractions"}

    # 2. 每日每景点游客数（最近 30 天）
    today = datetime(2023, 12, 31)  # 数据最新一天
    start = today - timedelta(days=30)
    daily_df = mysql_svc._query_df(
        "SELECT DATE(时间) AS date, 景点ID, COUNT(*) AS visitors "
        "FROM t_visit_record "
        "WHERE 时间 >= %s AND 时间 <= %s "
        "GROUP BY DATE(时间), 景点ID",
        (start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d 23:59:59")),
    )
    if daily_df.empty:
        return {"forecasts": []}

    daily_df["date"] = pd.to_datetime(daily_df["date"])
    daily_df["dow"] = daily_df["date"].dt.dayofweek
    daily_df["is_weekend"] = daily_df["dow"].isin([5, 6]).astype(int)

    # 3. 计算每个景点的统计量
    result = []
    for a in atts:
        aid = str(a["景点ID"])
        sub = daily_df[daily_df["景点ID"].astype(str) == aid]
        if sub.empty:
            predicted = 50
            yesterday = 0
            trend_7d = []
            weekday_factor = 1.0
        else:
            weekday_factor = 1.0 + 0.25 * (sub["is_weekend"].mean())  # 周末多的景点 → 1.0+0.25*比例
            avg_30 = sub["visitors"].mean()
            last_7 = sub.tail(7)["visitors"].tolist()
            trend_7d = last_7
            yesterday = int(sub["visitors"].iloc[-1]) if len(sub) else 0
            # 基础预测 = 近 7 天日均 × 周末因子
            avg_7 = float(np.mean(last_7)) if last_7 else avg_30
            predicted = round(avg_7 * weekday_factor)

        # 5. 调整后预测（用昨日 + 7 天趋势）
        if trend_7d:
            # 周末/工作日调整
            tomorrow_dow = (today.weekday() + 1) % 7
            weekend_adj = 1.20 if tomorrow_dow in [5, 6] else 0.85
            predicted = round((yesterday * 0.4 + np.mean(trend_7d) * 0.6) * weekend_adj)

        result.append({
            "景点ID": aid,
            "景点名称": a.get("景点名称"),
            "类型": a.get("类型"),
            "昨日游客": int(yesterday),
            "预测明日": int(predicted),
            "近7天日均": round(float(np.mean(trend_7d)) if trend_7d else 0, 1),
            "变化": round((predicted - yesterday) / max(yesterday, 1) * 100, 1),
            "趋势": trend_7d,
        })

    # 按预测明日降序
    result.sort(key=lambda x: x["预测明日"], reverse=True)
    return {"forecasts": result, "date": today.strftime("%Y-%m-%d")}


# ----------------------------------------------------------------------
# 2. 景点推荐（基于游客行为相似度）
# ----------------------------------------------------------------------
@router.get("/attraction-recommend")
def attraction_recommend(attraction_id: str = Query(...), top_k: int = 5):
    """
    给定一个景点，推荐 top_k 个"该景点游客常去的下一个景点"。
    方法：分析 visit_records，找到 visit A 后 2 小时内 visit B 的游客对（按 B 频次排序）。
    """
    rows = mysql_svc.query(
        "SELECT v1.游客ID AS visitor, v1.景点ID AS from_a, v1.时间 AS t1, v2.景点ID AS to_a, v2.时间 AS t2 "
        "FROM t_visit_record v1 "
        "JOIN t_visit_record v2 ON v1.游客ID = v2.游客ID "
        "WHERE v1.景点ID = %s AND v2.景点ID != %s "
        "AND v2.时间 > v1.时间 "
        "AND TIMESTAMPDIFF(HOUR, v1.时间, v2.时间) <= 6",
        (attraction_id, attraction_id),
    )
    if not rows:
        return {"from": attraction_id, "recommendations": []}

    counter = Counter([r["to_a"] for r in rows])
    total = sum(counter.values())

    atts = {a["景点ID"]: a for a in mysql_svc.query("SELECT 景点ID, 景点名称, 类型 FROM t_attraction")}

    recs = []
    for aid, count in counter.most_common(top_k):
        a = atts.get(aid, {})
        recs.append({
            "景点ID": aid,
            "景点名称": a.get("景点名称", aid),
            "类型": a.get("类型", ""),
            "频次": int(count),
            "概率": round(count / total, 3),
        })

    return {"from": attraction_id, "total_pairs": total, "recommendations": recs}


# ----------------------------------------------------------------------
# 3. 游玩路线推荐
# ----------------------------------------------------------------------
@router.get("/route-recommend")
def route_recommend(type: str = Query("", description="景点类型偏好：文化/娱乐/自然/运动"),
                    budget: int = Query(500, description="人均预算"),
                    hours: float = Query(6, description="可用游玩时间")):
    """
    根据类型偏好+预算+可用时间，生成一条推荐游玩路线。
    方法：
      1. 在指定类型中选 2-3 个景点
      2. 加上同游客常去的关联景点
      3. 估算总消费 + 总时间，给出路线
    """
    if type:
        candidates = mysql_svc.query(
            "SELECT 景点ID, 景点名称, 类型, 开放时间 FROM t_attraction WHERE 类型 = %s",
            (type,),
        )
    else:
        candidates = mysql_svc.query("SELECT 景点ID, 景点名称, 类型, 开放时间 FROM t_attraction")

    if not candidates:
        return {"error": f"no attraction of type {type}"}

    # 取每个景点的平均消费 + 平均游玩时长
    consumption = mysql_svc._query_df(
        "SELECT 景点ID, AVG(消费金额) AS avg_amount, COUNT(*) AS n "
        "FROM t_consumption GROUP BY 景点ID"
    )
    visits = mysql_svc._query_df(
        "SELECT 景点ID, AVG(游玩时长) AS avg_duration, COUNT(DISTINCT 游客ID) AS visitors "
        "FROM t_visit_record GROUP BY 景点ID"
    )

    candidates_df = pd.DataFrame(candidates)
    candidates_df["景点ID"] = candidates_df["景点ID"].astype(str)
    if not consumption.empty:
        consumption["景点ID"] = consumption["景点ID"].astype(str)
        candidates_df = candidates_df.merge(consumption, on="景点ID", how="left")
    if not visits.empty:
        visits["景点ID"] = visits["景点ID"].astype(str)
        candidates_df = candidates_df.merge(visits, on="景点ID", how="left")
    candidates_df = candidates_df.fillna({"avg_amount": 100, "avg_duration": 3.0, "visitors": 0})

    # 按游客数排序，取前 4 个作为候选
    candidates_df = candidates_df.sort_values("visitors", ascending=False).head(4)

    # 贪心选路线（按游客数/热度排序）
    route = []
    total_cost = 0
    total_hours = 0.0
    for _, row in candidates_df.iterrows():
        # 门票估算（按景点类型 + 平均消费打折）
        cost = 80 + float(row.get("avg_amount", 100)) * 0.2
        dur = float(row.get("avg_duration", 3.0))
        # 至少 1 个景点；预算/时间可超出 30% 允许边界
        if route:
            if total_cost + cost > budget * 1.3:
                break
            if total_hours + dur > hours * 1.3:
                break
        route.append({
            "景点ID": str(row["景点ID"]),
            "景点名称": row["景点名称"],
            "类型": row.get("类型", ""),
            "开放时间": row.get("开放时间", ""),
            "预计消费": round(cost, 0),
            "建议游玩时长": round(dur, 1),
        })
        total_cost += cost
        total_hours += dur

    return {
        "type": type or "全部",
        "budget": budget,
        "hours": hours,
        "route": route,
        "total_cost": round(total_cost, 0),
        "total_hours": round(total_hours, 1),
        "remaining_budget": round(budget - total_cost, 0),
    }


# ----------------------------------------------------------------------
# 4. 游客画像
# ----------------------------------------------------------------------
@router.get("/visitor-profile/{visitor_id}")
def visitor_profile(visitor_id: str):
    """
    给定游客 ID，返回完整画像：
      - 基本信息（年龄/性别/地区）
      - 行为统计（消费/游玩）
      - 高价值预测（sklearn 分类）
      - 群体归类（sklearn 聚类）
      - 兴趣偏好（top 3 类型）
    """
    # 1. 基本信息
    base = mysql_svc.query(
        "SELECT 游客ID, 姓名, 性别, 年龄, 地区 FROM t_visitor WHERE 游客ID = %s",
        (visitor_id,),
    )
    if not base:
        raise HTTPException(404, f"visitor {visitor_id} not found")
    v = base[0]

    # 2. 行为统计
    cons = mysql_svc.query(
        "SELECT COUNT(*) AS n, COALESCE(SUM(消费金额), 0) AS total, COALESCE(AVG(消费金额), 0) AS avg "
        "FROM t_consumption WHERE 游客ID = %s",
        (visitor_id,),
    )[0]
    visits = mysql_svc.query(
        "SELECT COUNT(*) AS n, COALESCE(AVG(游玩时长), 0) AS avg_dur, "
        "COUNT(DISTINCT 景点ID) AS unique_a "
        "FROM t_visit_record WHERE 游客ID = %s",
        (visitor_id,),
    )[0]

    # 3. 兴趣偏好
    pref_rows = mysql_svc.query(
        "SELECT a.类型, COUNT(*) AS n FROM t_visit_record vr "
        "JOIN t_attraction a ON vr.景点ID = a.景点ID "
        "WHERE vr.游客ID = %s GROUP BY a.类型 ORDER BY n DESC",
        (visitor_id,),
    )
    preferences = [{"类型": p["类型"], "次数": p["n"]} for p in pref_rows[:3]]

    # 4. 构造 6 特征 → 走 ML 模型
    features = {
        "age": int(v.get("年龄") or 30),
        "purchase_count": int(cons.get("n") or 0),
        "avg_amount": float(cons.get("avg") or 0),
        "visit_count": int(visits.get("n") or 0),
        "avg_duration": float(visits.get("avg_dur") or 0),
        "unique_attractions": int(visits.get("unique_a") or 0),
    }

    # 5. 高价值分类
    try:
        clf_result = model_svc.predict("high_value_visitor", features)
        is_high_value = clf_result.get("label") == "high_value"
        high_value_prob = clf_result.get("probability", 0)
    except Exception:
        is_high_value = False
        high_value_prob = 0

    # 6. 聚类分组
    try:
        clu_result = model_svc.predict("cluster", features)
        cluster = clu_result.get("cluster", 0)
        cluster_label = clu_result.get("label", "")
        cluster_tip = clu_result.get("tip", "")
    except Exception:
        cluster = 0
        cluster_label = ""
        cluster_tip = ""

    # 7. 消费预测
    try:
        cons_pred = model_svc.predict("consumption_amount", features)
        predicted_amount = cons_pred.get("prediction", 0)
    except Exception:
        predicted_amount = 0

    return {
        "visitor": {
            "游客ID": v.get("游客ID"),
            "姓名": v.get("姓名"),
            "性别": v.get("性别"),
            "年龄": v.get("年龄"),
            "地区": v.get("地区"),
        },
        "behavior": {
            "消费笔数": int(cons.get("n") or 0),
            "消费总额": round(float(cons.get("total") or 0), 2),
            "平均消费": round(float(cons.get("avg") or 0), 2),
            "游玩次数": int(visits.get("n") or 0),
            "平均游玩时长": round(float(visits.get("avg_dur") or 0), 2),
            "去过的景点数": int(visits.get("unique_a") or 0),
        },
        "preferences": preferences,
        "ml_predictions": {
            "高价值游客": is_high_value,
            "高价值概率": round(high_value_prob, 3),
            "群体归类": cluster,
            "群体标签": cluster_label,
            "运营建议": cluster_tip,
            "预测消费": round(predicted_amount, 0),
        },
    }


# ----------------------------------------------------------------------
# 5. 今日 vs 预测明日对比（KPI 卡片用）
# ----------------------------------------------------------------------
@router.get("/tomorrow-summary")
def tomorrow_summary():
    """聚合预测，给 dashboard KPI 用。"""
    fr = attraction_forecast()
    if "forecasts" not in fr:
        return {"error": "no data"}
    total_yesterday = sum(f["昨日游客"] for f in fr["forecasts"])
    total_tomorrow = sum(f["预测明日"] for f in fr["forecasts"])
    top = fr["forecasts"][0] if fr["forecasts"] else {}
    return {
        "昨日总游客": total_yesterday,
        "预测明日": total_tomorrow,
        "变化": round((total_tomorrow - total_yesterday) / max(total_yesterday, 1) * 100, 1),
        "最热门景点": {"id": top.get("景点ID"), "name": top.get("景点名称"), "predicted": top.get("预测明日")},
    }
