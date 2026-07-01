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
    # 规范化 ID: "1" -> "V00001", "V00001" -> "V00001"
    vid = visitor_id.strip()
    if not vid.upper().startswith("V"):
        try:
            vid = f"V{int(vid):05d}"
        except ValueError:
            pass
    else:
        vid = vid.upper()

    # 1. 基本信息
    base = mysql_svc.query(
        "SELECT 游客ID, 姓名, 性别, 年龄, 地区 FROM t_visitor WHERE 游客ID = %s",
        (vid,),
    )
    if not base:
        raise HTTPException(404, f"visitor {vid} not found")
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


# ----------------------------------------------------------------------
# 5.5 FPGrowth 关联规则 Sankey 数据
# ----------------------------------------------------------------------
@router.get("/fpgrowth-sankey")
def fpgrowth_sankey(limit: int = Query(20, ge=5, le=100)):
    """
    把 FPGrowth 关联规则转为 Sankey 图数据。
    方法：对每条规则 (a1, a2, ... ) → (b1, b2, ...)，
    拆成 |antecedent| × |consequent| 个 (a, b) 边，权重 = support × confidence / |a|/|b|
    然后按权重聚合，得到 (from, to, weight) 列表。
    """
    import json
    from pathlib import Path
    p = Path("/shared/models/fpgrowth_rules.json")
    if not p.exists():
        return {"error": "fpgrowth rules not trained", "nodes": [], "links": []}
    with open(p, "r", encoding="utf-8") as f:
        rules = json.load(f)
    if not rules:
        return {"error": "no rules", "nodes": [], "links": []}

    # 节点 + 边 聚合
    node_set = set()
    edge_agg = {}  # (from_id, to_id) -> weight

    for r in rules:
        a_items = r.get("antecedent", [])
        c_items = r.get("consequent", [])
        if not a_items or not c_items:
            continue
        sup = float(r.get("support", 0))
        conf = float(r.get("confidence", 0))
        # 每条规则贡献 (sup × conf / |a| / |c|) 给每对 (a, c)
        w = sup * conf / max(len(a_items) * len(c_items), 1)
        for a in a_items:
            for c in c_items:
                if a["景点ID"] == c["景点ID"]:
                    continue
                key = (a["景点ID"], c["景点ID"])
                edge_agg[key] = edge_agg.get(key, 0) + w

    # 排序取前 N (去除环：只保留 a→b 的边，不保留 b→a 的反向边)
    sorted_edges = sorted(edge_agg.items(), key=lambda x: -x[1])
    seen = set()
    edges = []
    for (f, t), w in sorted_edges:
        # 去除 cycle: 不允许 t→f 已经添加过
        if (t, f) in seen:
            continue
        seen.add((f, t))
        edges.append({"from": f, "to": t, "value": round(w, 6)})
        if len(edges) >= limit:
            break
    # 涉及的节点
    involved = set()
    for e in edges:
        involved.add(e["from"])
        involved.add(e["to"])
    # 节点信息
    atts = mysql_svc.query("SELECT 景点ID, 景点名称, 类型 FROM t_attraction")
    att_map = {a["景点ID"]: a for a in atts}
    nodes = []
    for aid in involved:
        a = att_map.get(aid, {})
        # 累计 value 作为节点的 size
        total_value = sum(e["value"] for e in edges if e["from"] == aid) + \
                      sum(e["value"] for e in edges if e["to"] == aid)
        nodes.append({
            "景点ID": aid,
            "name": a.get("景点名称", aid),
            "type": a.get("类型", ""),
            "value": round(total_value, 6),
        })

    return {
        "total_rules": len(rules),
        "shown_edges": len(edges),
        "nodes": nodes,
        "links": edges,
    }


# ----------------------------------------------------------------------
# 6. 多日预测（7 天 / 30 天）
# ----------------------------------------------------------------------
@router.get("/multi-day-forecast")
def multi_day_forecast(days: int = Query(7, ge=1, le=90, description="预测天数 1-90")):
    """
    对每个景点 + 景区总客流做未来 N 天预测。
    方法:
      1. 拉取每个景点最近 90 天的每日数据
      2. 计算 7 天/30 天滚动均值（基础水平）
      3. 应用星期因子 (Mon-Sun)
      4. 应用月因子 (1-12 月，淡旺季)
      5. 加 ±10% 随机扰动模拟模型不确定性
    """
    if days > 30:
        days = 30
    days = min(days, 90)

    # 1. 拉最近 90 天每日每景点数据
    today = datetime(2023, 12, 31)
    start = today - timedelta(days=90)
    df = mysql_svc._query_df(
        "SELECT DATE(时间) AS date, 景点ID, COUNT(*) AS visitors "
        "FROM t_visit_record "
        "WHERE 时间 >= %s AND 时间 <= %s "
        "GROUP BY DATE(时间), 景点ID ORDER BY date",
        (start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d 23:59:59")),
    )
    if df.empty:
        return {"error": "no data", "days": days}

    df["date"] = pd.to_datetime(df["date"])
    df["dow"] = df["date"].dt.dayofweek
    df["month"] = df["date"].dt.month

    # 2. 星期因子（每个景点：1.0-1.3 高峰，0.7-1.0 平时）
    #    简单做法：训练数据中每个 (attraction, dow) 的均值 / 总均值
    overall_avg = df.groupby("景点ID")["visitors"].mean().to_dict()
    dow_avg = df.groupby(["景点ID", "dow"])["visitors"].mean().reset_index()
    dow_avg["dow_factor"] = dow_avg.apply(
        lambda r: r["visitors"] / overall_avg.get(r["景点ID"], 1), axis=1
    )
    dow_factor = dow_avg.set_index(["景点ID", "dow"])["dow_factor"].to_dict()

    # 3. 月因子
    month_avg = df.groupby(["景点ID", "month"])["visitors"].mean().reset_index()
    month_avg["month_factor"] = month_avg.apply(
        lambda r: r["visitors"] / overall_avg.get(r["景点ID"], 1), axis=1
    )
    month_factor = month_avg.set_index(["景点ID", "month"])["month_factor"].to_dict()

    # 4. 全局趋势（最近 7 天 vs 之前 30 天）
    cutoff = today - timedelta(days=7)
    recent_avg = df[df["date"] > cutoff].groupby("景点ID")["visitors"].mean().to_dict()
    older_avg = df[df["date"] <= cutoff].groupby("景点ID")["visitors"].mean().to_dict()
    trend = {}
    for aid in overall_avg:
        r = recent_avg.get(aid, overall_avg[aid])
        o = older_avg.get(aid, overall_avg[aid])
        if o == 0:
            trend[aid] = 1.0
        else:
            t = r / o
            trend[aid] = max(0.7, min(1.3, t))  # 限幅

    # 5. 预测
    atts = mysql_svc.query("SELECT 景点ID, 景点名称, 类型 FROM t_attraction")
    forecast = []
    np.random.seed(42)  # 可重复

    for a in atts:
        aid = str(a["景点ID"])
        base = overall_avg.get(aid, 30)
        t = trend.get(aid, 1.0)
        daily = []
        for i in range(1, days + 1):
            future_date = today + timedelta(days=i)
            fdow = future_date.weekday()
            fmonth = future_date.month
            df_factor = dow_factor.get((aid, fdow), 1.0)
            mf = month_factor.get((aid, fmonth), 1.0)
            noise = 1.0 + np.random.uniform(-0.08, 0.08)
            pred = max(0, round(base * t * df_factor * mf * noise))
            daily.append({
                "date": future_date.strftime("%Y-%m-%d"),
                "dow": ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][fdow],
                "predicted": pred,
                "is_weekend": fdow in [5, 6],
            })
        total = sum(d["predicted"] for d in daily)
        forecast.append({
            "景点ID": aid,
            "景点名称": a["景点名称"],
            "类型": a["类型"],
            "基础日均": round(base, 1),
            "近期趋势": round(t, 3),
            f"未来{days}天总计": total,
            f"未来{days}天日均": round(total / days, 1),
            "daily": daily,
        })

    # 6. 整体总客流预测
    total_daily = []
    for i in range(days):
        day_total = sum(
            next((d["predicted"] for d in f["daily"] if d["date"] == (today + timedelta(days=i+1)).strftime("%Y-%m-%d")), 0)
            for f in forecast
        )
        future_date = today + timedelta(days=i+1)
        total_daily.append({
            "date": future_date.strftime("%Y-%m-%d"),
            "dow": ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][future_date.weekday()],
            "total_visitors": day_total,
            "is_weekend": future_date.weekday() in [5, 6],
        })

    forecast.sort(key=lambda x: x[f"未来{days}天总计"], reverse=True)
    return {
        "days": days,
        "today": today.strftime("%Y-%m-%d"),
        "景点预测": forecast,
        "总客流": total_daily,
    }
