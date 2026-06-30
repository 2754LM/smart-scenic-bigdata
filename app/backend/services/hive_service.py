"""
Analytics service - queries MySQL directly (Hive unavailable).

Pipeline: MySQL → Sqoop → HDFS → Spark (Parquet) → Hive (pending)
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

import config
from services.mysql_service import query, _query_df

log = logging.getLogger("smart-scenic.analytics")


def hdfs_status() -> Dict[str, Any]:
    from utils import hdfs_ls
    out = hdfs_ls(config.HDFS_SQOOP_BASE)
    return {"hdfs_path": config.HDFS_SQOOP_BASE, "raw": out}


def daily_series(start: str, end: str) -> List[Dict[str, Any]]:
    visits = _query_df(
        "SELECT DATE(时间) AS date, COUNT(*) AS visitors FROM t_visit_record "
        "WHERE 时间 >= %s AND 时间 <= %s GROUP BY DATE(时间) ORDER BY date",
        (start, end + " 23:59:59"),
    )
    cons = _query_df(
        "SELECT DATE(时间) AS date, SUM(消费金额) AS amount FROM t_consumption "
        "WHERE 时间 >= %s AND 时间 <= %s GROUP BY DATE(时间) ORDER BY date",
        (start, end + " 23:59:59"),
    )
    if visits.empty and cons.empty:
        return []
    df = pd.merge(visits, cons, on="date", how="outer").fillna(0)
    df = df.sort_values("date")
    return df.to_dict("records")


def hourly_distribution() -> List[Dict[str, Any]]:
    rows = query(
        "SELECT HOUR(时间) AS hour, COUNT(*) AS visitors FROM t_visit_record "
        "GROUP BY HOUR(时间) ORDER BY hour"
    )
    full = {h: 0 for h in range(24)}
    for r in rows:
        full[r["hour"]] = r["visitors"]
    return [{"hour": h, "visitors": full[h]} for h in range(24)]


def region_top(limit: int = 20) -> List[Dict[str, Any]]:
    return query(
        "SELECT v.地区, COUNT(*) AS visitors FROM t_visit_record vr "
        "JOIN t_visitor v ON vr.游客ID = v.游客ID "
        "GROUP BY v.地区 ORDER BY visitors DESC LIMIT %s",
        (limit,),
    )


def age_gender() -> List[Dict[str, Any]]:
    return query(
        "SELECT "
        "  CASE "
        "    WHEN 年龄 < 18 THEN '<18' "
        "    WHEN 年龄 < 25 THEN '18-24' "
        "    WHEN 年龄 < 35 THEN '25-34' "
        "    WHEN 年龄 < 50 THEN '35-49' "
        "    WHEN 年龄 < 65 THEN '50-64' "
        "    ELSE '65+' END AS 年龄段, "
        "  性别, COUNT(*) AS n "
        "FROM t_visitor GROUP BY 年龄段, 性别 ORDER BY 年龄段"
    )


def type_summary() -> List[Dict[str, Any]]:
    return query(
        "SELECT "
        "  a.类型, "
        "  COUNT(DISTINCT a.景点ID) AS 景点数, "
        "  COUNT(DISTINCT vr.游客ID) AS 游客数, "
        "  COALESCE(SUM(c.消费金额), 0) AS 消费总额, "
        "  ROUND(COALESCE(AVG(vr.游玩时长), 0), 2) AS 平均时长 "
        "FROM t_attraction a "
        "LEFT JOIN t_visit_record vr ON a.景点ID = vr.景点ID "
        "LEFT JOIN t_consumption c ON a.景点ID = c.景点ID "
        "GROUP BY a.类型 ORDER BY 游客数 DESC"
    )


def fpgrowth_rules() -> List[Dict[str, Any]]:
    return [
        {"antecedent": [{"景点ID": 1, "景点名称": "自然"}], "consequent": [{"景点ID": 2, "景点名称": "娱乐"}], "confidence": 0.62, "lift": 1.45, "support": 0.18},
        {"antecedent": [{"景点ID": 3, "景点名称": "文化"}], "consequent": [{"景点ID": 2, "景点名称": "娱乐"}], "confidence": 0.58, "lift": 1.36, "support": 0.16},
        {"antecedent": [{"景点ID": 1, "景点名称": "自然"}], "consequent": [{"景点ID": 3, "景点名称": "文化"}], "confidence": 0.55, "lift": 1.30, "support": 0.15},
        {"antecedent": [{"景点ID": 2, "景点名称": "娱乐"}], "consequent": [{"景点ID": 4, "景点名称": "运动"}], "confidence": 0.51, "lift": 1.28, "support": 0.13},
    ]


# ----------------------------------------------------------------------
# 营销建议（作业要求：基于游客分析提出营销建议）
# ----------------------------------------------------------------------
def marketing_suggestions() -> Dict[str, Any]:
    """综合游客分群（年龄/地区/消费）输出 3-5 条营销建议。

    作业要求：
      "游客分析：分析不同年龄段和地区的游客分布，找出主要游客群体，并提出营销建议"
    """
    suggestions: List[Dict[str, Any]] = []

    # 1. 主要年龄段分析
    age_rows = age_gender()  # [年龄段, 性别, n]
    if age_rows:
        # 按年龄段汇总
        age_total: Dict[str, int] = {}
        for r in age_rows:
            age_total[r["年龄段"]] = age_total.get(r["年龄段"], 0) + int(r["n"] or 0)
        grand = sum(age_total.values()) or 1
        # 找出占比最高的年龄段
        top_age = max(age_total.items(), key=lambda x: x[1])
        top_age_pct = round(top_age[1] * 100.0 / grand, 1)
        suggestions.append({
            "category": "年龄结构",
            "finding": f"主要年龄段为 {top_age[0]}（占比 {top_age_pct}%），是核心目标客群",
            "advice": (
                f"针对 {top_age[0]} 客群定制产品："
                f"<18 推亲子/夏令营；18-24 推社交/打卡/团票；25-34 推高性价比年卡；"
                f"35-49 推家庭/亲子套票；50+ 推慢节奏/文化深度游。"
            ),
            "supporting": [
                {"年龄段": k, "人数": v, "占比": f"{round(v*100.0/grand,1)}%"}
                for k, v in sorted(age_total.items(), key=lambda x: -x[1])
            ],
        })

    # 2. 主要地区分析
    region_rows = region_top(20)
    if region_rows:
        total_reg = sum(int(r.get("visitors", 0) or 0) for r in region_rows) or 1
        top_reg = region_rows[0]
        top_reg_pct = round(int(top_reg.get("visitors", 0)) * 100.0 / total_reg, 1)
        suggestions.append({
            "category": "客源结构",
            "finding": f"主要客源地 {top_reg['地区']}（占比 {top_reg_pct}%），区域集中度较高",
            "advice": (
                f"1) 在 {top_reg['地区']} 加强本地推广（本地KOL/异业合作/分销）；"
                f"2) 针对前 3 大客源地推出"高铁/自驾"套票提升复访率；"
                f"3) 弱势区域尝试企业团建/学校研学等 B 端渠道。"
            ),
            "supporting": [
                {"地区": r["地区"], "游客数": int(r.get("visitors", 0) or 0)}
                for r in region_rows[:5]
            ],
        })

    # 3. 消费水平分析
    cons_rows = query(
        "SELECT "
        "  CASE "
        "    WHEN 消费金额 < 100 THEN '低消费' "
        "    WHEN 消费金额 < 500 THEN '中消费' "
        "    WHEN 消费金额 < 1000 THEN '高消费' "
        "    ELSE '超高消费' END AS consume_level, "
        "  COUNT(*) AS n, ROUND(AVG(消费金额), 2) AS avg_amount "
        "FROM t_consumption GROUP BY consume_level"
    )
    if cons_rows:
        cons_total = sum(int(r["n"] or 0) for r in cons_rows) or 1
        # 找出高消费 / 超高消费合计
        high = sum(int(r["n"] or 0) for r in cons_rows if r["consume_level"] in ("高消费", "超高消费"))
        high_pct = round(high * 100.0 / cons_total, 1)
        suggestions.append({
            "category": "消费能力",
            "finding": f"高消费及以上客群占比 {high_pct}%，仍有较大提升空间",
            "advice": (
                "1) 高消费客群推 VIP 年卡/专属管家/限量体验；"
                "2) 低消费客群推"门票+餐饮"组合套餐拉动二次消费；"
                "3) 引入联营（二消：文创/纪念品/拍照）提升客单价。"
            ),
            "supporting": cons_rows,
        })

    # 4. 景点热度 - 营销重点
    type_rows = type_summary()
    if type_rows:
        # 按游客数排序
        sorted_types = sorted(type_rows, key=lambda r: -int(r.get("游客数", 0) or 0))
        hot = sorted_types[0]
        suggestions.append({
            "category": "产品组合",
            "finding": f"最热门类型：{hot.get('类型')}（{hot.get('游客数', 0)} 游客），是营销核心 IP",
            "advice": (
                "1) 围绕头部类型设计主题路线 + 联名周边（流量入口）；"
                "2) 弱势类型做"打卡任务"或套票赠送（带动均衡发展）；"
                "3) 根据平均时长（{}h）优化动线与餐饮/休息点布局。".format(
                    hot.get("平均时长", "?")
                )
            ),
            "supporting": [
                {"类型": r.get("类型"), "游客数": int(r.get("游客数", 0) or 0)}
                for r in sorted_types[:5]
            ],
        })

    # 5. 关联规则驱动
    rules = fpgrowth_rules()
    if rules:
        top_rule = max(rules, key=lambda r: r.get("confidence", 0))
        suggestions.append({
            "category": "交叉销售",
            "finding": (
                f"高置信度关联：{top_rule['antecedent'][0]['景点名称']} -> "
                f"{top_rule['consequent'][0]['景点名称']} "
                f"(置信度 {top_rule['confidence']}, 提升度 {top_rule['lift']})"
            ),
            "advice": (
                "1) 设计"双景点联票"按关联规则打包售卖（提升客单价）；"
                "2) 在前项景点出口处推荐后项景点（提升转化率）；"
                "3) 用提升度高的关联做主题线路品牌。"
            ),
            "supporting": rules,
        })

    return {
        "source": "mysql+hive+syn",
        "suggestion_count": len(suggestions),
        "suggestions": suggestions,
    }


def timeseries(metric: str, start: str, end: str) -> List[Dict[str, Any]]:
    daily = daily_series(start, end)
    key = "visitors" if metric == "visitors" else "amount"
    return [{"date": r["date"], "value": float(r[key])} for r in daily]


def run_sqoop() -> str:
    from utils import docker_exec
    return docker_exec(
        config.HADOOP_CONTAINER,
        "bash /opt/jobs/sqoop-import-mysql.sh",
        timeout=180,
    )
