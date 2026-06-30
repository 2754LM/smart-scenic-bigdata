# 智能景区大数据平台 - 项目实现文档

> **作业**: 选题十八 智能景区管理系统  
> **技术栈**: MySQL 5.7 · Sqoop · HDFS · Spark · Hive 3.1.3 · HBase · scikit-learn  
> **架构**: 15 容器 Docker 集群 (Hadoop HA + Spark + Hive 多实例 + HBase + FastAPI)

---

## 目录

1. [系统架构](#1-系统架构)
2. [数据流与处理链路](#2-数据流与处理链路)
3. [数据采集与存储 (MySQL + Sqoop + HDFS)](#3-数据采集与存储)
4. [Spark 数据清洗 / 预处理](#4-spark-数据清洗--预处理)
5. [Hive 表设计与查询](#5-hive-表设计与查询)
6. [机器学习模型训练 (PySpark MLlib)](#6-机器学习模型训练)
7. [场景化预测 API](#7-场景化预测-api)
8. [前端可视化](#8-前端可视化)
9. [如何运行](#9-如何运行)
10. [关键技术点总结](#10-关键技术点总结)

---

## 6. 场景化预测 API

**文件**: `app/backend/routers/predict_tourism.py`

### 6.1 API 列表

| 端点 | 用途 | 算法 |
|---|---|---|
| `GET /attraction-forecast` | 10 景点 × 明日客流量 | 7日均 + 周末因子 |
| `GET /attraction-recommend?attraction_id=X` | 游玩 A 后最常去的下一个景点 | MySQL 序列分析 (6 小时内) |
| `GET /route-recommend?type=X&budget=X&hours=X` | 智能游玩路线 | 类型过滤 + 贪心选路线 |
| `GET /visitor-profile/{vid}` | 游客画像 (消费/高价值/群体/偏好/建议) | ML 4 模型组合 |
| `GET /tomorrow-summary` | 聚合 KPI (昨日/明日/变化/最热) | - |
| `GET /multi-day-forecast?days=7` | 未来 7/14/30 天每日总客流 | 日均 × 星期因子 × 月因子 × 趋势 × 噪声 |
| `GET /fpgrowth-sankey?limit=20` | FPGrowth Sankey 图数据 | lift × support 聚合 + 去环 |

### 6.2 客流预测算法详解

```python
# 未来 N 天每日预测 = 基础日均 × 近期趋势 × 星期因子 × 月因子 × 噪声
基础日均 = 该景点最近 90 天日均游客
近期趋势 = 最近 7 天日均 / 之前 30 天日均 (限幅 0.7-1.3)
星期因子 = (景点, 星期) 历史均值 / 整体均值
月因子   = (景点, 月份) 历史均值 / 整体均值
噪声     = 1.0 ± uniform(0, 0.08)
```

### 6.3 游客画像

```
输入: 游客 ID
↓
1. MySQL 聚合 6 维特征 (消费笔数/总额/平均/游玩次数/时长/景点数)
2. 兴趣偏好: 该游客去过的景点按 类型 分组 top 3
3. ML 模型推理 (sklearn):
   - consumption_amount → 消费总额回归预测 (Linear/Lasso/Ridge/RF)
   - high_value_visitor → 是否高频回头客 (RF/DT/GBT/LR 4 选, 3 特征)
   - cluster            → 群体归类 (KMeans k=4)
↓
输出: 完整画像 + 群体标签 + 运营建议
```

---

## 7. 前端可视化

### 7.1 页面结构 (4 个 HTML)

| 页面 | URL | 内容 |
|---|---|---|
| 总览大屏 | index.html | 8 KPI + 4 ML 预测 + 7 图表 (游客/消费/排行) |
| 数据分析 | analysis.html | 6 图表 + FPGrowth Sankey |
| 模型预测 | predict.html | 4 场景化卡 + 真实 vs 预测折线 + 模型对比 |
| 业务管理 | manage.html | 景点/游客/消费/游玩 + 系统管理 |

### 7.2 设计风格

- **高对比深色主题**: 主色 `#0a0e27` / `#131a3a` / `#1a2350`，强调色 `#00d4ff` (青) / `#a855f7` (紫) / `#10b981` (绿)
- **无白色背景**: 所有 card / 表头 / 状态块用深色
- **ECharts 5.4.3**: 折线 / 柱状 / 饼图 / Sankey / 散点
- **响应式**: 1280px 桌面优先，移动端降级
- **缓存控制**: `?v=21` 等版本号强制刷新

### 7.3 关键图表

- **景点明日客流量预测**: bar 图对比 昨日实际 vs 预测明日，10 景点排序
- **多日客流预测 (7/30 天)**: 总客流柱状图 (周末紫/工作日青) + 各景点累计列表
- **真实 vs 预测对比**: 折线图 (蓝色真实 + 黄色虚线预测) + 分割日 markLine
- **FPGrowth 关联规则**: ECharts Sankey (类型着色) - 替代原表格
- **24h 时段分布**: 真实开放时间过滤 (0-5 时 0 游客，9-16 时 4000+ 峰值)

---

## 8. 如何运行

### 8.1 启动 Docker 集群

```bash
cd D:\Desktop\smart-scenic-bigdata
docker compose up -d
# 等待 ~30 秒所有容器 healthy
```

### 8.2 初始化数据 (一次性)

```bash
# 通过前端系统管理 tab 一键初始化
# 或手动:
docker exec demo-backend python3 -c "
import requests
requests.post('http://localhost:8000/api/admin/pipeline',
  json={'actions': ['load_csv', 'sqoop', 'spark_clean', 'spark_train']})
"
```

### 8.3 访问前端

```
http://localhost:8080
- 总览大屏:  http://localhost:8080/index.html
- 数据分析:  http://localhost:8080/analysis.html
- 模型预测:  http://localhost:8080/predict.html
- 实时流:    http://localhost:8080/realtime.html
- 业务管理:  http://localhost:8080/manage.html
```

### 8.4 关键命令

```bash
# 重新训练所有模型
docker exec spark-master bash -c "cd /opt/jobs/ml && python3 train.py"

# 仅重新训练分类模型
docker exec spark-master python3 /tmp/retrain_clf.py

# 重跑 Spark 清洗
docker exec spark-master bash -c "cd /opt/jobs && spark-submit --master spark://spark-master:7077 /opt/jobs/spark/clean.py"

# 重跑 Sqoop 导入
docker exec hadoop-namenode bash -c "bash /opt/jobs/sqoop-import-mysql.sh"

# 重新生成 FPGrowth 规则
docker exec spark-master bash -c "cd /opt/jobs && spark-submit --master spark://spark-master:7077 /opt/jobs/ml/fpgrowth.py"
```

---

## 9. 关键技术点总结

1. **PySpark → sklearn 双轨推理**: 训练在 spark-master (PySpark MLlib)，推理在 demo-backend (joblib 加载 .pkl)，毫秒级响应，无 PySpark 依赖
2. **数据真实性修复**: 24h 时段分布按景点开放时间过滤，避免凌晨有游客的不真实数据
3. **Sankey 去环**: FPGrowth 关联规则可能产生 A→B 和 B→A 的环，按权重排序时去除
4. **多日预测因子合成**: 基础日均 × 趋势 × 星期因子 × 月因子 × 噪声，简单但有效
5. **场景化预测设计**: 与景区业务深度结合的 ML (客流预测/智能推荐/路线规划/游客画像)，而非单纯的 6 特征输入
6. **Hive 数仓真实可达**: pyhive-via-beeline (beeline 在 hive-server-1 容器内通过 10000 端口连 HS2, 解析 TSV 输出). 0 静默 fallback, DDL 没跑会 503.
7. **MySQL 5.7 兼 Hive Metastore**: DataNucleus 4.2 + MySQL 8.0 不兼容 (DEFAULT CHARACTER SET 语法). 5.7 兼容, 一容器双角色.

---

**完成日期**: 2026-06-30  
**GitHub**: https://github.com/2754LM/smart-scenic-bigdata
