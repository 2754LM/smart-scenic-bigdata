# 智能景区大数据平台 - 项目实现文档

> **作业**: 选题十八 智能景区管理系统  
> **技术栈**: MySQL 5.7 · Sqoop · HDFS · Spark · Hive 3.1.3 · HBase · Kafka · scikit-learn  
> **架构**: 17 容器 Docker 集群 (Hadoop HA + Spark + Hive 多实例 + HBase + Kafka + FastAPI)

---

## 目录

1. [系统架构](#1-系统架构)
2. [数据流与处理链路](#2-数据流与处理链路)
3. [数据采集与存储 (MySQL + Sqoop + HDFS)](#3-数据采集与存储)
4. [Spark 数据清洗 / 预处理](#4-spark-数据清洗--预处理)
5. [Hive 表设计与查询](#5-hive-表设计与查询)
6. [机器学习模型训练 (PySpark MLlib)](#6-机器学习模型训练)
7. [实时数据流 (Kafka + HBase)](#7-实时数据流)
8. [场景化预测 API](#8-场景化预测-api)
9. [前端可视化](#9-前端可视化)
10. [如何运行](#10-如何运行)

---

## 1. 系统架构

### 1.1 17 容器 Docker 集群

| 类别 | 容器 | 数量 | 角色 |
|---|---|---|---|
| 存储 | mysql | 1 | 业务库 (4 张表) + Hive Metastore (`hive_metastore` 库) |
| 存储 | hadoop-namenode, hadoop-datanode-{1,2} | 3 | HDFS HA (单 NN + 2 DN) |
| 计算 | spark-master, spark-worker-1 | 2 | Spark 分布式计算 |
| 查询 | hive-server-{1,2} | 2 | HiveServer2 (:10000), 共享 MySQL metastore |
| 存储 | hbase-master, hbase-regionserver-{1,2} | 3 | HBase NoSQL |
| 消息 | kafka-{1,2} | 2 | KRaft 模式 (不依赖 ZK) |
| 协调 | zookeeper-{1,2,3} | 3 | 仅供 HBase 用 |
| 应用 | demo-backend | 1 | FastAPI 后端 (端口 8000) |

### 1.2 后端服务

`demo-backend` 容器内的 FastAPI 应用按职责拆分为 8 个 router：

```
app/backend/
├── main.py                  # 入口，包含所有 router
├── config.py                # 端口/连接配置
├── routers/
│   ├── overview.py          # /api/overview/*  (KPI/时序/排行)
│   ├── attractions.py       # /api/attractions/*  (景点 CRUD)
│   ├── visitors.py          # /api/visitors/*    (游客 CRUD)
│   ├── consumption.py       # /api/consumption/*  (消费+游玩)
│   ├── analysis.py          # /api/analysis/*   (日/小时/地区/类型)
│   ├── predict.py           # /api/predict/*    (模型预测)
│   ├── predict_tourism.py   # /api/predict-tourism/*  ⭐ 场景化预测
│   ├── realtime.py          # /api/realtime/*   (Kafka/HBase)
│   └── admin.py             # /api/admin/*     (容器/模型/任务)
└── services/
    ├── mysql_service.py     # MySQL 查询 (query/_query_df)
    ├── hive_service.py      # 复杂分析查询
    ├── hbase_service.py     # HBase 读写
    ├── kafka_producer.py    # Kafka 发布
    ├── kafka_consumer.py    # Kafka 消费 (后台线程)
    ├── model_service.py     # sklearn joblib 推理
    ├── auto_train.py        # 模型自动训练
    ├── pyspark_loader.py    # PySpark 推理 (双轨模式)
    └── admin_service.py     # 异步任务编排
```

---

## 2. 数据流与处理链路

整体数据流：

```
[CSV 原始数据]
  ↓ load-csv-to-mysql.py
[MySQL scenic 库: t_attraction/t_visitor/t_consumption/t_visit_record]
  ↓ Sqoop import (m=1)
[HDFS /scenic/sqoop/* Parquet]
  ↓ Spark clean.py (清洗)
[HDFS /scenic/cleaned/* Parquet]
  ↓ Spark train.py (MLlib)
[sklearn joblib 模型 /shared/models/sklearn/*.pkl]
  ↓ FastAPI 模型服务
[前端 predict.html 调用]
```

并发实时链路：

```
[前端实时流页面]
  ↓ POST /api/realtime/task/trigger
[Kafka topic: scenic_events / scenic_reviews]
  ↓ 后台 Kafka Consumer 线程
[HBase table: scenic_realtime]
  ↓ GET /api/realtime/visit-recent
[前端实时表格展示]
```

---

## 3. 数据采集与存储

### 3.1 CSV 原始数据 (data/raw_data/)

4 张表，CSV 格式，UTF-8 编码，中文字段名：

| 文件 | 行数 | 主键 | 字段 |
|---|---|---|---|
| attractions.csv | 10 | 景点ID | 景点ID, 景点名称, 类型, 位置, 开放时间 |
| visitors.csv | 10,000 | 游客ID | 游客ID, 姓名, 性别, 年龄, 地区 |
| consumption.csv | 100,000 | 消费ID | 消费ID, 时间, 游客ID, 景点ID, 消费金额 |
| visit_records.csv | 100,000 | 记录ID | 记录ID, 时间, 游客ID, 景点ID, 游玩时长 |

### 3.2 MySQL 业务库

启动时通过 `mysql-init/01-init-business.sql` 自动建表 (4 张表, 100,000+ 行)。

### 3.3 Sqoop 导入 HDFS

`app/jobs/sqoop-import-mysql.sh`：

```bash
sqoop import \
  --connect jdbc:mysql://mysql:3306/scenic \
  --username root --password root123 \
  --table t_attraction --m 1 --delete-target-dir \
  --target-dir hdfs://hadoop-namenode:9000/scenic/sqoop/t_attraction
# (类似命令导入 t_visitor, t_consumption, t_visit_record)
```

---

## 4. Spark 数据清洗 / 预处理

**文件**: `app/jobs/spark/clean.py`  
**执行**: `spark-submit --master spark://spark-master:7077 /opt/jobs/spark/clean.py`

**输入**: HDFS `/scenic/sqoop/*` (Sqoop 导出的原始 Parquet)  
**输出**: HDFS `/scenic/cleaned/*` (清洗后 Parquet)

### 4.1 清洗规则 (4 张表分别执行)

| 表 | 规则 |
|---|---|
| t_attraction | 1. 去重  2. 过滤 _c0 主键为空 |
| t_visitor | 1. 去重  2. 过滤 _c4 主键为空  3. 年龄 ∈ [0, 120] |
| t_consumption | 1. 去重  2. 过滤 _c2/_c4 空  3. 金额 > 0 |
| t_visit_record | 1. 去重  2. 过滤 _c4/_c1 空  3. 时长 > 0 且 < 24h |

### 4.2 字段重命名 (Sqoop 列顺序调整)

```
_sqoop 实际列顺序: _c0=位置, _c1=开放时间, _c2=景点ID, _c3=景点名称, _c4=类型
```

### 4.3 派生字段

| 表 | 派生字段 | 规则 |
|---|---|---|
| t_visitor | `age_group` | <18 未成年 / <30 青年 / <45 中年 / <60 中老年 / 其他 老年 |
| t_consumption | `consume_level` | <100 低消费 / <500 中消费 / <1000 高消费 / 其他 超高 |
| t_consumption | `consume_date` | `to_date(consume_time)` |
| t_visit_record | `visit_date` | `to_date(visit_time)` |

### 4.4 清洗效果

```
t_attraction      : 10 行 (无变化)
t_visitor         : 10,000 行
t_consumption     : 100,000 行
t_visit_record    : 100,000 行
```

> **数据真实性补充修复**: `hive_service.py:hourly_distribution()` JOIN `t_attraction` 后按 开放时间 字段过滤，只统计 HOUR(时间) 在该景点开放区间内的记录。修复后 0:00-5:00 / 22:00-23:00 显示 0 游客 (符合现实)。同时 `type_summary()` 改为分阶段查询避免大表 JOIN 超时。

---

## 5. Hive 表设计与查询

**文件**: `app/jobs/hive/{ddl.sql, views.sql, queries.sql}`

**架构** (关键设计):
- `mysql` 容器 (mysql:5.7) 同时承担业务库 + Hive Metastore
- `hive-server-1` 首次启动跑 `schematool -dbType mysql -initSchema` (sentinel 文件幂等)
- `hive-server-2` 等 hive-server-1 就绪后启动 (共享同一 metastore, HS2 多实例 HA)
- 后端 `hive_service.py` 用 `pyhive` 直连 `hive-server-1:10000`，**无 fallback**

**为什么 MySQL 5.7 而不是 8.0**:
- DataNucleus 4.2 (绑死在 Hive 3.1.3) 生成的 DDL 含 `DEFAULT CHARACTER SET charset_name`
- MySQL 8.0 不再支持该语法 (hive/applications 一直未升级到 DataNucleus 5+)
- 5.7 是 Apache 官方测试矩阵里最稳的版本

### 5.1 DDL (4 张 Parquet 外表)

```sql
CREATE DATABASE IF NOT EXISTS scenic_ext;

CREATE EXTERNAL TABLE scenic_ext.ext_t_attraction (
    attraction_id   STRING,
    attraction_name STRING,
    attraction_type STRING,
    location        STRING,
    open_time       STRING
) STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:9000/scenic/cleaned/t_attraction';
-- 类似 ext_t_visitor, ext_t_consumption, ext_t_visit_record
```

### 5.2 视图 (views.sql)

- `v_attraction_summary`: 景点总收入/总游客/平均时长
- `v_daily_visits`: 每日游客量
- `v_high_value_visitors`: 高消费游客 (≥1000)
- `v_attraction_hourly_heat`: 景点时段热度

### 5.3 复杂查询 (queries.sql)

8 类查询：景点日均游客/高消费 Top20/时段热度/RANK 排行/年龄消费偏好/地区客单价/月度趋势/周末对比/消费 Top10 景点

---

## 6. 机器学习模型训练

### 6.1 训练特征 (6 维)

```
[age, purchase_count, avg_amount, visit_count, avg_duration, unique_attractions]
```

- `age` - 游客年龄
- `purchase_count` - 历史消费笔数
- `avg_amount` - 历史平均消费金额
- `visit_count` - 历史游玩次数
- `avg_duration` - 平均游玩时长 (h)
- `unique_attractions` - 去过的景点数

### 6.2 训练流程 (PySpark MLlib)

**文件**: `app/jobs/ml/train.py`

1. **数据准备** (`Spark 4 张 Parquet` → `特征 DataFrame`)
   - 从 `t_visitor` 取 `age`
   - 从 `t_consumption` 聚合 → `total_amount`, `purchase_count`, `avg_amount`
   - 从 `t_visit_record` 聚合 → `visit_count`, `avg_duration`, `unique_attractions`
   - 派生 `high_value_label = (total_amount > 500)`

2. **Pipeline 预处理**
   ```python
   VectorAssembler → StandardScaler → (模型)
   ```

3. **训练 4 类模型 (sklearn 兼容 joblib)**
   - 回归: `Linear / Lasso / Ridge / RandomForest` (预测 `total_amount`)
   - 聚类: `KMeans (k=4)` (silhouette=0.3644)
   - 分类: `RandomForest / DecisionTree / GBT / LogisticReg` (预测 `is_repeat_visitor = (visit_count >= median)`，仅用 3 个非相关特征避免数据泄漏)

4. **保存为 joblib**
   - `/shared/models/sklearn/regression_{linear,lasso,ridge,rf}.pkl`
   - `/shared/models/sklearn/clustering_kmeans.pkl`
   - `/shared/models/sklearn/classification_{rf,dt,gbt,lr}.pkl`

### 6.3 模型性能 (测试集, 真实指标 — 修复了数据泄漏)

**数据泄漏修复历史**：
- v1: 包含 `purchase_count` + `avg_amount` 标签=`(total_amount > 500)` → 因 `total_amount = purchase_count × avg_amount` 直接计算，`Acc=1.0`
- v2: 移除 `purchase_count`/`avg_amount`，但保留 `visit_count` → `Acc=0.99+`
- **v3 (当前)**: 移除 `visit_count`，标签改为 `is_repeat_visitor = (visit_count >= median)` → 真实准确率 0.77-0.79, AUC=0.86-0.88

| 类别 | 模型 | 测试集指标 |
|---|---|---|
| 回归 | Linear | RMSE=320.89, R²=0.9694 |
| 回归 | Lasso  | RMSE=320.91, R²=0.9694 |
| 回归 | Ridge  | RMSE=320.92, R²=0.9694 |
| 回归 | RF     | RMSE=719.38, R²=0.8463 |
| 聚类 | KMeans k=4 | silhouette=0.3644 |
| 分类 | RF     | Acc=0.7834, F1=0.7914, AUC=0.8750 |
| 分类 | DT     | Acc=0.7762, F1=0.7888, AUC=0.8654 |
| 分类 | GBT    | Acc=0.7813, F1=0.7933, AUC=0.8763 |
| 分类 | LR     | Acc=0.7854, F1=0.7776, AUC=0.8753 |

### 6.4 FPGrowth 关联规则 (app/jobs/ml/fpgrowth.py)

- 输入: `t_consumption` 按 visitor_id 分组 → 每个游客的 `attraction_id` 列表
- `FPGrowth(itemsCol="items", minSupport=0.02, minConfidence=0.3)`
- 输出: `/shared/models/fpgrowth_rules.json` (5010 条规则)
- 视图: Sankey 图 (按 lift × support 聚合，类型着色，去除环)

### 6.5 推理架构 (毫秒级 sklearn)

```
前端 → POST /api/predict {type, features}
        ↓
    FastAPI (demo-backend)
        ↓
    model_service.predict(task, features)
        ↓
    joblib.load("regression_ridge.pkl")
        ↓ (内存中)
    sklearn Pipeline.predict(features)  # 0-10ms
        ↓
    返回 {prediction, model, engine, elapsed_ms}
```

无 PySpark 依赖。`demo-backend` 容器只装 `joblib + scikit-learn + numpy`。

---

## 7. 实时数据流

### 7.1 架构

```
🌐 浏览器 (realtime.html)
   ↓ POST /api/realtime/task/trigger
   body: {task_type, count, attraction_id}
   ↓
📨 Kafka Producer (kafka_producer.py)
   topic: scenic_events (event_type: enter/exit/consume)
   topic: scenic_reviews (rating + comment)
   ↓
⚙️ Kafka Consumer (kafka_consumer.py, 后台 daemon 线程)
   group: smart-scenic-backend
   ↓
💾 HBase (hbase_service.put_realtime_event / put_review)
   table: scenic_realtime (row_key: E{ts}_{vid}_{aid})
   table: scenic_reviews (row_key: {aid}_{ts}_{vid})
   ↓
🔍 前端 GET /api/realtime/visit-recent → 表格展示
```

### 7.2 任务类型 (TaskTriggerIn)

| task_type | 行为 |
|---|---|
| `random_events` | 随机混合 enter/exit/review |
| `consume_burst` | 每个游客: enter + consume + exit 套餐 |
| `review_flood` | 大量 review (评分 3-5) |

### 7.3 HBase Schema

**scenic_realtime**:
- `V{vid:08d}` 游客最近活动 (cf: total_visits, last_attr, last_ts)
- `A{aid:04d}` 景点最近访客 (cf: visitor_id, last_ts)
- `E{ts}_{vid}_{aid}` 事件流水 (cf: event_type)

**scenic_reviews**:
- `{aid}_{ts}_{vid}` 评论 (cf: rating, comment)

### 7.4 Kafka 不可用时的处理

Kafka 不可用时直接返回 `503 (Kafka publish failed)`。
**不做降级直写 HBase** — 项目要求"真正跑起来"，Kafka 链路是端到端可观测的必经之路。
故障通过 manage.html → 系统管理 → Kafka 卡片可见（绿色=正常），或 `docker compose logs kafka-1`。

---

## 8. 场景化预测 API

**文件**: `app/backend/routers/predict_tourism.py`

### 8.1 API 列表

| 端点 | 用途 | 算法 |
|---|---|---|
| `GET /attraction-forecast` | 10 景点 × 明日客流量 | 7日均 + 周末因子 |
| `GET /attraction-recommend?attraction_id=X` | 游玩 A 后最常去的下一个景点 | MySQL 序列分析 (6 小时内) |
| `GET /route-recommend?type=X&budget=X&hours=X` | 智能游玩路线 | 类型过滤 + 贪心选路线 |
| `GET /visitor-profile/{vid}` | 游客画像 (消费/高价值/群体/偏好/建议) | ML 4 模型组合 |
| `GET /tomorrow-summary` | 聚合 KPI (昨日/明日/变化/最热) | - |
| `GET /multi-day-forecast?days=7` | 未来 7/14/30 天每日总客流 | 日均 × 星期因子 × 月因子 × 趋势 × 噪声 |
| `GET /fpgrowth-sankey?limit=20` | FPGrowth Sankey 图数据 | lift × support 聚合 + 去环 |

### 8.2 客流预测算法详解

```python
# 未来 N 天每日预测 = 基础日均 × 近期趋势 × 星期因子 × 月因子 × 噪声
基础日均 = 该景点最近 90 天日均游客
近期趋势 = 最近 7 天日均 / 之前 30 天日均 (限幅 0.7-1.3)
星期因子 = (景点, 星期) 历史均值 / 整体均值
月因子   = (景点, 月份) 历史均值 / 整体均值
噪声     = 1.0 ± uniform(0, 0.08)
```

### 8.3 游客画像

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

## 9. 前端可视化

### 9.1 页面结构 (5 个 HTML)

| 页面 | URL | 内容 |
|---|---|---|
| 总览大屏 | index.html | 8 KPI + 4 ML 预测 + 7 图表 (游客/消费/排行) |
| 数据分析 | analysis.html | 6 图表 + FPGrowth Sankey |
| 模型预测 | predict.html | 4 场景化卡 + 真实 vs 预测折线 + 模型对比 |
| ⚡ 实时流 | realtime.html | 引擎状态 + 数据流图 + 触发任务 + HBase 验证 |
| 业务管理 | manage.html | 景点/游客/消费/游玩 + 系统管理 |

### 9.2 设计风格

- **高对比深色主题**: 主色 `#0a0e27` / `#131a3a` / `#1a2350`，强调色 `#00d4ff` (青) / `#a855f7` (紫) / `#10b981` (绿)
- **无白色背景**: 所有 card / 表头 / 状态块用深色
- **ECharts 5.4.3**: 折线 / 柱状 / 饼图 / Sankey / 散点
- **响应式**: 1280px 桌面优先，移动端降级
- **缓存控制**: `?v=21` 等版本号强制刷新

### 9.3 关键图表

- **景点明日客流量预测**: bar 图对比 昨日实际 vs 预测明日，10 景点排序
- **多日客流预测 (7/30 天)**: 总客流柱状图 (周末紫/工作日青) + 各景点累计列表
- **真实 vs 预测对比**: 折线图 (蓝色真实 + 黄色虚线预测) + 分割日 markLine
- **FPGrowth 关联规则**: ECharts Sankey (类型着色) - 替代原表格
- **24h 时段分布**: 真实开放时间过滤 (0-5 时 0 游客，9-16 时 4000+ 峰值)

---

## 10. 如何运行

### 10.1 启动 Docker 集群

```bash
cd D:\Desktop\smart-scenic-bigdata
docker compose up -d
# 等待 ~30 秒所有容器 healthy
```

### 10.2 初始化数据 (一次性)

```bash
# 通过前端系统管理 tab 一键初始化
# 或手动:
docker exec demo-backend python3 -c "
import requests
requests.post('http://localhost:8000/api/admin/pipeline',
  json={'actions': ['load_csv', 'sqoop', 'spark_clean', 'spark_train']})
"
```

### 10.3 访问前端

```
http://localhost:8080
- 总览大屏:  http://localhost:8080/index.html
- 数据分析:  http://localhost:8080/analysis.html
- 模型预测:  http://localhost:8080/predict.html
- 实时流:    http://localhost:8080/realtime.html
- 业务管理:  http://localhost:8080/manage.html
```

### 10.4 关键命令

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

## 11. 关键技术点总结

1. **PySpark → sklearn 双轨推理**: 训练在 spark-master (PySpark MLlib)，推理在 demo-backend (joblib 加载 .pkl)，毫秒级响应，无 PySpark 依赖
2. **数据真实性修复**: 24h 时段分布按景点开放时间过滤，避免凌晨有游客的不真实数据
3. **Kafka Consumer 后台线程**: 不阻塞 API 请求，独立线程消费 → 写 HBase
4. **Kafka 失败直接报错 503**: 不绕过 Kafka (项目要求链路真实可观测)
5. **Sankey 去环**: FPGrowth 关联规则可能产生 A→B 和 B→A 的环，按权重排序时去除
6. **多日预测因子合成**: 基础日均 × 趋势 × 星期因子 × 月因子 × 噪声，简单但有效
7. **场景化预测设计**: 与景区业务深度结合的 ML (客流预测/智能推荐/路线规划/游客画像)，而非单纯的 6 特征输入

---

**完成日期**: 2026-06-30  
**GitHub**: https://github.com/2754LM/smart-scenic-bigdata
