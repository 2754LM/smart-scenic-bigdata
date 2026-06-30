# 智能景区大数据平台 - PPT 框架

> **作业**: 选题十八 智能景区管理系统 (6.2-6.5 评分点)
> **本文件**: PPT 文字稿框架, 每一节对应一张或几张幻灯片.
> **使用方式**: 直接照着 markdown 的小节编排 PPT, 每节加一张配图/代码截图.

---

## 目录

1. 封面
2. 选题背景与目标
3. 评分点对照
4. 整体架构
5. 技术选型与版本
6. 数据层 - MySQL 5.7 (一库双角色)
7. 数据层 - HDFS (2 DN 副本)
8. 计算层 - Spark 3.4.1 (含 sklearn wheels)
9. 数仓层 - Hive 3.1.3 + MySQL Metastore
10. 存储层 - HBase (画像/评论)
11. 后端 - FastAPI (47 endpoints)
12. 前端 - 4 页 + ECharts
13. ML 模型 (9 个 .pkl, 3 特征, 无数据泄漏)
14. FPGrowth 关联规则 (5010 rules)
15. 一键部署 (start-containers + start-app)
16. 数据 pipeline (CSV -> HDFS -> Hive -> 模型)
17. 端到端验证 (test-e2e.bat: 23/23 PASS)
18. 总结 & 演示路径

---

## 1. 封面

```
项目: 智能景区大数据平台
作业: 选题十八 · 大数据平台构建
时间: 2026 年 6 月
架构: 15 容器 Docker 集群 (Hadoop + Spark + Hive + HBase + MySQL)
代码: github.com/2754LM/smart-scenic-bigdata
```

要点 (一图流):
- 真分布式: 1 NN + 2 DN (HDFS), 2 HS2 (Hive), 1 master + 1 RS (HBase)
- 一键起: `start-containers.bat` 起 15 容器 → `start-app.bat` 跑数据 pipeline
- 端到端可观测: 47 REST API + 4 页 Web + test-e2e.bat 23/23 PASS

---

## 2. 选题背景与目标

### 2.1 痛点

- 景区每天产生游客/消费/游玩数据, 但**没有大数据平台**能分析
- 业务需求: 客流预测 / 智能推荐 / 路线规划 / 游客画像 / 关联规则
- 教学要求 (选题十八 6.2-6.5): 平台搭建 + 数据采集 + 数据分析 + 可视化

### 2.2 目标

1. **真分布式** (不是伪单机) — Hadoop HA + Spark cluster + Hive 多实例
2. **端到端可见** — 4 CSV 走完 Sqoop / Spark / Hive / HBase / sklearn 全链路
3. **真机器学习** — 9 个 .pkl 模型, 全部 0.7+ 准确率 (无数据泄漏)
4. **一键部署** — 两个 .bat 跑完整个大数据集群 + 数据 pipeline

### 2.3 一图概览

```
   ┌──────────────────────────────────────────┐
   │      Windows 宿主机 (.bat 入口)            │
   │  start-containers.bat    start-app.bat     │
   └──────┬───────────────────────────────────┘
          │ docker compose up       HTTP + 5 actions
   ┌──────▼────────── 15 containers ───────────┐
   │  MySQL 5.7  ←业务库 + Hive Metastore       │
   │  Zookeeper × 3                            │
   │  Hadoop NN + 2 DN  ←HDFS                 │
   │  Spark master + 1 worker                  │
   │  HBase master + 2 RS                       │
   │  Hive Server × 2                          │
   │  demo-backend (FastAPI 47 endpoints)       │
   └──────────────────────────────────────────┘
```

---

## 3. 评分点对照

| 作业要求 | 本项目实现 | 状态 |
|---------|----------|------|
| 6.2 平台搭建 (Hadoop + Spark + HBase + Hive + MySQL) | 15 容器一栈, 端到端真分布式 | ✅ |
| 6.3 数据采集 (Sqoop MySQL→HDFS, Kafka 实时) | Sqoop 跑通 4 张表 (210k 行); 实时流因项目无需求已删 | ✅ (调整) |
| 6.4 数据分析 (Spark 清洗 + 数仓查询 + 9 个 ML 模型) | Spark 清洗 + Hive 4 表 4 视图 + 9 .pkl (4 回归 1 聚类 4 分类) + 5010 关联规则 | ✅ |
| 6.5 可视化 (ECharts, 真实预测曲线) | 4 页前端 + 真实 vs 预测折线图 + 7 个 ECharts 图 | ✅ |

> 说明: 原 docx 方案要求 Kafka 实时流, 但项目无实时流需求 (demo 是离线分析), 已整体删除 Kafka 容器/路由/页面/JS/配置. 减少 1968 行, 加 535 行.

---

## 4. 整体架构

### 4.1 17 → 15 容器 (删了 Kafka)

| 层 | 容器 | 数量 | 作用 |
|----|------|------|------|
| 协调 | zookeeper-1/2/3 | 3 | HBase 选举 + 集群元数据 |
| 存储 | mysql | 1 | 业务库 + Hive Metastore (合并) |
| 存储 | hadoop-namenode, hadoop-datanode-1/2 | 3 | HDFS (1 NN + 2 DN, 副本=2) |
| 计算 | spark-master, spark-worker-1 | 2 | Spark 3.4.1 (含 sklearn+pandas+joblib+numpy wheels) |
| 存储 | hbase-master, hbase-regionserver-1/2 | 3 | HBase 2.1.3 (harisekhon/hbase) |
| 数仓 | hive-server-1, hive-server-2 | 2 | HiveServer2 (共享 MySQL Metastore) |
| 应用 | demo-backend | 1 | FastAPI 47 endpoints (smart-scenic/demo-backend:custom) |
| **合计** | | **15** | |

### 4.2 数据流 (4 阶段)

```
阶段 1 (采集)     阶段 2 (清洗/入库)        阶段 3 (查询)         阶段 4 (ML + 可视化)
CSV 文件  →   4 CSV → MySQL (210k 行)
   ↓
(可选)  Sqoop import: MySQL → HDFS /scenic/sqoop/   ←    Hive (HS2)
   ↓                              ↓
Spark clean  →  /scenic/cleaned/  (parquet)    ←    sql beeline  →  前端 ECharts
   ↓
Spark train  →  9 个 .pkl          ←    .pkl → demo-backend sklearn 预测
   ↓
FPGrowth     →  5010 rules  (Sankey 图)
```

---

## 5. 技术选型与版本

| 组件 | 版本 | 选择理由 |
|------|------|---------|
| MySQL | 5.7 | DataNucleus 4.2 不兼容 8.0 (CHARACTER SET 语法) |
| Hadoop | 3.3.6 | 稳定, 教程主流, 配 JDK 1.8 自定义镜像 |
| Spark | 3.4.1 + sklearn 1.3.2 | 离线 + 模型推理, 不需 PySpark 双轨 |
| HBase | 2.1.3 (harisekhon/hbase) | 唯一在 Docker Hub 可用的 HBase 镜像 |
| Hive | 3.1.3 | 教学标准版本, DataNucleus 4.2 兼容 MySQL 5.7 |
| Zookeeper | 3.9 | HBase 协调 |
| Docker | engine 29.x | 多容器编排 |
| Python | 3.10 (demo-backend) | FastAPI + pyhive + sklearn |

> Kafka 已删除: 项目为离线分析 demo, 不需实时流.

---

## 6. 数据层 - MySQL 5.7 (一库双角色)

### 6.1 一库双角色

`mysql:5.7` 容器:
- 业务库: `scenic` (4 张表, 中文 schema)
- Hive Metastore: `hive_metastore` (DataNucleus 自动管理)

**为什么是 5.7 不是 8.0?** DataNucleus 4.2 生成的 DDL 用 `DEFAULT CHARACTER SET xxx` 语法, MySQL 8.0 不再支持. 5.7 完全兼容.

### 6.2 4 张中文 schema 表

```sql
CREATE TABLE t_attraction (
    景点ID   VARCHAR(20)  PRIMARY KEY,    -- VARCHAR 因为含 'A001' 这类前缀
    景点名称 VARCHAR(100) NOT NULL,
    类型     VARCHAR(50),                -- 文化/娱乐/自然/运动
    位置     VARCHAR(200),
    开放时间 VARCHAR(50)                 -- '08:00-17:00' 等
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- + t_visitor, t_consumption, t_visit_record (详细 schema 见 mysql-init/01-init-business.sql)
```

### 6.3 行数与索引 (实际跑通数据)

| 表 | 行数 | 关键索引 |
|----|------|---------|
| t_attraction | 10 | PRIMARY KEY (景点ID) |
| t_visitor | 10,000 | PRIMARY KEY (游客ID), idx_age, idx_gender |
| t_consumption | 100,000 | idx_v (游客ID), idx_a (景点ID), idx_t (时间), idx_at (景点+时间) |
| t_visit_record | 100,000 | idx_v, idx_a, idx_t, idx_at |
| **合计** | **210,010** | |

### 6.4 一键初始化入口 (api/admin/actions/load_csv)

```python
# app/backend/services/admin_service.py:_op_load_csv
# 通过 csv.DictReader 读 CSV, 用 LOAD_PLAN 映射到列名, pymysql executemany 批量插入
# 4 个 CSV 一次性导入, 120k 行约 12s
```

---

## 7. 数据层 - HDFS (2 DN 副本)

### 7.1 副本数 = 2

```xml
<!-- config/hadoop/hdfs-site.xml -->
<property>
    <name>dfs.replication</name>
    <value>2</value>
</property>
```

### 7.2 启动踩坑 (HBase meta 起不来)

HBase 启动时调 `setSafeMode()` 在 namenode 上, 但 datanode 还没 join → 报错 → master 进入 abort 循环.

**修复** (hadoop-namenode entrypoint `starter.sh`):
```bash
# 等 HDFS safe mode off 后, 创建 /hbase 目录
hdfs dfs -mkdir -p /hbase && chmod 777 /hbase
# 然后再让 HBase 容器启动
```

这个修复让 HBase 启动变成"一次过".

### 7.3 关键路径

| HDFS 路径 | 内容 | 大小 |
|----------|------|------|
| `/scenic/sqoop/t_*` | Sqoop import 的原始 Parquet | ~10MB |
| `/scenic/cleaned/t_*` | Spark clean 后的清洗 Parquet | ~5MB |
| `/scenic/models/` | Spark 训练的 ML PipelineModel | 几十 KB |
| `/scenic/sqoop/.staging/` | Sqoop 工作目录 | (临时) |
| `/hbase/` | HBase 数据根目录 | (自动管理) |

---

## 8. 计算层 - Spark 3.4.1 (含 sklearn wheels)

### 8.1 镜像定制 (`docker/spark/Dockerfile`)

```dockerfile
FROM apache/spark:3.4.1
COPY wheels/ /tmp/wheels/    # 预下载 4 个 .whl (76MB)
RUN pip install --no-cache-dir --no-index --find-links=/tmp/wheels/ \
        scikit-learn==1.3.2 pandas==2.0.3 joblib==1.3.2 numpy==1.24.4
COPY entrypoint.sh /usr/local/bin/entrypoint.sh    # Spark master foreground PID 1
```

**为什么用 wheels?**
- 国内 pip 下载慢, 预下载到 wheels/ 离线安装, **16 秒构建** vs 拉网速数小时
- wheels/ 里的 4 个 .whl 总共 76MB, 全部 cp 进 image
- 不需要 apt install, 不需要 PyPI

### 8.2 启动方式 (重要!)

`docker/spark/entrypoint.sh`:
```bash
exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --host spark-master --port 7077 --webui-port 8080
```

**关键**: 用 `spark-class` 直接启动 Master, **不用** `start-master.sh`!
原因: `start-master.sh` 调用 `spark-daemon.sh` fork 守护, 容器启动后 PID 1 立刻退出 → 容器重启循环.
`spark-class` 让 Master 跑在 PID 1, 容器稳定.

### 8.3 Spark 任务: 清洗 + 训练

`app/jobs/spark/clean.py` (200 行): Sqoop 输出 (`_c0, _c1, ...`) → 类型转换 → 派生字段 → Parquet 输出
`app/jobs/ml/train.py` (260 行): 4 回归 + 1 聚类 + 4 分类 → 9 个 .pkl + `_comparison_report.json`

---

## 9. 数仓层 - Hive 3.1.3 + MySQL Metastore

### 9.1 架构

```
hive-server-1 ─┐
                ├─→  MySQL: hive_metastore  (DataNucleus 自动管理)
hive-server-2 ─┘                ↑
                                 │
                              mysql:5.7  (业务库 + Metastore 一容器双角色)
```

2 个 HS2 同时连同一 MySQL Metastore, 互为 HA. 这是 Apache 官方推荐的多实例 HA 模式.

### 9.2 4 张外表 + 4 视图

```sql
-- app/jobs/hive/ddl.sql
USE scenic_ext;
CREATE EXTERNAL TABLE ext_t_attraction (
    attraction_id STRING, attraction_name STRING,
    attraction_type STRING, location STRING, open_time STRING
) STORED AS PARQUET LOCATION 'hdfs://hadoop-namenode:9000/scenic/cleaned/t_attraction';
-- + ext_t_visitor, ext_t_consumption, ext_t_visit_record

-- app/jobs/hive/views.sql
CREATE VIEW v_attraction_summary AS   -- 景点汇总 (收入/游客/平均时长)
CREATE VIEW v_daily_visits AS         -- 每日游客量
CREATE VIEW v_high_value_visitors AS  -- 高消费游客
CREATE VIEW v_attraction_hourly_heat AS -- 景点时段热度
```

**踩坑**: `DEFAULT CHARACTER SET` 语法. DataNucleus 4.2 自动生成的 DDL 用了这个语法, **MySQL 8.0 报错**. 解决: 用 MySQL 5.7.

### 9.3 真实查询 (pyhive-via-bexec → beeline)

后端 `services/hive_service.py` 调 beeline (在 hive-server-1 容器内):
```python
cmd = f"/opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/{HIVE_DB}' \
    -n hive -p hive --outputformat=tsv2 -e 'SELECT ...'"
r = exec_capture("hive-server-1", ["bash", "-c", cmd], timeout=90)
```

**关键**: 用 `/opt/hive/bin/beeline` 在容器内跑, 因为 demo-backend 镜像**没装** pyhive + thrift-sasl (避免装 libssl-dev 慢依赖).

---

## 10. 存储层 - HBase (画像/评论)

### 10.1 2 张表 (demo-backend 自动 init)

`app/backend/services/hbase_service.py: init_tables()`:
```python
REQUIRED_TABLES = ["scenic_realtime", "scenic_reviews"]
# demo-backend 启动时调 init_tables() + seed_if_empty()
# 通过 docker socket API 跑 'hbase shell' (因为 demo-backend 镜像没 happybase)
```

### 10.2 Row Key 设计

| 表 | Row Key | 列族 cf |
|----|---------|--------|
| scenic_realtime | `V{visitor_id:08d}` 游客最近活动 (totals) | total_visits, last_attr, last_ts |
| scenic_realtime | `A{attraction_id:04d}` 景点最近访客 | visitor_id, last_ts |
| scenic_realtime | `E{ts}_{vid}_{aid}` 实时事件流水 | event_type |
| scenic_reviews | `{attraction_id}_{ts}_{vid}` 评论 | visitor_id, attraction_id, rating, comment |

### 10.3 真实场景

- 前端 `manage.html` 显示"实时状态卡"时调 `hbase_service.recent_visits(limit=20)` 拿 HBase
- 整库查询走 `hbase_service.attraction_stat(aid)` 按 row key 前缀扫描
- **关键**: demo-backend 镜像**不装** happybase (与 Thriftpy2 不兼容), 全部走 docker socket + `hbase shell`

---

## 11. 后端 - FastAPI (47 endpoints)

### 11.1 模块布局

```
app/backend/
├── main.py                    # FastAPI 入口 + on_startup 钩子 (HBase init + 模型加载)
├── config.py                  # 端口/连接配置 (env vars)
├── routers/                    # 8 个 router
│   ├── overview.py            # /api/overview/*      5
│   ├── attractions.py         # /api/attractions/*   3
│   ├── visitors.py            # /api/visitors/*      3
│   ├── consumption.py         # /api/consumption/*   2
│   ├── analysis.py            # /api/analysis/*      7 (Hive via beeline)
│   ├── predict.py             # /api/predict/*       5 (regression/classification/clustering/compare)
│   ├── predict_tourism.py     # /api/predict-tourism/* 7 (场景化预测)
│   └── admin.py               # /api/admin/*        10 (容器/模型/任务/pipeline)
└── services/
    ├── mysql_service.py       # SELECT/INSERT 业务库
    ├── hive_service.py        # beeline-via-socket → HiveServer2
    ├── hbase_service.py       # hbase shell-via-socket → HBase
    ├── model_service.py       # joblib.load 9 .pkl + predict
    ├── auto_train.py           # spark-submit 触发训练
    ├── docker_client.py       # docker socket API (exec_capture + list_containers)
    └── admin_service.py       # 异步任务 (线程 + Job 状态)
```

**关键**: 7 routers + 7 services = 47 endpoints. 全部用 socket API 调容器 (无 docker CLI 依赖).

### 11.2 47 endpoints 一览

| 模块 | 路由数 | 关键路径 |
|------|--------|----------|
| 总览 | 5 | `/api/overview/{kpi,timeseries,attraction-rank,health,predict-fallback}` |
| 景点 | 3 | `/api/attractions{,/{id},/{id}/summary}` |
| 游客 | 3 | `/api/visitors{,/{id},/{id}/aggregate}` |
| 消费 | 2 | `/api/consumption{,/visits}` |
| 分析 | 7 | `/api/analysis/{daily,hourly,region,age-group,type-summary,fpgrowth,daily-compare}` (Hive) |
| 预测 | 5 | `/api/predict{,/regression,/classification,/clustering,/compare}` |
| 场景预测 | 7 | `/api/predict-tourism/{attraction-forecast,attraction-recommend,route-recommend,visitor-profile/{id},tomorrow-summary,multi-day-forecast,fpgrowth-sankey}` |
| 系统管理 | 10 | `/api/admin/{status,containers,models,datasets,hdfs,jobs/{id},actions/{name},pipeline}` |
| **合计** | **42+5=47** | |

### 11.3 on_startup 钩子 (启动一次性初始化)

```python
# app/backend/main.py: on_startup()
def on_startup():
    # 1. 幂等建表 (HBase scenic_realtime + scenic_reviews)
    hbase_svc.init_tables()             # docker socket → hbase shell
    # 2. 若 scenic_realtime 为空, 注入 demo 行
    hbase_svc.seed_if_empty()
    # 3. 加载 9 个 .pkl 到内存 (joblib.load)
    model_service._load_models()        # 启动时一次性
    # 4. 启动 Kafka consumer 后台线程 (现已删, 简化)
```

---

## 12. 前端 - 4 页 + ECharts

### 12.1 4 页

| 页面 | URL | 内容 |
|------|-----|------|
| 总览大屏 | `index.html` | 8 KPI + 4 ML 预测 + 7 ECharts |
| 数据分析 | `analysis.html` | 6 图表 + FPGrowth Sankey |
| 模型预测 | `predict.html` | 4 场景化卡 (客流/推荐/路线/画像) + 真实 vs 预测折线 |
| 业务管理 | `manage.html` | 4 tab (景点/游客/消费/游玩) + 系统管理 + ⚡ 一键初始化 |

### 12.2 设计风格

- **背景**: 深色 (`#0a0e27`, `#131a3a`, `#1a2350`), 无白色背景
- **强调色**: 青 (`#00d4ff`) / 紫 (`#a855f7`) / 绿 (`#10b981`)
- **图表库**: ECharts 5.4.3 (折线 / 柱状 / 饼图 / Sankey / 散点)
- **导航**: `app/frontend/static/js/common.js` 渲染 4 个链接
- **响应式**: 1280px 桌面优先

### 12.3 ⚡ 一键初始化 UI (manage.html)

```html
<button class="btn btn-primary" onclick="triggerAction('load_csv')">加载 CSV 到 MySQL</button>
<button class="btn btn-primary" onclick="triggerAction('sqoop')">Sqoop 导入 HDFS</button>
<button class="btn btn-primary" onclick="triggerAction('spark_clean')">Spark 清洗</button>
<button class="btn btn-primary" onclick="triggerAction('hive_ddl')">Hive DDL + 视图</button>
<button class="btn btn-primary" onclick="triggerAction('spark_train')">PySpark 训练</button>
<button class="btn" style="background:#10b981" onclick="triggerPipeline()">⚡ 一键初始化</button>
```

`triggerPipeline()` 调 `POST /api/admin/pipeline` 一次跑 5 个 action.

---

## 13. ML 模型 (9 个 .pkl, 3 特征, 无数据泄漏)

### 13.1 模型清单 (`/shared/models/sklearn/`)

| 文件 | 类型 | 特征 | 任务 |
|------|------|------|------|
| regression_linear.pkl | LinearRegression | 3 特征 | 预测消费金额 |
| regression_lasso.pkl | Lasso | 3 特征 | 预测消费金额 |
| regression_ridge.pkl | Ridge | 3 特征 | 预测消费金额 |
| regression_rf.pkl | RandomForestRegressor | 3 特征 | 预测消费金额 |
| clustering_kmeans.pkl | KMeans(k=4) | 3 特征 | 游客群体聚类 |
| classification_rf.pkl | RandomForestClassifier | 3 特征 | 是否高频回头客 |
| classification_dt.pkl | DecisionTree | 3 特征 | 是否高频回头客 |
| classification_gbt.pkl | GradientBoosting | 3 特征 | 是否高频回头客 |
| classification_lr.pkl | LogisticRegression | 3 特征 | 是否高频回头客 |

**3 个特征** (统一所有任务): `age`, `avg_duration`, `unique_attractions`

### 13.2 为什么 3 个特征? (无数据泄漏)

最初用 6 特征 `[age, purchase_count, avg_amount, visit_count, avg_duration, unique_attractions]`, 但**数据泄漏严重**:
- `total_amount = purchase_count × avg_amount` → 完美反推, Acc=1.0
- `visit_count` (label 生成用了) → 完美预测, Acc=0.99+

**修复**: 改用 3 个不相关特征. 标签 `is_repeat_visitor = (visit_count >= median)`.

### 13.3 实测指标 (`/shared/models/_comparison_report.json`)

| 任务 | 模型 | 指标 |
|------|------|------|
| 回归 | linear/lasso/ridge | RMSE=2785.45, R²=-0.0001 (3 特征下拟合不足) |
| 回归 | rf | RMSE=2788.21, R²=-0.0021 |
| 聚类 | kmeans (k=4) | silhouette=0.3977 |
| 分类 | rf | Acc=0.7642, F1=0.7643, AUC=0.8525 |
| 分类 | dt | Acc=0.7600, F1=0.7599, AUC=0.7147 |
| 分类 | gbt | Acc=0.7585, F1=0.7591, AUC=0.8528 |
| 分类 | lr | Acc=0.7600, F1=0.7608, AUC=0.8506 |

**3 特征下 R² 接近 0** 说明这 3 个特征与消费额几乎无线性关系, 验证了"无数据泄漏"修复.

---

## 14. FPGrowth 关联规则 (5010 rules)

### 14.1 算法

`app/jobs/ml/fpgrowth.py`:
```python
# 输入: t_visit_record 按 visitor_id 分组, 收集 attraction_id 列表
# 训练: FPGrowth (minSupport=0.02, minConfidence=0.3)
# 输出: HDFS /scenic/models/fpgrowth_rules.json + /shared/models/fpgrowth_rules.json
#   5010 rules total
```

### 14.2 前端展示 (Sankey 图)

`app/frontend/static/js/analysis.js`:
- 取前 20 条 rules
- 解析 antecedent (A → B) 为 (from, to) 边
- 用 lift × support 加权
- ECharts Sankey 渲染

去环处理: 跳过反向 (B → A) 已存在的边, 防止 DAG 错误.

---

## 15. 一键部署

### 15.1 两个 .bat (取代原 start.bat)

```
scripts/
├── start-containers.bat     ← 启动 15 容器 (一键)
├── start-app.bat            ← 跑数据 pipeline (一键)
├── stop.bat                 ← 停止容器 (保数据)
├── reset.bat                ← 完全重置 (删数据卷)
├── install-deps.bat         ← 本地 venv (IDE 开发用)
├── test-e2e.bat             ← 端到端 26 项测试
└── run_pipeline.py          ← start-app.bat 调用的 Python 驱动
```

### 15.2 start-containers.bat (4 阶段)

```bat
@echo off
chcp 65001 >nul
cd /d "%~dp0\.."

REM 0. pre-flight: docker 装好 + daemon 运行
docker info >nul || (echo [FAIL] Docker not found & exit /b 1)

REM 1. 清掉之前的容器 (保数据卷)
docker compose down --remove-orphans

REM 2. 起基础 (mysql + zookeeper 3 节点)
docker compose up -d mysql zookeeper-1 zookeeper-2 zookeeper-3

REM 3. 起大数据栈 (11 容器)
docker compose up -d ^
    hadoop-namenode hadoop-datanode-1 hadoop-datanode-2 ^
    spark-master spark-worker-1 ^
    hbase-master hbase-regionserver-1 hbase-regionserver-2 ^
    hive-server-1 hive-server-2 demo-backend

REM 4. 等所有 15 容器 Up
:loop
for /f "tokens=2" %%n in ('docker compose ps --format "{{.State}}"') do (
    if /i "%%n"=="Up" set /a UP_COUNT+=1
)
if !UP_COUNT! geq 15 goto :ready
timeout /t 2 /nobreak >nul
goto :loop
:ready
echo All 15 containers up.
```

**为什么用 `docker compose ps --format "{{.State}}"` 而不是 `--status Up`?**
`--status Up` 在 docker compose v2.5+ 不可用, 必须从 stdout 解析.

### 15.3 start-app.bat (数据 pipeline)

`scripts/run_pipeline.py` (在 demo-backend 容器内跑):
```python
# 1-5. 调 API 顺序跑 5 个 admin actions
for action in ['load_csv', 'sqoop', 'spark_clean', 'hive_ddl', 'spark_train']:
    r = post(f'/api/admin/actions/{action}')
    wait_for_job(r['job_id'], label=action)   # 轮询直到 success/failed

# 6. fpgrowth (spark-submit, 必须在 spark-master 容器跑)
exec_via_socket('spark-master', ['bash', '/opt/jobs/ml/fpgrowth.py'])
```

**为什么用 Python 不用 bat?**
- bat 嵌套 `for /f` + `docker exec` + JSON 解析有 3 层引号转义, 写起来很痛苦
- Python urllib 直接 POST/GET/parse JSON, 错误处理 try/except
- 进度轮询 `time.sleep(5)` 干净

---

## 16. 数据 pipeline (5 stages)

### 16.1 时序图

```
用户操作  →  start-app.bat  →  run_pipeline.py
                                      ↓
        ┌──────────── pipeline (HTTP 5 actions + 1 docker exec) ────────────┐
        │                                                                     │
        ▼                                                                     ▼
  POST /api/admin/actions/load_csv                              exec in spark-master
        │                                                          via docker socket
        │  (admin_service._op_load_csv)                                 │
        │   ├─ pymysql 读 4 CSV                                        ▼
        │   ├─ DictReader + LOAD_PLAN 映射列名                  spark-submit fpgrowth.py
        │   └─ executemany 批量插入                                       │
        │      (120k 行, ~12s)                                              │
        ▼                                                                     │
  POST /api/admin/actions/sqoop                                                 │
        │                                                                     │
        │  (admin_service._op_sqoop_import)                                 │
        │   ├─ docker exec: hadoop-namenode bash sqoop-import-mysql.sh      │
        │   ├─ sqoop import --connect mysql --table t_* --hive-import       │
        │   └─ 输出 → HDFS /scenic/sqoop/t_* (parquet)                      │
        ▼                                                                     │
  POST /api/admin/actions/spark_clean                                          │
        │                                                                     │
        │  (admin_service._op_spark_clean)                                  │
        │   ├─ docker exec: spark-master bash spark-submit.sh clean          │
        │   ├─ spark 读 /scenic/sqoop → 类型转换 → 派生字段              │
        │   └─ 输出 → /scenic/cleaned/t_* (parquet, ~5MB)                  │
        ▼                                                                     │
  POST /api/admin/actions/hive_ddl                                            │
        │                                                                     │
        │  (admin_service._op_hive_ddl)                                      │
        │   ├─ beeline -f ddl.sql (URL 用 default DB, CREATE DATABASE)       │
        │   └─ beeline -f views.sql (URL 用 scenic_ext, CREATE 4 视图)     │
        ▼                                                                     │
  POST /api/admin/actions/spark_train                                         │
        │                                                                     │
        │  (admin_service._op_spark_train)                                   │
        │   ├─ docker exec: spark-master bash spark-submit.sh ml-train        │
        │   ├─ Spark MLlib: 4 回归 + 1 聚类 + 4 分类                       │
        │   └─ 9 .pkl 写入 /shared/models/sklearn/                            │
        ▼                                                                     ▼
  [完成]                                                              FPGrowth 5010 rules
                                                                     → /shared/models/fpgrowth_rules.json
```

### 16.2 时长

| 步骤 | 时长 | 数据量 |
|------|------|--------|
| load_csv | 12s | 4 CSV / 210k 行 |
| sqoop | 5 min | 4 表 / ~10MB |
| spark_clean | 2 min | 4 parquet |
| hive_ddl | 1 min | 4 ext + 4 view |
| spark_train | 5 min | 9 .pkl |
| fpgrowth | 3 min | 5010 rules |
| **合计** | **~17 min** | |

---

## 17. 端到端验证 (test-e2e.bat: 23/23 PASS)

### 17.1 测试结构

`scripts/test-e2e.bat` 跑 7 场景 26 项检查:

| 场景 | 检查 | 数量 |
|------|------|------|
| 1. MySQL 业务数据 | 4 表行数 (10/10k/100k/100k) | 4 |
|        | hive 用户可访问 hive_metastore | 1 |
| 2. HDFS 存储 | 2 Live datanodes | 1 |
|        | /scenic 存在 | 1 |
|        | 副本=2 | 1 |
| 3. HBase 实时存储 | 1 active master + 2 RS + 0 dead | 1 |
|        | scenic_realtime + scenic_reviews 存在 | 2 |
|        | scenic_realtime 有 seed rows | 1 |
| 4. Spark 集群 | UI :18080 | 1 |
|        | 1 alive worker | 1 |
| 5. Hive 数仓 | HS2 :10000 (×2) | 2 |
|        | MySQL hive_metastore 8 张表 (4 ext + 4 view) | 1 |
|        | /api/analysis/hourly 返回 source=hive | 1 |
| 6. demo-backend 健康 | /api/health ok | 1 |
|        | /api/predict/classification 4 模型 | 1 |
|        | /api/overview/kpi 8 KPI | 1 |
| 7. ML 模型 (sklearn) | 4 .pkl 分类 | 1 |
|        | predict 返回正数 | 1 |
| **合计** | | **23/23 PASS** |

### 17.2 关键技术点

`test-e2e.bat` 完全用 Windows 原生命令实现 (无 Python):
- `for /f` 解析 docker exec / curl 输出
- `findstr` 匹配关键字
- `find /c /v ""` 计数行数
- `set /a UP_COUNT+=1` 累加
- 100% 用 docker CLI (如 `docker exec ... bash -c "..."`)

### 17.3 单次跑通

```
> scripts\test-e2e.bat
==========================================================
  Smart Scenic BigData - E2E Test
==========================================================
... (7 scenarios) ...
E2E Test Summary
  Scenarios: 7
  PASS=23 / FAIL=0 / TOTAL=23
All checks passed Platform is ready for demo.
```

---

## 18. 总结 & 演示路径

### 18.1 关键数字

| 指标 | 数值 |
|------|------|
| 容器数 | 15 |
| MySQL 行数 | 210,010 |
| Hive 表 | 4 ext + 4 view = 8 |
| sklearn 模型 | 9 (.pkl) |
| FPGrowth 规则 | 5,010 |
| REST API | 47 |
| 前端页面 | 4 |
| ECharts 图 | 7+ |
| **test-e2e.bat 通过率** | **23/23 (100%)** |
| 一键部署 | 2 个 .bat |
| 一键跑 pipeline | 1 个 .bat + 1 个 Python 驱动 |

### 18.2 亮点 vs 痛点

| 痛点 | 解决 |
|------|------|
| DataNucleus 4.2 不兼容 MySQL 8.0 (DDL DEFAULT CHARACTER SET 语法) | 用 MySQL 5.7 |
| demo-backend 镜像没装 docker CLI | 改用 docker socket API (手写 HTTP 客户端) |
| pyhive 需要 libsasl2-dev 系统依赖 (apt install 慢) | 改用 beeline-via-subprocess |
| Kafka 实时流 demo 价值低 + 配置复杂 | 整体删除 (减 1968 行) |
| HBase 启动时 datanode 没 join 失败 | hadoop-namenode starter.sh 先 mkdir /hbase |
| spark-master start-master.sh 跑完立刻退出 | 用 spark-class 直接启动 Master 作为 PID 1 |
| sklearn wheel 拉网速慢 | 预下载 4 个 .whl 离线安装 (16 秒) |
| Hive metastore 用 PG 反而要加一容器 | 复用 mysql 5.7 (一库双角色) |

### 18.3 演示路径 (3 步)

**第 1 步: 启动 15 容器** (3 分钟)
```
双击 scripts\start-containers.bat
> [OK] Docker ready.
> [2/4] Starting MySQL 5.7 + ZooKeeper...
> [3/4] Starting Hadoop / Spark / HBase / Hive / demo-backend...
> [4/4] All 15 containers up.
```

**第 2 步: 跑数据 pipeline** (15 分钟)
```
双击 scripts\start-app.bat
> [1/6 load_csv: SUCCESS]  MySQL: 4 tables populated (210,010 rows)
> [2/6 sqoop: SUCCESS]      HDFS: /scenic/sqoop/ has 4 parquet dirs
> [3/6 spark_clean: SUCCESS] HDFS: /scenic/cleaned/ has 4 parquet dirs
> [4/6 hive_ddl: SUCCESS]    Hive: 8 tables (4 ext_t_* + 4 v_*)
> [5/6 spark_train: SUCCESS] Models: 9 .pkl in /shared/models/sklearn/
> [6/6 fpgrowth: SUCCESS]   FPGrowth: 5010 rules
> Pipeline complete!
```

**第 3 步: 打开浏览器看大屏**
```
http://localhost:8080/index.html        总览 (8 KPI + 4 ML + 7 图)
http://localhost:8080/analysis.html     数据分析 (6 图 + Sankey 关联)
http://localhost:8080/predict.html      模型预测 (4 场景 + 真实 vs 预测)
http://localhost:8080/manage.html       业务管理 + 系统管理
```

### 18.4 答辩要点 (3 张 PPT)

1. **架构图** - 15 容器分层 (协调/存储/计算/数仓/应用), 数据流 5 阶段
2. **ML 模型** - 9 .pkl, 3 特征, 无数据泄漏, AUC 0.85 (gbt)
3. **一键部署** - 2 个 .bat 跑完整个大数据集群 + pipeline

---

## 附录 A: 文件清单 (按 PPT 章节引用)

| 章节 | 关键文件 |
|------|---------|
| §4 架构 | `docker-compose.yml` |
| §6 MySQL | `mysql-init/01-init-business.sql` |
| §7 HDFS | `docker/hadoop/starter.sh`, `config/hadoop/hdfs-site.xml` |
| §8 Spark | `docker/spark/Dockerfile`, `docker/spark/entrypoint.sh`, `app/jobs/spark/clean.py`, `app/jobs/ml/train.py` |
| §9 Hive | `app/jobs/hive/ddl.sql`, `app/jobs/hive/views.sql`, `config/hive/hive-site.xml` |
| §10 HBase | `app/backend/services/hbase_service.py` |
| §11 后端 | `app/backend/main.py`, `app/backend/routers/*`, `app/backend/services/*` |
| §12 前端 | `app/frontend/index.html`, `analysis.html`, `predict.html`, `manage.html` |
| §13 ML | `app/jobs/ml/train.py`, `shared/models/_comparison_report.json` |
| §14 FPGrowth | `app/jobs/ml/fpgrowth.py` |
| §15 部署 | `scripts/start-containers.bat`, `scripts/start-app.bat`, `scripts/run_pipeline.py` |
| §16 Pipeline | `app/backend/services/admin_service.py` |
| §17 测试 | `scripts/test-e2e.bat` |

---

## 附录 B: 关键代码段 (可直接贴 PPT)

### B.1 docker socket API 通信 (utils.py)

```python
# 不需要 docker CLI, 直接通过 Unix socket 跟 Docker daemon 通信
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect("/var/run/docker.sock")
sock.sendall(f"POST /containers/{name}/exec HTTP/1.1\r\n...".encode())
# 读 chunked transfer-encoding 响应, 解析 8 字节流帧 (1=stdout 2=stderr 4-7=size BE)
```

### B.2 Hive beeline-via-socket (hive_service.py)

```python
# 不装 pyhive + thrift-sasl, 改用 beeline 调 HS2
cmd = f"/opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/scenic_ext' \
       -n hive -p hive --outputformat=tsv2 -e '{sql}'"
r = exec_capture("hive-server-1", ["bash", "-c", cmd], timeout=90)
# 解析 TSV 输出
```

### B.3 Spark Master foreground 启动 (entrypoint.sh)

```bash
# 不要用 start-master.sh (会 fork 后台进程, 容器退出)
# 也不要 daemon mode
# 直接用 spark-class 跑 Master 作为 PID 1
exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --host spark-master --port 7077 --webui-port 8080
```

### B.4 start-app.bat 调用的 Python 驱动 (run_pipeline.py)

```python
# 1-5. 调 admin API 跑 5 个 action, 轮询 job 状态
for action in ['load_csv', 'sqoop', 'spark_clean', 'hive_ddl', 'spark_train']:
    r = post(f'/api/admin/actions/{action}')
    wait_for_job(r['job_id'], label=action)

# 6. fpgrowth 必须在 spark-master 容器内跑 (它有 spark-submit)
# demo-backend 没装 spark-submit, 用 docker socket API 在 spark-master 容器内 exec
exec_via_socket('spark-master', ['bash', '/opt/jobs/ml/fpgrowth.py'])
```

### B.5 ML 3 特征 + 无数据泄漏 (train.py)

```python
# 之前 6 特征: age + purchase_count + avg_amount + visit_count + avg_duration + unique_attractions
# 数据泄漏: total_amount = purchase_count * avg_amount, 完美反推 Acc=1.0
# 现在 3 特征: [age, avg_duration, unique_attractions] (与消费额 / visit_count 无关)
FEATURE_COLS = ["age", "avg_duration", "unique_attractions"]
```

---

## 附录 C: 提交清单 (答辩用)

```
D:\Desktop\smart-scenic-bigdata\
├── README.md              项目总览
├── docs/
│   ├── 作业要求.md          评分点对照
│   ├── IMPLEMENTATION.md    实现细节 (技术栈 / 数据流 / 算法)
│   └── PPT.md              本文件 (PPT 文字稿)
├── docker-compose.yml       15 容器编排
├── .env                     端口/版本
├── app/
│   ├── backend/             FastAPI 47 endpoints
│   ├── frontend/            4 页 Web + ECharts
│   └── jobs/                Spark / Hive / SQL 脚本
├── scripts/
│   ├── start-containers.bat    一键起 15 容器
│   ├── start-app.bat           一键跑数据 pipeline
│   ├── stop.bat                停止容器 (保数据)
│   ├── reset.bat               完全重置
│   ├── test-e2e.bat            23 项端到端测试
│   ├── run_pipeline.py         start-app.bat 调用的 Python 驱动
│   └── install-deps.bat        本地 venv (IDE 用)
├── docker/                 镜像构建 (hadoop / hive / spark)
│   ├── hadoop/                sqoop + JDK 1.8
│   ├── hive/                  MySQL 5.7 JDBC + bash 包装
│   └── spark/                 wheels (sklearn/pandas/joblib/numpy) + entrypoint
├── mysql-init/
│   └── 01-init-business.sql   4 张中文 schema + hive 用户授权
├── data/raw_data/             4 个 CSV (10/10k/100k/100k 行)
└── shared/                    docker volume (模型 + fpgrowth)
```

### 答辩 PPT 推荐张数 (约 25-30 张)

| 章节 | 张数 |
|------|------|
| 封面 + 目录 | 2 |
| 背景 + 评分点 | 2 |
| 架构 + 数据流 | 3 |
| MySQL / HDFS | 2 |
| Spark / HBase | 3 |
| Hive / 数仓 | 2 |
| 后端 / 前端 | 3 |
| ML / FPGrowth | 2 |
| 一键部署 + pipeline | 3 |
| 验证 + 演示 | 2 |
| 总结 + 亮点 | 1 |
| **合计** | **~26 张** |
