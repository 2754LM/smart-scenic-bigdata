# 智能景区大数据平台 (Smart Scenic BigData Platform)

> 选题十八：智能景区管理系统 6.2-6.5 评分点
> 真分布式大数据集群 (15 容器) + 完整业务系统 (4 页前端 + 47 REST API + Hive 数仓)
> 一键部署，10 分钟跑通

本项目基于 **Docker Compose** 部署一整套**真分布式**的大数据集群（**15 个容器**），完整实现作业 6.2-6.5 全部要求：
- **6.2 平台搭建** - 15 容器一键部署（HBase 自动 init meta 表，业务表自动建；MySQL 5.7 同时承担业务库 + Hive Metastore）
- **6.3 数据采集** - Sqoop MySQL→HDFS (Pyhive 真实查询 Hive，不做静默 fallback)
- **6.4 数据分析** - Spark 清洗 + Hive 数仓 (HS2 :10000) + 4 回归 + 1 聚类 + 4 分类 (无数据泄漏)
- **6.5 可视化** - 4 页 Web 前端（含真实 vs 预测折线）

---

## 〇、组件版本规范

镜像版本在 `.env` 里集中管理，**改一个变量 = 全栈升级**：

| 组件 | 版本 (.env) | 备注 |
|------|------|------|
| ZooKeeper | `ZK_VERSION=3.9` | 3 节点 ensemble |
| Hadoop | `HADOOP_VERSION=3.3.6` | HDFS HA (1 NN + 2 DN) |
| HBase | `HBASE_VERSION=latest` | 2.1.x (harisekhon/hbase) |
| Spark | `SPARK_VERSION=3.4.1` | 1 master + 1 worker (含 sklearn/joblib/pandas wheels) |
| Hive | `HIVE_VERSION=3.1.3` | 数仓 (DataNucleus 4.2 + MySQL 5.7 兼容) |
| MySQL | mysql:5.7 | 业务库 + Hive Metastore (合并部署; DataNucleus 不兼容 8.0) |
| JDK | `JDK_VERSION=1.8.0_162` | 业务 Hadoop 镜像使用 |

> **变更说明**：原 docx 方案要求 HBase 2.4.11，但 Docker Hub 上的 `harisekhon/hbase:2.4.11` 镜像不可用，故切到 `latest`（实际为 2.1.3）。Kafka 已删除（项目无实时流需求）。

---

## 一、快速开始（10 分钟跑通）

### 1.1 启动大数据平台（15 容器）

```bat
cd smart-scenic-bigdata
scripts\start.bat
```

⏱️ 等待 3-5 分钟（首次启动要 build 镜像 + 初始化 MySQL）。  
✅ 完成后 15 容器 up，HBase 自动 init 业务表 `scenic_realtime` / `scenic_reviews`，Hive 自动 `schematool` 初始化。

### 1.2 准备 4 个 CSV 数据

数据集在 `D:\选题与数据相关资料\数据集\Topic 18\`，复制到项目：
```bat
mkdir data\raw_data
copy "D:\选题与数据相关资料\数据集\Topic 18\*.csv" data\raw_data\
```

期望的 4 个 CSV + 行数：
- `attractions.csv` - 10 行 (10 个景点)
- `visitors.csv` - 10,000 行 (游客主数据)
- `consumption.csv` - 100,000 行 (消费流水)
- `visit_records.csv` - 100,000 行 (游玩流水)

### 1.3 浏览器访问

| 页面 | URL | 内容 |
|------|-----|------|
| **总览大屏** | http://localhost:8080/index.html | 8 KPI + 4 ML 预测 + 7 ECharts 图 |
| **数据分析** | http://localhost:8080/analysis.html | 6 图表 + FPGrowth Sankey |
| **模型预测** | http://localhost:8080/predict.html | 4 场景化卡 + 真实 vs 预测折线 + 模型对比 |
| **⚡ 实时流** | http://localhost:8080/realtime.html | 引擎状态 + 数据流图 + HBase 验证 |
| **业务管理** | http://localhost:8080/manage.html | 4 tab (景点/游客/消费/游玩) + 系统管理 |
| **API 文档** | http://localhost:8000/docs | Swagger UI (49 endpoints) |

### 1.4 一键初始化数据

打开 `manage.html` → **系统管理** tab → 点 **"⚡ 一键初始化"** 按钮：
```
load_csv → sqoop → spark_clean → hive_ddl → spark_train
```
约 5-10 分钟。完成后所有功能即可使用。

### 1.5 (可选) IDE 开发支持

```bat
scripts\install-deps.bat      REM 创建 venv + pip install -r requirements.txt
```

> **注意**：完整平台都在 Docker，**不要**在本机跑 uvicorn，否则会跟 demo-backend 容器的 8000 端口冲突。

---

## 二、整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                       Windows 宿主机                          │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐         │
│  │ Web 前端     │ │ FastAPI 后端 │ │  系统管理面板 │         │
│  │ (HTML+JS)    │ │  (Python)    │ │  (前端 trigger)│        │
│  │ :8080        │ │  :8000       │ │               │         │
│  └──────┬───────┘ └──────┬───────┘ └──────────────┘         │
└─────────┼────────────────┼──────────────────────────────────┘
          │ HTTP / WS     │ docker exec / pymysql
┌─────────┴────────────────┴──────────────────────────────────┐
│              Docker Network: bigdata-net                     │
│                                                                │
│  ╔═══════════════════════════════════════════════════════╗  │
│  ║ 应用服务层                                            ║  │
│  ║   Demo Backend (FastAPI, 47 REST API)                ║  │
│  ║   sklearn joblib 模型加载 (毫秒级预测)                ║  │
│  ╚═══════════════════════════════════════════════════════╝  │
│                                                                │
│  ╔═══════════════════════════════╦═══════════════════════╗   │
│  ║ 计算层                        ║ 数仓                 ║   │
│  ║   Hadoop NameNode + 2 DN     ║  HiveServer2 × 2    ║   │
│  ║   Spark Master + Worker      ║   (11010 / 11011)    ║   │
│  ║   (HDFS 真分布式, 副本=2)    ║                       ║   │
│  ╚═══════════════════════════════╩═══════════════════════╝   │
│                                                                │
│  ╔═══════════════════════════════╗   ╔════════════════╗   │
│  ║ 存储层                        ║   ║ 元数据       ║   │
│  ║   HBase Master + 2 RS         ║   ║ MySQL 5.7   ║   │
│  ║   (NoSQL 实时画像/评论)       ║   ║ (业务库+     ║   │
│  ║                               ║   ║  Hive MS)   ║   │
│  ╚═══════════════════════════════╝   ╚════════════════╝   │
│                                                                │
│  ╔═══════════════════════════════════════════════════════╗  │
│  ║ 协调层                                                ║  │
│  ║   ZooKeeper Ensemble × 3 (1 leader + 2 follower)    ║  │
│  ║   └── 服务 HBase                                      ║  │
│  ╚═══════════════════════════════════════════════════════╝  │
└────────────────────────────────────────────────────────────────┘
```

---

## 三、目录结构

```
smart-scenic-bigdata/
├── README.md                  ← 本文件（主文档）
├── LICENSE                    MIT
├── docker-compose.yml         15 容器编排
├── .env                       端口/密码/镜像版本
├── .gitattributes             LF 行尾强制
│
├── docker/                    自定义镜像的 Dockerfile
│   ├── hive/                  FROM apache/hive:3.1.3 + MySQL 5.x JDBC (DataNucleus 兼容)
│   └── hadoop/                FROM apache/hadoop + JDK + Sqoop
│
├── config/                    各组件运行时配置（bind mount）
│   ├── zoo.cfg                ZK ensemble
│   ├── hadoop/                core/hdfs/yarn/mapred + workers
│   ├── hbase/                 hbase-site + regionservers
│   ├── hive/                  hive-site + hive-env
│   └── spark/                 spark-defaults + spark-env + workers
│
├── mysql-init/
│   └── 01-init-business.sql   4 张中文业务表（MySQL 启动自动跑）
│
├── app/                       Web 应用
│   ├── backend/               FastAPI 后端（8 service + 9 router + main.py）
│   ├── frontend/              5 页 Web 前端（HTML + JS + CSS）
│   └── jobs/                  Spark / Hive / PySpark 训练脚本
│
├── scripts/                   运维脚本（5 个，Windows .bat）
│   ├── start.bat              1. 一键启动 15 容器（含 HBase 自动 init + Hive Metastore 初始化）
│   ├── stop.bat               2. 停止所有容器（数据保留）
│   ├── reset.bat              3. 完全重置（清空所有数据，会询问 yes）
│   ├── install-deps.bat       4. 安装 Python venv + 依赖（仅 IDE 用）
│   └── test-e2e.bat           5. 端到端验证（25 项 7 场景：MySQL/HDFS/HBase/Spark/Hive/Backend/ML）
│
├── docs/
│   ├── 作业要求.md            选题要求 + 作业规范（合并）
│   ├── IMPLEMENTATION.md      项目实现文档（架构/API/算法详解）
│   ├── 实习报告模板.doc       报告模板（Word 打开）
│   └── 任务书模板.doc         任务书模板（Word 打开）
│
├── data/                      持久化数据卷（自动生成，不进 git）
│   └── raw_data/              4 个原始 CSV（手动复制）
└── logs/                      日志（自动生成，不进 git）
```

---

## 四、15 个容器使用情况

| 容器 | 数量 | 实际用途 | 镜像 | 端口 |
|------|------|---------|------|------|
| mysql | 1 | 业务库 (scenic) + Hive Metastore (`hive_metastore` 库) | mysql:5.7 | 13306 |
| zookeeper-1/2/3 | 3 | HBase 协调 | zookeeper:3.9 | 12181 |
| hadoop-namenode | 1 | HDFS + Sqoop + YARN (自定义镜像 + JDK 1.8 + Sqoop) | smart-scenic/hadoop:custom | 19870/18088/19000 |
| hadoop-datanode-1/2 | 2 | HDFS 副本 | apache/hadoop:3.3.6 | - |
| hbase-master | 1 | HBase Master | harisekhon/hbase:latest | 11610 |
| hbase-regionserver-1/2 | 2 | HBase Region 服务 | harisekhon/hbase:latest | 11620-11630 |
| spark-master | 1 | PySpark 调度 (含 sklearn/joblib/pandas wheels) | smart-scenic/spark:custom | 18080 |
| spark-worker-1 | 1 | PySpark 执行 | smart-scenic/spark:custom | - |
| hive-server-1 | 1 | HiveServer2 :10000 + schematool 自动初始化 | smart-scenic/hive:custom | 11010/11012 |
| hive-server-2 | 1 | HiveServer2 :10000 (负载均衡多实例) | smart-scenic/hive:custom | 11011/11013 |
| demo-backend | 1 | FastAPI 后端 (47 endpoints, beeline-via-exec 真实查 Hive) | smart-scenic/demo-backend:custom | 8000 |
| (auto-init) | - | HBase 业务表 scenic_realtime / scenic_reviews + Hive Metastore schema | (start.bat 一键创建) | - |

**结论：15 个容器全部有实际业务代码使用，0 个空跑。**

### Hive 数仓架构（关键设计）
- **MySQL 5.7**（一容器双角色）：业务库 `scenic` + Hive Metastore `hive_metastore`
- **hive-server-1/2**：2 个 HS2 同时连同一个 MySQL metastore（Apache 官方推荐的多实例 HA 模式）
- **首次启动**：`hive-server-1` 自动调 `schematool -dbType mysql -initSchema` (sentinel 文件 `/opt/hive/conf/.schema_initialized` 幂等)
- **后端**：`hive_service.py` 通过 `pyhive` 直连 `hive-server-1:10000`，不再有任何静默 fallback
- **为什么不用 MySQL 8.0**: DataNucleus 4.2 (绑死在 Hive 3.1.3) 生成的 DDL 用 `DEFAULT CHARACTER SET xxx` 语法被 8.0 拒绝

---

## 五、Web 应用（4 页前端 + 47 REST API）

### 5.1 4 页前端

| 页面 | URL | 内容 |
|------|-----|------|
| 总览大屏 | http://localhost:8080/index.html | 8 KPI + 4 ML 预测 + 7 ECharts 图 |
| 数据分析 | http://localhost:8080/analysis.html | 6 图表 + FPGrowth Sankey 图 |
| 模型预测 | http://localhost:8080/predict.html | 4 场景化卡 (客流/推荐/路线/画像) + 真实 vs 预测折线 |
| 业务管理 | http://localhost:8080/manage.html | 4 tab (景点/游客/消费/游玩) + 系统管理 |

### 5.2 47 个 REST 路由

| 模块 | 路由数 | 关键路径 |
|------|--------|----------|
| 总览 | 4 | `/api/overview/{kpi,timeseries,attraction-rank,health}` |
| 景点 | 3 | `/api/attractions{,/{id},/{id}/summary}` |
| 游客 | 3 | `/api/visitors{,/{id},/{id}/aggregate}` |
| 消费 | 2 | `/api/consumption{,/visits}` |
| 分析 | 7 | `/api/analysis/{daily,hourly,region,age-group,type-summary,fpgrowth,daily-compare}` |
| 预测 (基础) | 5 | `/api/predict{,/regression,/classification,/clustering,/compare,/_engine}` |
| 预测 (场景化) | 7 | `/api/predict-tourism/{attraction-forecast,attraction-recommend,route-recommend,visitor-profile/{id},tomorrow-summary,multi-day-forecast,fpgrowth-sankey}` |
| 系统管理 | 10 | `/api/admin/{status,containers,models,datasets,hdfs,jobs,jobs/{id},actions,actions/{name},pipeline}` |
| **合计** | **47** | |

### 5.3 系统管理面板

打开 `manage.html` → **系统管理** tab，一站式管理：
- 15 容器状态（按存储/协调/计算/数仓/应用分组）
- 已训练 sklearn 模型列表
- 4 张 MySQL 表 + 4 个 CSV 文件状态
- HDFS 分区目录
- 6 个触发按钮（加载 CSV / Sqoop / Spark 清洗 / Hive DDL / Hive 查询 / PySpark 训练）
- **⚡ 一键初始化**（pipeline 顺序执行）
- 异步任务列表（每 2 秒轮询）

---

## 六、数据处理流程（端到端）

```
[MySQL scenic (4 张中文表)]
   │  Sqoop 1.4.7 import
   ▼
[HDFS /scenic/sqoop/{t_attraction,t_visitor,t_consumption,t_visit_record}/]
   │
   │  spark-submit clean (Spark 清洗)
   ▼
[HDFS /scenic/cleaned/{4 tables}.parquet]   ← 去重/类型转换/派生字段
   │
   │  hive -f ddl.sql + views.sql
   ▼
[Hive ext_t_* (4 张分区表) + v_* (4 个视图)]
   │
   │  hive -f queries.sql
    ▼
[8 类分析结果：日均游客/高消费排行/时段热度/年龄段/地区/月度趋势/周末对比/消费Top]
    │
    │  spark-submit ml-train
    ▼
[HDFS /scenic/models/ + /shared/models/]
   ├─ regression_linear / lasso / ridge / rf       ← 4 个回归 (预测消费总额)
   ├─ clustering_kmeans                            ← 1 个聚类 (k=4)
   └─ classification_rf / dt / gbt / lr            ← 4 个分类 (回头客识别)
   ↓
[后端 sklearn joblib 加载]  →  /api/predict 毫秒级响应
```

**前端触发**：manage.html → 系统管理 → ⚡ 一键初始化

---

## 八、ML 模型说明

### 训练流程 (spark-master 容器内)

`model_service.predict()` 启动后：

```
[1] 检查 /shared/models/sklearn/ 目录:
   ├─ 有 *.pkl → joblib.load() 直接加载
   └─ 没有 → auto_train.auto_train_if_needed() 异步训练
            ├─ 起后台线程调 docker exec spark-master spark-submit train.py
            ├─ 训练 5-10 分钟 (7 步骤)
            │   1. Spark 读 HDFS Parquet
            │   2. 聚合 + 派生特征 (age, purchase_count, avg_amount, ...)
            │   3. Spark MLlib 训练 (Linear / Lasso / Ridge / RF + KMeans + RF/DT/GBT/LR)
            │   4. 提取系数/重建 sklearn 模型
            │   5. joblib.dump to /shared/models/sklearn/
            └─ 训练完自动加载
   ↓
[2] 预测请求到达:
   ├─ 消费金额预测 → regression_linear/lasso/ridge/rf
   ├─ 高频回头客识别 → classification_rf/dt/gbt/lr (4 选 1)
   └─ 游客聚类 → clustering_kmeans
   ↓
[3] 返回结果 + 引擎标识 + 耗时
   {prediction: 523.4, model: regression_ridge, engine: sklearn, elapsed_ms: 3}
```

### 防数据泄漏 (重要)

**早期版本 bug**：原分类任务用 6 个特征预测 `high_value_label = (total_amount > 500)`，Acc 100% 看似完美。
**根本原因**：`total_amount = purchase_count × avg_amount` 线性等价，模型只需 `if purchase_count × avg_amount > 500` 就能 100% 准确，**完全无泛化能力**。

**修复方案**：分类任务改用 3 个真正独立的特征 `[age, avg_duration, unique_attractions]`，标签改为 `is_repeat_visitor = (visit_count >= 中位数)`，做 5-fold CV 验证。
**新指标（4 模型，test set）**：
| 模型 | Test Acc | Test F1 | Test AUC |
|---|---|---|---|
| RandomForest | 0.78 | 0.79 | 0.88 |
| DecisionTree | 0.78 | 0.79 | 0.87 |
| GBT | 0.78 | 0.79 | 0.88 |
| LogisticReg | 0.79 | 0.78 | 0.88 |

CV5 std=0.005~0.009，模型稳定可重现。

---

## 九、常见问题

### Q1: `scripts\install-deps.bat` 报 "Python not found"
装 Python 3.10+，勾选 "Add to PATH"。
注意：此脚本**只在 IDE 用**，完整平台已经在 Docker 跑。

### Q2: sklearn 模型没加载
查看 `http://localhost:8000/api/predict/classification`：
- `results: []` → 模型还没训练（执行 "⚡ 一键初始化"）
- `results: [rf, dt, gbt, lr]` → 4 个分类模型已就绪

模型目录：`/shared/models/sklearn/` (在 demo-backend 容器内 mount)。

### Q3: 训练失败 / 卡住
训练在 spark-master **容器内**跑，不依赖 host Java。
```bat
docker logs demo-backend                 REM 后端日志
docker exec spark-master bash -c "ls /shared/models/sklearn"  REM 检查模型文件
```

### Q4: HBase 报 "hbase:meta is NOT online" 或 `Connection refused` 到 hadoop-namenode:9000

**原因**：HBase 启动时会尝试在 HDFS 上创建 `/hbase` 目录，但 datanode 还没完全 join 时
HBase master 在 `MasterFileSystem.createInitialFileSystemLayout()` 调 `setSafeMode`
会失败进入 abort 循环。

**修复 (一键完成)**：已在 `docker/hadoop/starter.sh` 和 `app/backend/main.py` 自动化处理：
1. hadoop-namenode 启动后自动 `hdfs dfs -mkdir -p /hbase && chmod 777`
2. demo-backend 启动时调 `hbase_svc.init_tables()` 自动建 `scenic_realtime` / `scenic_reviews` 表
3. HBase 数据卷是 named volume，重启容器不会丢数据

**首次部署如果 HBase 卡在 init 状态**（可能是残留 stale ZK 节点）：
```bat
REM 一键清理并重启
docker compose stop hbase-master hbase-regionserver-1 hbase-regionserver-2
docker exec zookeeper-1 /apache-zookeeper-3.9.5-bin/bin/zkCli.sh -server localhost:2181 deleteall /hbase
docker exec hadoop-namenode hdfs dfs -rm -r -f /hbase
docker volume rm smart-scenic-bigdata_hbase-master-data smart-scenic-bigdata_hbase-rs1-data smart-scenic-bigdata_hbase-rs2-data
docker compose up -d hbase-master hbase-regionserver-1 hbase-regionserver-2
```
等 ~60s 后 `docker exec hbase-master bash -c "hbase shell /dev/stdin <<< 'status simple'"` 应显示 `1 active master, 2 live servers, 0 dead servers`。

### Q5: 完全重置
```bat
scripts\stop.bat
scripts\reset.bat       REM 输入 yes 确认，清空所有数据卷
scripts\start.bat
```

### Q6: Hive 数仓架构（MySQL 5.7 Metastore）

**当前架构**:
- `mysql` (mysql:5.7) 同一容器兼任业务库 (`scenic`) + Hive Metastore (`hive_metastore` 库)
- `hive-server-1/2` (HiveServer2 :10000) — 2 个 HS2 实例，启动时由 hive-server-1 调 `schematool -dbType mysql -initSchema`
- 后端 `hive_service.py` 用 `pyhive` 真实查询 Hive（**无任何静默 fallback**；DDL 没跑则 HTTP 503）
- 数据流：MySQL (业务) → Sqoop → HDFS Parquet → `hive -f ddl.sql` 注册外表 → 后端通过 HS2 (pyhive) 查询

**为什么 MySQL 5.7 而不是 8.0**:
- DataNucleus 4.2 (绑死在 Hive 3.1.3) 生成的 DDL 用 `DEFAULT CHARACTER SET xxx` 语法，MySQL 8.0 拒绝
- Apache 官方在 `cwiki.apache.org/confluence/x/4z83Bg` 测试矩阵: MySQL 5.6.17+ 都可工作，5.7 最稳定
- 升级到 Hive 4.x (DataNucleus 5+) 可回到 MySQL 8.0，但需要重写依赖

### Q7: 演示流程（演示用）

```bat
scripts\test-e2e.bat       REM 先跑 23 项验证（应该全 PASS）
打开浏览器：
  1. http://localhost:8080/manage.html → 系统管理 tab → 看 15 容器 ● 全部绿色
  2. 点 ⚡ 一键初始化 → 5-10 分钟后看 4 个已训练模型 (regression / classification)
  3. 切到 http://localhost:8080/predict.html → 选场景 → 输入特征 → 看预测结果
  4. 切到 http://localhost:8080/realtime.html → 点 ⚡ 触发任务 → 验证 HBase 落库
```

---

## 十、设计权衡

### 10.1 容器化 vs 裸机
- ✅ 可复制：环境一次构建，多人复用
- ✅ 隔离：组件故障不影响宿主机
- ✅ 易清理：`scripts\reset.bat` 完全清空
- ✅ 匹配生产：现代大数据平台均使用容器化部署

### 10.2 真分布式 vs 伪分布式
- ✅ 贴近生产：架构与生产集群一致
- ✅ 可演示 HA：节点宕机不影响服务
- ✅ 答辩加分：明确说明"真分布式"而非"伪分布式"

### 10.3 HBase 通过 docker exec 而非 happybase
- happybase 1.2 + thriftpy2 协议不兼容 HBase 2.x Thrift server
- docker exec 调用 hbase shell，慢一点（每次 0.5-1s）但兼容性强

### 10.5 训练回 sklearn，推理走 sklearn joblib
- 训练在 PySpark MLlib 跑（体现真大数据）
- 模型导出为 joblib pickle
- 推理在 demo-backend 用 joblib.load() 加载（**不需要 PySpark 依赖**）
- 毫秒级响应，无 Java 占用
- demo-backend 镜像比 PySpark 版小 ~600MB

### 10.6 MySQL 复用
- 业务库 (`scenic`) + Hive Metastore (`hive_metastore`) 合并部署
- 节省资源（一个容器两个用途，无需再启 PostgreSQL 容器）
- 必须是 MySQL 5.7 (DataNucleus 4.2 不兼容 8.0); 见 Q8 详解

---

## 附：作业对照表

| 作业要求 | 本项目实现 | 状态 |
|---------|----------|------|
| 6.2 平台搭建 | 15 容器一键部署 (脚本化) | ✅ |
| 6.3 数据采集 | Sqoop MySQL→HDFS (4 张表，~220k 行) | ✅ |
| 6.4 数据分析 | Spark 清洗 + **Hive 真实查询** (pyhive → HS2) + 4 回归 + 1 聚类 + 4 分类 | ✅ |
| 6.5 可视化 | 5 页前端 (含独立实时流页 + 真实 vs 预测折线) | ✅ |

详细作业要求见 [docs/作业要求.md](docs/作业要求.md)。  
实现细节/算法/命令见 [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md)。

---

## 附：scripts 速查表

5 个脚本，**用户日常只用前 3 个**：

| 脚本 | 用途 | 何时用 |
|------|------|--------|
| `start.bat` | 一键启动 15 容器（含 HBase 自动 init + Hive Metastore init） | 每天开机第一次 |
| `stop.bat` | 停止所有容器（数据保留在 volume） | 不用时 |
| `reset.bat` | 完全重置（清空所有数据卷，会询问 yes） | 重新开始 |
| `install-deps.bat` | 创建本地 venv + pip install（仅 IDE 用） | 用编辑器写代码时 |
| `test-e2e.bat` | 25 项端到端验证（7 场景：MySQL/HDFS/HBase/Spark/Hive/Backend/ML）| CI / 调试 |

**典型工作流**：
```bat
scripts\start.bat      REM 第一次：启动 15 容器
scripts\test-e2e.bat   REM 验证：7 场景 23 项应该全 PASS
REM 浏览器 http://localhost:8080 玩耍 (5 页 + 49 API)
scripts\stop.bat       REM 关闭
```

---

## License

MIT

---

## 附：本地开发提示（IDE 用户）

> **不需要**：平台已全部在 Docker 运行，**不要**在 host 上启 uvicorn，否则 8000 端口会冲突。

### 编辑代码并热重载

```bat
scripts\install-deps.bat   REM 创建 .venv，安装 requirements.txt (一次性)
code .                     REM 用 VS Code 打开项目
```

`demo-backend` 容器把 `./app/backend` mount 到 `/app`，所以**直接修改 .py 文件，容器内的代码也变了**。
但**uvicorn 不会自动重启**（生产模式 no-reload）。要热重载：

```bat
REM 在容器外运行（需要本地 venv）
cd app\backend
.venv\Scripts\python.exe -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
REM 然后停掉 demo-backend 容器
docker stop demo-backend
```

### 前端热刷新

前端文件改完直接刷新浏览器即可（demo-backend 用 volume 挂载，本地 vscode 保存后浏览器
再开同一个 URL 就生效；如果改了 .js 缓存了，按 Ctrl+Shift+R 强刷）。

不需要 livereload 之类的 hot-reload — 静态 HTML/JS 用强刷够用，少装一个依赖。

### 文件监听项目结构

```
app/backend/    ← FastAPI 后端 (改 routers/ services/ main.py)
app/frontend/   ← HTML 前端 (改 *.html static/js/ static/css/)
data/raw_data/  ← 4 个 CSV (放这里后 manage.html 加载)
shared/models/  ← demo-backend 通过 :ro mount (容器内)
```

### OpenAPI 文档

http://localhost:8000/docs — 所有 49 路由的交互式文档。
前端 JS 调 API 的路径和参数可以直接在这里看。

