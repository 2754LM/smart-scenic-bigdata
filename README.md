# 智能景区大数据平台 (Smart Scenic BigData Platform)

> 选题十八：智能景区管理系统 6.2-6.5 评分点
> 真分布式大数据平台 + 完整业务系统 5 页 Web 前端 + 40+ REST API + Kafka 实时流
> 一键部署，10 分钟跑通

本项目基于 **Docker Compose** 部署一整套**真分布式**的大数据集群（**17 个容器**），完整实现作业 6.2-6.5 全部要求：
- **6.2 平台搭建** - 17 容器一键部署 (HBase 自动 init meta 表)
- **6.3 数据采集** - Sqoop MySQL→HDFS + Kafka 实时流
- **6.4 数据分析** - Spark SQL + Hive 仓库 + 4 回归 + 1 聚类 + 4 分类 (无数据泄漏)
- **6.5 可视化** - 5 页 Web 前端 (含 1 个独立实时流页面) + Kafka 实时推送

---

## 〇、组件版本规范

镜像版本严格对齐 docx 原始方案：

| 组件 | 版本 | 备注 |
|------|------|------|
| Ubuntu | 16.04 | 原始方案基线 |
| JDK | 1.8.0_162 | `jdk-8u162-linux-x64.tar.gz` |
| Hadoop | 3.1.0 | HDFS + YARN |
| ZooKeeper | 3.6.3 | `apache-zookeeper-3.6.3-bin.tar.gz` |
| HBase | 2.4.11 | 2.x 稳定版 |
| Kafka | 3.1.0 | `kafka_2.12-3.1.0.tgz`（Scala 2.12）|
| Maven | 3.8.5 | `apache-maven-3.8.5-bin.tar.gz` |
| Hive | 3.1.3 | 数仓 |
| Spark | 3.1.0 | 计算引擎 |

镜像 tag 在 `.env` 里集中管理，**改一个变量 = 全栈升级**。

---

## 一、快速开始（10 分钟跑通）

### 1.1 启动大数据平台（17 容器）

```bat
cd smart-scenic-bigdata
scripts\start.bat
```

⏱️ 等待 3-5 分钟（首次启动要 build 镜像 + 初始化 MySQL）。

### 1.2 准备 4 个 CSV 数据

数据集在 `D:\选题与数据相关资料\数据集\Topic 18\`，复制到项目：
```bat
mkdir data\raw_data
copy "D:\选题与数据相关资料\数据集\Topic 18\*.csv" data\raw_data\
```

### 1.3 启动 Web 应用

```bat
scripts\start-app.bat      REM 启动 Web（自动装依赖 + 智能双轨）
```

启动后浏览器打开：

| 页面 | URL | 内容 |
|------|-----|------|
| **总览** | http://localhost:8080 | 8 KPI + 4 ML 预测 + 7 ECharts 图表 |
| **数据分析** | http://localhost:8080/analysis.html | 6 图表 + FPGrowth Sankey |
| **机器学习预测** | http://localhost:8080/predict.html | 4 场景化卡 + 真实 vs 预测折线 |
| **⚡ 实时流** | http://localhost:8080/realtime.html | 数据流图 + 触发任务 + HBase 验证 |
| **业务管理** | http://localhost:8080/manage.html | 景点/游客/消费/游玩 + 系统管理 |
| **API 文档** | http://localhost:8000/docs | Swagger UI |
| **引擎状态** | http://localhost:8000/api/predict/_engine | PySpark vs sklearn |

### 1.4 一键初始化数据

打开 `manage.html` → **系统管理** tab → 点 **"⚡ 一键初始化"** 按钮：
```
load_csv → sqoop → spark_clean → hive_ddl → spark_train
```
约 5-10 分钟。完成后所有功能即可使用。

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
          │ HTTP / WS     │ docker exec / kafka-py / pymysql
┌─────────┴────────────────┴──────────────────────────────────┐
│              Docker Network: bigdata-net                     │
│                                                                │
│  ╔═══════════════════════════════════════════════════════╗  │
│  ║ 应用服务层                                            ║  │
│  ║   Demo Backend (FastAPI)                              ║  │
│  ║   Kafka Consumer 后台线程                              ║  │
│  ║   PySpark Loader (智能双轨 ML)                        ║  │
│  ╚═══════════════════════════════════════════════════════╝  │
│                                                                │
│  ╔═══════════════════════════════╦═══════════════════════╗   │
│  ║ 计算层                        ║ 数仓                 ║   │
│  ║   Hadoop NameNode + 2 DN     ║  HiveServer2 × 2    ║   │
│  ║   Spark Master + Worker      ║   (11010 / 11011)    ║   │
│  ║   (HDFS 真分布式, 副本=2)    ║                       ║   │
│  ╚═══════════════════════════════╩═══════════════════════╝   │
│                                                                │
│  ╔═══════════════════════════════╦═══════════════════════╗   │
│  ║ 存储层                        ║ 消息层              ║   │
│  ║   HBase Master + 2 RS         ║  Kafka Broker × 2   ║   │
│  ║   MySQL 8.0                   ║  (ZK 模式)         ║   │
│  ╚═══════════════════════════════╩═══════════════════════╝   │
│                                                                │
│  ╔═══════════════════════════════════════════════════════╗  │
│  ║ 协调层                                                ║  │
│  ║   ZooKeeper Ensemble × 3 (1 leader + 2 follower)    ║  │
│  ║   └── 服务 HBase + Kafka                             ║  │
│  ╚═══════════════════════════════════════════════════════╝  │
└────────────────────────────────────────────────────────────────┘
```

---

## 三、目录结构

```
smart-scenic-bigdata/
├── README.md                  ← 本文件（主文档）
├── LICENSE                    MIT
├── docker-compose.yml         17 容器编排
├── .env                       端口/密码/镜像版本
├── .gitattributes             LF 行尾强制
│
├── docker/                    自定义镜像的 Dockerfile
│   ├── hive/                  FROM apache/hive + MySQL JDBC
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
│   ├── start.bat              1. 启动大数据平台（17 容器）
│   ├── start-app.bat          2. 启动 Web 应用（自动装依赖）
│   ├── stop.bat               3. 停止所有容器
│   ├── reset.bat              4. 完全重置（清空数据）
│   └── test-e2e.bat           5. 端到端业务测试（CI 用）
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

## 四、17 个容器使用情况

| 容器 | 数量 | 实际用途 | 镜像 | 端口 |
|------|------|---------|------|------|
| mysql | 1 | 业务库 + Hive Metastore | mysql:8.0 | 13306 |
| zookeeper-1/2/3 | 3 | HBase + Kafka 协调 | zookeeper:3.6.3 | 12181 |
| hadoop-namenode | 1 | HDFS + Sqoop + YARN | smart-scenic/hadoop:custom | 19870/18088/19000 |
| hadoop-datanode-1/2 | 2 | HDFS 副本 | apache/hadoop:3.1.0 | - |
| hbase-master | 1 | HBase Master + Thrift | harisekhon/hbase:2.4.11 | 11610 |
| hbase-regionserver-1/2 | 2 | HBase Region 服务 | harisekhon/hbase:2.4.11 | 11620-11630 |
| kafka-1/2 | 2 | 消息队列（ZK 模式）| apache/kafka:3.1.0 | 19092/19095 |
| spark-master | 1 | PySpark 训练调度 | apache/spark:3.1.0 | 18080 |
| spark-worker-1 | 1 | PySpark 训练执行 | apache/spark:3.1.0 | - |
| hive-server-1/2 | 2 | 数仓查询 | smart-scenic/hive:custom | 11010/11011/11012/11013 |
| demo-backend | 1 | FastAPI 后端 | python:3.10-slim | 8000 |

**结论：17 个容器全部有实际业务代码使用，0 个空跑。**

---

## 五、Web 应用（5 页前端 + 47 REST API）

### 5.1 5 页前端

| 页面 | URL | 内容 |
|------|-----|------|
| 总览 | http://localhost:8080 | 8 KPI + 4 ML 预测 + 7 ECharts 图表 |
| 数据分析 | http://localhost:8080/analysis.html | 6 图表 + FPGrowth Sankey |
| 机器学习预测 | http://localhost:8080/predict.html | 4 场景化卡 (客流/推荐/路线/画像) + 真实 vs 预测 |
| ⚡ 实时流 | http://localhost:8080/realtime.html | 数据流图 + 触发任务 + HBase 验证 |
| 业务管理 | http://localhost:8080/manage.html | 4 tab (景点/游客/消费/游玩) + 系统管理 |

### 5.2 47 个 REST 路由

| 模块 | 路由数 | 关键路径 |
|------|--------|----------|
| 总览 | 4 | `/api/overview/{kpi,timeseries,attraction-rank,health}` |
| 景点 | 3 | `/api/attractions{,/{id},/{id}/summary}` |
| 游客 | 3 | `/api/visitors{,/{id},/{id}/aggregate}` |
| 消费 | 2 | `/api/consumption{,/visits}` |
| 分析 | 7 | `/api/analysis/{daily,hourly,region,age-group,type-summary,fpgrowth,daily-compare}` |
| 预测 (基础) | 6 | `/api/predict{,/regression,/classification,/clustering,/compare,/_engine}` |
| 预测 (场景化) | 7 | `/api/predict-tourism/{attraction-forecast,attraction-recommend,route-recommend,visitor-profile/{id},tomorrow-summary,multi-day-forecast,fpgrowth-sankey}` |
| 实时 | 8 | `/api/realtime/{visit-recent,visitor/{id},attraction/{id},publish/review,publish/event,kafka/status,task/trigger,hbase/clear}` |
| 系统管理 | 10 | `/api/admin/{status,containers,models,datasets,hdfs,jobs,jobs/{id},actions,actions/{name},pipeline}` |

### 5.3 系统管理面板

打开 `manage.html` → **系统管理** tab，一站式管理：
- 17 容器状态（按存储/协调/计算/NoSQL/消息/数仓/应用分组）
- 已训练 PySpark 模型列表
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
   └─ classification_rf / dt / gbt / lr            ← 4 个分类 (高价值游客识别)
   ↓
[后端 sklearn joblib 加载]  →  /api/predict 毫秒级响应
```

**前端触发**：manage.html → 系统管理 → ⚡ 一键初始化

---

## 七、Kafka 实时流（完整业务实现）

```
[前端 realtime.html] → POST /api/realtime/{publish/*,task/trigger,hbase/clear}
   ↓
[kafka_producer.publish_*()]
   ↓
[Kafka broker (kafka-1:9092)]
   │  topic: scenic_reviews / scenic_events
   ↓
[kafka_consumer 后台线程]  启动时自动起
   ↓
[hbase_service.put_review() / put_realtime_event()]
   ↓
[HBase scenic_reviews / scenic_realtime]
   ↓
[前端 GET /api/realtime/{visit-recent,visitor/{id},attraction/{id}}]  验证落库
```

**任务触发器** (`POST /task/trigger`)：一键生成 50-500 个事件，模拟：
- `random_events`: 随机混合 enter/exit/review
- `consume_burst`: enter + consume + exit 套餐
- `review_flood`: 大量评分评论

---

## 八、智能双轨 ML 模式

`model_service.predict()` 启动后：

```
[1] 检测 /shared/models/ 是否有 PySpark 模型
   ├─ 有 → pyspark_loader.load_all() 加载 PipelineModel
   └─ 没有 → auto_train.auto_train_if_needed() 异步训练
            ├─ 起后台线程调 docker exec spark-master spark-submit
            ├─ 训练 5-10 分钟
            └─ 训练完自动加载
   ↓
[2] 预测请求到达
   ├─ 优先用 PySpark 模型（如果已加载）
   │  └─ model.transform(spark_df).collect() 拿 prediction
   └─ fallback sklearn（如果 PySpark 失败）
      └─ model.predict(arr)
   ↓
[3] 返回结果 + 引擎标识
   {"prediction": 523.4, "model": "regression_rf (PySpark)", "engine": "pyspark"}
```

**用户不需要任何手动操作**。

---

## 九、常见问题

### Q1: `scripts\start-app.bat` 报 "Python not found"
装 Python 3.10+，勾选 "Add to PATH"。或选模式 2（Docker 模式）。

### Q2: PySpark 模型没加载
看 `http://localhost:8000/api/predict/_engine`。
- `models_loaded: []` → 模型还没训练好（等 5-10 分钟）
- `models_loaded: ["regression_linear", ...]` → 已就绪

### Q3: 训练失败（Java 缺失）
训练在 spark-master **容器内**跑，不依赖 host Java。
看后端日志：`%TEMP%\backend.log` 或 `docker logs demo-backend`。

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
等 ~60s 后 `docker exec hbase-master bash -c "hbase shell /tmp/status.hbase"` 应显示 `1 active master, 2 live servers, 0 dead servers`。

### Q5: Kafka 实时流没数据
看 `manage.html` → Kafka 实时流 tab：
- `producer.enabled: true` ✓
- `consumer.running: true` ✓
- `consumer.stats.messages_consumed > 0` ✓

### Q6: 端口被占用
修改 `.env` 文件（如 `PORT_KAFKA=29092`），重启 `docker compose up -d`。

### Q7: 完全重置
```bat
scripts\stop.bat
scripts\reset.bat       REM 清空所有数据卷
scripts\start.bat
```

### Q8: 答辩演示
1. `manage.html` → 系统管理 → 看 17 容器 ● 全部绿色
2. 点 ⚡ 一键初始化 → 5 分钟后看 4 个已训练模型
3. 切到 predict.html → 选模型 → 输入特征 → 看预测结果
4. 切到 manage.html → Kafka 实时流 → 发评论 → 验证 HBase 落库

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

### 10.3 Kafka ZK 模式（3.1.0）
- ✅ 对齐 docx 原始方案
- ✅ 3 ZK 节点服务 HBase + Kafka（充分利旧）
- ❌ 不上 KRaft（4.x 才有）以保持版本一致性

### 10.4 HBase 通过 docker exec 而非 happybase
- happybase 1.2 + thriftpy2 协议不兼容 HBase 2.x Thrift server
- 详见本文 10.4 节
- docker exec 慢一点但稳

### 10.5 智能双轨 ML
- PySpark 训练体现真大数据（作业要求）
- sklearn 预测保证毫秒级响应
- 启动时自动训练，**用户零操作**

### 10.6 MySQL 复用
- 业务库 + Metastore 合并部署
- 节省资源（一个容器两个用途）
- MySQL 单点对小型项目够用

---

## 附：作业对照表

| 作业要求 | 本项目实现 | 状态 |
|---------|----------|------|
| 6.2 平台搭建 | 17 容器一键部署 | ✅ |
| 6.3 数据采集 | Sqoop MySQL→HDFS + Kafka 实时流 | ✅ |
| 6.4 数据分析 | Spark SQL + Hive 仓库 + 4 回归 + 1 聚类 + 4 分类 | ✅ |
| 6.5 可视化 | 5 页前端 (含独立实时流页) + Kafka 实时推送 | ✅ |

详细作业要求见 [docs/作业要求.md](docs/作业要求.md)。  
实现细节/算法/命令见 [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md)。

---

## 附：scripts 速查表

5 个脚本，**用户日常只用前 3 个**：

| 脚本 | 用途 | 何时用 |
|------|------|--------|
| `start.bat` | 启动 17 容器大数据平台 | 每天开机第一次 |
| `start-app.bat` | 启动 Web 应用（自动装依赖）| 想用 Web 时 |
| `stop.bat` | 停止所有容器 | 不用时 |
| `reset.bat` | 完全重置（清空数据）| 重新开始 |
| `test-e2e.bat` | 端到端业务测试 | CI / 调试 |

**典型工作流**：
```bat
scripts\start.bat          REM 第一次
scripts\start-app.bat      REM 启动 Web
REM 浏览器 http://localhost:8080 玩耍
scripts\stop.bat           REM 关闭
```

---

## License

MIT

## 开发环境（热重载）

> 运行时编辑代码，改完即生效，不用重启。

### 启动开发服务器

```bat
scripts\start.bat          REM 启动大数据平台（首次）
scripts\dev-start.bat      REM 启动开发模式（热重载）
```

### 热重载机制

| 层 | 技术 | 触发自动刷新 |
|------|------|------------|
| 后端 Python | uvicorn --reload | 改 .py 文件自动重启 |
| 前端 HTML/JS/CSS | livereload | 改 .html/.js/.css 浏览器自动刷新 |

### 开发 vs 生产

| 场景 | 命令 | 特点 |
|------|------|------|
| **开发**（改代码） | dev-start.bat | 热重载、即时生效 |
| **演示**（看效果） | start-app.bat | 稳定、不装 livereload |

### 前后端代码位置

`
app/backend/    ← FastAPI 后端（改 routers/ services/ main.py）
app/frontend/   ← HTML 前端（改 *.html static/js/ static/css/）
`

改完保存，浏览器自动刷新。后端 .py 文件改了也能自动重载。

### OpenAPI 文档

http://localhost:8000/docs — 所有 30+ 路由的交互式文档。
前端 JS 调 API 的路径和参数可以直接在这里看。

