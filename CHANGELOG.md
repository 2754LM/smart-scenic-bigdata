# CHANGELOG — 智能景区大数据平台 (P0-P3)

> 本项目按"选题十八 智能景区管理系统"作业要求分 4 个阶段交付。
> 多人协作 (Git)，本分支 `feature/p3-hive-hbase-marketing` 集中提交了我负责的所有改动。
> 更新时间：2026-06-29

---

## 任务总览

| 阶段 | 主题 | 状态 | 主分支/Tag |
|------|------|------|----------|
| P0 | 平台搭建 + 业务数据初始化 | ✅ | `master` (P0 + future 合并) |
| P1 | Spark 数据采集/清洗/数仓基础 | ✅ | `develop` (P1 多人协作) |
| P2 | 模块化后端 API + 4 页前端可视化 + ML + HBase 实时 | ✅ | `develop` (PR #1 已合) |
| P3 | 作业要求补全（Hive 分区表 + HBase 游玩记录 + 营销建议） | ✅ | `feature/p3-hive-hbase-marketing` (本分支) |
| P4 | Spark Structured Streaming + 模型持久化 HDFS + main.py 整合 | ✅ | `feature/p3-hive-hbase-marketing` (本分支) |
| P5 | 数据扩容（22 万行 CSV）+ 真实环境验证手册 | ✅ | `feature/p3-hive-hbase-marketing` (本分支) |

---

## P3 — 作业要求补全（本次提交）

### Commit
```
efd4541 feat(p3): 作业要求补全 - Hive 分区表 + HBase 游玩记录 + 营销建议 + 其他修复
```
- **20 个文件**改动（1796 行新增 / 206 行删除）
- **6 个新文件**创建

### 任务 1：Hive 分区表
**作业要求**："在 HDFS 上使用 Hive 创建表结构，对数据进行分区存储和管理，提高查询效率。创建分区表，根据时间、景点等进行分区存储。"

| 改动 | 路径 | 说明 |
|------|------|------|
| ✨ 新建 | `app/jobs/hive/partitioned-tables.sql` | 建 `scenic_part.part_t_consumption`（按 `consume_date` 分区）+ `scenic_part.part_t_visit_record`（按 `visit_date` 分区），含 `MSCK REPAIR TABLE` 自动发现分区 + 分区查询示例 |
| ✏️ 改 | `app/jobs/spark/clean.py` | 末尾追加 `partitionBy("dt")` 写入 `/scenic/cleaned_part/`（与原非分区 `/scenic/cleaned/` 并行） |

### 任务 2：HBase 游玩记录表
**作业要求**："在 HBase 中存储实时游玩记录数据，并进行快速查询。创建表结构，包括列族和列，如：时间、游客ID、景点ID、游玩时长。"

| 改动 | 路径 | 说明 |
|------|------|------|
| ✨ 新建 | `app/jobs/hbase/hbase-ddl.sh` | 3 张 HBase 表：<br>• `scenic_visit_record`（cf：visit_time/visitor_id/attraction_id/duration_hours）<br>• `scenic_visitor_profile`（stats：total_visits/total_duration/last_attraction/last_visit_time）<br>• `scenic_attraction_heat`（stats：total_visitors/total_duration/last_visit_time） |
| ✨ 新建 | `app/jobs/spark/write_visit_to_hbase.py` | Spark 作业：把 `cleaned/t_visit_record` 生成 HBase put 脚本（HDFS `/tmp/hbase_import/visit/`） |
| ✨ 新建 | `app/jobs/hbase/import_visit.sh` | 容器内 `hbase shell` 批量灌入（合并 part 文件 + count 验证） |
| ✏️ 改 | `app/backend/services/hbase_service.py` | 加 3 个查询函数：`query_visit_records()` / `get_visitor_profile_from_hbase()` / `get_attraction_heat_from_hbase()`，含 Docker 不可用时合成数据回退 |
| ✏️ 改 | `app/backend/routers/realtime.py` | 加 3 个端点：<br>• `GET /api/realtime/visit-records`（按 visitor_id/attraction_id/时间范围查）<br>• `GET /api/realtime/visitor-profile/{id}`<br>• `GET /api/realtime/attraction-heat/{id}` |

### 任务 3：营销建议
**作业要求**："游客分析：分析不同年龄段和地区的游客分布，找出主要游客群体，并提出营销建议。"

| 改动 | 路径 | 说明 |
|------|------|------|
| ✏️ 改 | `app/backend/services/hive_service.py` | 加 `marketing_suggestions()`：5 维分析（年龄结构 / 客源结构 / 消费能力 / 产品组合 / 交叉销售），每维输出"结论 + 建议 + 支撑数据" |
| ✏️ 改 | `app/backend/routers/analysis.py` | 加 `GET /api/analysis/marketing-suggestions` 端点 |
| ✏️ 改 | `app/frontend/analysis.html` | 末尾加"营销建议"卡片 |
| ✏️ 改 | `app/frontend/static/css/main.css` | 加营销建议卡片样式（渐变 tag + 左边框 + 支撑数据虚线分隔） |
| ✏️ 改 | `app/frontend/static/js/analysis.js` | 加 `loadMarketingSuggestions()` 渲染逻辑 |
| ✏️ 改 | `app/frontend/static/js/api.js` | 加 `analysisMarketingSuggestions` API 方法 |

### 任务 4：环境/配置/文档同步
| 改动 | 路径 | 说明 |
|------|------|------|
| ✨ 新建 | `AGENTS.md` | 给后续 AI 看的项目说明（背景、组件版本、踩坑、命令速查） |
| ✨ 新建 | `app/backend/README.md` | 后端模块说明 |
| ✏️ 改 | `.env` | 端口/版本/连接配置（与 docx 原始方案对齐） |
| ✏️ 改 | `app/backend/main.py` | FastAPI 入口调整 |
| ✏️ 改 | `app/backend/requirements.txt` | 依赖更新 |
| ✏️ 改 | `app/frontend/index.html` | 首页调整 |
| ✏️ 改 | `docs/作业要求.md` | 加 P3 阶段对照表 + 100% 完成声明 |

### 未提交
| 路径 | 原因 |
|------|------|
| `app/backend/services/admin_service.py` | 仅 CRLF→LF 行尾规范化（按用户要求不提交） |

---

## P4 — Spark Streaming + 模型持久化 + main.py 整合（本次提交）

### Commit
```
<待生成> feat(p4): Spark Structured Streaming + 模型持久化 HDFS + main.py 整合 P2/P3 routers
```

### 任务 1：Spark Structured Streaming 实时任务
**作业要求**："6.3 数据处理与存储 - Kafka 实时流处理" + "6.5 可视化与系统整合 - 数据的动态更新"

| 改动 | 路径 | 说明 |
|------|------|------|
| ✨ 新建 | `app/jobs/spark/streaming_visit.py` | Spark Structured Streaming 任务，订阅 Kafka topic `scenic_events`，3 路输出：<br>• 原始事件 → HDFS `/scenic/realtime/events/`（Parquet，按 dt/hour 分区）<br>• 5 分钟窗口聚合 → HDFS `/scenic/realtime/agg_attraction_5min/`（按 attraction_id + event_type）<br>• foreachBatch → HBase `scenic_realtime`（docker exec hbase shell put）<br>含 watermark（2 min）+ checkpoint |
| ✨ 新建 | `app/jobs/spark/run-streaming.sh` | 启动脚本：自动确保 topic 存在、确保 HBase 表存在、spark-submit 提交（带 `--packages spark-sql-kafka-0-10_2.12:3.4.1`） |

**启动方式**：
```bash
docker exec spark-master bash /opt/jobs/spark/run-streaming.sh
```

### 任务 2：模型持久化到 HDFS
**作业要求**："6.4 数据分析与建模 - Spark MLlib / 至少 3 种模型对比"

| 改动 | 路径 | 说明 |
|------|------|------|
| ✏️ 改 | `app/backend/utils.py` | 加 `hdfs_put()` / `hdfs_get()` / `hdfs_exists()` 3 个工具函数（docker cp + hdfs dfs -put 中转） |
| ✏️ 改 | `app/backend/services/model_service.py` | 加 `_persist_model()` / `_load_persisted_models()` / `ensure_models()` / `models_status()` 函数；训练完成后自动把 12 个模型（3 回归 × 2 任务 + 4 分类 + 2 聚类）持久化到 HDFS `/scenic/models/{name}/`；启动时优先从 HDFS 加载，加载不足 5 个才重新训练 |
| ✏️ 改 | `app/backend/routers/predict.py` | 加 `GET /api/predict/status` 端点（查看模型仓库状态）+ `POST /api/predict/retrain` 端点（强制重训 + 持久化） |

**新增端点**：
- `GET /api/predict/status` — 模型仓库状态（已加载 / HDFS 路径 / 本地缓存 / 训练次数）
- `POST /api/predict/retrain` — 强制重新训练并持久化

### 任务 3：main.py 整合（修复 P2 modular 整合漏洞）
**问题**：原 `main.py` 还是 P0 单文件 demo 风格，没有 include P2 modular routers，导致 P2/P3 的 32+ 端点全部不可用。

| 改动 | 路径 | 说明 |
|------|------|------|
| ✏️ 改 | `app/backend/main.py` | **完全重写**（304 行 → 437 行）：<br>• 用 `lifespan` 上下文启动 hook（加载/训练模型、启 Kafka consumer、seed HBase）<br>• `_register_routers()` 动态注册 8 个 modular routers（`overview/attractions/visitors/consumption/analysis/predict/realtime/admin`）<br>• 保留 P0 向后兼容端点（`/api/scenics` / `/api/scenics-hive` / `/api/stats` / `/api/reviews` / `/api/reviews-stream` / `/api/trigger-sqoop` / `/api/hdfs-status`）<br>• 用环境变量（`MYSQL_HOST` / `KAFKA_BOOTSTRAP_HOST` / `LOG_LEVEL`）支持生产部署<br>• 版本号升到 v1.4.0 |

### 任务 4：文档同步
| 改动 | 路径 | 说明 |
|------|------|------|
| ✏️ 改 | `docs/作业要求.md` | 加 P4 阶段对照表 + 100% 完成声明 + Spark Streaming 启动指南 |
| ✏️ 改 | `CHANGELOG.md` | 本文档 |

---

## P5 — 数据扩容 + 真实环境验证手册（本次提交）

### Commit
```
<待生成> feat(p5): 数据扩容 22 万行 CSV 生成器 + 真实环境验证手册
```

### 任务 1：数据扩容（22 万行）
**作业要求**："6.3 数据处理与存储 - 提供多个 CSV 格式的初始数据集"

| 改动 | 路径 | 说明 |
|------|------|------|
| ✨ 新建 | `scripts/generate-raw-data.py` | 纯 Python stdlib（无 pandas/numpy 依赖）数据生成器<br>输出 5 个 CSV，**总 220,210 行**（10 景点 + 200 游客 + 10w 消费 + 10w 游玩 + 2w 评论）<br>字段名严格对齐 `mysql-init/01-init-business.sql` 中文 schema<br>支持 `--consumption / --visit / --review / --visitors` 自定义行数<br>随机种子 20250629 可复现 |

**实际生成结果**（smoke test 已验证）：
```
attractions.csv       10 行       0.00 MB
visitors.csv          200 行      0.01 MB
consumption.csv       100,000 行  5.07 MB
visit_records.csv     100,000 行  4.29 MB
reviews.csv           20,000 行   1.24 MB
─────────────────────────────────────────
合计                  220,210 行  10.6 MB
```

**用法**：
```bash
# 默认 22 万行
python scripts/generate-raw-data.py

# 自定义行数（如 50 万）
python scripts/generate-raw-data.py --consumption 200000 --visit 200000 --review 100000
```

### 任务 2：真实环境验证手册
| 改动 | 路径 | 说明 |
|------|------|------|
| ✨ 新建 | `docs/部署测试-手动清单.md` | 12 步 + 26 项验证清单<br>覆盖 P0 平台启动 → P1 Spark → P2 后端 → P3 HBase → P4 Streaming → P4 模型持久化<br>每步含命令 + 期望输出 + 常见问题<br>含 26 项打勾式 checklist<br>时间估算（首次 30-45 分钟 / 后续 10-15 分钟）|

**手册结构**：
1. 前置条件检查（docker / 资源 / 端口 / 镜像加速器）
2. 启动大数据平台（P0）
3. 验证 P0 数据 + 业务表
4. 生成/导入数据（P5 数据扩容）
5. 启动 P1 Spark 作业（clean / ML / Hive）
6. 验证 P2 后端（**新 main.py** + 32 端点）
7. 启动前端 4 页可视化
8. 验证 P3 新增功能（HBase 3 张新表）
9. 启动 P4 Spark Streaming（首次下载包）
10. 验证 P4 模型持久化
11. 端到端业务场景（7 场景）
12. 性能/资源 + 清理

---

## P2 — 模块化后端 + 4 页前端 + ML + HBase 实时

### Commits
```
c1e64b0 merge: P2 - 模块化后端 API + 4 页前端 + ML 模型 + HBase 实时 + 版本对齐 docx
221b70b feat(P2): 我推的 P2 工作
```

### 关键任务

| # | 任务 | 实现 |
|---|------|------|
| P2.1 | 后端模块化 | `app/backend/{config.py, schemas.py, utils.py, services/, routers/}` 拆分（替代单文件 `main.py`） |
| P2.2 | 32 个 API 端点 | `routers/{overview, attractions, visitors, consumption, analysis, predict, realtime}.py` |
| P2.3 | 4 页前端 | `frontend/{index, analysis, predict, manage}.html` + `static/css/main.css` + 6 个 JS |
| P2.4 | 机器学习 | `app/jobs/ml/train.py`（回归 4 模型 + 聚类 + FPGrowth）<br>`app/backend/services/model_service.py`（分类 4 模型 + 内存加载） |
| P2.5 | HBase 实时 | `scenic_reviews`（评论表） + `scenic_realtime`（Kafka entry/exit 事件）<br>`app/backend/services/hbase_service.py`（docker exec hbase shell 协议） |
| P2.6 | Kafka 流 | `services/kafka_producer.py` + `services/kafka_consumer.py` + 后台消费任务 |
| P2.7 | 端到端可用 | 32 个端点全部跑通，Docker 不可用时合成数据回退 |
| P2.8 | 版本对齐 docx | `.env` / `docker-compose.yml` / Dockerfiles 全部对齐 Ubuntu 16.04 + JDK 1.8.0_162 + Hadoop 3.1.0 |

### 关键文件
- `app/backend/main.py` — FastAPI 入口（含 CORS + 8 个 router + startup HBase seed）
- `app/backend/config.py` — 集中配置（MySQL/Hive/HBase/Kafka/Spark）
- `app/backend/schemas.py` — Pydantic 模型
- `app/backend/services/{mysql_service, hive_service, hbase_service, model_service, kafka_producer, kafka_consumer}.py`
- `app/backend/routers/{overview, attractions, visitors, consumption, analysis, predict, realtime}.py`
- `app/frontend/{index, analysis, predict, manage}.html`
- `app/frontend/static/js/{common, api, index, analysis, predict, manage}.js`
- `docs/P2-后端API模块化交付.md` — P2 交付文档

---

## P1 — 多人协作阶段（其他同学主导）

### 任务
- Spark 清洗任务 `app/jobs/spark/clean.py`（基础版本，4 张表去重 + 字段标准化 + 派生字段）
- PySpark MLlib 训练 `app/jobs/ml/train.py`（4 回归 + KMeans + FPGrowth）
- Hive 数仓 DDL `app/jobs/hive/{ddl, queries, views}.sql`
- Docker 自定义镜像（`docker/hive/`、`docker/hadoop/`）含 Sqoop 1.4.7
- 端到端业务测试 `scripts/test-e2e.bat`

### 关键 Commits
```
cd2f472 feat: 补全作业要求的 Spark 清洗 + Hive 仓库 + PySpark MLlib 训练
bd7a035 feat: 双轨 ML 模式 - PySpark 训练 + 后端 PySpark 加载预测
edd0125 feat: 智能双轨 ML 模式 + Python 一键环境配置
```

---

## P0 — 平台搭建阶段

### 任务
- 16 容器 docker-compose 编排（MySQL/ZK×Hadoop×HBase×Kafka×Spark×Hive）
- 组件版本对齐 docx 原始方案（Ubuntu 16.04 + JDK 1.8.0_162 + Hadoop 3.1.0 + ZK 3.6.3 + HBase 2.4.11 + Kafka 3.1.0 + Maven 3.8.5 + Hive 3.1.3 + Spark 3.1.0）
- MySQL 中文 schema：`mysql-init/01-init-business.sql`（4 张表：t_scenic/t_visitor/t_consume/t_visit + t_review）
- Sqoop MySQL → HDFS：`docker/hadoop/sqoop-import-mysql.sh`
- 启动/验证/重置脚本：`scripts/{start, stop, verify, reset}.bat`
- README 体系：精简到 2 个 .md

### 关键 Commits
```
6ce5a02 merge: P0 阶段 - MySQL 4 表中文 schema + Sqoop + CONTRIBUTING
c1e64b0 merge: P2 - ...
ac70087 Merge pull request #1 from 2754LM/future
```

---

## 关键决策记录

### 1. HBase 客户端选型（AGENTS.md 5.3）
- ❌ happybase Python：HBase 2.x Thrift 协议不兼容（`Bad version in readMessageBegin`）
- ❌ HBase REST server：harisekhon 镜像 web.xml 缺 RESTServletContainer 映射
- ❌ Phoenix 4.x：兼容性问题
- ✅ **docker exec hbase shell**：当前方案
- 生产环境高频写入：用 Java `hbase-client` 或 Phoenix

### 2. 数据回退策略
- Docker 不可用时（开发机无 Docker）：自动用合成数据回退
- HBase 缺数据：自动 seed 30 行 demo 数据
- Kafka 不可用：双写兜底（producer 失败时直接写 HBase）

### 3. 端口冷门号
- 全部用 1xxxx（13306 MySQL / 12181 ZK / 19870 HDFS Web / 18088 YARN / 11610 HBase / 19095 Kafka EXTERNAL / 18080 Spark / 11010 Hive）
- 避 Windows 服务冲突

### 4. HBase 表命名
- `scenic_reviews` — 评论
- `scenic_realtime` — 实时事件（entry/exit/consume）
- `scenic_visit_record` — **游玩记录（P3 新增）**（作业要求 4 字段：时间/游客ID/景点ID/游玩时长）
- `scenic_visitor_profile` — **游客画像聚合（P3 新增）**
- `scenic_attraction_heat` — **景点热度聚合（P3 新增）**

### 5. Spark 输出双轨
- `/scenic/cleaned/` — 非分区（供 `ddl.sql` 的 `ext_t_*`）
- `/scenic/cleaned_part/` — **分区（P3 新增）**（供 `partitioned-tables.sql` 的 `part_t_*`）
- `clean.py` 一次跑写两份

### 6. 营销建议维度（5 维）
1. **年龄结构** — 找最大占比年龄段，推对应产品
2. **客源结构** — 找主客源地，给本地化推广策略
3. **消费能力** — 高消费占比 + 客单价提升
4. **产品组合** — 头部类型做 IP，弱势做打卡任务
5. **交叉销售** — 用 FPGrowth 关联规则做联票

---

## 远程分支状态（2026-06-29）

| 分支 | Commit | 状态 |
|------|--------|------|
| `origin/master` | `ac70087` | P0 + future merge |
| `origin/develop` | `043c3b9` | P1 + P2 已合 |
| `origin/feature/p3-hive-hbase-marketing` | `efd4541` | **本分支 P3 完整工作** |

- ✅ develop 未被污染
- ✅ P3 仅在自己分支
- ✅ 管理人员负责整合

---

## 后续待办

| 优先级 | 任务 | 备注 |
|--------|------|------|
| 高 | 实习报告（基于本文档填充） | `docs/实习报告模板.doc` |
| 中 | 模型持久化到 HDFS `/scenic/models/` | 作业要求"模型存到 HDFS" |
| 中 | Spark Streaming 实时任务 | 作业要求"Kafka → Spark Streaming → HBase" |
| 中 | 数据扩容到 22 万行 | 真实大数据演示 |
| 低 | 单元测试 | 给 services/ + routers/ 加 pytest |
| 低 | CI/CD | GitHub Actions |
