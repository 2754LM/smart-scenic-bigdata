# Web 应用 - 智能景区大数据平台

> 完整实现作业 6.2 / 6.3 / 6.4 / 6.5 全部要求。
> FastAPI 后端 + 4 页 Web 前端 + Kafka 实时流推送，覆盖 7 大大数据组件。

## 功能总览

| 作业要求 | Web 应用实现 | 状态 |
|---------|------------|------|
| 6.2 平台搭建 | 通过 17 个 Docker 容器一键部署 | ✅ |
| 6.3 数据采集 | Sqoop MySQL→HDFS + Kafka 实时流 | ✅ |
| 6.4 数据分析 | Spark SQL + Hive 仓库 + 4 个回归 + 1 聚类 + 2 分类模型 | ✅ |
| 6.5 可视化 | 4 页前端（总览/分析/预测/管理）+ Kafka 实时推送 | ✅ |

## 架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                   Web 应用 (app/)                                    │
│                                                                       │
│   ┌────────────────────────────┐  ┌─────────────────────────────┐   │
│   │ FastAPI 后端 (app/backend) │  │ 4 页 Web 前端 (app/frontend)│   │
│   │ - 32 个 REST 路由          │  │ - index.html (总览)           │   │
│   │ - 5 个 service 模块        │  │ - analysis.html (分析)       │   │
│   │ - Kafka 后台消费者         │  │ - predict.html (预测)        │   │
│   │ - 双轨 ML (PySpark+sklearn)│  │ - manage.html (管理 + Kafka) │   │
│   └────────────┬───────────────┘  └────────────┬────────────────┘   │
│                │                                 │                   │
│   ┌────────────┴─────────────────────────────┴────┐               │
│   │ 浏览器 HTTP / WebSocket                          │               │
│   │   GET/POST /api/* (32 个路由)                    │               │
│   └─────────────────────────────────────────────────┘               │
│                              │                                       │
│   ┌──────────────────────────┴─────────────────────────────┐       │
│   │ 业务服务层                                                 │       │
│   │  mysql_service / hdfs_service / hbase_service             │       │
│   │  hive_service / model_service / kafka_producer             │       │
│   │  kafka_consumer / pyspark_loader / auto_train              │       │
│   └────────────┬──────────────────────────────────────────────┘       │
└───────────────┼──────────────────────────────────────────────────────┘
                │
   ┌────────────┴────────────┬────────────┬────────────┬────────────┐
   ▼                         ▼            ▼            ▼            ▼
[MySQL]              [HDFS/Hive]    [HBase]      [Kafka]      [Spark]
   ▲                         ▲            ▲            ▲            ▲
   └─── Sqoop ───────────────┘            └──Spark─────┘────────────┘
```

## 4 页前端

| 页面 | URL | 内容 |
|------|-----|------|
| **总览** | `http://localhost:8080` | 8 KPI + 7 ECharts 图表 |
| **数据分析** | `http://localhost:8080/analysis.html` | FPGrowth 关联规则 + 时段分析 |
| **机器学习预测** | `http://localhost:8080/predict.html` | 回归/分类/聚类动态表单 |
| **管理 + Kafka 实时流** | `http://localhost:8080/manage.html` | 5 tab + Kafka 发布按钮 |

## 32 个 REST 路由

按业务模块分组：

| 模块 | 路由 | 组件 |
|------|------|------|
| 总览 | `/api/overview/{kpi,timeseries,attraction-rank,health}` | MySQL + HBase |
| 景点 | `/api/attractions{,/{id},/{id}/summary}` | MySQL |
| 游客 | `/api/visitors{,/{id},/{id}/aggregate}` | MySQL |
| 消费 | `/api/consumption{,/visits}` | MySQL |
| 分析 | `/api/analysis/{daily,hourly,region,age-group,type-summary,fpgrowth}` | HBase + 算法 |
| 预测 | `/api/predict{,/regression,/classification,/clustering,/compare}` | PySpark/sklearn 双轨 |
| 实时 | `/api/realtime/{visit-recent,visitor/{id},attraction/{id}}` | HBase |
| Kafka | `/api/realtime/publish/{review,event}` | Kafka producer |
| 引擎 | `/api/predict/_engine` | 状态查询 |
| Kafka | `/api/realtime/kafka/status` | 引擎状态 |

## 5 个业务 Service

```
services/
├── mysql_service.py       # pymysql → MySQL
├── hbase_service.py       # docker exec hbase shell (绕过 Thrift 协议)
├── hive_service.py        # pyhive / beeline → Hive
├── kafka_producer.py      # kafka-python → Kafka producer
├── kafka_consumer.py      # 后台线程消费 Kafka → 写 HBase
├── model_service.py       # 双轨 ML：优先 PySpark 加载，fallback sklearn
├── pyspark_loader.py      # PySpark PipelineModel 加载器
└── auto_train.py          # 启动时智能检测 + 自动训练
```

## 启动方式

### 1. 一键启动（推荐）

```cmd
REM 1. 启动大数据平台（17 个容器）
scripts\start.bat

REM 2. 启动 Web 应用（自动检测 + 智能双轨）
scripts\start-app.bat
```

启动后浏览器打开：

- Web 仪表盘：`http://localhost:8080`
- API 文档：`http://localhost:8000/docs`
- 引擎状态：`http://localhost:8000/api/predict/_engine`

### 2. 手动启动

#### 后端（FastAPI）

```bash
cd app/backend
pip install -r requirements.txt
python main.py
```

#### 前端（HTML + ECharts）

```bash
cd app/frontend
python -m http.server 8080
```

## 智能双轨 ML 模式

Web 应用的机器学习功能采用**智能双轨**：

| 状态 | 行为 |
|------|------|
| `/shared/models/` 已有训练好的 PySpark 模型 | 直接加载，预测走 PySpark |
| 没有模型 | 后台线程自动调 `spark-submit` 训练（不阻塞启动）|
| 训练失败或 Spark 不可用 | 自动 fallback 到 sklearn |

**用户不需要任何手动操作**。详见 [docs/快速启动.md](../docs/快速启动.md) 第 5 节。

## Kafka 实时流

完整业务实现：

```
[前端 manage.html] → POST /api/realtime/publish/review|event
   ↓
[kafka_producer.publish_*()]
   ↓
[Kafka scenic_reviews / scenic_events topic]
   ↓
[kafka_consumer 后台线程]
   ↓
[hbase_service.put_review() / put_realtime_event()]
   ↓
[HBase scenic_reviews / scenic_realtime]
   ↓
[前端 GET /api/realtime/visit-recent] 验证落库
```

## 给后端开发者的说明

你**不需要学任何大数据开发的东西**。你只需要把每个组件当成一个微服务来调：

| 你熟悉的 | 大数据组件 | 调法 |
|---------|----------|------|
| MySQL driver | MySQL | `pymysql.connect()` |
| MongoDB driver | HBase | `hbase shell` via docker exec |
| RocketMQ client | Kafka | `kafka-python` |
| AWS S3 CLI | HDFS | `hdfs dfs` |
| Presto / Trino | Hive | `pyhive` |
| DataX 跑批 | Sqoop | `bash sqoop-import-mysql.sh` |
| Celery 提交任务 | Spark | `spark-submit` |

所有调用模式都一样：**发请求 → 等待 → 返回结果**。不用写一行 MapReduce 代码。

## 相关文档

- 📘 [docs/快速启动.md](../docs/快速启动.md) - 完整启动指南
- 📘 [docs/选题要求.md](../docs/选题要求.md) - 作业要求 + 项目对照
- 📘 [app/jobs/README.md](jobs/README.md) - Spark 清洗 / Hive 仓库 / PySpark 训练脚本
- 📘 [../README.md](../README.md) - 大数据平台总说明
- 📘 [../AGENTS.md](../AGENTS.md) - 项目背景与设计权衡