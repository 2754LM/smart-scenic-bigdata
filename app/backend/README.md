# FastAPI 后端 - 完整实现

> Web 应用的核心后端，完整实现作业 6.3 / 6.4 / 6.5 全部业务逻辑。
> 32 个 REST 路由 + 5 个 service + 智能双轨 ML 模式 + Kafka 实时流。

## 目录结构

```
app/backend/
├── main.py                 # FastAPI 入口 + CORS + 启动钩子（PySpark + Kafka 智能初始化）
├── config.py               # 所有连接配置（MySQL/Kafka/HBase/Hive/HDFS/Spark）
├── schemas.py              # 共享 Pydantic 模型
├── utils.py                # docker exec / CSV cache / 工具函数
├── requirements.txt        # 依赖（含 PySpark）
├── services/               # 数据访问与业务逻辑层（8 个）
│   ├── mysql_service.py    # MySQL CRUD + 聚合
│   ├── hbase_service.py    # HBase 实时画像 + Kafka Consumer 落库
│   ├── hive_service.py     # Hive 数仓查询
│   ├── kafka_producer.py   # Kafka producer（publish_review / publish_event）
│   ├── kafka_consumer.py   # 后台线程消费 → 写 HBase
│   ├── model_service.py    # 双轨 ML：优先 PySpark 加载，fallback sklearn
│   ├── pyspark_loader.py   # PySpark PipelineModel 加载器
│   └── auto_train.py       # 启动时智能检测 + 自动训练
└── routers/                # API 路由层（7 个模块）
    ├── overview.py         # /api/overview/*  (4 路由)
    ├── attractions.py      # /api/attractions/* (3 路由)
    ├── visitors.py         # /api/visitors/* (3 路由)
    ├── consumption.py      # /api/consumption/* (2 路由)
    ├── analysis.py         # /api/analysis/* (6 路由)
    ├── predict.py          # /api/predict/* (5 路由)
    └── realtime.py         # /api/realtime/* (7 路由：5 读 + 2 publish)
```

## 安装

```bash
cd app/backend
pip install -r requirements.txt
```

或用项目脚本一键装：
```cmd
scripts\install-deps.bat
```

**核心依赖**：fastapi / uvicorn / pymysql / kafka-python / pydantic / pandas / numpy / scikit-learn / pyspark

## 运行

```bash
python main.py
# 或
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

启动后：
- Swagger UI: <http://localhost:8000/docs>
- ReDoc:      <http://localhost:8000/redoc>
- 引擎状态:   <http://localhost:8000/api/predict/_engine>

## 数据源

| 数据 | 真实数据源 | 后端读法 |
|------|------------|----------|
| 业务表 4 张 | MySQL `scenic` 库 | pymysql |
| 4 个原始 CSV | `data/raw_data/*.csv` | pandas (本地缓存) |
| 清洗后数据 | HDFS `/scenic/cleaned/*.parquet` | Spark 写入 |
| 数仓 | Hive `scenic_ext.ext_t_*` 表 | pyhive |
| 实时画像 | HBase `scenic_realtime` | docker exec hbase shell |
| 评论 | HBase `scenic_reviews` | docker exec hbase shell |
| 实时流 | Kafka scenic_reviews / scenic_events | kafka-python |
| ML 模型 | PySpark MLlib 训练 → HDFS + shared volume | PipelineModel.load |

## 32 个 REST 路由

### Overview（总览大屏）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/overview/kpi` | 8 项 KPI（游客/景点/消费/游玩/平均/笔数/日均） |
| GET  | `/api/overview/timeseries` | 日游客/日消费时序 |
| GET  | `/api/overview/attraction-rank` | 景点热度 TOP N |
| GET  | `/api/overview/health` | MySQL/HBase/Hive 健康检查 |

### Attractions（景点）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/attractions` | 全部景点 |
| GET  | `/api/attractions/{id}` | 单个景点 |
| GET  | `/api/attractions/{id}/summary` | 景点汇总（游客/消费/时长） |

### Visitors（游客）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/visitors` | 分页+过滤（gender/min_age/max_age） |
| GET  | `/api/visitors/{id}` | 单个游客 |
| GET  | `/api/visitors/{id}/aggregate` | 个人消费/游玩汇总 |

### Consumption（消费/游玩）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/consumption` | 消费记录分页+过滤 |
| GET  | `/api/consumption/visits` | 游玩记录分页+过滤 |

### Analysis（数据分析）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/analysis/daily` | 日客流量/日消费额时序 |
| GET  | `/api/analysis/hourly` | 24h 时段分布 |
| GET  | `/api/analysis/region` | 地区分布 TOP N |
| GET  | `/api/analysis/age-group` | 年龄×性别分布 |
| GET  | `/api/analysis/type-summary` | 景点类型汇总 |
| GET  | `/api/analysis/fpgrowth` | 关联规则（FPGrowth） |

### Predict（机器学习预测 - 智能双轨）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/predict` | 实时预测（消费/客流量/高价值） |
| GET  | `/api/predict/regression` | 回归模型对比报告 |
| GET  | `/api/predict/classification` | 分类模型对比报告 |
| GET  | `/api/predict/clustering` | 聚类分群解释 |
| GET  | `/api/predict/compare` | 全部模型对比 |
| GET  | `/api/predict/_engine` | 引擎状态（PySpark vs sklearn） |

### Realtime（实时数据 + Kafka 流）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/realtime/visit-recent` | 最近游玩记录（HBase） |
| GET  | `/api/realtime/visitor/{id}` | 游客画像 |
| GET  | `/api/realtime/attraction/{id}` | 景点统计 |
| POST | `/api/realtime/publish/review` | Kafka 发布评论 |
| POST | `/api/realtime/publish/event` | Kafka 发布实时事件 |
| GET  | `/api/realtime/kafka/status` | Kafka 引擎状态 |

## 智能双轨 ML 流程

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

## Kafka 实时流流程

```
[前端 manage.html] → POST /api/realtime/publish/review|event
   ↓
[kafka_producer.publish_*()]
   ↓
[Kafka scenic_reviews / scenic_events topic]
   ↓
[kafka_consumer 后台线程] 启动时起
   ↓
[hbase_service.put_review() / put_realtime_event()]
   ↓
[HBase scenic_reviews / scenic_realtime]
   ↓
[前端 GET /api/realtime/visit-recent] 验证 Consumer 落库
```

## 设计权衡

- **为什么用 docker exec 调 hbase shell？**
  - happybase 1.2 + thriftpy2 协议不兼容 HBase 2.x Thrift server
  - 详见根目录 `AGENTS.md` 5.3 节
- **为什么采用双轨 ML？**
  - PySpark 训练体现真大数据，sklearn 预测保证实时响应
  - 启动时自动训练，用户不需要任何手动操作
  - 训练失败自动 fallback，永远可用
- **为什么用 Kafka 而不是直接写 HBase？**
  - Kafka 解耦生产者和消费者，便于未来加 Spark Streaming / Flink
  - 后台 consumer 单线程落 HBase，保证数据最终一致

## 相关文档

- 📘 [app/README.md](../README.md) - Web 应用总览
- 📘 [app/jobs/README.md](../jobs/README.md) - Spark / Hive / PySpark 训练脚本
- 📘 [docs/快速启动.md](../../docs/快速启动.md) - 完整启动指南
- 📘 [docs/选题要求.md](../../docs/选题要求.md) - 作业要求对照