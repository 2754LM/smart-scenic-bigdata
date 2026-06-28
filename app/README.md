# Demo 应用 - 智能景区大数据平台

用最少代码演示**微服务后端**如何跟大数据平台**全部 8 个组件**交互。给后端开发者用，作为大数据组件的入门 demo。

## 架构图

```
              ┌─────────────────────────────┐
              │   FastAPI 后端 (app/backend) │
              │   - pymysql     → MySQL      │
              │   - hdfs dfs    → HDFS       │
              │   - hbase shell → HBase      │
              │   - kafka-py    → Kafka      │
              │   - spark-submit → Spark     │
              │   - docker exec → Sqoop      │
              └──────────────┬───────────────┘
                             │
            ┌────────┬───────┼────────┬─────────┐
            ▼        ▼       ▼        ▼         ▼
         [MySQL]  [HDFS]  [HBase]   [Kafka]  [Spark]
            │        ▲       ▲         │        │
            └──Sqoop─┘       └─Spark───┘────────┘
```

## 启动方式

### 1. 一键启动（推荐）

```cmd
REM 1. 启动大数据平台（16 个容器）
scripts\start.bat

REM 2. 启动 Demo 应用（后端 + 前端）
scripts\start-app.bat
```

启动后浏览器打开：

- 前端演示页：`http://localhost:8080`
- API 文档：`http://localhost:8000/docs`

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

或者直接双击 `app/frontend/index.html` 在浏览器打开。

## 接口说明

| 路径 | 组件 | 用途 | 作业对应 |
|------|------|------|---------|
| `GET /api/scenics` | MySQL | OLTP 查景点列表 | - |
| `GET /api/scenics-hive` | HDFS | OLAP 查同一份数据（Sqoop 导入的） | 6.3 |
| `GET /api/stats` | Spark | Spark SQL 聚合：景点访问次数 | 6.4 |
| `POST /api/reviews` | HBase | 实时写评论（NoSQL） | 6.5 |
| `GET /api/reviews/{id}` | HBase | 实时读评论 | 6.5 |
| `POST /api/reviews-stream` | Kafka | 异步发布消息 | 6.3 |
| `GET /api/reviews-stream` | Kafka | 异步消费消息 | 6.3 |
| `POST /api/trigger-sqoop` | Sqoop | 触发批量 ETL | 6.3 |
| `GET /api/hdfs-status` | HDFS | 看 HDFS 文件 | 6.3 |

## 给后端开发者的核心 takeaway

你**不需要学任何大数据开发的东西**。你只需要把每个组件当成一个微服务来调：

| 你熟悉的 | 大数据组件 | 调法 |
|---------|----------|------|
| MySQL driver | MySQL | `pymysql.connect()` |
| MongoDB driver | HBase | `happybase` 或 `hbase shell` |
| RocketMQ client | Kafka | `kafka-python` |
| Celery 提交任务 | Spark | `spark-submit` |
| AWS S3 CLI | HDFS | `hdfs dfs` |
| DataX 跑批 | Sqoop | `sqoop import` |
| Presto / Trino | Hive | `pyhive` 或 beeline |

所有调用模式都一样：**发请求 → 等待 → 返回结果**。你不会写一行 MapReduce 代码，除非性能真有压力。

## Demo 演示路径（5 分钟）

打开 `http://localhost:8080`：

1. **MySQL OLTP** — 点 "Reload from MySQL" 看 10 行景点
2. **HDFS OLAP** — 点 "Reload from Hive" 看同一份数据（Sqoop 导入的）
3. **触发 ETL** — 点 "Trigger Sqoop Import" 看日志，再点 "Show HDFS Files" 看 `/scenic/sqoop/` 下文件
4. **Spark 统计** — 点 "Run Spark Job" 看分布式计算输出
5. **HBase 实时写** — 填评论表单 → "Write to HBase" → "Scan HBase" 看刚写的行
6. **Kafka 流** — 填消息 → "Publish" → "Consume Messages" 看刚发的消息

每点一次按钮，后端都会调对应组件，证明整个链路通畅。

## 相关文档

- 📘 [组件说明.md](组件说明.md) - 8 个大数据组件对照后端经验
- 📘 [../README.md](../README.md) - 大数据平台总说明
- 📘 [../docs/部署文档.md](../docs/部署文档.md) - 完整部署步骤