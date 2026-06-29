# Demo 后端 - FastAPI

单文件 FastAPI Demo 后端，连 6 个大数据组件：

| 组件 | 连接方式 | 说明 |
|------|---------|------|
| MySQL | pymysql | OLTP 业务查询 |
| HDFS | subprocess 调 `hdfs dfs` | 读取 Sqoop 导入的数据 |
| HBase | subprocess 调 `hbase shell` | 通过 docker exec 写/读 |
| Kafka | kafka-python | 发布 + 消费消息 |
| Spark | subprocess 调 `spark-submit` | 提交分布式计算任务 |
| Sqoop | subprocess 调 docker exec | 触发 ETL 导入 |

> 注：HBase 客户端也可以用 happybase（Thrift 协议），但 HBase 2.x Thrift server 默认 compact 协议跟 Python happybase 1.x 不兼容。本 Demo 用 `hbase shell` 代替，绕过协议问题。

## 安装

```bash
pip install -r requirements.txt
```

## 运行

```bash
cd app/backend
python main.py
# 或
uvicorn main:app --host 0.0.0.0 --port 8000
```

打开 `http://localhost:8000/docs` 看 Swagger UI。

## 接口列表

| 方法 | 路径 | 组件 | 说明 |
|------|------|------|------|
| GET  | /api/health | - | 健康检查 |
| GET  | /api/scenics | MySQL | 查所有景点（OLTP 路径） |
| GET  | /api/scenics-hive | HDFS | 同一份数据，从 HDFS 来（OLAP 路径） |
| GET  | /api/stats | Spark | Spark SQL 聚合：景点访问次数 |
| POST | /api/reviews | HBase | 写一条实时评论到 HBase |
| GET  | /api/reviews/{scenic_id} | HBase | 查某景点的所有评论 |
| POST | /api/reviews-stream | Kafka | 发布评论到 Kafka topic |
| GET  | /api/reviews-stream | Kafka | 从 Kafka topic 消费消息 |
| POST | /api/trigger-sqoop | Sqoop | 触发 Sqoop 批量导入 |
| GET  | /api/hdfs-status | HDFS | 看 HDFS 上 Sqoop 输出的文件 |

## 架构

后端跑在 Windows 宿主机，通过 Docker 端口转发连所有容器：

```
后端 (Python/FastAPI)
    │
    ├── localhost:13306 → mysql:3306           (MySQL)
    ├── localhost:19095 → kafka-1:9094         (Kafka EXTERNAL)
    ├── localhost:18080 → spark-master:8080    (Spark Web)
    ├── localhost:19870 → hadoop-namenode:9870 (HDFS Web)
    ├── localhost:18088 → hadoop-namenode:8088 (YARN Web)
    ├── localhost:11610 → hbase-master:16010   (HBase Web)
    ├── localhost:11010 → hive-server-1:10000  (Hive Thrift)
    └── docker exec hadoop-namenode            (Sqoop / HDFS cli)
    └── docker exec hbase-master               (hbase shell)
    └── docker exec spark-master               (spark-submit)
```