# REFERENCE - 项目补充文档

> 本文件包含 README.md 未覆盖的细节。日常使用看 README 即可。

## 一、容器启动顺序

```
[MySQL]              ──┐
[ZooKeeper×3]        ──┤
                        ├──→ [Hadoop NN + DN×2]
                        │       │
                        │       ├──→ [HBase Master + RS×2]
                        │       │       │
                        │       │       └──→ [HiveServer2×2]
                        │       │
                        │       └──→ [Spark Master + Worker]
                        │
                        └──→ [Kafka×2]
```

详见 `docker-compose.yml` 中的 `depends_on` 配置。

---

## 二、组件交互关系

### 2.1 离线批量路径

```
[MySQL 业务表]
      │
      │ Sqoop Import 1.4.7 (jobs/sqoop-import-mysql.sh)
      ▼
[HDFS /scenic/sqoop/...]
      │
      │ Spark 清洗转换
      ▼
[HDFS /scenic/cleaned/...]
      │
      ├──→ [Hive 数仓]  ──→  [HiveServer2]  ──→ 后端 SQL 查询
      │
      └──→ [Spark MLlib 建模]
                │
                └──→ 模型保存 HDFS  ──→ 后端加载预测
```

### 2.2 实时流路径

```
[前端 / 数据采集器]
      │
      │ Kafka Producer
      ▼
[Kafka Topic: scenic-events]
      │
      │ Kafka Consumer
      ▼
[HBase: 实时游客分布、消费记录]
      │
      │ HBase Shell via docker exec（当前 Demo）
      ▼
[后端]  ──→  [前端 Web 实时图表]
```

---

## 三、数据模型

### 3.1 业务库（MySQL `scenic` 库）

#### t_scenic（景点表）

| 字段 | 类型 | 说明 |
|------|------|------|
| scenic_id | VARCHAR(20) PK | 景点ID |
| scenic_name | VARCHAR(100) | 景点名 |
| scenic_type | VARCHAR(50) | 类型（natural/culture/entertainment）|
| location | VARCHAR(200) | 所在区域 |
| open_time | VARCHAR(50) | 开放时间 |
| description | TEXT | 描述 |
| ticket_price | DECIMAL | 门票价格 |

#### t_visitor（游客表）

| 字段 | 类型 | 说明 |
|------|------|------|
| visitor_id | VARCHAR(20) PK | 游客ID |
| visitor_name | VARCHAR(50) | 姓名 |
| gender | CHAR(2) | 性别 |
| age | INT | 年龄 |
| age_group | VARCHAR(20) | 年龄段 |
| phone | VARCHAR(20) | 手机 |
| region | VARCHAR(50) | 地区 |

#### t_consume（消费记录）

| 字段 | 类型 | 说明 |
|------|------|------|
| consume_id | INT PK | 自增 |
| visitor_id | VARCHAR(20) | 游客 |
| scenic_id | VARCHAR(20) | 景点 |
| consume_type | VARCHAR(50) | 类型（ticket/food/hotel/shop）|
| amount | DECIMAL | 金额 |

#### t_visit（游玩记录）

| 字段 | 类型 | 说明 |
|------|------|------|
| visit_id | INT PK | 自增 |
| visitor_id | VARCHAR(20) | 游客 |
| scenic_id | VARCHAR(20) | 景点 |
| entry_time | DATETIME | 入园时间 |
| exit_time | DATETIME | 出园时间 |
| duration_min | INT | 时长（分钟）|
| satisfaction | INT | 满意度（1-5）|

#### t_review（评论表，扩展）

| 字段 | 类型 | 说明 |
|------|------|------|
| review_id | INT PK | 自增 |
| visitor_id | VARCHAR(20) | 游客 |
| scenic_id | VARCHAR(20) | 景点 |
| rating | INT | 评分 1-5 |
| comment | VARCHAR(500) | 评论 |

### 3.2 数据流向映射

| 作业 | 数据源 | 处理引擎 | 存储目标 |
|------|--------|---------|---------|
| 6.3 采集 | MySQL `scenic.*` | Sqoop 1.4.7 | HDFS `/scenic/sqoop/` |
| 6.4 仓库 | HDFS CSV | Hive | 数仓 `scenic_ext` |
| 6.4 建模 | HDFS CSV | Spark MLlib | HDFS `/scenic/models/` |
| 6.3 实时 | Kafka topic | Producer/Consumer | HBase `scenic_reviews` |
| 6.5 展示 | HBase/HDFS | 后端 API | 前端 ECharts |

---

## 四、故障排查（FAQ）

### Q1: 启动时报 `no space left on device`

Docker Desktop → Settings → Resources → Disk 调到 60 GB+，或 `docker system prune -a`。

### Q2: 镜像拉取失败 / 超时（国内用户）

配置 Docker 镜像加速器，编辑 `C:\Users\<用户名>\.docker\daemon.json`：

```json
{
  "registry-mirrors": [
    "https://docker.m.daocloud.io",
    "https://docker.1ms.run",
    "https://docker.hlmirror.com"
  ]
}
```

改完必须**完全重启** Docker Desktop（Quit → 重开）。

### Q3: HBase 启动很慢 / status 显示 `initializing`

正常首次启动 1-3 分钟。如果超过 5 分钟：

```cmd
docker compose logs -f hbase-master
docker exec hadoop-namenode hdfs dfs -ls /  # 确认 HDFS OK
```

### Q4: HiveServer2 启动报错 `Metastore connection failed`

MySQL 还没准备好就启了 Hive。手动重启：

```cmd
docker compose restart hive-server-1
```

### Q5: Kafka 启动失败 / 一直重启

KRaft 集群 ID 不一致。重置数据：

```cmd
docker compose down
docker volume rm smart-scenic-bigdata_kafka1-data smart-scenic-bigdata_kafka2-data
docker compose up -d kafka-1 kafka-2
```

### Q6: Spark Worker 一直显示 `UNKNOWN`

```cmd
docker logs spark-worker-1 | tail -30
docker compose restart spark-worker-1
```

### Q7: `hdfs dfs -put` 报 `Could not get block locations`

DataNode 没注册。等 30 秒，或重启：

```cmd
docker exec hadoop-namenode hdfs dfsadmin -report
docker compose restart hadoop-datanode-1 hadoop-datanode-2
```

### Q8: Windows 宿主机连不到 MySQL

```cmd
docker port mysql
mysql -h localhost -P 13306 -uroot -proot123
```

### Q9: HBase e2e 测试 get/count 失败

HBase Region 还在分配（master 重启后）。等 1-2 分钟重试，或：

```cmd
docker exec hadoop-namenode hdfs dfs -rm -r -f /hbase
docker compose restart hbase-master hbase-regionserver-1 hbase-regionserver-2
```

---

## 五、设计权衡

### 5.1 容器化 vs 裸机

- ✅ 可复制：环境一次构建，多人复用
- ✅ 隔离：组件故障不影响宿主机
- ✅ 易清理：`scripts/reset.bat` 完全清空
- ✅ 匹配生产：现代大数据平台均使用容器化部署

### 5.2 真分布式 vs 伪分布式

- ✅ 贴近生产：架构与生产集群一致（多节点、主机名发现、副本机制）
- ✅ 可演示 HA：节点宕机不影响服务
- ✅ 答辩加分：明确说明"分布式"而非"伪分布式"

### 5.3 Kafka KRaft 模式

- 不依赖 ZooKeeper（Kafka 自己的协调）
- 启动更快，运维更简单
- ZK 留给 HBase 用，专注

### 5.4 MySQL 复用

- 业务库 + Metastore 合并部署，节省资源
- MySQL 单点对小型项目够用
- Metastore 数据量小，单实例性能足够

---

## 六、与 docx 原始方案对比

| 项 | docx 方案（Ubuntu 16.04 + 3 VM）| 本项目（Docker） |
|----|-------------------------------|----------------|
| 部署方式 | ssh 登录每台手装 | docker compose 一键 |
| JDK | `tar -zxf jdk-8u162` 手解压 | 烤进 `/opt/jdk8` 镜像 |
| Hadoop | `tar -zxf hadoop-3.1.0` 手解压 | `apache/hadoop:3.3.6` 镜像 |
| 集群拓扑 | 3 台真容器，靠 ssh + workers 文件互联 | 16 个容器，靠 docker 网络互联 |
| 启动命令 | ssh 到每台 + `start-dfs.sh` + `start-yarn.sh` | `docker compose up -d` |
| 内存需求 | 每台 ≥4GB = 共 12GB+ | 总共 ≤16GB |
| 上手成本 | 高（要 VMware + ssh + 同步）| 低（只需 Docker Desktop）|

**结果一致**：都是真分布式 Hadoop 集群。docx 更"教学"（每步都看清楚），本项目更"工程"（一键复用官方镜像）。