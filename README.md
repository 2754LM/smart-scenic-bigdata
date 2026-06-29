# 智能景区大数据平台 (Smart Scenic BigData Platform)

> 选题十八：智能景区管理系统 — 大数据底层平台  
> 一键部署、分布式、面向教学的完整大数据解决方案

本项目基于 **Docker Compose** 一键部署一套**真分布式**的大数据集群环境，涵盖 Hadoop/HDFS / Sqoop / HBase / Kafka / Spark / Hive / ZooKeeper / MySQL 全部核心组件，共 **16 个容器**。用于支撑"智能景区管理系统"的全部数据处理、分析、机器学习任务。

---

## 〇、组件版本规范

| 组件 | 版本 | 备注 |
|------|------|------|
| Ubuntu | 16.04 | 原始方案基线 |
| JDK | 1.8.0_162 | `jdk-8u162-linux-x64.tar.gz` |
| Hadoop | 3.1.0 | HDFS + YARN |
| ZooKeeper | 3.6.3 | `apache-zookeeper-3.6.3-bin.tar.gz` |
| HBase | 2.4.11 | 2.x 稳定版 |
| Kafka | 3.1.0 | `kafka_2.12-3.1.0.tgz`（Scala 2.12） |
| Maven | 3.8.5 | `apache-maven-3.8.5-bin.tar.gz` |
| Hive | 3.1.3 | 数仓 |
| Spark | 3.1.0 | 计算引擎 |

镜像 tag 在 `.env` 里集中管理，**改一个变量 = 全栈升级**。

---

## 一、项目背景

"智能景区管理系统"作业要求：

| 考核点 | 内容 |
|--------|------|
| 6.2 | 平台环境搭建 — Hadoop/HDFS、Sqoop、HBase、Kafka、Spark 等组件安装与配置 |
| 6.3 | 数据处理与存储 — 数据采集、清洗、分布式存储 |
| 6.4 | 数据分析与建模 — Spark SQL / MLlib / Hive 数仓 / 至少 3 种模型对比 |
| 6.5 | 可视化与系统整合 — 可视化平台 + 数据动态展示 |

本项目对应 **6.2 平台搭建** 考核点，提供了：
- ✅ 一键部署脚本（`scripts\start.bat`）
- ✅ 完整连通性验证（`scripts\verify.bat` - 38 项）
- ✅ 端到端业务流测试（`scripts\test-e2e.bat` - 7 大业务场景）
- ✅ 7 大组件的真分布式部署（含副本机制、HA、Ensemble）
- ✅ 业务数据初始化（**5 张业务表**）
- ✅ **Demo 应用**：`app/` 目录下 FastAPI 后端 + 前端演示，覆盖全部组件调用
- ✅ 完整的部署文档和常见问题

> **关于 Sqoop**：Sqoop 1.4.7 已自动安装到 `hadoop-namenode` 容器（首次启动自动下载 JDK + Sqoop）。在容器内执行 `bash /opt/jobs/sqoop-import-mysql.sh` 可一键将 5 张业务表导入 HDFS `/scenic/sqoop/`。

---

## 二、整体架构

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Windows 宿主机                                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                           │
│  │  Demo 前端   │  │  Demo 后端   │  │   Jupyter    │   ← 6.5 可视化             │
│  │  (HTML+JS)   │  │  (FastAPI)   │  │  (PySpark)   │                           │
│  │  :8080       │  │  :8000       │  │              │                           │
│  └──────────────┘  └──────┬───────┘  └──────────────┘                           │
└─────────────────────────┼───────────────────────────────────────────────────────┘
                          │ JDBC / Thrift / REST API
┌─────────────────────────┴───────────────────────────────────────────────────────┐
│                    Docker Network: bigdata-net                                  │
│                                                                                 │
│  ╔══════════════════════════ 服务层 ═══════════════════════════════════╗         │
│  ║  HiveServer2 × 2  (11010 / 11011)                                ║         │
│  ║  Spark Master    (18080)                                          ║         │
│  ╚════════════════════════════════════════════════════════════════════════╝         │
│                                                                                 │
│  ╔══════════════════════════ 计算层 ═══════════════════════════════════╗         │
│  ║  Hadoop NameNode + 2 DataNode (HDFS 真分布式, 副本=2)            ║         │
│  ║  Spark Worker                                          │         ║         │
│  ╚════════════════════════════════════════════════════════════════╝         │
│                                                                                 │
│  ╔══════════════════════════ 存储层 ═══════════════════════════════════╗         │
│  ║  HBase Master + 2 RegionServer ──┐                │         ║         │
│  ║  MySQL 8.0  (业务库 + Hive Metastore)               │         ║         │
│  ╚════════════════════════════════════════════════════════════════╝         │
│                                                                                 │
│  ╔══════════════════════════ 消息层 ═══════════════════════════════════╗         │
│  ║  Kafka Broker × 2 (KRaft 模式, 不依赖 ZK)            ║         │
│  ╚════════════════════════════════════════════════════════════════╝         │
│                                                                                 │
│  ╔══════════════════════════ 协调层 ═══════════════════════════════════╗         │
│  ║  ZooKeeper Ensemble × 3 (1 leader + 2 follower)                 ║         │
│  ║  └── 仅 HBase 使用（Kafka 用 KRaft 自带元数据）                 ║         │
│  ╚════════════════════════════════════════════════════════════════╝         │
└─────────────────────────────────────────────────────────────────────────────────┘
```

> 注：图中箭头表示**数据流/控制流**方向，不是物理网络。HBase 通过 ZK 找 Master/RegionServer；Kafka 集群内部自己协调；Spark 任务由 Master 调度到 Worker。

---

## 三、组件清单与功能

### 3.1 MySQL (1 容器)

| 项 | 说明 |
|----|------|
| 镜像 | `mysql:8.0` |
| 端口 | `13306` (宿) / `3306` (容器内) |
| 容器 | `mysql` |
| mem_limit | 1 GB |
| 作用 | ① 业务数据存储（5 张表）<br>② HiveServer2 的 Metastore 数据库 |

**业务表（5 张，初始化在 `mysql-init/01-init-business.sql`）**：

| 表名 | 说明 | 测试数据量 |
|------|------|-----------|
| `t_scenic` | 景点信息（10 个景点） | 10 行 |
| `t_visitor` | 游客信息（含年龄段、地区） | 20 行 |
| `t_consume` | 消费记录（门票/餐饮/纪念品/交通） | 32 行 |
| `t_visit` | 游玩记录（含入园出园时间、满意度） | 20 行 |
| `t_review` | 评论表（扩展表，演示用） | 10 行 |

### 3.2 ZooKeeper (3 容器)

| 项 | 说明 |
|----|------|
| 镜像 | `zookeeper:3.9` |
| 端口 | `12181` (宿) / `2181` (容器内) |
| 容器 | `zookeeper-1`, `zookeeper-2`, `zookeeper-3` |
| mem_limit | 512 MB × 3 |
| 作用 | **HBase 协调服务**（保存 Region 位置、Master 选举）|

**为什么 3 个**：奇数节点保证选举时不会脑裂，容忍 1 节点宕机。

> Kafka **不依赖 ZK**（用 KRaft 自带的 controller 节点协调）

### 3.3 Hadoop (3 容器)

| 项 | 说明 |
|----|------|
| 镜像 | `apache/hadoop:3.3.6` |
| 容器 | `hadoop-namenode`, `hadoop-datanode-1`, `hadoop-datanode-2` |
| 端口 | NN Web `19870`, YARN Web `18088`, NN RPC `19000` |
| 组件 | HDFS (分布式文件系统) + YARN (资源调度) + Sqoop (自动装在 namenode) |
| 副本数 | 2（每个文件存在 2 个 DN 上） |
| mem_limit | NN 1.5 GB + DN×2 各 1 GB |

**作用**：
- HDFS：分布式存储业务原始数据、清洗后数据、模型文件
- YARN：调度 MapReduce / Spark 作业
- Sqoop（首次启动自动装）：定时把 MySQL 数据搬到 HDFS

### 3.4 HBase (3 容器)

| 项 | 说明 |
|----|------|
| 镜像 | `harisekhon/hbase:latest` |
| 容器 | `hbase-master`, `hbase-regionserver-1`, `hbase-regionserver-2` |
| 端口 | **Web `11610`（HBase 唯一对外 Web 端口）**<br>**Thrift `19090`（Python happybase 客户端用）** |
| mem_limit | Master 1 GB + RS×2 各 1.3 GB |
| 存储后端 | HDFS（数据存在 `/hbase` 路径） |
| 协调 | ZooKeeper |

**作用**：实时读写游客数据（如实时入园人数、实时消费记录）

### 3.5 Kafka (2 容器)

| 项 | 说明 |
|----|------|
| 镜像 | `apache/kafka:latest` |
| 模式 | **KRaft 模式**（不依赖 ZooKeeper）|
| 容器 | `kafka-1`, `kafka-2` |
| 端口 | Broker `19092` (kafka-1 内部), `19095` (外部 EXTERNAL listener) |
| mem_limit | 1 GB × 2 |
| 副本数 | 默认 2，容忍 1 节点宕机 |

**作用**：实时数据流（游客行为数据 → Kafka → Spark Streaming → HBase）

> 注意：外部 Python 客户端必须用 `localhost:19095`（EXTERNAL listener），不能用 `localhost:19092`（容器内部 PLAINTEXT listener 会告诉客户端去找 `kafka-1:9092`，Windows 解析不到）。

### 3.6 Spark (2 容器)

| 项 | 说明 |
|----|------|
| 镜像 | `apache/spark:3.4.1` |
| 容器 | `spark-master`, `spark-worker-1` |
| 端口 | Web UI `18080`, RPC `17077` |
| mem_limit | Master 1 GB + Worker 1.3 GB |

**作用**：
- Spark SQL：数据查询分析（对应 6.4）
- Spark MLlib：机器学习建模（至少 3 种模型对比）
- Spark Streaming：实时数据处理

### 3.7 Hive (2 容器)

| 项 | 说明 |
|----|------|
| 镜像 | `apache/hive:3.1.3` |
| 容器 | `hive-server-1`, `hive-server-2` |
| 端口 | Thrift `11010`/`11011`, Web `11012`/`11013` |
| mem_limit | 1 GB × 2 |
| Metastore | 直连 MySQL（嵌入式 metastore） |

**作用**：数据仓库 + SQL 查询引擎（HiveQL → MapReduce 执行）

---

## 四、完整端口对照表

> **注意**：端口号全部是冷门号（1xxxx），避开 Windows 服务冲突。

| 服务 | 容器内部端口 | 宿主机端口 | 协议 | 备注 |
|------|------------|----------|------|------|
| **MySQL** | 3306 | **13306** | TCP | |
| **ZooKeeper** | 2181 | **12181** | TCP | 客户端端口 |
| **Hadoop NN Web** | 9870 | **19870** | HTTP | |
| **Hadoop YARN Web** | 8088 | **18088** | HTTP | |
| **Hadoop NN RPC** | 9000 | **19000** | TCP | |
| **HBase Web** | 16010 | **11610** | HTTP | Web UI |
| **HBase Thrift** | 9090 | **19090** | Thrift | Python happybase 用 |
| **HBase Master RPC** | (随机) | - | - | 通过 ZK 自动发现 |
| **HBase RS RPC** | (随机) | - | - | 通过 ZK 自动发现 |
| **Kafka Broker (kafka-1)** | 9092 | **19092** | TCP | 容器内部通信用 |
| **Kafka External (kafka-1)** | 9094 | **19095** | TCP | **外部 Python 客户端用这个** |
| **Kafka External (kafka-2)** | 9094 | **19094** | TCP | 备用 |
| **Kafka Controller** | 9093 | (内部) | TCP | KRaft 选举 |
| **Spark Web** | 8080 | **18080** | HTTP | |
| **Spark RPC** | 7077 | **17077** | TCP | Worker 注册用 |
| **Spark Worker Web** | 8081 | - | - | 未对外暴露 |
| **HiveServer2 #1 Thrift** | 10000 | **11010** | Thrift/JDBC | |
| **HiveServer2 #1 Web** | 10002 | **11012** | HTTP | |
| **HiveServer2 #2 Thrift** | 10000 | **11011** | Thrift/JDBC | |
| **HiveServer2 #2 Web** | 10002 | **11013** | HTTP | |

---

## 五、数据流向

```
[MySQL 业务库 scenic]
   │
   │  Sqoop 1.4.7 import (hadoop-namenode 自动执行 jobs/sqoop-import-mysql.sh)
   ▼
[HDFS /scenic/sqoop/]
   │
   │  Spark / MapReduce 清洗 (作业 6.3)
   ▼
[HDFS /scenic/cleaned/]
   │
   ├──→ [Hive 数仓 scenic_dw] (作业 6.4)
   │       │
   │       └──→ [HiveServer2] ← 后端 SQL 查询
   │
   ├──→ [Spark MLlib 建模] (作业 6.4)
   │       │
   │       └──→ [HDFS /scenic/models/] ← 后端加载预测
   │
   └──→ [Kafka 实时流] (作业 6.3)
            │
            └──→ [Spark Streaming]
                    │
                    └──→ [HBase 实时表] ← 前端实时图表
```

---

## 六、Demo 应用（推荐演示）

`app/` 目录下有一个完整的 **FastAPI 后端 + HTML 前端** Demo 应用，覆盖全部大数据组件调用：

```cmd
REM 启动大数据平台
scripts\start.bat

REM 启动 Demo 应用
scripts\start-app.bat
```

启动后浏览器打开：
- 前端演示页：`http://localhost:8080`
- API 文档：`http://localhost:8000/docs`

**Demo 功能（前端按钮 → 后端调用的组件）**：

| 前端按钮 | 后端接口 | 调用的组件 | 演示内容 |
|---------|---------|-----------|---------|
| Reload from MySQL | `GET /api/scenics` | MySQL | OLTP 查景点 |
| Reload from Hive | `GET /api/scenics-hive` | HDFS | 同一份数据从 HDFS 来 |
| Trigger Sqoop Import | `POST /api/trigger-sqoop` | Sqoop | 跑 5 张表 ETL 导入 |
| Show HDFS Files | `GET /api/hdfs-status` | HDFS | 看 HDFS 上 Sqoop 输出的文件 |
| Run Spark Job | `GET /api/stats` | Spark | Spark SQL 聚合统计 |
| Write to HBase | `POST /api/reviews` | HBase | 实时写评论（NoSQL） |
| Scan HBase | `GET /api/reviews/{id}` | HBase | 实时读评论 |
| Publish (Kafka) | `POST /api/reviews-stream` | Kafka | 异步发布消息 |
| Consume (Kafka) | `GET /api/reviews-stream` | Kafka | 异步消费消息 |

每个按钮都能验证对应组件是否正常工作。详细架构和组件讲解见 `app/组件说明.md` 和 `app/README.md`。

---

## 七、运维命令

### 7.1 启动平台

**前置条件（一次性）**：
1. 安装 Docker Desktop ≥ 4.10
2. Docker Desktop → Settings → Resources → Memory 调到 **16 GB**
3. （国内用户）Settings → Docker Engine，添加镜像加速器：
   ```json
   {
     "registry-mirrors": [
       "https://docker.m.daocloud.io",
       "https://docker.1ms.run",
       "https://docker.hlmirror.com"
     ]
   }
   ```

**启动**：
```cmd
cd D:\Desktop\smart-scenic-bigdata
scripts\start.bat
```

启动会分 4 阶段：
- **阶段 1**：mysql、zookeeper×3（核心组件）
- **阶段 2**：hadoop×3、hive×2（数据层）
- **阶段 3**：kafka×2、hbase×3、spark×2（扩展组件，镜像可能还在拉取）
- **阶段 4**：等待 Sqoop 后台自动安装（首次 ~3 分钟），完成后自动跑 5 张表 Sqoop import

首次启动约 **10-15 分钟**（需下载 6+ GB 镜像 + 200MB JDK）。

### 7.2 验证平台（组件连通性）

```cmd
scripts\verify.bat
```

会跑 **38 项**测试，覆盖 7 大组件（端口、进程、副本机制等）。

### 7.3 端到端业务测试

```cmd
scripts\test-e2e.bat
```

会跑 **7 大业务场景**（MySQL 数据 / HDFS 存储 / Hive / Kafka 流 / HBase CRUD / Spark 集群 / Sqoop 数据采集），共 **36 项**测试。

### 7.4 启动 Demo 应用

```cmd
scripts\start-app.bat
```

同时启动 FastAPI 后端（端口 8000）和前端 HTTP 服务（端口 8080）。

### 7.5 停止平台（保留数据）

```cmd
scripts\stop.bat
```

容器停止，但 Docker volumes（MySQL 数据、HDFS 数据等）保留。

### 7.6 重置平台（清空数据）

```cmd
scripts\reset.bat
```

会：
1. 删除所有容器 + volumes
2. 清理 `data/` 和 `logs/` 目录
3. 清理 Docker 悬空资源

输入 `yes` 确认后执行。

### 7.7 单独启动某个服务

```cmd
docker compose up -d kafka-1 kafka-2
docker compose restart hive-server-1
docker compose logs -f hadoop-namenode
```

---

## 八、目录结构

```
smart-scenic-bigdata/
├── docker-compose.yml          # 16 容器编排
├── .env                        # 端口/密码/版本配置
├── README.md                   # 本文件
├── AGENTS.md                   # 给后续 AI/开发者看的项目说明
│
├── docker/                     # 自定义镜像的 Dockerfile（把 jar/脚本烤进镜像）
│   ├── hive/
│   │   ├── Dockerfile          # apache/hive + MySQL JDBC
│   │   └── mysql-connector-java-8.0.33.jar
│   └── hadoop/
│       ├── Dockerfile          # apache/hadoop + JDK 1.8 + Sqoop 1.4.7 + 脚本
│       ├── mysql-connector-java-8.0.33.jar
│       ├── commons-lang-2.6.jar
│       ├── install-sqoop.sh    # 备用：单独重装 Sqoop 用
│       ├── starter.sh          # 容器启动入口
│       └── sqoop-import-mysql.sh  # 5 张表 ETL 导入
│
├── config/                     # 各组件运行时配置（bind mount）
│   ├── zoo.cfg                 # ZK ensemble 配置
│   ├── hadoop/                 # HDFS/YARN/MapReduce 配置
│   ├── hbase/                  # HBase 配置 + regionservers
│   ├── hive/                   # Hive 配置
│   ├── spark/                  # Spark 配置
│   └── kafka/                  # Kafka KRaft 说明
│
├── mysql-init/                 # MySQL 初始化脚本
│   └── 01-init-business.sql    # 5 张业务表 + 测试数据
│
├── app/                        # Demo 应用
│   ├── README.md               # Demo 总说明
│   ├── 组件说明.md              # 8 个大数据组件对照后端经验
│   ├── backend/
│   │   ├── main.py             # FastAPI 单文件后端
│   │   ├── requirements.txt
│   │   └── README.md
│   └── frontend/
│       └── index.html          # 单文件前端（HTML + ECharts）
│
├── scripts/                    # 运维脚本（Windows .bat）
│   ├── start.bat               # 一键启动大数据平台
│   ├── start-app.bat           # 一键启动 Demo 应用
│   ├── stop.bat                # 停止（保留数据）
│   ├── verify.bat              # 38 项组件连通性测试
│   ├── test-e2e.bat            # 7 大业务场景测试
│   └── reset.bat               # 完全重置（清空数据）
│
├── data/                       # 持久化数据卷（自动生成）
├── logs/                       # 日志（自动生成）
│
└── docs/                       # 详细文档
    ├── 部署文档.md
    ├── 架构说明.md
    └── 常见问题.md
```

**设计原则**：
- **`docker/`** 装不常改的东西（jar、启动脚本）→ 烤进镜像
- **`config/`** 装常改的运行时配置 → bind mount，改完 `restart` 即生效
- **`mysql-init/`** 装首次初始化 SQL → bind mount，MySQL 容器首次启动自动跑

---

## 九、常见问题

### Q1: Docker 内存分配多少？
**至少 16 GB**（当前 .wslconfig 设置）。Docker Desktop → Settings → Resources → Memory。

> 注：Windows 宿主机本身的 WSL2 `.wslconfig` 限制在 `C:\Users\<用户>\.wslconfig`，本平台默认 `memory=16GB`。

### Q2: 启动后哪些组件没起来？
查看 `docker ps`，如果有容器 `Restarting` 状态，运行：
```cmd
docker logs <container-name>
```
看具体错误。常见原因：镜像没下载完（重新 `docker pull`）、端口冲突（修改 `.env`）。

### Q3: 镜像加速器配置？
详见 [docs/常见问题.md](docs/常见问题.md) Q2。

### Q4: 端口被占用？
本项目已用 1xxxx 冷门号，但仍可能被 `wslrelay`（Docker 残留）占用。解决：
```cmd
netstat -ano | findstr :19094
```
重启 Docker Desktop 释放。

### Q5: 数据能持久化吗？
✅ 能。所有数据存在 Docker volumes 中：

| Volume | 用途 |
|--------|------|
| `mysql-data` | MySQL 业务数据 + Hive Metastore |
| `namenode-data`, `datanode1-data`, `datanode2-data` | HDFS |
| `zk1-data`, `zk2-data`, `zk3-data` | ZooKeeper |
| `hbase-master-data`, `hbase-rs1-data`, `hbase-rs2-data` | HBase |
| `kafka1-data`, `kafka2-data` | Kafka 日志 |
| `hive-server1-data`, `hive-server2-data` | Hive 配置 |
| `shared-data` | Spark Master/Worker 共享 |

`scripts\stop.bat` 不会删 volumes，`scripts\reset.bat` 会删。

### Q6: HBase 客户端怎么连？
两种方式：
1. **Thrift 协议**（推荐用于 Python）：连 `localhost:19090`
2. **ZK 自动发现**（推荐用于生产）：用 ZK quorum 让客户端自动找 Master

详见 [docs/常见问题.md](docs/常见问题.md) Q13。

### Q7: Kafka 客户端怎么连？
**必须用 EXTERNAL listener**：
- ✅ `localhost:19095`（外部 Python 客户端）
- ❌ `localhost:19092`（容器内部通信，Windows 解析不到 `kafka-1` host）

详见 `app/backend/README.md` 端口对照表。

### Q8: 加新业务表要改 docker 吗？
**不需要改 compose**。改 2 个文件：
1. `mysql-init/01-init-business.sql` 加 DDL + 初始数据
2. `docker/hadoop/sqoop-import-mysql.sh` 加表名到 `for t in ...` 循环

然后：
```cmd
REM 1. 重置 MySQL（让新 SQL 重新跑）
docker compose down mysql
docker volume rm smart-scenic-bigdata_mysql-data
docker compose up -d mysql

REM 2. 因为 sqoop-import-mysql.sh 在镜像里，需要 rebuild
docker compose build hadoop-namenode
docker compose up -d --force-recreate hadoop-namenode

REM 3. 触发 Sqoop import
docker exec hadoop-namenode bash /opt/jobs/sqoop-import-mysql.sh
```

---

## 十、Web UI 一览

| 服务 | URL | 用途 |
|------|-----|------|
| HDFS | http://localhost:19870 | 浏览文件系统 |
| YARN | http://localhost:18088 | 资源管理 |
| HBase | http://localhost:11610 | 集群状态 |
| Spark | http://localhost:18080 | 任务监控 |
| HiveServer2 #1 Web | http://localhost:11012 | 查询界面 |
| HiveServer2 #2 Web | http://localhost:11013 | 查询界面 |
| **Demo 前端** | **http://localhost:8080** | **组件演示页** |
| **Demo API** | **http://localhost:8000/docs** | **Swagger UI** |

JDBC 连接字符串：
- `jdbc:hive2://localhost:11010` (HiveServer2 #1)
- `jdbc:hive2://localhost:11011` (HiveServer2 #2)

---

## 十一、文档导航

- 📘 [部署文档](docs/部署文档.md) - 完整部署步骤与验收清单
- 🏗️ [架构说明](docs/架构说明.md) - 组件交互、数据流、目录挂载
- ❓ [常见问题](docs/常见问题.md) - FAQ 与故障排查
- 🎯 [Demo 应用说明](app/README.md) - FastAPI + 前端演示
- 📚 [组件说明（给后端开发者）](app/组件说明.md) - 大数据组件对照后端经验

---

**版本**：v1.3  
**最后更新**：2026-06-28  
**配套作业**：选题 18 — 智能景区管理系统