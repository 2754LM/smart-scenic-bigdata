# AGENTS.md - 智能景区大数据平台 项目说明

> 给后续 AI/开发者看的项目背景文档。请从头阅读这份文件再开始任何修改。

---

## 1. 项目是什么

**选题十八 - 智能景区管理系统** 作业的 **6.2 平台搭建** 评分点。

一键部署一套**真分布式**的大数据集群，**16 个 Docker 容器**，涵盖：

| 组件 | 数量 | 作用 | 类比 |
|------|------|------|------|
| MySQL | 1 | OLTP 业务库 + Hive Metastore | 你熟悉的 MySQL |
| ZooKeeper | 3 | 分布式协调（HBase 用） | etcd / Consul |
| Hadoop (HDFS+YARN) | 3 | 分布式文件存储 + 资源调度 | MinIO + Celery |
| **Sqoop** | 1 (hadoop-namenode) | MySQL → HDFS ETL | DataX / Airbyte |
| HBase | 3 | HDFS 之上的 KV NoSQL | MongoDB |
| Kafka | 2 | 消息队列（KRaft 模式，不依赖 ZK） | RocketMQ |
| Spark | 2 | 分布式计算 | Celery worker cluster |
| Hive | 2 | HDFS 上的 SQL 引擎 | Presto / Trino |

**作业要求（6.2-6.5）**：
- 6.2 平台搭建 ✅（本项目核心）
- 6.3 数据采集（MySQL → HDFS 用 Sqoop；Kafka 实时流）
- 6.4 数据分析（Spark SQL + MLlib + Hive）
- 6.5 可视化（`app/` 目录下的 FastAPI + 前端 Demo）

---

## 2. 关键事实速览

### 2.1 端口全部是冷门号（1xxxx）

```
MySQL       13306
ZK          12181
HDFS NN     19000 (RPC) / 19870 (Web)
YARN        18088 (Web)
HBase       11610 (Web) / 19090 (Thrift) / 11620 (RS RPC)
Kafka       19092 (内部) / 19095 (EXTERNAL 给 host)
Spark       17077 (RPC) / 18080 (Web)
Hive        11010/11011 (Thrift) / 11012/11013 (Web)
```

**特别注意**：外部 Python 客户端连 Kafka **必须用 `localhost:19095`**（EXTERNAL listener），
不能用 `localhost:19092`（容器内部 PLAINTEXT listener 会告诉客户端去找 `kafka-1:9092`，
Windows host 解析不到 `kafka-1` 这个 hostname）。

### 2.2 容器默认用户问题

- `apache/hadoop:3.3.6` 镜像默认用户是 `hadoop` (uid 1000)，**不是 root**
- `apache/hive:3.1.3` 镜像默认用户是 `root`
- `harisekhon/hbase:latest` 默认用户是 `root`
- `apache/spark:3.4.1` 默认用户是 `root`

→ 装 JDK、Sqoop 等需要 `docker exec --user root xxx`。

### 2.3 JAVA_HOME 路径

| 镜像 | JAVA_HOME |
|------|-----------|
| `apache/hadoop:3.3.6` 官方 | `/usr/lib/jvm/jre`（仅 JRE）|
| `smart-scenic/hadoop:custom`（本项目）| `/opt/jdk8`（JDK 1.8.0_202 预装）|
| `apache/hive:3.1.3` 官方 | `/usr/local/openjdk-8` |
| `smart-scenic/hive:custom`（本项目）| 同上 + MySQL JDBC 预装 |
| `apache/spark:3.4.1` | `/opt/java/openjdk` |
| `harisekhon/hbase:latest` | `/usr/lib/jvm/java-1.8-openjdk` |
| `zookeeper:3.9` | 内置 |
| `apache/kafka:latest` | `/opt/java/openjdk` |

### 2.4 YARN 必须手动启

`apache/hadoop` 镜像的 `start-dfs.sh` 只启 HDFS 不启 YARN。Sqoop import 需要 YARN 跑 MapReduce job。
所以 `docker/hadoop/starter.sh` 手动启：
```
yarn --daemon start resourcemanager
yarn --daemon start nodemanager
```

---

## 3. 目录结构（必读）

```
smart-scenic-bigdata/
├── docker-compose.yml          # 16 容器编排（核心文件）
├── .env                        # 端口/密码/镜像版本
├── README.md                   # 主说明（中文）
├── AGENTS.md                   # 本文件（给后续 AI 看）
│
├── docker/                     # 自定义镜像的 Dockerfile + 文件
│   ├── hive/
│   │   ├── Dockerfile          # FROM apache/hive + COPY mysql-connector-java jar
│   │   └── mysql-connector-java-8.0.33.jar
│   └── hadoop/
│       ├── Dockerfile          # FROM apache/hadoop + 装 JDK + 装 Sqoop + 拷脚本
│       ├── mysql-connector-java-8.0.33.jar
│       ├── commons-lang-2.6.jar
│       ├── install-sqoop.sh    # 备用：单独重装 Sqoop 用
│       ├── starter.sh          # 容器 PID 1：启 NN + YARN
│       └── sqoop-import-mysql.sh  # 5 张表 ETL 导入
│
├── config/                     # 各组件运行时配置（bind mount，需重启生效）
│   ├── zoo.cfg                 # ZK ensemble（clientPort=2181, standaloneEnabled=false）
│   ├── hadoop/                 # core/hdfs/yarn/mapred-site.xml + workers
│   ├── hbase/                  # hbase-site.xml + regionservers
│   ├── hive/                   # hive-site.xml + hive-env.sh
│   ├── spark/                  # spark-defaults.conf + spark-env.sh + workers
│   └── kafka/                  # KRaft 说明 README
│
├── mysql-init/
│   └── 01-init-business.sql    # 5 张业务表 + 测试数据（UTF-8 编码）
│                               # bind mount 到 /docker-entrypoint-initdb.d
│                               # MySQL 容器首次启动自动跑
│
├── app/                        # Demo 应用（给后端开发者演示用）
│   ├── README.md               # Demo 总说明
│   ├── 组件说明.md              # 8 组件对照后端经验
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
├── data/                       # 持久化数据卷（自动生成，**不要提交**）
├── logs/                       # 日志（自动生成，**不要提交**）
│
└── docs/                       # 详细文档
    ├── 部署文档.md
    ├── 架构说明.md
    └── 常见问题.md
```

---

## 4. 数据流向

```
[MySQL scenic]
   │  Sqoop 1.4.7 import（docker/hadoop/sqoop-import-mysql.sh）
   ▼
[HDFS /scenic/sqoop/]
   │
   ├──→ [Hive 数仓]    ← 后端 SQL 查询
   ├──→ [Spark MLlib]   ← 后端统计/预测
   └──→ [Kafka 实时]    ← 后端异步消息
            │
            └──→ [HBase]   ← 实时 NoSQL 读写
```

---

## 5. 常见坑（必读！避免重复踩）

### 5.1 PowerShell 的 `>nul` 会创建文件！

```bat
REM ❌ 错误：PowerShell 把 2 当文件描述符，写入根目录的 "2" 文件
docker exec hadoop-namenode hdfs dfs -ls /scenic 2>nul

REM ✅ 正确：加空格
docker exec hadoop-namenode hdfs dfs -ls /scenic 2> nul
```

如果发现根目录有 `20`、`30`、`n1` 这种诡异文件，**一定是这个 bug**。直接删除即可。

### 5.2 CMD 脚本 `(...)` 嵌套解析

CMD 把 `(...)` 当成 `if () else ()` 解析。**不要在 `if () else ()` 块里的 echo 加括号**：

```bat
REM ❌ 错误：CMD 解析 `(` 失败
if "%X%"=="1" (
    echo Triggering Sqoop (4 tables)...
) else (
    ...
)

REM ✅ 正确：用 - 或 . 代替括号
if "%X%"=="1" (
    echo Triggering Sqoop - 4 tables...
) else (
    ...
)
```

### 5.3 HBase 客户端选型（已用 docker exec）

**问题**：HBase 2.x Thrift server 默认 `TBinaryProtocol`，但 Python `happybase 1.2.0` + `thriftpy2` 
协议协商失败，报 `Bad version in readMessageBegin`。

**尝试过的方案**：
1. ❌ **happybase Python**：Thrift 协议不兼容（见上）
2. ❌ **HBase REST server**：`harisekhon/hbase:latest` 镜像的 `web.xml` 缺少 `RESTServletContainer` 映射，
   即使手动改了 web.xml 也找不到正确的 `javax.ws.rs.Application` 类（Jersey init-param 缺）。
   详细见 commit history。
3. ❌ **Phoenix 4.x**：跟 HBase 2.1.3 兼容性测试麻烦，时间成本高
4. ✅ **`docker exec hbase shell`**：当前方案

**本项目方案**：Demo backend 用 `docker exec hbase shell` 代替 happybase。
- 影响：每次写/读 1-2 秒延迟（手动点击无感）
- 生产环境（高频写入）：用 Java `hbase-client` 或 Phoenix

**生产替代**（如果真要高性能 HBase Python 客户端）：
- Java 端用 `hbase-client`（无协议问题）
- Phoenix 4.x + `phoenixdb`（绕开 Thrift，走 SQL 层）
- 自定义 webapp（修复 web.xml + 写 Application class，工程量大）

### 5.4 HBase Thrift 必须手动启

`harisekhon/hbase:latest` 的 entrypoint 只启 HBase master，**Thrift server 没启**。
`scripts/start.bat` 加了：
```bat
docker exec --user root hbase-master hbase-daemon.sh start thrift
```

### 5.5 Kafka advertised listeners

容器内 `KAFKA_ADVERTISED_LISTENERS` 必须告诉客户端用什么地址连回 broker。
容器内默认 `kafka-1:9092`（Windows 解析不到）。改成 `EXTERNAL://localhost:19095` 后，
**只有用 EXTERNAL listener 的客户端才能连**（Python 客户端用 `localhost:19095`）。

### 5.6 docker-compose depends_on condition

```yaml
depends_on:
  hadoop-namenode:
    condition: service_healthy  # ← 强依赖 healthy，namenode healthcheck 慢
```

会导致 datanode 永远进 Created 状态（因为 namenode healthcheck curl 失败）。
→ 改成 `condition: service_started` 即可。

### 5.7 docker-compose command 多行

YAML `command: ["/bin/bash", "-c", "..."]` 里的脚本里**不要用 `if () else ()` 多行块**，
CMD 解析会有问题。用单条命令 `&&` 或简单 shell。

### 5.8 修改自定义镜像后必须 rebuild

改 `docker/hadoop/Dockerfile` 或 `docker/hadoop/sqoop-import-mysql.sh` 后，
只重启容器不生效，必须重新 build：

```bash
docker compose build hadoop-namenode     # 重新 build 镜像
docker compose up -d --force-recreate hadoop-namenode   # 用新镜像重建容器
```

第一次 build 慢（要下载 JDK + Sqoop ~800MB），后续 build 走 Docker 缓存只几秒。

---

## 6. 开发流程

### 6.1 添加新业务表

**不需要改 docker**。改 2 个文件：

1. `mysql-init/01-init-business.sql` 加 DDL + 数据
2. `docker/hadoop/sqoop-import-mysql.sh` 加表名到 `for t in ...` 循环

然后：
```bash
docker compose down mysql
docker volume rm smart-scenic-bigdata_mysql-data
docker compose up -d mysql   # 首次启动会自动跑 mount 进去的 SQL

# 重建 hadoop 镜像（因为 sqoop-import-mysql.sh 在镜像里）
docker compose build hadoop-namenode
docker compose up -d --force-recreate hadoop-namenode
docker exec hadoop-namenode bash /opt/jobs/sqoop-import-mysql.sh
```

### 6.2 修改组件运行时配置

直接改 `config/<component>/*.xml` 等。容器用的是 bind mount，**重启容器即生效**：
```bash
docker compose restart <service-name>
```

不需要 rebuild 镜像。

### 6.3 修改 docker-compose.yml（端口、内存、依赖等）

```bash
docker compose up -d --force-recreate <service-name>
```

### 6.4 修改 Dockerfile 或其中的脚本

```bash
docker compose build <service-name>
docker compose up -d --force-recreate <service-name>
```

### 6.5 添加新 demo 后端接口

直接改 `app/backend/main.py`，FastAPI 会自动 reload（如果用 `--reload` 模式启动）。

---

## 7. 验证命令（排查流程）

```bash
# 1. 看所有容器状态
docker ps --format "table {{.Names}}\t{{.Status}}"

# 2. 看具体容器日志
docker logs --tail 50 <container-name>

# 3. 看自定义镜像是否构建成功
docker images | grep smart-scenic

# 4. 跑连通性测试
scripts\verify.bat

# 5. 跑业务场景测试
scripts\test-e2e.bat

# 6. Demo 应用验证
scripts\start-app.bat
# 浏览器打开 http://localhost:8080
```

---

## 8. 给后续 AI 的建议

1. **先读 `README.md`** 了解项目总览
2. **再读本文件（AGENTS.md）** 了解踩过的坑
3. **改任何东西前先看 `git diff`**，确认改的范围
4. **不要改 `docker-compose.yml` 的 `image:` 版本号**（除了自定义的 `smart-scenic/*:custom`），
   除非你清楚新版本兼容性
5. **新加表不要改 `docker-compose.yml`**，只改 SQL 和 sqoop 脚本
6. **跑测试用 `scripts\verify.bat` 和 `scripts\test-e2e.bat`**，别手动测
7. **遇到 HBase/Kafka/Spark 问题先看 `docs/常见问题.md`**
8. **容器是临时的，重启即丢所有改动**，持久化用：
   - MySQL/HDFS 数据 → docker volumes
   - 配置文件 → bind mount 到 `config/`
   - JAR 包 + 启动脚本 + Sqoop → 烤进 `docker/<component>/` 镜像
   - MySQL init SQL → bind mount 到 `mysql-init/`

---

## 9. 关键命令速查

```bash
# 平台
docker compose up -d                    # 启所有
docker compose down                    # 停所有（保留数据）
docker compose down -v                 # 停 + 删 volumes（清空数据）
docker compose restart <svc>           # 重启某服务（应用新 bind mount 配置）
docker compose up -d --force-recreate <svc>  # 应用新 command/volumes/build

# 自定义镜像
docker compose build <svc>             # rebuild 某服务的镜像
docker images | grep smart-scenic      # 看自定义镜像

# 容器内操作
docker exec -it <svc> bash             # 进 shell
docker exec <svc> <cmd>                # 单条命令
docker exec --user root <svc> <cmd>    # root 权限执行

# HDFS
docker exec hadoop-namenode hdfs dfs -ls /
docker exec hadoop-namenode hdfs dfs -cat /scenic/sqoop/t_scenic/part-m-00000
docker exec hadoop-namenode hdfs dfs -rm -r -f /path

# Sqoop（重跑）
docker exec hadoop-namenode bash /opt/jobs/sqoop-import-mysql.sh

# HBase
docker exec -it hbase-master hbase shell
docker exec --user root hbase-master hbase-daemon.sh status thrift

# Kafka
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic test-topic --from-beginning --max-messages 5

# MySQL
docker exec mysql mysql -uroot -proot123 --default-character-set=utf8mb4 -e "USE scenic; SHOW TABLES;"
```

---

## 10. 已知遗留问题

- **happybase Python 协议不兼容** HBase 2.x Thrift（已用 `hbase shell` 绕过）
- **`pyhive` 装不上**（sasl C 编译失败），已用 `hdfs dfs -cat` 读 HDFS CSV
- **Kafka 4.x 默认协议与 kafka-python 2.0.2 不完全兼容**，需用 EXTERNAL listener
- **`harisekhon/hbase:latest` Master RPC 端口随机**，必须用 ZK 自动发现

---

**最后更新**：2026-06-28  
**版本**：v1.4  
**配套作业**：选题 18 — 智能景区管理系统