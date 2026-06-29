# CONTRIBUTING - 分支与交付约定

> 给团队整合负责人（manager）看的项目协作说明  
> 配套作业：选题 18 — 智能景区管理系统  
> GitHub: https://github.com/2754LM/smart-scenic-bigdata

---

## 一、分支总览

| 分支 | 负责人 | 当前状态 | 整合优先级 | 备注 |
|---|---|---|---|---|
| `master` | （平台搭建人） | ✅ v1.4 平台 16 容器 | 基线 | 不要直接 force push |
| `feature/data-pipeline` | 数据层开发 | ✅ P0 已交付 | **P0 优先合** | 改了 MySQL DDL、Sqoop、CSV 加载器，**与 master 冲突较大** |
| `feature/backend-api` | （待开发） | ⏳ 空分支 | P1 | 等数据层合到 master 后从 master 切 |
| `feature/frontend-viz` | （待开发） | ⏳ 空分支 | P2 | 等后端合到 master 后从 master 切 |

> 三个 feature/* 分支**都从 master 拉出**，互不依赖，按 **P0 → 后端 → 前端** 顺序合到 master。

---

## 二、feature/data-pipeline 详细说明

### 2.1 已交付（P0）

| Commit | 说明 |
|---|---|
| `7b91ceb` | MySQL DDL 重写（4 张表，中文字段）、CSV→MySQL 加载脚本、Sqoop 4 表脚本、.gitattributes 强制 LF |
| `2c44fe3` | P0 阶段交付文档（[docs/P0-数据层交付.md](P0-数据层交付.md)） |

### 2.2 关键变更点（影响整合）

⚠️ **MySQL DDL 完全重构**，与 master 上 [mysql-init/01-init-business.sql](../../mysql-init/01-init-business.sql) 完全不同：

| 项目 | master（v1.4） | feature/data-pipeline（P0） |
|---|---|---|
| 表数 | 5 张 | **4 张**（删 t_review） |
| 表名 | t_scenic / t_visitor / t_consume / t_visit / t_review | **t_attraction / t_visitor / t_consumption / t_visit_record** |
| 字段名 | 英文（scenic_id 等） | **中文（景点ID 等）** |
| Seed 数据 | 90 行内嵌 | **删**，由 CSV 加载器从 data/raw_data/*.csv 加载 220k 行 |
| 索引 | 少 | 大量二级索引（游客ID、景点ID、时间、景点ID+时间） |

⚠️ **docker/hadoop/sqoop-import-mysql.sh 完全重写**：
- 表名从 5 张变 4 张
- JDBC URL 加 `useUnicode=true&characterEncoding=utf8&serverTimezone=UTC`
- 加 `--null-string/--null-non-string/--as-textfile`

⚠️ **新增 scripts/load-csv-to-mysql.{py,sh}**：
- 新目录 `scripts/load-csv-to-mysql.py`（master 没有）
- 新目录 `scripts/load-csv-to-mysql.sh`（master 没有）

⚠️ **新增 .gitattributes**：
- 强制 `*.sh/*.py/*.sql/Dockerfile` 为 LF 行尾
- master 没有这个文件
- 整合时建议保留

### 2.3 整合步骤建议

```bash
# 1. 在 master 上拉取最新
git checkout master
git pull

# 2. 合入 feature/data-pipeline
git merge feature/data-pipeline --no-ff -m "Merge feature/data-pipeline (P0: data layer rewrite)"

# 3. 推送到 origin
git push origin master
```

### 2.4 VM 部署命令（参考 [docs/P0-数据层交付.md](P0-数据层交付.md) 第三节）

```bash
# A. fetch + 切分支
git fetch origin
git checkout feature/data-pipeline
chmod +x scripts/load-csv-to-mysql.sh docker/hadoop/sqoop-import-mysql.sh

# B. 重置 MySQL（新 DDL）
docker compose down mysql
docker volume rm smart-scenic-bigdata_mysql-data
docker compose up -d mysql
sleep 30

# C. 同步 data/raw_data/*.csv 到 VM（如果 data/ 没在 git 里）
#    方法: scp -r data/raw_data user@vm:/path/to/proj/data/

# D. 加载 CSV
pip install pymysql
./scripts/load-csv-to-mysql.sh

# E. 重建 hadoop 镜像（新 sqoop 脚本）
docker compose build hadoop-namenode
docker compose up -d --force-recreate hadoop-namenode

# F. 触发 Sqoop
docker exec hadoop-namenode bash /opt/jobs/sqoop-import-mysql.sh

# G. 验证
docker exec mysql mysql -uroot -proot123 -e "
  SELECT 't_attraction' t, COUNT(*) n FROM t_attraction UNION
  SELECT 't_visitor', COUNT(*) FROM t_visitor UNION
  SELECT 't_consumption', COUNT(*) FROM t_consumption UNION
  SELECT 't_visit_record', COUNT(*) FROM t_visit_record;
"
# 期望: 10 / 10000 / 100000 / 100000
```

---

## 三、整合顺序与冲突预案

### 3.1 推荐顺序

```
master (v1.4 平台)
   │
   ├─→ feature/data-pipeline (P0 ✅) ← 先合这个
   │       ↓ 合到 master
   │
   ├─→ feature/backend-api (P1，待开发) ← 后端从 master 切
   │       ↓ 合到 master
   │
   └─→ feature/frontend-viz (P2，待开发) ← 前端从 master 切
           ↓ 合到 master
```

### 3.2 已知冲突点

#### 冲突 A：mysql-init/01-init-business.sql
- 平台同学可能已经改了这份文件（添加 Hive Metastore 用户、添加测试数据等）
- 整合时以 **feature/data-pipeline 版本为准**（与新 Sqoop 脚本、新数据流程强绑定）
- 但需要保留平台同学对 MySQL 用户的设置（已在 P0 版本中保留 `'hive'@'%'` 用户）

#### 冲突 B：docker/hadoop/sqoop-import-mysql.sh
- 平台版本：5 张英文表
- P0 版本：4 张中文表
- **以 P0 版本为准**

#### 冲突 C：app/ 目录（前端 + 后端 demo）
- 平台版本：组件连通性 demo（9 个接口，单文件 HTML）
- 与本次 3 个 feature 分支**不冲突**（feature/data-pipeline 没动 app/）
- 但后端和前端分支会改这里，整合时**最后合后端+前端**

#### 冲突 D：docker-compose.yml / .env / 各种 config/
- 平台同学可能改了端口、内存
- **不冲突**（feature/data-pipeline 没动这些文件）

### 3.3 整合检查清单

整合完成后请确认：

- [ ] MySQL 有 4 张新表，名字匹配 `t_attraction / t_visitor / t_consumption / t_visit_record`
- [ ] MySQL 表里 4 张表数据量是 10 / 10000 / 100000 / 100000
- [ ] HDFS `/scenic/sqoop/` 下有 4 个目录
- [ ] `data/raw_data/` 下 4 个 CSV 存在
- [ ] `scripts/load-csv-to-mysql.sh` 可执行
- [ ] `docker/hadoop/sqoop-import-mysql.sh` 可执行
- [ ] .gitattributes 存在
- [ ] 老的 5 张英文表（t_scenic/t_consume/t_visit/t_review）已删除

---

## 四、后续阶段预览

### feature/data-pipeline 即将产出（未推送）

- **P1.1** Spark 清洗作业（脏数据注入演示 + 清理）
- **P1.2** Spark FPGrowth 关联规则
- **P1.3** 回归三模型：LinearRegression / Lasso / Ridge
- **P1.4** 聚类：KMeans / DBSCAN
- **P1.5** 分类：DecisionTree / RandomForest
- **P1.6** 模型对比报告
- **P1.7** Hive 分区表 + 视图
- **P1.8** HBase 业务表

### feature/backend-api 即将产出

- 后端按业务模块拆分（attraction / visitor / consumption / analysis / predict）
- 9 个 demo 接口 → 30+ 业务接口
- 对接 Spark 作业输出、ML 模型加载

### feature/frontend-viz 即将产出

- 多页面大屏（总览/分析/预测/管理）
- 按时间/景点动态过滤
- ECharts 可视化

---

## 五、联系方式

- 数据层：[XIAOhe211](https://github.com/XIAOhe211) / 2639519370@qq.com
- 整合问题：通过 GitHub Issue 沟通
