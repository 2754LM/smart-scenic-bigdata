# Spark / Hive / ML 作业脚本

> 选题十八 智能景区管理系统的所有大数据作业脚本（Spark 清洗 + Hive 仓库 + MLlib 训练）。
> 跟 `app/backend/` 的 FastAPI 后端配套使用。

---

## 目录结构

```
app/jobs/
├── README.md                  本文件
├── spark-submit.sh            统一运行脚本
├── spark/
│   └── clean.py               Spark 数据清洗（去重/类型转换/派生字段）
├── hive/
│   ├── ddl.sql                Hive 分区表 DDL
│   ├── views.sql              Hive 视图（4 个常用查询）
│   └── queries.sql            HiveQL 复杂查询（8 个分析场景）
└── ml/
    └── train.py               PySpark MLlib 训练（回归/聚类/分类）
```

## 数据流

```
[MySQL scenic 4 表]
     │  Sqoop 1.4.7
     ▼
[HDFS /scenic/sqoop/{t_attraction,t_visitor,t_consumption,t_visit_record}/]
     │
     │  spark-submit clean.py
     ▼
[HDFS /scenic/cleaned/{...}.parquet]  ← 清洗后（去重/派生字段）
     │
     │  spark-sql 跑 ddl.sql
     ▼
[Hive external tables: ext_t_attraction / ext_t_visitor / ext_t_consumption / ext_t_visit_record]
     │          (consumption 按 dt 分区, visit_record 按 dt+attraction_id 双分区)
     │
     │  spark-sql 跑 views.sql
     ▼
[Hive views: v_attraction_summary / v_daily_visits / v_high_value_visitors / v_attraction_hourly_heat]
     │
     │  spark-sql 跑 queries.sql
     ▼
[8 类分析结果: 景点日均游客/高消费游客排行/时段热度/年龄段偏好/地区客单价/月度趋势/周末对比/消费 Top 景点]
     │
     │  spark-submit ml/train.py
     ▼
[HDFS /scenic/models/{regression_*,clustering_*,classification_*}]
     │         (PySpark MLlib 模型)
     │
     │  FastAPI 后端 load 训练好的模型
     ▼
[后端 /api/predict 接口]  ← 实时预测（sklearn 加载 PySpark 模型）
```

## 执行顺序

### 1. 数据已 Sqoop 导入到 HDFS（自动）

```bash
docker exec hadoop-namenode bash /opt/jobs/sqoop-import-mysql.sh
```

### 2. Spark 清洗

```bash
docker exec spark-master bash /opt/jobs/spark-submit.sh clean
# 输出：HDFS /scenic/cleaned/
```

### 3. Hive 建表（外部表读 parquet）

```bash
docker exec hive-server-1 hive -f /opt/jobs/hive/ddl.sql
docker exec hive-server-1 hive -f /opt/jobs/hive/views.sql
```

### 4. 跑 Hive 复杂查询

```bash
docker exec hive-server-1 hive -f /opt/jobs/hive/queries.sql
```

### 5. PySpark MLlib 训练

```bash
docker exec spark-master bash /opt/jobs/spark-submit.sh ml-train
# 输出：HDFS /scenic/models/...
```

### 6. 后端加载模型做预测

`app/backend/services/model_service.py` 启动时会从 `/scenic/models/` 加载训练好的模型。

## 作业要求对照

| 作业要求 | 本项目实现 | 脚本 |
|---------|----------|------|
| Spark 数据清洗 | ✅ spark-submit clean | `spark/clean.py` |
| Spark 基础分析（游客数/时长/消费） | ✅ queries.sql 8 个查询 | `hive/queries.sql` |
| 高级分析（游客/景点/消费） | ✅ queries.sql 3-5 节 | `hive/queries.sql` |
| 关联规则（Apriori/FPGrowth）| ✅ FPGrowth in `app/backend/services/model_service.py` | sklearn + fallback |
| 回归（线性/Lasso/Ridge/RF） | ✅ `ml/train.py` | PySpark MLlib |
| 聚类（KMeans）| ✅ `ml/train.py` | PySpark MLlib |
| 分类（DT/RF）| ✅ `ml/train.py` | PySpark MLlib |
| Hive 分区表 | ✅ ddl.sql 按 dt + attraction_id 双分区 | `hive/ddl.sql` |
| Hive 视图 | ✅ 4 个常用查询视图 | `hive/views.sql` |
| HiveQL 复杂查询 | ✅ 8 个分析场景 | `hive/queries.sql` |
| HBase 实时查询 | ✅ `app/backend/services/hbase_service.py` | - |

## 一键运行所有作业

```bash
# 在 spark-master 容器内
docker exec spark-master bash -c "
  bash /opt/jobs/spark-submit.sh clean
  bash /opt/jobs/spark-submit.sh ml-train
"

# 在 hive-server-1 容器内
docker exec hive-server-1 bash -c "
  hive -f /opt/jobs/hive/ddl.sql &&
  hive -f /opt/jobs/hive/views.sql &&
  hive -f /opt/jobs/hive/queries.sql
"
```

## 部署到 Docker 容器

`scripts/start.bat` 阶段 5 之后会自动把 `app/jobs/` 目录挂载到 spark-master 容器 `/opt/jobs/`，这样脚本修改即时生效，不需要 rebuild 镜像。

具体配置见 `docker-compose.yml`：
```yaml
spark-master:
  volumes:
    - ./app/jobs:/opt/jobs:ro   # 挂载为只读
```

> 注意：当前 docker-compose.yml 的 spark-master 没有挂载 jobs/，需要手动添加（见下节）。

## 待办

- [ ] `docker-compose.yml` 给 spark-master 和 hive-server-1 加 `app/jobs` 挂载
- [ ] `app/backend/services/model_service.py` 加启动时从 HDFS 加载 PySpark 模型的逻辑
- [ ] 加 `scripts/train-all.bat` 一键训练脚本（容器内 spark-submit + hive -f）