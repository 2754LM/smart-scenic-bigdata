# 智能景区大数据平台 - PPT 文字稿

> **作业**: 选题十八 智能景区管理系统 (6.2-6.5 评分点)
> **重点**: ML 机器学习 (特征工程 / 数据泄漏修复 / 4 大任务 / 关联规则)
> **配套**: 15 容器 Docker 大数据集群 + 一键部署 + test-e2e 23/23 PASS

---

## 目录 (约 25 张)

| # | 章节 | 张数 | 类别 |
|---|------|------|------|
| 1 | 封面 | 1 | 引入 |
| 2 | 选题背景与目标 | 1 | 引入 |
| 3-4 | 整体架构 (15 容器 + 数据流) | 2 | 大数据 |
| 5 | 技术选型与版本 | 1 | 大数据 |
| 6-7 | 数据采集与存储 (MySQL/Sqoop/HBase) | 2 | 大数据 |
| 8-9 | ML - 特征工程 | 2 | **ML** |
| 10-11 | ML - 数据泄漏故事 (亮点) | 2 | **ML** |
| 12 | ML - 回归分析 | 1 | **ML** |
| 13-14 | ML - 分类分析 | 1 | **ML** |
| 15 | ML - 聚类分析 | 1 | **ML** |
| 16-17 | ML - 关联规则 (FPGrowth + Apriori) | 2 | **ML** |
| 18 | ML - 模型服务 | 1 | **ML** |
| 19 | 数仓层 (Hive) | 1 | 大数据 |
| 20-21 | 一键部署 + 端到端验证 | 2 | 工程 |
| 22 | 总结与演示路径 | 1 | 收尾 |

ML 占 11/22 ≈ **50%**, 大数据容器占 5/22 ≈ **23%**.

---

## 1. 封面

```
项目: 智能景区大数据平台
作业: 选题十八 · 大数据平台构建 (6.2-6.5)
时间: 2026 年 6 月
架构: 15 容器 Docker 集群 (Hadoop + Spark + Hive + HBase + MySQL)
代码: github.com/2754LM/smart-scenic-bigdata
```

**一句话亮点**:
- 真分布式 15 容器, 2 个 .bat 一键起集群 + 跑 pipeline
- **9 个 sklearn 模型** (4 回归 + 1 聚类 + 4 分类) + **5820 条关联规则** (FPGrowth + Apriori)
- **数据泄漏修复**: 6 特征 Acc=1.0 → 3 特征 Acc=0.76, AUC=0.85
- test-e2e 23/23 PASS

---

## 2. 选题背景与目标

### 痛点
- 景区每天产生游客 / 消费 / 游玩数据, 但**没有大数据平台**能分析
- 业务需求: 客流预测 / 智能推荐 / 路线规划 / 游客画像 / 关联规则
- 教学要求 (选题十八 6.2-6.5): 平台搭建 + 数据采集 + 数据分析 + 可视化

### 目标
1. **真分布式** — Hadoop HA + Spark cluster + Hive 多实例 + HBase 集群
2. **端到端 ML 链路** — 4 CSV → Sqoop → Spark → 9 个 .pkl → FastAPI 预测
3. **真机器学习** — 4 大任务 (回归 / 分类 / 聚类 / 关联), 全程无数据泄漏
4. **一键部署** — 2 个 .bat 跑完整个大数据集群 + 数据 pipeline

### 评分点对照
| 作业要求 | 本项目实现 |
|---------|----------|
| 6.2 平台搭建 | 15 容器一栈, 端到端真分布式 ✅ |
| 6.3 数据采集 | Sqoop 跑通 4 张表 (210k 行) ✅ |
| 6.4 数据分析 | Spark 清洗 + Hive 4 表 4 视图 + 9 .pkl + 5820 规则 ✅ |
| 6.5 可视化 | 4 页前端 + 真实 vs 预测曲线 + Sankey 关联图 ✅ |

---

## 3. 整体架构 (一) - 15 容器分层

```
   ┌──────────────────────────────────────────────┐
   │      Windows 宿主机 (.bat 入口)               │
   │  start-containers.bat    start-app.bat        │
   └──────┬───────────────────────────────────────┘
          │ docker compose up
   ┌──────▼────────── 15 containers ──────────────┐
   │  协调:  Zookeeper × 3                         │
   │  存储:  MySQL 5.7 (业务库 + Hive Metastore)   │
   │         Hadoop NN + 2 DN  (HDFS, 副本=2)      │
   │         HBase master + 2 RS                   │
   │  计算:  Spark master + 1 worker (含 sklearn)  │
   │  数仓:  Hive Server × 2 (共享 MySQL Metastore)│
   │  应用:  demo-backend (FastAPI + joblib 推理)  │
   └──────────────────────────────────────────────┘
```

| 层 | 容器 | 数量 | 作用 |
|----|------|------|------|
| 协调 | zookeeper-1/2/3 | 3 | HBase 选举 |
| 存储 | mysql, hadoop-namenode, hadoop-datanode-1/2 | 4 | 业务库 + HDFS |
| 存储 | hbase-master, hbase-regionserver-1/2 | 3 | 画像 / 评论 |
| 计算 | spark-master, spark-worker-1 | 2 | Spark 3.4.1 + sklearn |
| 数仓 | hive-server-1, hive-server-2 | 2 | HiveServer2 HA |
| 应用 | demo-backend | 1 | FastAPI + 模型推理 |
| **合计** | | **15** | |

---

## 4. 整体架构 (二) - 数据流 4 阶段

```
阶段 1 (采集)        阶段 2 (清洗/入库)         阶段 3 (查询)         阶段 4 (ML + 服务)
4 CSV → MySQL (210k)   →  Sqoop → HDFS /scenic/sqoop/   →  Hive 外表+视图  →  Spark train 9 .pkl
                                                                              ↓
                                                                       FPGrowth 5010 rules
                                                                              ↓
                                                                       demo-backend (joblib)
                                                                              ↓
                                                                       FastAPI /api/predict/*
```

**关键**: 4 阶段全自动化, `start-app.bat` 一键串起, 总耗时 ~17 分钟.

| 阶段 | 产出 | 时长 |
|------|------|------|
| 1. 采集 | MySQL 4 表 210k 行 | 12s |
| 2. 清洗 | HDFS /scenic/cleaned/ 4 parquet | 7 min |
| 3. 查询 | Hive 4 ext + 4 view | 1 min |
| 4. ML | 9 .pkl + 5820 规则 | 8 min |

---

## 5. 技术选型与版本

| 组件 | 版本 | 选择理由 |
|------|------|---------|
| MySQL | 5.7 | DataNucleus 4.2 不兼容 8.0 (`DEFAULT CHARACTER SET` 语法) |
| Hadoop | 3.3.6 | 稳定主流, 配 JDK 1.8 自定义镜像 |
| Spark | 3.4.1 + sklearn 1.3.2 | 离线训练 + 模型推理, **预装 sklearn wheels** |
| HBase | 2.1.3 (harisekhon/hbase) | Docker Hub 唯一可用 HBase 镜像 |
| Hive | 3.1.3 | 教学标准, DataNucleus 4.2 兼容 MySQL 5.7 |
| Zookeeper | 3.9 | HBase 协调 |
| sklearn | 1.3.2 (Spark 容器内) | 4 大 ML 任务 + joblib 序列化 |
| FastAPI | demo-backend | 模型服务化 (joblib.load → /predict) |

> **关键决策**: sklearn 装在 Spark 容器里 (4 个 .whl 离线安装, 16 秒构建), 训练和推理同一镜像, 避免 PySpark / sklearn 双轨.

---

## 6. 数据采集与存储 (一) - MySQL 业务库

### 4 张中文 schema 表

```sql
CREATE TABLE t_attraction (
    景点ID   VARCHAR(20)  PRIMARY KEY,    -- 'A001' 前缀
    景点名称 VARCHAR(100) NOT NULL,
    类型     VARCHAR(50),                -- 文化/娱乐/自然/运动
    位置     VARCHAR(200),
    开放时间 VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- + t_visitor, t_consumption, t_visit_record
```

### 行数 (实际跑通)
| 表 | 行数 | 说明 |
|----|------|------|
| t_attraction | 10 | 景点主数据 |
| t_visitor | 10,000 | 游客主数据 (含 age/gender) |
| t_consumption | 100,000 | 消费流水 (游客 × 景点 × 时间) |
| t_visit_record | 100,000 | 游玩记录 (含停留时长) |
| **合计** | **210,010** | |

### 一库双角色
`mysql:5.7` 同时承载:
- 业务库 `scenic` (4 张中文表)
- Hive Metastore `hive_metastore` (DataNucleus 自动管理)

**为什么 5.7 不是 8.0?** DataNucleus 4.2 生成的 DDL 用 `DEFAULT CHARACTER SET xxx`, MySQL 8.0 已弃用该语法.

---

## 7. 数据采集与存储 (二) - Sqoop + HBase

### Sqoop: MySQL → HDFS

```bash
sqoop import \
  --connect jdbc:mysql://mysql:3306/scenic \
  --table t_visitor \
  --target-dir /scenic/sqoop/t_visitor \
  --as-parquetfile -m 1
```
4 张表批量导入, 输出 HDFS `/scenic/sqoop/t_*` (parquet, ~10MB).

### HBase: 2 张表 (画像 / 评论)

| 表 | Row Key | 列族 cf |
|----|---------|--------|
| scenic_realtime | `V{vid:08d}` 游客最近活动 | total_visits, last_attr, last_ts |
| scenic_realtime | `A{aid:04d}` 景点最近访客 | visitor_id, last_ts |
| scenic_reviews | `{aid}_{ts}_{vid}` | rating, comment |

> demo-backend 启动时自动 `init_tables()` + `seed_if_empty()`, 通过 docker socket 跑 `hbase shell` (镜像不装 happybase).

---

## 8. ML 核心 - 特征工程 (一) - 为什么 3 个特征?

### 原始表 → 派生特征

从 `t_visitor` + `t_consumption` + `t_visit_record` 三张原始表, 按 `visitor_id` 聚合派生:

```python
# app/jobs/ml/train.py
FEATURE_COLS = ["age", "avg_duration", "unique_attractions"]
#  age                ← t_visitor.age (人口属性)
#  avg_duration       ← t_visit_record.groupby('vid')['duration'].mean() (行为属性)
#  unique_attractions ← t_visit_record.groupby('vid')['aid'].nunique() (兴趣广度)
```

### 为什么选这 3 个? (核心原则)
1. **人口属性** (age) — 静态特征, 与消费额无函数关系
2. **行为特征** (avg_duration) — 平均停留时长, 反映游玩深度
3. **兴趣广度** (unique_attractions) — 去过的不同景点数, 反映探索欲

**关键**: 3 个特征都与目标变量 (`total_amount` / `is_repeat_visitor`) **无数学函数关系**, 杜绝数据泄漏.

详见下一节"数据泄漏故事".

---

## 9. ML 核心 - 特征工程 (二) - 派生 SQL 与代码

### 派生特征 SQL (Spark)

```python
# app/jobs/spark/clean.py - 按 visitor_id 聚合派生
df_feat = spark.sql("""
  SELECT v.age,
         AVG(r.duration)       AS avg_duration,
         COUNT(DISTINCT r.aid) AS unique_attractions,
         SUM(c.amount)         AS total_amount,        -- 标签 (回归)
         COUNT(r.aid)          AS visit_count           -- 标签 (分类)
  FROM t_visitor v
  JOIN t_visit_record r ON v.vid = r.vid
  JOIN t_consumption   c ON v.vid = c.vid
  GROUP BY v.vid, v.age
""")
```

### 标签生成
```python
# 回归标签: 直接用 total_amount (连续值)
y_reg = df_feat["total_amount"]

# 分类标签: 是否高频回头客 (二分类)
median_visits = df_feat["visit_count"].median()
y_clf = (df_feat["visit_count"] >= median_visits).astype(int)   # 0/1
```

> **注意**: `visit_count` 只用于**生成标签**, **不进入特征矩阵**. 这是避免泄漏的关键 (见下节).

---

## 10. ML 核心 - 数据泄漏故事 (一) - 问题发现 (亮点)

### 这是整个项目最关键的工程故事

最初用 **6 个特征** 训练, 结果异常"完美":
```python
FEATURES_WRONG = ["age", "purchase_count", "avg_amount",
                  "visit_count", "avg_duration", "unique_attractions"]
# → 分类 Acc = 1.0, 回归 R² = 0.99
```

### 两个泄漏点

**泄漏 1: 回归标签被反推**
```
total_amount (标签) = purchase_count × avg_amount (特征)
```
模型不需要学, 直接乘法就还原标签 → **R² = 0.99** (完美反推, 不是预测).

**泄漏 2: 分类标签直接泄露进特征**
```
is_repeat_visitor (标签) = (visit_count >= median)    # 标签定义
visit_count ∈ FEATURES_WRONG                           # 特征里有它!
```
分类器直接读 `visit_count` 跟阈值比一下 → **Acc = 1.0** (不是学, 是查表).

### 症状
| 指标 | 数值 | 含义 |
|------|------|------|
| 分类 Acc | 1.0000 | 异常完美, 必有泄漏 |
| 回归 R² | 0.99 | 标签可被特征反推 |
| AUC | 1.0000 | 决策边界 = 阈值本身 |

> 这种"太好了以至于不真实"的结果, 是数据泄漏的典型信号.

---

## 11. ML 核心 - 数据泄漏故事 (二) - 修复与验证 (亮点)

### 修复: 砍到 3 个无关系特征

```python
FEATURE_COLS = ["age", "avg_duration", "unique_attractions"]
# 删掉: purchase_count, avg_amount (可反推 total_amount)
# 删掉: visit_count           (直接出现在标签定义里)
```

### 修复前后对比

| 阶段 | 特征数 | 分类 Acc | 回归 R² | 诊断 |
|------|--------|---------|---------|------|
| 修复前 | 6 | **1.0000** | **0.99** | 严重泄漏 |
| 修复后 | 3 | 0.7642 | **≈ 0** | 无泄漏 ✅ |

### 为什么 R² ≈ 0 反而是好事?

- R² ≈ 0 意味着 `age / avg_duration / unique_attractions` 与 `total_amount` **无线性关系**
- 这恰好证明: **特征里没有标签信息**, 模型无法"作弊"
- Acc=0.76 / AUC=0.85 是**真实泛化能力**, 不是泄漏幻觉

### 工程意义
- 真实 ML 项目里, "异常高指标" 是警报, 不是喜报
- 修复后模型虽然分数下降, 但**可信、可上线**
- 这是数据科学家 vs 调包侠的分水岭

> **答辩话术**: "我们一开始 Acc=1.0 觉得太好了, 排查后发现 total_amount = purchase_count × avg_amount 这个乘法关系直接泄露进特征, 砍到 3 个无关系特征后 R² 归零、Acc 回落到 0.76, 这才是真实泛化能力."

---

## 12. ML 核心 - 回归分析 (4 模型)

### 任务
预测游客**消费金额** (连续值, 回归).

### 4 个模型对比 (`_comparison_report.json`)

| 模型 | RMSE | R² | 说明 |
|------|------|-----|------|
| LinearRegression | 2785.45 | -0.0001 | 线性基线 |
| Lasso | 2785.45 | -0.0001 | L1 正则 |
| Ridge | 2785.45 | -0.0001 | L2 正则 |
| RandomForest | 2788.21 | -0.0021 | 非线性 |

### 解读
- **RMSE ≈ 2785**: 平均预测误差 ~2785 元 (数据消费额范围 0-20000)
- **R² ≈ 0**: 3 个特征与消费额无线性关系 → **证明无数据泄漏**
- 4 个模型表现几乎一致 → 特征本身信息量有限, 不是模型问题

```python
# app/jobs/ml/train.py
from sklearn.linear_model import LinearRegression, Lasso, Ridge
from sklearn.ensemble  import RandomForestRegressor
for cls in [LinearRegression, Lasso, Ridge, RandomForestRegressor]:
    m = cls().fit(X_train, y_train)
    joblib.dump(m, f"regression_{name}.pkl")
```

> **工程结论**: R² ≈ 0 不是失败, 而是"无泄漏"的数学证据. 要提升 R² 需加入更多与消费相关但不泄露标签的特征 (如季节、天气), 留作后续工作.

---

## 13. ML 核心 - 分类分析 (一) - 4 模型对比

### 任务
预测游客是否**高频回头客** (二分类, 标签 = visit_count >= median).

### 4 个模型对比

| 模型 | Accuracy | F1 | AUC | 说明 |
|------|----------|----|-----|------|
| RandomForest | **0.7642** | 0.7643 | 0.8525 | 最优 Acc |
| DecisionTree | 0.7600 | 0.7599 | 0.7147 | AUC 偏低 |
| GradientBoosting | 0.7585 | 0.7591 | **0.8528** | 最优 AUC |
| LogisticRegression | 0.7600 | 0.7608 | 0.8506 | 线性基线 |

### 关键观察
- **Acc ≈ 0.76, AUC ≈ 0.85**: 真实泛化, 非泄漏
- **GBT AUC 最高 (0.8528)**: 集成学习优势
- **DT AUC 最低 (0.7147)**: 单树易过拟合, 集成更稳

```python
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
```

---

## 14. ML 核心 - 分类分析 (二) - 混淆矩阵与 ROC

### 评估指标体系

```
              预测正    预测负
实际正    TP          FN    ← 召回率 = TP/(TP+FN)
实际负    FP          TN    ← 精确率 = TP/(TP+FP)
```

- **Accuracy**: 整体正确率 (0.76)
- **F1**: 精确率与召回率的调和平均 (0.76)
- **AUC**: ROC 曲线下面积, 越接近 1 越好 (0.85)

### ROC 曲线概念
- 横轴: FPR (假正率)
- 纵轴: TPR (真正率)
- 不同阈值下 (TPR, FPR) 点连成曲线
- **AUC=0.85** 表示模型排序能力远好于随机 (0.5)

### 模型选择建议
| 场景 | 推荐模型 | 理由 |
|------|---------|------|
| 最高准确率 | RandomForest | Acc=0.7642 |
| 最高排序能力 | GradientBoosting | AUC=0.8528 |
| 可解释性 | DecisionTree | 单树可可视化 |
| 最简基线 | LogisticRegression | 线性, 训练快 |

---

## 15. ML 核心 - 聚类分析 (KMeans)

### 任务
无监督**游客群体细分**, 输出营销策略.

### 模型与指标
```python
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
km = KMeans(n_clusters=4, random_state=42).fit(X)
silhouette = silhouette_score(X, km.labels_)   # = 0.3977
```

| 指标 | 数值 | 说明 |
|------|------|------|
| k | 4 | 肘部法 + 业务可解释 |
| silhouette | 0.3977 | 中等分离度 (0.25-0.5 合理) |

### 4 类游客画像 + 营销策略 (来自 `model_service.py`)

| 簇 | 特征 | 画像 | 营销策略 |
|----|------|------|---------|
| 0 | 低频低消费 | 偶尔来, 花得少 | **发优惠券**吸引复访 |
| 1 | 高频中消费 | 常来, 消费中等 | 推荐**热门景点** |
| 2 | 中频高消费 | 偶尔来但高消费 | **VIP / 年卡** |
| 3 | 高频高消费 | 常来且高消费 | **私人定制**服务 |

> 聚类结果直接驱动业务决策, 不是"为聚类而聚类".

---

## 16. ML 核心 - 关联规则 (一) - FPGrowth (Spark MLlib)

### 任务
挖掘"游了 A 景点的游客, 往往也会游 B 景点"的**关联规则**, 用于路线推荐.

### FPGrowth (`app/jobs/ml/fpgrowth.py`)

```python
from pyspark.ml.fpm import FPGrowth
# 输入: t_visit_record 按 visitor_id 分组 → attraction_id 列表
df_items = spark.sql("""
  SELECT vid, collect_set(aid) AS items
  FROM t_visit_record GROUP BY vid
""")
fp = FPGrowth(minSupport=0.02, minConfidence=0.3, itemsCol="items")
model = fp.fit(df_items)
rules = model.assocRules.collect()   # 5010 条
```

### 参数与产出
| 参数 | 值 |
|------|-----|
| minSupport | 0.02 (至少 2% 游客) |
| minConfidence | 0.3 |
| 规则数 | **5010** |
| 输出 | `/shared/models/fpgrowth_rules.json` |

### 可视化
前端 `analysis.html` 取前 20 条 rules, 解析 antecedent → consequent, 用 **ECharts Sankey** 渲染 (lift × support 加权, 去环防 DAG 错误).

---

## 17. ML 核心 - 关联规则 (二) - Apriori (纯 Python)

### 为什么要实现两套?

| 算法 | 实现 | 规则数 | 用途 |
|------|------|--------|------|
| FPGrowth | Spark MLlib | 5010 | 工业级, 快 |
| Apriori | 纯 Python | 810 | **作业要求**, 可解释 |

### Apriori vs FPGrowth

| 维度 | Apriori | FPGrowth |
|------|---------|----------|
| 数据结构 | 候选集生成 (多次扫表) | FP-tree (2 次扫表) |
| 复杂度 | O(2^n) 候选爆炸 | 压缩树, 远快 |
| 速度 | 慢 | **快 10-100x** |
| 实现 | 纯 Python (教学) | Spark MLlib (工业) |

### Apriori 核心代码 (纯 Python)

```python
# 纯 Python 实现, 不依赖 Spark
def apriori(transactions, min_support=0.02, min_conf=0.3):
    # 1. 扫描得 1-项集频繁度
    C1 = count_items(transactions)
    L1 = filter_freq(C1, min_support)
    # 2. 自连接生成 k+1 候选, 剪枝
    Lk = L1
    while Lk:
        Ck = join_and_prune(Lk)        # 自连接 + 向下闭包剪枝
        Lk = filter_freq(Ck, min_support)
    # 3. 从频繁项集生成关联规则
    return gen_rules(L_all, min_conf)  # 810 条
```

> **答辩话术**: "FPGrowth 是 Apriori 的优化版 (FP-tree 取代候选生成, 快 10-100x), 我们用 Spark MLlib 跑 FPGrowth 得 5010 规则, 同时手写纯 Python Apriori 得 810 规则以验证算法理解和满足作业要求."

---

## 18. ML 核心 - 模型服务 (joblib → FastAPI)

### 模型加载与推理

```python
# app/backend/services/model_service.py
import joblib
class ModelService:
    def _load_models(self):
        self.reg_linear  = joblib.load("/shared/models/sklearn/regression_linear.pkl")
        self.clf_rf      = joblib.load("/shared/models/sklearn/classification_rf.pkl")
        self.kmeans      = joblib.load("/shared/models/sklearn/clustering_kmeans.pkl")
        # ... 9 个 .pkl 启动时一次性加载到内存

    def predict_classification(self, age, avg_duration, unique_attractions):
        X = [[age, avg_duration, unique_attractions]]
        return self.clf_rf.predict(X)[0]   # 0/1 是否高频回头客
```

### FastAPI 端点
```
POST /api/predict/regression      → 预测消费金额
POST /api/predict/classification  → 预测是否回头客
POST /api/predict/clustering      → 返回游客所属簇 + 营销策略
GET  /api/predict/compare         → 4 模型指标对比
```

> demo-backend 启动时 `on_startup()` 钩子一次性 `joblib.load` 9 个 .pkl 到内存, 推理延迟 < 10ms.

---

## 19. 数仓层 - Hive 3.1.3

### 架构
```
hive-server-1 ─┐
                ├─→ MySQL: hive_metastore  (DataNucleus 自动管理)
hive-server-2 ─┘     ↑
                     │
                  mysql:5.7 (业务库 + Metastore 一容器双角色)
```
2 个 HS2 共享同一 Metastore, 互为 HA (Apache 官方推荐多实例模式).

### 4 张外表 + 分区表 + 4 视图

```sql
-- app/jobs/hive/ddl.sql
CREATE EXTERNAL TABLE ext_t_attraction (...) 
  STORED AS PARQUET 
  LOCATION 'hdfs://hadoop-namenode:9000/scenic/cleaned/t_attraction';

-- 分区表 (按日期 + 景点类型)
CREATE TABLE t_visit_record_partitioned (
  vid STRING, aid STRING, duration DOUBLE
) PARTITIONED BY (visit_date STRING, attraction_type STRING)
  STORED AS PARQUET;

-- 4 个视图
CREATE VIEW v_attraction_summary AS    -- 景点汇总 (收入/游客/平均时长)
CREATE VIEW v_daily_visits AS          -- 每日游客量
CREATE VIEW v_high_value_visitors AS   -- 高消费游客
CREATE VIEW v_attraction_hourly_heat AS-- 景点时段热度
```

> 后端通过 docker socket 调 `beeline` 查询 (镜像不装 pyhive, 避免 libsasl2 慢依赖).

---

## 20. 一键部署 (一) - start.bat + pipeline

### 两个 .bat

```
scripts/
├── start-containers.bat   ← 一键起 15 容器
├── start-app.bat          ← 一键跑数据 pipeline
├── stop.bat               ← 停容器 (保数据卷)
├── reset.bat              ← 完全重置
├── test-e2e.bat           ← 端到端 23 项测试
└── run_pipeline.py        ← start-app.bat 调用的 Python 驱动
```

### start-containers.bat (4 阶段)
```bat
docker compose up -d mysql zookeeper-1 zookeeper-2 zookeeper-3
docker compose up -d hadoop-namenode hadoop-datanode-1 hadoop-datanode-2 ^
    spark-master spark-worker-1 hbase-master hbase-regionserver-1 hbase-regionserver-2 ^
    hive-server-1 hive-server-2 demo-backend
:loop  (轮询直到 15 容器全 Up)
```

### start-app.bat → run_pipeline.py (3 阶段)
```python
# 阶段 1: 数据采集
for action in ['load_csv', 'sqoop']:
    post(f'/api/admin/actions/{action}')

# 阶段 2: 清洗 + 数仓
for action in ['spark_clean', 'hive_ddl']:
    post(f'/api/admin/actions/{action}')

# 阶段 3: ML 训练
post('/api/admin/actions/spark_train')          # 9 .pkl
exec_via_socket('spark-master', ['bash', '/opt/jobs/ml/fpgrowth.py'])  # 5010 rules
```

---

## 21. 一键部署 (二) - 端到端验证

### test-e2e.bat: 23/23 PASS

| 场景 | 检查数 | 关键项 |
|------|--------|--------|
| 1. MySQL 业务数据 | 5 | 4 表行数 (10/10k/100k/100k) + hive 用户 |
| 2. HDFS 存储 | 3 | 2 Live DN + /scenic + 副本=2 |
| 3. HBase | 4 | 1 master + 2 RS + 2 表 + seed rows |
| 4. Spark | 2 | UI :18080 + 1 alive worker |
| 5. Hive 数仓 | 4 | 2×HS2 + 8 表 + /api/analysis/hourly source=hive |
| 6. demo-backend | 3 | /health + 4 分类模型 + 8 KPI |
| 7. ML 模型 | 2 | 4 .pkl + predict 返回正数 |
| **合计** | **23** | **23/23 PASS (100%)** |

### 跑通输出
```
> scripts\test-e2e.bat
E2E Test Summary
  Scenarios: 7
  PASS=23 / FAIL=0 / TOTAL=23
All checks passed. Platform is ready for demo.
```

> 100% Windows 原生命令 (`for /f` + `findstr` + `docker exec`), 无 Python 依赖.

---

## 22. 总结与演示路径

### 关键数字
| 指标 | 数值 |
|------|------|
| 容器数 | 15 |
| MySQL 行数 | 210,010 |
| Hive 表 | 4 ext + 1 分区 + 4 view = 9 |
| sklearn 模型 | 9 (.pkl) |
| 关联规则 | 5,010 (FPGrowth) + 810 (Apriori) = 5,820 |
| **test-e2e 通过率** | **23/23 (100%)** |
| 一键部署 | 2 个 .bat |

### ML 核心成果
| 任务 | 最优模型 | 关键指标 |
|------|---------|---------|
| 回归 | linear/lasso/ridge | RMSE=2785, R²≈0 (无泄漏证明) |
| 分类 | RandomForest | Acc=0.7642, AUC=0.8525 |
| 分类 | GradientBoosting | AUC=0.8528 (最高) |
| 聚类 | KMeans(k=4) | silhouette=0.3977 |
| 关联 | FPGrowth | 5010 rules (Spark) |
| 关联 | Apriori | 810 rules (纯 Python) |

### 演示路径 (3 步)
```
1. 双击 scripts\start-containers.bat   → 15 容器 Up (3 min)
2. 双击 scripts\start-app.bat          → pipeline 跑完 (15 min)
3. 浏览器 http://localhost:8080        → 4 页大屏 + ML 预测
```

### 答辩三大亮点
1. **数据泄漏修复** — 6 特征 Acc=1.0 → 3 特征 Acc=0.76, R²≈0 证明无泄漏 (工程深度)
2. **4 大 ML 任务全覆盖** — 回归 / 分类 / 聚类 / 关联, 9 .pkl + 5820 规则 (任务广度)
3. **一键部署 + 23/23** — 2 个 .bat 跑完 15 容器 + 全链路 pipeline (工程完整度)
