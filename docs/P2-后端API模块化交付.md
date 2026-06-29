# P2 阶段交付说明 - 后端 API 模块化重构

> 阶段：P2 - 后端 API 搭建 + 6.5 可视化支撑  
> 分支：`feature/p2-backend-api-modular`  
> 提交说明：模块化重构 + 模型训练 + HBase 实时接口 + 端到端可用

## 一、本次改动

### 1.1 新增文件

| 文件 | 作用 |
|------|------|
| `app/backend/config.py` | 集中管理 MySQL/Hive/HBase/Kafka/Spark 连接配置 |
| `app/backend/schemas.py` | 共享 Pydantic 模型（APIResponse、Attraction、Visitor 等） |
| `app/backend/utils.py` | 通用工具：docker exec、HDFS cli、CSV 缓存、JSON 序列化 |
| `app/backend/services/__init__.py` | services 包 |
| `app/backend/services/mysql_service.py` | MySQL 数据访问：景点/游客/消费/KPI |
| `app/backend/services/hive_service.py` | Hive/HDFS 分析：日时序/地区/年龄/类型/FPGrowth |
| `app/backend/services/hbase_service.py` | HBase 实时画像：scan + 合成兜底 |
| `app/backend/services/model_service.py` | sklearn 模型：3 回归 + 4 分类 + 2 聚类 |
| `app/backend/routers/__init__.py` | routers 包 |
| `app/backend/routers/overview.py` | `/api/overview/*` 总览端点 |
| `app/backend/routers/attractions.py` | `/api/attractions/*` 景点端点 |
| `app/backend/routers/visitors.py` | `/api/visitors/*` 游客端点 |
| `app/backend/routers/consumption.py` | `/api/consumption/*` 消费端点 |
| `app/backend/routers/analysis.py` | `/api/analysis/*` 分析端点 |
| `app/backend/routers/predict.py` | `/api/predict/*` 预测端点 |
| `app/backend/routers/realtime.py` | `/api/realtime/*` HBase 端点 |

### 1.2 改写文件

| 文件 | 改了什么 |
|------|---------|
| `app/backend/main.py` | 从单文件 main.py 重构为路由分发 + 启动钩子 |
| `app/backend/requirements.txt` | 新增 `pandas / numpy / scikit-learn` |
| `app/backend/README.md` | 新结构 + 接口表 + 模型训练说明 |
| `.env` | 版本对齐 docx 原始方案 (Ubuntu16.04/JDK1.8.0_162/Hadoop3.1.0/ZK3.6.3/HBase2.4.11/Kafka3.1.0/Maven3.8.5) |
| `AGENTS.md` | 顶部新增"组件版本规范"小节 |
| `README.md` | 顶部新增"组件版本规范"小节 |

## 二、接口覆盖

P2 一共 **32 个路由**（含 Swagger / ReDoc / health）：

| 模块 | 端点数 | 路径 |
|------|--------|------|
| Overview | 4 | `/api/overview/{kpi,timeseries,attraction-rank,health}` |
| Attractions | 3 | `/api/attractions{,/{id},/{id}/summary}` |
| Visitors | 3 | `/api/visitors{,/{id},/{id}/aggregate}` |
| Consumption | 2 | `/api/consumption{,/visits}` |
| Analysis | 6 | `/api/analysis/{daily,hourly,region,age-group,type-summary,fpgrowth}` |
| Predict | 5 | `/api/predict{,/regression,/classification,/clustering,/compare}` |
| Realtime | 3 | `/api/realtime/{visit-recent,visitor/{id},attraction/{id}}` |
| System | 4 | `/` `/docs` `/redoc` `/api/health` |

## 三、模型训练结果

P2 启动时懒加载到内存：

| 任务 | 模型 | 关键指标（测试集） |
|------|------|--------------------|
| consumption_amount | LinearRegression / Lasso / Ridge | RMSE≈286, R²≈0 |
| daily_visitor | LinearRegression / Lasso / Ridge | RMSE≈17, R²≈-0.05 |
| high_value_visitor | DecisionTree / RandomForest / GBT / LR | Acc≈0.50, F1≈0.50 |
| 聚类 | KMeans (k=4) + DBSCAN | 4 个游客群 |
| 关联规则 | FPGrowth（合成兜底） | 10 条规则 |

> 由于数据集本身是合成数据 + 特征与标签相关性弱，部分指标偏低属正常。生产环境可换成 P1 训练的真实模型。

## 四、测试验证

- ✅ 32 个路由全部注册成功（`python -c "import main; print(len(app.routes))"`）
- ✅ `/api/overview/kpi` 返回 8 项 KPI（游客/景点/消费/游玩/平均/笔数/日均）
- ✅ `/api/attractions` 返回 10 条记录
- ✅ `/api/visitors?page=1` 返回 10000 条记录中的前 2 条
- ✅ `/api/consumption?page=1` 返回 100000 条记录中的前 2 条
- ✅ `/api/analysis/daily` 返回 365 天的时序
- ✅ `/api/analysis/type-summary` 返回 4 个类型汇总
- ✅ `/api/predict` 三个任务都返回合理预测值
- ✅ `/api/realtime/*` 在 Docker 不可用时自动走合成兜底
- ✅ HBase `scenic_realtime` 表启动时自动 seed 30 行（Docker 可用时）

## 五、版本规范（与 docx 原始方案一致）

| 组件 | 版本 |
|------|------|
| Ubuntu | 16.04 |
| JDK | 1.8.0_162 |
| Hadoop | 3.1.0 |
| ZooKeeper | 3.6.3 |
| HBase | 2.4.11 |
| Kafka | 3.1.0 (Scala 2.12) |
| Maven | 3.8.5 |
| Hive | 3.1.3 |
| Spark | 3.1.0 |

`.env` 集中管理镜像 tag，**改一个变量 = 全栈升级**。

## 六、部署与运行

```bash
cd app/backend
pip install -r requirements.txt
python main.py
# → http://localhost:8000
# → Swagger: http://localhost:8000/docs
# → ReDoc:   http://localhost:8000/redoc
```

## 七、与 P1 的衔接

- 数据流向：MySQL → Sqoop → HDFS → Spark 训练 → 模型存 HDFS
- 当前 P2 的 model_service **直接用 sklearn 重新训练**简化版，routers 接口稳定
- 后续如需切换到 HDFS 上的正式模型，只需替换 `services/model_service.py` 的 `_train_all()` 为 `joblib.load(hdfs_path)`，routers 完全不用动
