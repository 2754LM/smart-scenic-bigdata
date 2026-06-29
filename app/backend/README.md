# P2 后端 - 模块化 FastAPI

P2 把原来的单文件 demo (`main.py`) 重构成 **config / schemas / services / routers** 四层结构，
覆盖 7 类业务接口和 1 个 `/api/health` 健康检查端点。

## 目录结构

```
app/backend/
├── main.py              # FastAPI 入口 + CORS + 启动钩子
├── config.py            # 所有连接配置（MySQL/Kafka/HBase/Hive/HDFS）
├── schemas.py           # 共享 Pydantic 模型
├── requirements.txt     # 依赖
├── utils.py             # docker exec / CSV cache / 工具函数
├── services/            # 数据访问与业务逻辑层
│   ├── __init__.py
│   ├── mysql_service.py    # MySQL CRUD + 聚合
│   ├── hive_service.py     # Hive/HDFS 分析（日/时/地区/年龄/类型/FPGrowth）
│   ├── hbase_service.py    # HBase 实时画像
│   └── model_service.py    # sklearn 模型训练与预测
└── routers/             # API 路由层
    ├── __init__.py
    ├── overview.py        # /api/overview/*
    ├── attractions.py     # /api/attractions/*
    ├── visitors.py        # /api/visitors/*
    ├── consumption.py     # /api/consumption/*
    ├── analysis.py        # /api/analysis/*
    ├── predict.py         # /api/predict/*
    └── realtime.py        # /api/realtime/*
```

## 安装

```bash
cd app/backend
pip install -r requirements.txt
```

> 与 P1 demo 相比新增 `pandas / numpy / scikit-learn` 三个 ML 依赖。

## 运行

```bash
python main.py
# 或
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

启动后：

- Swagger UI: <http://localhost:8000/docs>
- ReDoc:      <http://localhost:8000/redoc>

## 数据源

| 路径 | 真实数据源 | 后端读法 |
|------|------------|----------|
| 业务表 5 张 | MySQL `scenic` 库 | pymysql |
| 全量游客/消费/游玩记录 | `data/raw_data/*.csv` | pandas (本地缓存) |
| 景点 | MySQL `t_scenic` 或本地 CSV | 先 MySQL，没数据回退到 CSV |
| 实时画像 | HBase `scenic_realtime` | docker exec hbase shell |
| 关联规则 | 合成（来自 P1 训练产物） | 服务端模拟 |
| ML 模型 | sklearn 训练后驻留内存 | 懒加载到 `services/model_service.py` |

> HDFS CSV 与 MySQL 业务表内容完全一致（Sqoop 导入）。
> 真正的 10K+ 行大表来自 P1 清洗后的 `data/raw_data/*.csv`。

## 接口清单

### Overview（总览大屏）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/overview/kpi` | 8 项 KPI（游客/景点/消费/游玩/平均/笔数/日均） |
| GET  | `/api/overview/timeseries` | 日游客/日消费时序，支持 metric+起止日期 |
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

### Predict（预测）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/predict` | 实时预测（消费/客流量/高价值） |
| GET  | `/api/predict/regression` | 回归模型对比报告 |
| GET  | `/api/predict/classification` | 分类模型对比报告 |
| GET  | `/api/predict/clustering` | 聚类分群解释 |
| GET  | `/api/predict/compare` | 全部模型对比 |

### Realtime（HBase）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/realtime/visit-recent` | 最近游玩记录 |
| GET  | `/api/realtime/visitor/{id}` | 游客画像（V+8位ID） |
| GET  | `/api/realtime/attraction/{id}` | 景点统计（A+4位ID） |

## 模型训练流程

`model_service` 启动后第一次调用会做以下事情：

1. 读 `data/raw_data/visitors.csv` / `consumption.csv` / `visit_records.csv`
2. 训练 3 个回归 + 4 个分类 + 2 个聚类模型
3. 把训练好的对象缓存在 `_MODELS` 字典里
4. 后续预测全部走内存，毫秒级响应

P1 在 HDFS 上训练的模型（`/scenic/models/*.pkl`）正式上线后可改成从 HDFS
load 替换 `model_service` 里的 sklearn 训练逻辑，routers 不需要改。

## 设计权衡

- **为什么读 CSV 而不是连 Hive？**
  - `pyhive` 装不上（sasl C 编译失败），CSV 跟 HDFS 是同一份数据（Sqoop 导的）
  - 在 HDFS → CSV 路径上可以无痛替换成 Hive JDBC
- **为什么用 docker exec 调 hbase shell？**
  - happybase 1.2 + thriftpy2 协议不兼容 HBase 2.x Thrift server
  - 详见根目录 `AGENTS.md` 5.3 节
- **为什么 P2 重构成 routers/services？**
  - 方便后续按业务模块分配给不同组员
  - 每个 router 都能独立测试
  - ML 模型层（services/model_service.py）可以独立替换
