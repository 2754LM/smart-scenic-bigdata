# 后端 API（P2 阶段交付）

> 分支：`feature/backend-api`　|　日期：2026-06-29

---

## 一、模块概览

FastAPI 后端，**30+ 业务接口**分 7 个模块：

| 模块 | 路由前缀 | 接口数 | 数据源 |
|---|---|---|---|
| 总览 | `/api/overview` | 4 | MySQL |
| 景点 | `/api/attractions` | 3 | MySQL + HBase |
| 游客 | `/api/visitors` | 3 | MySQL + HBase |
| 消费与游玩 | `/api/consumption` | 2 | MySQL |
| 数据分析 | `/api/analysis` | 6 | Hive / MySQL fallback |
| 机器学习预测 | `/api/predict` | 5 | HDFS（模型+报告） |
| 实时数据 | `/api/realtime` | 3 | HBase |

**自动文档**：`http://localhost:8000/docs`

---

## 二、目录结构

```
app/backend/
├── main.py                  # FastAPI 入口
├── config.py                # pydantic-settings 配置
├── requirements.txt         # 依赖
├── README.md                # 本文件
├── api/                     # API 路由层
│   ├── overview.py
│   ├── attractions.py
│   ├── visitors.py
│   ├── consumption.py
│   ├── analysis.py
│   ├── predict.py
│   └── realtime.py
├── services/                # 业务逻辑层
│   ├── mysql_service.py
│   ├── hive_service.py      # 带 MySQL fallback
│   ├── hbase_service.py     # Stargate REST
│   ├── hdfs_service.py      # hdfs lib (WebHDFS)
│   └── model_service.py     # 加载 P1 模型报告 + 启发式预测
└── models/
    └── schemas.py           # Pydantic
```

---

## 三、所有接口清单

### 总览
- `GET /api/overview/kpi` — KPI 卡片 (8 个核心指标)
- `GET /api/overview/timeseries?metric=consumption|visit|visitors&start=...&end=...`
- `GET /api/overview/attraction-rank` — 景点热度排名
- `GET /api/overview/health` — MySQL / Hive 健康检查

### 景点
- `GET /api/attractions` — 全部景点列表
- `GET /api/attractions/{id}` — 单个景点
- `GET /api/attractions/{id}/summary` — 景点综合分析（MySQL+HBase）

### 游客
- `GET /api/visitors?page=&page_size=&gender=&min_age=&max_age=` — 分页 + 过滤
- `GET /api/visitors/{id}` — 单个游客
- `GET /api/visitors/{id}/aggregate` — 游客聚合（消费/游玩 + HBase 画像）

### 消费与游玩
- `GET /api/consumption?page=&page_size=&start_date=&end_date=&visitor_id=&attraction_id=`
- `GET /api/consumption/visits?同上`

### 数据分析（Spark SQL / Hive）
- `GET /api/analysis/daily?start=&end=` — 日客流量时序
- `GET /api/analysis/hourly` — 时段分布
- `GET /api/analysis/region?limit=20` — 地区分布 Top
- `GET /api/analysis/age-group` — 年龄段 × 性别
- `GET /api/analysis/type-summary` — 景点类型汇总
- `GET /api/analysis/fpgrowth` — Top 关联规则（P1.2 报告）

### 机器学习预测
- `POST /api/predict` — 通用预测（type: consumption_amount | daily_visitor | high_value_visitor）
- `GET /api/predict/regression` — 回归三模型对比
- `GET /api/predict/classification` — 分类四模型对比
- `GET /api/predict/clustering` — 聚类解释
- `GET /api/predict/compare` — 全部报告

### 实时数据（HBase）
- `GET /api/realtime/visit-recent?limit=20`
- `GET /api/realtime/visitor/{id}`
- `GET /api/realtime/attraction/{id}`

---

## 四、VM 部署

### 4.1 安装依赖

```bash
cd /opt/scenic/app/backend
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 4.2 配置（env vars）

```bash
# 复制示例 env
cp .env.example .env

# 关键配置 (默认值已对齐 docker-compose)
export MYSQL_HOST=mysql
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASS=root123
export MYSQL_DB=scenic
export HIVE_HOST=hive-server-1
export HIVE_PORT=10000
export HIVE_DB=scenic_dw
export HDFS_NAMENODE=hadoop-namenode
export HDFS_PORT=9870
export HBASE_REST_URL=http://hbase-master:8085
```

### 4.3 启动

```bash
# 开发模式 (auto reload)
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# 生产模式
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

访问：
- API: http://localhost:8000/
- 文档: http://localhost:8000/docs
- 健康: http://localhost:8000/api/overview/health

### 4.4 Docker 部署（推荐）

在 [docker/backend/](../../docker/backend/) 已有 Dockerfile，挂入项目后可走 docker-compose 拉起。

---

## 五、容错策略

| 组件 | 容错 |
|---|---|
| MySQL | 连接失败 → API 返回 5xx；前端展示错误 |
| Hive | 连接失败 → 自动 fallback 到 MySQL 聚合 |
| HBase REST | 连接失败 → 返回 `null` 数据 + 警告 |
| HDFS | 读报告失败 → 报告 API 返回 `null` + 提示运行 P1 |
| ML 模型 | 启发式 fallback（按 类型×月 规则），保证接口有响应 |

所有服务连接超时 5s，失败日志通过 `loguru` 输出到 stdout。

---

## 六、依赖项

| 依赖 | 版本 | 用途 |
|---|---|---|
| fastapi | 0.110.0 | Web 框架 |
| uvicorn | 0.27.1 | ASGI server |
| pydantic | 2.6.1 | 数据校验 |
| pymysql | 1.1.0 | MySQL 客户端 |
| PyHive | 0.7.0 | Hive Thrift 客户端 |
| hdfs | 2.7.3 | HDFS WebHDFS 客户端 |
| requests | 2.31.0 | HBase Stargate REST |
| loguru | - | 日志 |

---

## 七、待办 / 已知限制

1. **P2.6 未做**：Spark MLlib 模型加载（PySpark 容器内运行），当前用启发式 fallback。如果要真实加载模型，需要起一个常驻 Spark 服务，本地服务通过 RPC 调用。
2. **HBase REST Stargate**：默认镜像已含，但需要先 `enable_table`。
3. **分页上限**：`page_size <= 500`，避免大查询。
4. **CORS**：默认 `*`，生产应收紧。
