"""
Smart Scenic BigData - Demo Backend (FastAPI 入口 / 模块化集成).

本文件是后端的统一入口（向后兼容 P0 单文件 demo 端点 + 加载 P2/P3/P4 modular routers）:
  - 注册 P2 modular routers（attractions/visitors/consumption/analysis/predict/realtime）
  - 注册 P3 admin router（系统管理面板 + HBase 游玩记录端点）
  - 启动时加载/训练模型（从 HDFS /scenic/models/ 加载 → 否则训练 + 持久化）
  - 启动时 init Kafka consumer 后台线程
  - 启动时 seed HBase scenic_realtime 表（Docker 不可用时跳过）

可独立运行（向后兼容 P0 demo）：
  uvicorn main:app --reload --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from contextlib import asynccontextmanager
from typing import Any, Dict

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

# ============== 关键：项目根目录加到 sys.path ==============
import sys
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

log = logging.getLogger("smart-scenic.main")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(name)s] %(levelname)s - %(message)s",
)


# ============== 简易工具 ==============
def _run(cmd: list, timeout: int = 30) -> str:
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise HTTPException(500, f"cmd failed: {' '.join(cmd)}\n{r.stderr}")
    return r.stdout


# ============== FastAPI lifespan ==============
@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用启动 / 关闭时执行的钩子"""
    # ---- 启动 ----
    log.info("=== Smart Scenic Backend starting ===")

    # 1) 加载 / 训练模型
    try:
        import services.model_service as model_svc
        st = model_svc.ensure_models()
        log.info("models: %s", st)
    except Exception as e:
        log.warning("model init failed: %s", e)

    # 2) Kafka consumer 启动
    try:
        from services import kafka_consumer
        kafka_consumer.start()
        log.info("kafka consumer started")
    except Exception as e:
        log.warning("kafka consumer start failed: %s", e)

    # 3) HBase realtime seed（如果为空）
    try:
        from services import hbase_service as hbase_svc
        if hbase_svc.seed_if_empty():
            log.info("HBase scenic_realtime table seeded with demo rows")
    except Exception as e:
        log.warning("hbase seed skipped: %s", e)

    log.info("=== Smart Scenic Backend ready ===")

    yield

    # ---- 关闭 ----
    try:
        from services import kafka_consumer
        kafka_consumer.stop()
    except Exception:
        pass


# ============== 创建 App ==============
app = FastAPI(
    title="Smart Scenic BigData Demo",
    version="1.4.0",
    description=(
        "智能景区大数据平台 - 后端 API\n\n"
        "组件：MySQL + HDFS + HBase + Kafka + Spark + Hive + Sqoop + Spark Streaming\n\n"
        "主要端点：\n"
        "- GET  /api/health\n"
        "- GET  /api/scenics            (MySQL OLTP)\n"
        "- GET  /api/scenics-hive       (HDFS via hdfs dfs -cat)\n"
        "- GET  /api/stats              (Spark SQL)\n"
        "- POST /api/reviews            (HBase 写)\n"
        "- GET  /api/reviews/{id}       (HBase 扫)\n"
        "- POST /api/reviews-stream     (Kafka 发)\n"
        "- GET  /api/reviews-stream     (Kafka 拉)\n"
        "- POST /api/trigger-sqoop      (Sqoop import)\n"
        "- GET  /api/hdfs-status        (HDFS 状态)\n"
        "- P2 routers: /api/overview, /api/attractions, /api/visitors, /api/consumption, "
        "/api/analysis, /api/predict, /api/realtime\n"
        "- P3 admin: /api/admin\n"
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============== 注册 P2/P3 Modular Routers ==============
def _register_routers() -> None:
    """注册所有 modular routers（失败时降级 log，不阻塞启动）"""
    routes_to_try = [
        ("routers.overview",        "/api/overview"),
        ("routers.attractions",     "/api/attractions"),
        ("routers.visitors",        "/api/visitors"),
        ("routers.consumption",     "/api/consumption"),
        ("routers.analysis",        "/api/analysis"),
        ("routers.predict",         "/api/predict"),
        ("routers.realtime",        "/api/realtime"),
        ("routers.admin",           "/api/admin"),
    ]
    registered = 0
    for module_name, prefix in routes_to_try:
        try:
            mod = __import__(module_name, fromlist=["router"])
            if hasattr(mod, "router"):
                app.include_router(mod.router)
                registered += 1
                log.info("router registered: %s -> %s", module_name, prefix)
        except Exception as e:
            log.warning("router load failed: %s (%s)", module_name, e)
    log.info("modular routers registered: %d/%d", registered, len(routes_to_try))


_register_routers()


# ============== P0 向后兼容端点 ==============
@app.get("/api/health")
def health():
    return {"status": "ok", "ts": time.time(), "version": "1.4.0"}


# ---------- MySQL ----------
import pymysql

MYSQL_CFG = dict(host=os.getenv("MYSQL_HOST", "localhost"),
                 port=int(os.getenv("MYSQL_PORT", "13306")),
                 user=os.getenv("MYSQL_USER", "root"),
                 password=os.getenv("MYSQL_PASSWORD", "root123"),
                 database=os.getenv("MYSQL_DB", "scenic"),
                 charset="utf8mb4")


def _mysql_query(sql: str, args=None):
    conn = pymysql.connect(**MYSQL_CFG)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, args)
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/api/scenics")
def list_scenics():
    """Read scenic list from MySQL (OLTP path)."""
    rows = _mysql_query(
        "SELECT scenic_id, scenic_name, scenic_type, location, ticket_price "
        "FROM t_scenic ORDER BY scenic_id"
    )
    return {"source": "mysql", "count": len(rows), "data": rows}


@app.get("/api/scenics-hive")
def list_scenics_hive():
    """Read scenic list from HDFS (the same data Sqoop imported)."""
    cmd = ("export JAVA_HOME=/opt/jdk8 && export HADOOP_HOME=/opt/hadoop && "
           "export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH && "
           "hdfs dfs -cat /scenic/sqoop/t_scenic/part-m-00000 | head -20")
    try:
        out = _run(["docker", "exec", "hadoop-namenode", "sh", "-c", cmd], timeout=30)
    except Exception as e:
        raise HTTPException(500, f"hdfs error: {e}")
    rows = []
    for line in out.strip().split("\n"):
        parts = line.split(",")
        if len(parts) >= 7:
            rows.append({
                "scenic_id": parts[0],
                "scenic_name": parts[1],
                "scenic_type": parts[2],
                "location": parts[3],
                "open_time": parts[4],
                "ticket_price": parts[6],
            })
    return {"source": "hdfs (via hdfs dfs -cat)", "count": len(rows), "data": rows}


@app.get("/api/stats")
def spark_stats():
    """Trigger Spark SQL job to compute scenic visit counts."""
    spark_sql = '''
spark.sql("""
  SELECT s.scenic_id, s.scenic_name, COUNT(v.visit_id) AS visits,
         AVG(v.satisfaction) AS avg_rating
  FROM scenic t_scenic s LEFT JOIN visit t_visit v
    ON s.scenic_id = v.scenic_id
  GROUP BY s.scenic_id, s.scenic_name
  ORDER BY visits DESC
""").show()
'''
    script = f"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("scenic-stats").getOrCreate()
spark.read.csv("hdfs://hadoop-namenode:9000/scenic/sqoop/t_scenic",
               header=False, inferSchema=True).createOrReplaceTempView("scenic")
spark.read.csv("hdfs://hadoop-namenode:9000/scenic/sqoop/t_visit",
               header=False, inferSchema=True).createOrReplaceTempView("visit")
{spark_sql}
spark.stop()
"""
    p = subprocess.Popen(
        ["docker", "exec", "-i", "spark-master", "sh", "-c",
         "cat > /tmp/scenic_stats.py"],
        stdin=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    p.communicate(script.encode("utf-8"), timeout=10)
    out = _run(["docker", "exec", "spark-master",
                "/opt/spark/bin/spark-submit", "--master",
                "spark://spark-master:7077", "/tmp/scenic_stats.py"], timeout=120)
    return {"source": "spark", "output": out[-2000:]}


# ---------- HBase via hbase shell ----------
class Review(BaseModel):
    scenic_id: str
    visitor_id: str
    rating: int
    comment: str


def _hbase_exec(cmds: str, timeout: int = 30) -> str:
    write_script = (
        "cat > /tmp/hbase-cmd.cmd << 'HBCMD'\n"
        + cmds
        + "\nHBCMD\n"
        "echo '---HBASE_OUT_START---'\n"
        "hbase shell -n < /tmp/hbase-cmd.cmd > /tmp/hbase-out.log 2>&1 &\n"
        "HB_PID=$!\n"
        "for i in $(seq 1 15); do\n"
        "  if ! kill -0 $HB_PID 2>/dev/null; then break; fi\n"
        "  sleep 1\n"
        "done\n"
        "kill $HB_PID 2>/dev/null; sleep 1\n"
        "kill -9 $HB_PID 2>/dev/null\n"
        "cat /tmp/hbase-out.log\n"
    )
    r = subprocess.run(
        ["docker", "exec", "hbase-master", "sh", "-c", write_script],
        capture_output=True, text=True, timeout=timeout,
    )
    out = r.stdout or ""
    if "---HBASE_OUT_START---" in out:
        out = out.split("---HBASE_OUT_START---", 1)[1]
    return out


@app.post("/api/reviews")
def write_review_hbase(r: Review):
    """Write review to HBase via hbase shell (real-time NoSQL path)."""
    ts = int(time.time() * 1000)
    row_key = f"{r.scenic_id}_{ts}"
    cmds = (
        'list_namespace_tables "default" 2>/dev/null\n'
        'exists "scenic_reviews"\n'
        f'put "scenic_reviews", "{row_key}", "cf:visitor_id", "{r.visitor_id}"\n'
        f'put "scenic_reviews", "{row_key}", "cf:rating", "{r.rating}"\n'
        f'put "scenic_reviews", "{row_key}", "cf:comment", "{r.comment}"\n'
    )
    out = _hbase_exec(cmds)
    if "ERROR" in out and "exist" not in out.lower():
        create_cmds = (
            'create "scenic_reviews", "cf"\n'
            f'put "scenic_reviews", "{row_key}", "cf:visitor_id", "{r.visitor_id}"\n'
            f'put "scenic_reviews", "{row_key}", "cf:rating", "{r.rating}"\n'
            f'put "scenic_reviews", "{row_key}", "cf:comment", "{r.comment}"\n'
        )
        out = _hbase_exec(create_cmds)
    return {"status": "ok", "row_key": row_key, "log": out[-500:]}


@app.get("/api/reviews/{scenic_id}")
def list_reviews_hbase(scenic_id: str):
    """Scan HBase for reviews of one scenic via hbase shell."""
    cmds = f'scan "scenic_reviews", {{ROWPREFIXFILTER => "{scenic_id}", LIMIT => 50}}\n'
    out = _hbase_exec(cmds)
    rows = []
    for line in out.split("\n"):
        line = line.strip()
        if line and line[0].isdigit() and " column=" in line:
            try:
                key_part, rest = line.split(" column=", 1)
                col, _, val = rest.partition(" timestamp=")
                cf, _, qf = col.partition(":")
                _, _, val_clean = val.rpartition(" value=")
                rows.append({
                    "row_key": key_part.strip(),
                    "cf": cf.strip(),
                    "qualifier": qf.strip(),
                    "value": val_clean.strip(),
                })
            except Exception:
                pass
    return {"scenic_id": scenic_id, "count": len(rows), "data": rows}


# ---------- Kafka ----------
from kafka import KafkaProducer, KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost:19095")
KAFKA_TOPIC = "scenic_reviews"


@app.post("/api/reviews-stream")
def publish_review_kafka(r: Review):
    """Publish review to Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    payload = r.dict()
    payload["ts"] = time.time()
    future = producer.send(KAFKA_TOPIC, payload)
    future.get(timeout=10)
    producer.flush()
    producer.close()
    return {"status": "ok", "topic": KAFKA_TOPIC, "payload": payload}


@app.get("/api/reviews-stream")
def consume_reviews_kafka(timeout_ms: int = 3000):
    """Consume up to N messages from Kafka topic (one-shot poll)."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    msgs = []
    for m in consumer:
        msgs.append(m.value)
    consumer.close()
    return {"topic": KAFKA_TOPIC, "count": len(msgs), "data": msgs}


# ---------- Sqoop / HDFS ----------
@app.post("/api/trigger-sqoop")
def trigger_sqoop():
    """Trigger Sqoop import: MySQL scenic.* -> HDFS /scenic/sqoop/."""
    out = _run(["docker", "exec", "hadoop-namenode",
                "bash", "/opt/jobs/sqoop-import-mysql.sh"], timeout=180)
    return {"status": "ok", "hdfs_path": "/scenic/sqoop/", "log_tail": out[-500:]}


@app.get("/api/hdfs-status")
def hdfs_status():
    """List Sqoop output files in HDFS."""
    cmd = ("export JAVA_HOME=/opt/jdk8 && export HADOOP_HOME=/opt/hadoop && "
           "export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH && "
           "hdfs dfs -ls -R /scenic/sqoop/")
    out = _run(["docker", "exec", "hadoop-namenode", "sh", "-c", cmd], timeout=30)
    return {"hdfs_path": "/scenic/sqoop/", "raw": out}


# ---------- 根路径 ----------
@app.get("/", response_class=HTMLResponse)
def root():
    return (
        "<h1>Smart Scenic BigData API v1.4.0</h1>"
        "<ul>"
        "<li><a href='/docs'>/docs</a> - Swagger UI</li>"
        "<li><a href='/redoc'>/redoc</a> - ReDoc</li>"
        "<li><a href='http://localhost:8080'>前端</a> (需启动 scripts/start-app.bat)</li>"
        "</ul>"
        "<h2>主要端点</h2>"
        "<ul>"
        "<li><code>GET /api/health</code></li>"
        "<li><code>GET /api/scenics</code> (MySQL)</li>"
        "<li><code>GET /api/scenics-hive</code> (HDFS)</li>"
        "<li><code>GET /api/stats</code> (Spark)</li>"
        "<li><code>POST /api/reviews</code> (HBase 写)</li>"
        "<li><code>GET /api/reviews/{scenic_id}</code> (HBase 扫)</li>"
        "<li><code>POST /api/reviews-stream</code> (Kafka 发)</li>"
        "<li><code>GET /api/reviews-stream</code> (Kafka 拉)</li>"
        "<li><code>POST /api/trigger-sqoop</code> (Sqoop)</li>"
        "<li><code>GET /api/hdfs-status</code> (HDFS 状态)</li>"
        "</ul>"
        "<h2>Modular 端点（P2/P3）</h2>"
        "<ul>"
        "<li><code>GET /api/overview/kpi</code> 总览 KPI</li>"
        "<li><code>GET /api/attractions</code> 景点列表</li>"
        "<li><code>GET /api/visitors</code> 游客列表</li>"
        "<li><code>GET /api/consumption</code> 消费列表</li>"
        "<li><code>GET /api/analysis/*</code> 数据分析（5 个端点 + 营销建议）</li>"
        "<li><code>POST /api/predict</code> 模型预测</li>"
        "<li><code>GET /api/predict/{regression,classification,clustering,compare,status}</code></li>"
        "<li><code>POST /api/predict/retrain</code> 重训并持久化 HDFS</li>"
        "<li><code>GET /api/realtime/*</code> 实时数据（事件/评论/游玩记录/游客画像/景点热度）</li>"
        "<li><code>GET /api/admin/*</code> 系统管理面板</li>"
        "</ul>"
    )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
