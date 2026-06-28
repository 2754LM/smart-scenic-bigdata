"""
Smart Scenic BigData - Demo Backend (FastAPI single-file).

Connects to all 8 big-data components:
  - MySQL   (PyMySQL)
  - Hive    (skipped - use HDFS CSV via subprocess)
  - HBase   (docker exec hbase shell - happybase protocol incompat)
  - Kafka   (kafka-python)
  - Spark   (spark-submit via docker exec into spark-master)
  - Sqoop   (docker exec into hadoop-namenode)
  - HDFS    (hdfs cli via docker exec)
"""
import json
import subprocess
import time
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn

app = FastAPI(title="Smart Scenic BigData Demo", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def run(cmd: list, timeout: int = 30) -> str:
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise HTTPException(500, f"cmd failed: {' '.join(cmd)}\n{r.stderr}")
    return r.stdout


# ---------- MySQL ----------
import pymysql

MYSQL_CFG = dict(host="localhost", port=13306, user="root",
                 password="root123", database="scenic", charset="utf8mb4")


def mysql_query(sql: str, args=None):
    conn = pymysql.connect(**MYSQL_CFG)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, args)
            return cur.fetchall()
    finally:
        conn.close()


# ---------- HBase via hbase shell (docker exec) ----------
def hbase_exec(cmds: str, timeout: int = 30) -> str:
    """Run hbase shell commands via docker exec using file indirection.
    hbase shell -n doesn't exit cleanly from PowerShell pipes, so we
    write cmds to a file, run hbase shell, and read the output file."""
    # Write commands to a file in container
    write_script = (
        "cat > /tmp/hbase-cmd.cmd << 'HBCMD'\n"
        + cmds
        + "\nHBCMD\n"
        "echo '---HBASE_OUT_START---'\n"
        "hbase shell -n < /tmp/hbase-cmd.cmd > /tmp/hbase-out.log 2>&1 &\n"
        "HB_PID=$!\n"
        # wait up to 15s for hbase to finish
        "for i in $(seq 1 15); do\n"
        "  if ! kill -0 $HB_PID 2>/dev/null; then break; fi\n"
        "  sleep 1\n"
        "done\n"
        # if still running, kill it (SIGTERM then SIGKILL)
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


# ---------- Kafka ----------
from kafka import KafkaProducer, KafkaConsumer

KAFKA_BOOTSTRAP = "localhost:19095"
KAFKA_TOPIC = "scenic_reviews"


# ---------- Routes ----------
class Review(BaseModel):
    scenic_id: str
    visitor_id: str
    rating: int
    comment: str


@app.get("/api/health")
def health():
    return {"status": "ok", "ts": time.time()}


@app.get("/api/scenics")
def list_scenics():
    """Read scenic list from MySQL (OLTP path)."""
    rows = mysql_query(
        "SELECT scenic_id, scenic_name, scenic_type, location, ticket_price "
        "FROM t_scenic ORDER BY scenic_id"
    )
    return {"source": "mysql", "count": len(rows), "data": rows}


@app.get("/api/scenics-hive")
def list_scenics_hive():
    """Read scenic list from HDFS (the same data Sqoop imported).
    Hive path: in production you'd query Hive via JDBC/ODBC; here we read
    the same HDFS files via hdfs cli for portability."""
    cmd = ("export JAVA_HOME=/opt/jdk8 && export HADOOP_HOME=/opt/hadoop && "
           "export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH && "
           "hdfs dfs -cat /scenic/sqoop/t_scenic/part-m-00000 | head -20")
    try:
        out = run(["docker", "exec", "hadoop-namenode", "sh", "-c", cmd],
                  timeout=30)
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
    return {"source": "hdfs (via hdfs dfs -cat)", "count": len(rows),
            "data": rows}


@app.get("/api/stats")
def spark_stats():
    """Trigger Spark SQL job to compute scenic visit counts."""
    spark_sql = """
spark.sql(\"\"\"
  SELECT s.scenic_id, s.scenic_name, COUNT(v.visit_id) AS visits,
         AVG(v.satisfaction) AS avg_rating
  FROM scenic t_scenic s LEFT JOIN visit t_visit v
    ON s.scenic_id = v.scenic_id
  GROUP BY s.scenic_id, s.scenic_name
  ORDER BY visits DESC
\"\"\").show()
"""
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
    script_path = "/tmp/scenic_stats.py"
    run(["docker", "exec", "-i", "spark-master",
         "sh", "-c", f"cat > {script_path}"], timeout=10)
    # write content via heredoc to avoid quoting hell
    p = subprocess.Popen(
        ["docker", "exec", "-i", "spark-master", "sh", "-c",
         f"cat > {script_path}"],
        stdin=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    p.communicate(script.encode("utf-8"), timeout=10)

    out = run(["docker", "exec", "spark-master",
               "/opt/spark/bin/spark-submit", "--master",
               "spark://spark-master:7077", script_path], timeout=120)
    return {"source": "spark", "output": out[-2000:]}


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
    out = hbase_exec(cmds)
    if "ERROR" in out and "exist" not in out.lower():
        # table doesn't exist, create then retry
        create_cmds = (
            'create "scenic_reviews", "cf"\n'
            f'put "scenic_reviews", "{row_key}", "cf:visitor_id", "{r.visitor_id}"\n'
            f'put "scenic_reviews", "{row_key}", "cf:rating", "{r.rating}"\n'
            f'put "scenic_reviews", "{row_key}", "cf:comment", "{r.comment}"\n'
        )
        out = hbase_exec(create_cmds)
    return {"status": "ok", "row_key": row_key, "log": out[-500:]}


@app.get("/api/reviews/{scenic_id}")
def list_reviews_hbase(scenic_id: str):
    """Scan HBase for reviews of one scenic via hbase shell."""
    cmds = f'scan "scenic_reviews", {{ROWPREFIXFILTER => "{scenic_id}", LIMIT => 50}}\n'
    out = hbase_exec(cmds)
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


@app.post("/api/trigger-sqoop")
def trigger_sqoop():
    """Trigger Sqoop import: MySQL scenic.* -> HDFS /scenic/sqoop/."""
    out = run(["docker", "exec", "hadoop-namenode",
               "bash", "/opt/jobs/sqoop-import-mysql.sh"], timeout=180)
    return {"status": "ok", "hdfs_path": "/scenic/sqoop/",
            "log_tail": out[-500:]}


@app.get("/api/hdfs-status")
def hdfs_status():
    """List Sqoop output files in HDFS."""
    cmd = ("export JAVA_HOME=/opt/jdk8 && export HADOOP_HOME=/opt/hadoop && "
           "export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH && "
           "hdfs dfs -ls -R /scenic/sqoop/")
    out = run(["docker", "exec", "hadoop-namenode", "sh", "-c", cmd],
              timeout=30)
    return {"hdfs_path": "/scenic/sqoop/", "raw": out}


@app.get("/", response_class=HTMLResponse)
def root():
    return "<h1>Smart Scenic BigData API</h1>" \
           "<p>Visit <a href='/docs'>/docs</a> for Swagger UI</p>" \
           "<p>Or open <a href='http://localhost:8080'>frontend</a></p>"


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)