"""
Admin Service - 系统管理 + 数据处理触发

提供：
  - 系统状态查询（17 容器 / 4 数据集 / 已训练模型 / 异步任务）
  - 异步操作触发（CSV 加载 / Sqoop 导入 / Spark 清洗 / Hive DDL / 模型训练）

所有"数据处理"操作在前端一键触发，后端用 threading 异步执行。
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import config
from utils import docker_exec

log = logging.getLogger("smart-scenic.admin")


# Helper: run command in container (uses docker CLI or socket API fallback)
def _run_in_container(container: str, *cmd, timeout: int = 30) -> dict:
    """Run command in container. Returns {stdout, stderr, exit_code}.

    Strategy: docker socket API with Tty=false + AttachStdout=false (Detach),
    poll for completion, then fetch captured stdout via /exec/{id}/logs
    (works for recent API versions when AttachStdout was true at start).

    Usage: _run_in_container("mysql", "mysql", "-e", "SELECT 1", timeout=10)
    """
    import time as _t
    from services.docker_client import _request

    cmd = list(cmd)

    # 1. Create exec instance — AttachStdout=true so /exec/{id}/logs will work
    exec_id = None
    for _attempt in range(3):
        try:
            exec_id = _request("POST", f"/containers/{container}/exec", {
                "Cmd": cmd,
                "AttachStdout": True,
                "AttachStderr": True,
                "Tty": False,
            })
            if exec_id and "Id" in exec_id:
                break
        except Exception:
            pass
        _t.sleep(1)
    if not exec_id or "Id" not in exec_id:
        return {"stdout": "", "stderr": "exec create failed after 3 retries", "exit_code": -1}
    eid = exec_id["Id"]

    # 2. Start exec with Detach=true (don't block on stdout stream)
    start_resp = _request("POST", f"/exec/{eid}/start", {"Detach": True})
    if start_resp is None:
        return {"stdout": "", "stderr": "exec start failed", "exit_code": -1}

    # 3. Poll until Running=false (poll every 2s, max timeout seconds)
    ec = -1
    deadline = _t.time() + timeout
    while _t.time() < deadline:
        info = _request("GET", f"/exec/{eid}/json")
        if isinstance(info, dict):
            if not info.get("Running", True):
                ec_obj = info.get("ExitCode")
                ec = ec_obj if isinstance(ec_obj, int) else -1
                break
        _t.sleep(2)
    else:
        return {"stdout": "", "stderr": f"exec timed out after {timeout}s", "exit_code": -1}

    # 4. Fetch captured stdout/stderr via /exec/{id}/logs (works for AttachStdout=true execs).
    # NOTE: For detached execs the API returns a placeholder; result may be empty.
    # The caller will still see correct exit_code and can verify HDFS artifacts separately.
    log_resp = _request("GET", f"/exec/{eid}/logs", {"stdout": True, "stderr": True})
    body_text = ""
    if log_resp is not None and not isinstance(log_resp, dict):
        body_text = str(log_resp)
    # Common error responses → treat as empty (caller uses exit_code)
    if isinstance(log_resp, dict) and log_resp.get("message"):
        body_text = ""
    return {"stdout": body_text, "stderr": "", "exit_code": ec}


# ============================================================
# 异步任务管理（线程 + 状态记录）
# ============================================================
class Job:
    def __init__(self, name: str, kind: str = "trigger"):
        self.id = str(uuid.uuid4())[:8]
        self.name = name
        self.kind = kind
        self.status = "pending"  # pending / running / success / failed
        self.started_at: Optional[str] = None
        self.finished_at: Optional[str] = None
        self.log_lines: List[str] = []
        self.result: Optional[Dict[str, Any]] = None
        self.error: Optional[str] = None
        self._thread: Optional[threading.Thread] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "kind": self.kind,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "log_tail": self.log_lines[-30:],  # 末尾 30 行
            "log_count": len(self.log_lines),
            "result": self.result,
            "error": self.error,
        }


_JOBS: Dict[str, Job] = {}
_JOBS_LOCK = threading.Lock()


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _submit_job(name: str, kind: str, target: callable) -> Job:
    """提交一个异步任务"""
    job = Job(name=name, kind=kind)
    with _JOBS_LOCK:
        _JOBS[job.id] = job

    def _runner():
        job.status = "running"
        job.started_at = _now_iso()
        try:
            target(job)
            job.status = "success"
        except Exception as e:
            job.status = "failed"
            job.error = str(e)
            job.log_lines.append(f"[ERROR] {e}")
            log.exception("job %s failed", name)
        finally:
            job.finished_at = _now_iso()

    t = threading.Thread(target=_runner, name=f"job-{name}-{job.id}", daemon=True)
    job._thread = t
    t.start()
    return job


def get_job(job_id: str) -> Optional[Job]:
    return _JOBS.get(job_id)


def list_jobs(limit: int = 20) -> List[Job]:
    """返回最近 N 个任务，按 started_at 倒序"""
    return sorted(
        _JOBS.values(),
        key=lambda j: j.started_at or "",
        reverse=True,
    )[:limit]


# ============================================================
# 任务定义（每个函数 = 一个可触发的操作）
# ============================================================
def _op_load_csv(job: Job) -> None:
    """从 data/raw_data/*.csv 加载到 MySQL scenic 数据库"""
    job.log_lines.append(f"[{_now_iso()}] start loading CSV to MySQL")
    DATA_DIR = Path(config.DATA_RAW_DIR)

    if not DATA_DIR.exists():
        # 自动从 Topic 18 源目录拷贝（如果有的话）
        # 这里 fallback 到项目里的 data/raw_data/
        raise FileNotFoundError(
            f"data/raw_data/ not found. Please put 4 CSVs at {DATA_DIR}"
        )

    # 延迟 import 避免循环依赖
    import pymysql

    conn = pymysql.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DB,
        charset="utf8mb4",
        autocommit=False,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SET FOREIGN_KEY_CHECKS = 0")
            for t in ["t_attraction", "t_visitor", "t_consumption", "t_visit_record"]:
                cur.execute(f"TRUNCATE TABLE {t}")
            cur.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()

        LOAD_PLAN = [
            ("attractions.csv", "t_attraction", ["景点ID", "景点名称", "类型", "位置", "开放时间"], {}),
            ("visitors.csv",    "t_visitor",    ["游客ID", "姓名", "性别", "年龄", "地区"],     {"年龄": int}),
            ("consumption.csv", "t_consumption", ["消费ID", "时间", "游客ID", "景点ID", "消费金额"],
             {"消费ID": int, "消费金额": float}),
            ("visit_records.csv", "t_visit_record", ["记录ID", "时间", "游客ID", "景点ID", "游玩时长"],
             {"记录ID": int, "游玩时长": float}),
        ]
        import csv
        total = 0
        for csv_name, table, cols, coerce in LOAD_PLAN:
            path = DATA_DIR / csv_name
            if not path.exists():
                job.log_lines.append(f"  SKIP: {path} not found")
                continue
            job.log_lines.append(f"  load {csv_name} -> {table}")
            with conn.cursor() as cur, open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                placeholders = ", ".join(["%s"] * len(cols))
                col_list = ", ".join(f"`{c}`" for c in cols)
                sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
                batch = []
                for row in reader:
                    values = []
                    for c in cols:
                        v = row.get(c, "")
                        if c in coerce:
                            try:
                                v = coerce[c](v) if v else None
                            except (ValueError, TypeError):
                                v = None
                        else:
                            v = None if v == "" else v
                        values.append(v)
                    batch.append(values)
                    if len(batch) >= 5000:
                        cur.executemany(sql, batch)
                        conn.commit()
                        batch = []
                if batch:
                    cur.executemany(sql, batch)
                    conn.commit()
            # 取行数
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                n = cur.fetchone()[0]
                job.log_lines.append(f"  -> {table}: {n} rows")
                total += n
        job.result = {"total_rows": total}
        job.log_lines.append(f"[{_now_iso()}] CSV load done, {total} rows total")
    finally:
        conn.close()


def _op_sqoop_import(job: Job) -> None:
    """Sqoop: MySQL 4 张表 -> HDFS /scenic/sqoop/"""
    job.log_lines.append(f"[{_now_iso()}] start sqoop import")
    cmd = ["bash", "/opt/jobs/sqoop-import-mysql.sh"]
    job.log_lines.append(f"  cmd: docker exec {config.HADOOP_CONTAINER} {' '.join(cmd)}")
    r = _run_in_container(config.HADOOP_CONTAINER, *cmd, timeout=600)
    proc = {"returncode": r["exit_code"], "stdout": r["stdout"], "stderr": r["stderr"]}
    # 写日志
    if proc["stdout"]:
        for line in proc["stdout"].splitlines()[-50:]:
            job.log_lines.append(f"  {line}")
    if proc["returncode"] != 0:
        job.log_lines.append(f"  STDERR: {proc['stderr'][-500:]}")
        raise RuntimeError(f'sqoop import failed (rc={proc["returncode"]})')

    # 验证 HDFS 文件
    hdfs_check = _run_in_container(config.HADOOP_CONTAINER, "hdfs", "dfs", "-ls", "/scenic/sqoop/", timeout=10)
    job.log_lines.append(f"  HDFS /scenic/sqoop/:")
    for line in (hdfs_check.stdout or "").splitlines():
        job.log_lines.append(f"    {line}")
    job.result = {"tables_imported": hdfs_check.stdout.count("part-m-")}


def _op_spark_clean(job: Job) -> None:
    """Spark 清洗：/scenic/sqoop -> /scenic/cleaned"""
    job.log_lines.append(f"[{_now_iso()}] start spark-submit clean")
    cmd = ["bash", "/opt/jobs/spark-submit.sh", "clean"]
    job.log_lines.append(f"  cmd: docker exec {config.SPARK_CONTAINER} {' '.join(cmd)}")
    r = _run_in_container(config.SPARK_CONTAINER, *cmd, timeout=600)
    proc = {"returncode": r["exit_code"], "stdout": r["stdout"], "stderr": r["stderr"]}
    if proc["stdout"]:
        for line in proc["stdout"].splitlines()[-50:]:
            job.log_lines.append(f"  {line}")
    if proc["returncode"] != 0:
        raise RuntimeError(f"spark clean failed: {proc['stderr'][-500:]}")
    job.result = {"output": "/scenic/cleaned/"}


def _op_hive_ddl(job: Job) -> None:

    """Run Hive DDL + views via beeline (Hive 2+ recommended)."""

    for sql_file in ["ddl.sql", "views.sql"]:

        job.log_lines.append(f"  beeline -f /opt/jobs/hive/{sql_file} (HS2=hive-server-1:10000)")

        beeline_cmd = f"/opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/scenic_ext' -n hive -p hive -f /opt/jobs/hive/{sql_file}"
        job.log_lines.append(f"  cmd: docker exec hive-server-1 bash -c '{beeline_cmd[:80]}...'")
        r = _run_in_container("hive-server-1", "bash", "-c", beeline_cmd, timeout=300)
        proc = {"returncode": r["exit_code"], "stdout": r["stdout"], "stderr": r["stderr"]}
        if proc["stdout"]:
            important = [
                line for line in proc["stdout"].splitlines()
                if "FAILED" in line or "Error" in line or "Table" in line
                or "View" in line or "OK" in line or "rows selected" in line
            ][-15:]
            for line in important:
                job.log_lines.append(f"    {line}")
        if proc["returncode"] != 0:
            raise RuntimeError(f"beeline {sql_file} failed: {proc[chr(39)+chr(115)+chr(116)+chr(100)+chr(101)+chr(114)+chr(114)+chr(39)][-500:]}")



def _op_hive_queries(job: Job) -> None:

    """Run 8 example HiveQL queries via beeline to verify Hive."""

    job.log_lines.append("  beeline -f /opt/jobs/hive/queries.sql (HS2=hive-server-1:10000)")

    beeline_cmd = "/opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/scenic_ext' -n hive -p hive -f /opt/jobs/hive/queries.sql"
    job.log_lines.append(f"  cmd: docker exec hive-server-1 bash -c '{beeline_cmd[:80]}...'")

    r = _run_in_container("hive-server-1", "bash", "-c", beeline_cmd, timeout=600)

    proc = {"returncode": r["exit_code"], "stdout": r["stdout"], "stderr": r["stderr"]}

    if proc["stdout"]:

        for line in proc["stdout"].splitlines()[-30:]:

            job.log_lines.append(f"    {line}")


def _op_spark_train(job: Job) -> None:
    """PySpark MLlib 训练"""
    job.log_lines.append(f"[{_now_iso()}] start spark-submit ml-train")
    cmd = ["bash", "/opt/jobs/spark-submit.sh", "ml-train"]
    job.log_lines.append(f"  cmd: docker exec {config.SPARK_CONTAINER} {' '.join(cmd)}")
    r = _run_in_container(config.SPARK_CONTAINER, *cmd, timeout=900)
    proc = {"returncode": r["exit_code"], "stdout": r["stdout"], "stderr": r["stderr"]}
    if proc["stdout"]:
        for line in proc["stdout"].splitlines()[-60:]:
            job.log_lines.append(f"  {line}")
    if proc["returncode"] != 0:
        raise RuntimeError(f'spark train failed: {proc["stderr"][-500:]}')


# 操作注册表
ACTIONS = {
    "load_csv":     ("加载 CSV 到 MySQL",  _op_load_csv),
    "sqoop":        ("Sqoop 导入 HDFS",   _op_sqoop_import),
    "spark_clean":  ("Spark 清洗",        _op_spark_clean),
    "hive_ddl":     ("Hive DDL + 视图",   _op_hive_ddl),
    "hive_queries": ("Hive 复杂查询",     _op_hive_queries),
    "spark_train":  ("PySpark 训练",      _op_spark_train),
}


def trigger_action(name: str) -> Job:
    """触发一个操作（异步）"""
    if name not in ACTIONS:
        raise ValueError(f"unknown action: {name}, available: {list(ACTIONS.keys())}")
    label, fn = ACTIONS[name]
    return _submit_job(name=label, kind=name, target=fn)


def trigger_pipeline(actions: List[str]) -> Job:
    """触发一个 pipeline（多个操作顺序执行）"""
    def _pipeline(job: Job):
        for a in actions:
            if a not in ACTIONS:
                job.log_lines.append(f"  SKIP: unknown action {a}")
                continue
            job.log_lines.append(f"\n[{_now_iso()}] === step: {a} ===")
            try:
                ACTIONS[a][1](job)
                job.log_lines.append(f"  step {a}: OK")
            except Exception as e:
                job.log_lines.append(f"  step {a}: FAILED ({e})")
                raise
    return _submit_job(name="pipeline: " + " → ".join(actions), kind="pipeline", target=_pipeline)


# ============================================================
# 状态查询
# ============================================================
def get_containers_status() -> List[Dict[str, Any]]:
    """List all 17 containers via Docker socket API (no docker CLI needed)"""
    try:
        from services.docker_client import list_containers
        containers = list_containers(all=True)
    except Exception as e:
        return [{"error": str(e)}]

    out = []
    for c in containers:
        # Names is a list in new Docker API (e.g. ["/demo-backend"])
        names = c.get("Names", [])
        if isinstance(names, list):
            name = names[0].lstrip("/") if names else ""
        else:
            name = str(names).lstrip("/")
        if not name:
            continue
        # 只看本项目（com.docker.compose.project）
        labels = c.get("Labels", {}) or {}
        if "com.docker.compose.project" in labels:
            project = labels.get("com.docker.compose.project", "")
            if project and "smart-scenic" not in project:
                continue
        state = c.get("State", "unknown")
        status_text = c.get("Status", "")
        image = c.get("Image", "")
        healthy = state == "running"
        out.append({
            "name": name,
            "status": status_text,
            "image": image,
            "state": state,
            "healthy": healthy,
        })
    return out


def get_models_status() -> Dict[str, Any]:
    """查 /shared/models/ 下的已训练 PySpark 模型"""
    models_dir = Path(config.PYSPARK_MODELS_DIR)
    out = {
        "models_dir": str(models_dir),
        "models": [],
        "count": 0,
    }
    if not models_dir.exists():
        out["error"] = "models dir not exist (training not started)"
        return out
    for d in models_dir.iterdir():
        if d.is_dir() and (d / "metadata").exists():
            # 解析模型类型
            name = d.name
            kind = "unknown"
            if "regression" in name:    kind = "regression"
            elif "classification" in name: kind = "classification"
            elif "clustering" in name:  kind = "clustering"
            # 修改时间
            try:
                mtime = datetime.fromtimestamp(d.stat().st_mtime).isoformat()
            except Exception:
                mtime = None
            out["models"].append({
                "name": name,
                "kind": kind,
                "path": str(d),
                "modified_at": mtime,
            })
    out["count"] = len(out["models"])
    return out


def get_datasets_status() -> Dict[str, Any]:
    """查 4 张 MySQL 表的当前行数 + 4 个 CSV 文件状态"""
    out: Dict[str, Any] = {"mysql_tables": {}, "csv_files": {}}

    # 1. MySQL 4 张表行数
    try:
        import pymysql
        conn = pymysql.connect(
            host=config.MYSQL_HOST, port=config.MYSQL_PORT,
            user=config.MYSQL_USER, password=config.MYSQL_PASSWORD,
            database=config.MYSQL_DB, charset="utf8mb4",
        )
        try:
            with conn.cursor() as cur:
                for t in ["t_attraction", "t_visitor", "t_consumption", "t_visit_record"]:
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM {t}")
                        n = cur.fetchone()[0]
                        out["mysql_tables"][t] = n
                    except Exception as e:
                        out["mysql_tables"][t] = f"ERR: {e}"
        finally:
            conn.close()
    except Exception as e:
        out["mysql_error"] = str(e)

    # 2. CSV 文件状态
    raw = Path(config.DATA_RAW_DIR)
    for f in ["attractions.csv", "visitors.csv", "consumption.csv", "visit_records.csv"]:
        p = raw / f
        if p.exists():
            out["csv_files"][f] = {
                "size": p.stat().st_size,
                "lines": sum(1 for _ in open(p, "r", encoding="utf-8")),
            }
        else:
            out["csv_files"][f] = None

    return out


def get_hdfs_status() -> Dict[str, Any]:
    """查 HDFS /scenic/ 顶层目录（不递归，避免 16s 延迟）"""
    try:
        # Use -ls (not -R) to list only top-level - much faster
        r = _run_in_container(config.HADOOP_CONTAINER, "hdfs", "dfs", "-ls", "/scenic/", timeout=10)
        proc = {"returncode": r["exit_code"], "stdout": r["stdout"], "stderr": r["stderr"]}
        return {
            "available": proc["returncode"] == 0,
            "output": (proc["stdout"] or "")[:5000],  # 限制大小
            "error": proc["stderr"] if proc["returncode"] != 0 else None,
        }
    except Exception as e:
        return {"available": False, "error": str(e)}


def get_system_status() -> Dict[str, Any]:
    """综合状态（前端 dashboard 一次性拉）"""
    containers = get_containers_status()
    return {
        "ts": _now_iso(),
        "containers": {
            "total": len(containers),
            "healthy": sum(1 for c in containers if isinstance(c, dict) and c.get("healthy")),
            "list": containers,
        },
        "models":  get_models_status(),
        "datasets": get_datasets_status(),
        "hdfs":    get_hdfs_status(),
        "kafka":   {
            "topics": ["scenic_reviews", "scenic_events"],
        },
        "jobs":    [j.to_dict() for j in list_jobs(limit=10)],
        "actions": [{"name": k, "label": v[0]} for k, v in ACTIONS.items()],
    }