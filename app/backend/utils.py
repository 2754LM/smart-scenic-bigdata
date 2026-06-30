"""
Shared helpers: docker exec, HDFS cli, CSV loaders, etc.
Avoid scattering subprocess calls across services.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

import config

log = logging.getLogger("smart-scenic.backend")


# ----------------------------------------------------------------------
# Docker / HDFS / HBase shell helpers
# ----------------------------------------------------------------------
def docker_exec(
    container: str,
    cmd: str,
    timeout: int = 30,
    user: Optional[str] = None,
    check: bool = False,
) -> str:
    """Run `cmd` inside a docker container. Returns stdout."""
    args = ["docker", "exec"]
    if user:
        args += ["--user", user]
    args += [container, "sh", "-c", cmd]
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        log.warning("docker exec timeout: %s %s", container, cmd[:120])
        return ""
    if check and r.returncode != 0:
        raise RuntimeError(f"docker exec failed: {r.stderr[:300]}")
    return r.stdout or ""


def hdfs_ls(path: str) -> str:
    """List HDFS path. Empty string on error."""
    cmd = (
        "export JAVA_HOME=/opt/jdk8 && export HADOOP_HOME=/opt/hadoop && "
        "export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH && "
        f"hdfs dfs -ls -R {path}"
    )
    return docker_exec(config.HADOOP_CONTAINER, cmd, timeout=30)


def hdfs_cat(path: str, n: int = 5) -> List[str]:
    """Read first n lines of an HDFS file."""
    cmd = (
        "export JAVA_HOME=/opt/jdk8 && export HADOOP_HOME=/opt/hadoop && "
        "export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH && "
        f"hdfs dfs -cat {path} 2>/dev/null | head -{n}"
    )
    out = docker_exec(config.HADOOP_CONTAINER, cmd, timeout=30)
    return [ln for ln in out.splitlines() if ln]


def hdfs_put(local_path: str, hdfs_path: str, timeout: int = 60) -> bool:
    """Upload a local file to HDFS via `hdfs dfs -put`.

    容器里的 hadoop 客户端会从 hadoop-namenode 写入 NameNode RPC
    （见 docker-compose.yml 端口映射 19000）。
    """
    # 容器里的 hadoop 客户端看不到 host 的本地路径，
    # 所以我们走 docker cp 中转：先 cp 到 hadoop-namenode:/tmp，再 -put
    container = config.HADOOP_CONTAINER
    basename = os.path.basename(local_path)
    cp = subprocess.run(
        ["docker", "cp", local_path, f"{container}:/tmp/{basename}"],
        capture_output=True, text=True, timeout=30,
    )
    if cp.returncode != 0:
        log.warning("hdfs_put: docker cp failed: %s", cp.stderr[:200])
        return False
    cmd = (
        "export JAVA_HOME=/opt/jdk8 && export HADOOP_HOME=/opt/hadoop && "
        "export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH && "
        f"mkdir -p $(dirname {hdfs_path}) && "
        f"hdfs dfs -put -f /tmp/{basename} {hdfs_path} && "
        f"rm /tmp/{basename}"
    )
    r = subprocess.run(
        ["docker", "exec", container, "sh", "-c", cmd],
        capture_output=True, text=True, timeout=timeout,
    )
    return r.returncode == 0


def hdfs_get(hdfs_path: str, local_path: str, timeout: int = 60) -> bool:
    """Download a file from HDFS to a local path via `hdfs dfs -get`."""
    container = config.HADOOP_CONTAINER
    basename = os.path.basename(hdfs_path)
    cmd = (
        "export JAVA_HOME=/opt/jdk8 && export HADOOP_HOME=/opt/hadoop && "
        "export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH && "
        f"hdfs dfs -get {hdfs_path} /tmp/{basename} && "
        f"chmod 666 /tmp/{basename}"
    )
    r = subprocess.run(
        ["docker", "exec", container, "sh", "-c", cmd],
        capture_output=True, text=True, timeout=timeout,
    )
    if r.returncode != 0:
        log.warning("hdfs_get: hdfs -get failed: %s", r.stderr[:200])
        return False
    cp = subprocess.run(
        ["docker", "cp", f"{container}:/tmp/{basename}", local_path],
        capture_output=True, text=True, timeout=30,
    )
    subprocess.run(
        ["docker", "exec", container, "rm", "-f", f"/tmp/{basename}"],
        capture_output=True, text=True, timeout=10,
    )
    return cp.returncode == 0


def hdfs_exists(path: str) -> bool:
    """Check if HDFS path exists (`hdfs dfs -test -e`)."""
    cmd = (
        "export JAVA_HOME=/opt/jdk8 && export HADOOP_HOME=/opt/hadoop && "
        "export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH && "
        f"hdfs dfs -test -e {path}"
    )
    r = subprocess.run(
        ["docker", "exec", config.HADOOP_CONTAINER, "sh", "-c", cmd],
        capture_output=True, text=True, timeout=15,
    )
    return r.returncode == 0


def hbase_shell(commands: str, timeout: int = 30) -> str:
    """Run hbase shell commands via docker exec, returns the combined stdout.

    hbase shell -n never exits cleanly from a piped stdin, so we redirect
    the command set into a file inside the container and read the output file.
    """
    script = (
        "cat > /tmp/hbase-cmd.cmd << 'HBCMD'\n"
        + commands
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
    out = docker_exec(config.HBASE_CONTAINER, script, timeout=timeout)
    if "---HBASE_OUT_START---" in out:
        out = out.split("---HBASE_OUT_START---", 1)[1]
    return out


# ----------------------------------------------------------------------
# JSON-safe conversion
# ----------------------------------------------------------------------
def safe_json(obj: Any) -> Any:
    """Recursively convert pandas / numpy objects to JSON-safe primitives."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(k): safe_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [safe_json(v) for v in obj]
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if hasattr(obj, "item"):  # numpy scalar
        try:
            return obj.item()
        except Exception:
            pass
    if isinstance(obj, (int, float, str, bool)):
        return obj
    return str(obj)


# ----------------------------------------------------------------------
# CSV cache for raw_data/*.csv
# ----------------------------------------------------------------------
_CACHE: Dict[str, pd.DataFrame] = {}


def load_csv(name: str, force: bool = False) -> pd.DataFrame:
    """Load a raw CSV with light caching.

    name: bare file name under data/raw_data, e.g. "consumption.csv"
    """
    if name in _CACHE and not force:
        return _CACHE[name]
    path = Path(config.DATA_RAW_DIR) / name
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    df = pd.read_csv(path)
    _CACHE[name] = df
    return df


def clear_cache() -> None:
    _CACHE.clear()


# ----------------------------------------------------------------------
# Misc
# ----------------------------------------------------------------------
def now_ms() -> int:
    return int(time.time() * 1000)


def now_iso() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def to_json(obj: Any) -> str:
    return json.dumps(safe_json(obj), ensure_ascii=False)
