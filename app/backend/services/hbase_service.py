"""
HBase service - shell out to `hbase shell` via Docker socket API
(uses services.admin_service._run_in_container for proper socket-based exec).

Why socket API? demo-backend image has no docker CLI installed; only the
docker socket is mounted. socket-based exec avoids the docker CLI requirement.

详见 README.md 第十节"设计权衡"。
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Optional

import config
from utils import hbase_shell, now_iso, now_ms
from services.admin_service import _run_in_container


def _exec(container: str, cmd: str, timeout: int = 30) -> str:
    """Run cmd inside container via Docker socket API (no docker CLI needed).
    Uses docker_client.exec_capture which captures stdout via streaming exec.
    """
    from services.docker_client import exec_capture
    r = exec_capture(container, ["sh", "-c", cmd], timeout=timeout)
    return r.get("stdout", "") or ""

log = logging.getLogger("smart-scenic.hbase")

# When Docker is **unavailable on host** (developer running IDE-only without
# docker daemon, e.g. test code on a fresh clone), fall back to a synthetic
# in-memory dataset so the front-end still has data to show.
# This is NOT a fallback for when HBase itself is down - in that case the
# docker-exec call returns an empty list and the API surfaces real empty data.
_SYN_DEMO: Optional[List[Dict[str, Any]]] = None


def _ensure_syn() -> List[Dict[str, Any]]:
    """Build a 50-row synthetic HBase-style dataset for demo / offline runs."""
    global _SYN_DEMO
    if _SYN_DEMO is not None:
        return _SYN_DEMO
    base_ts = now_ms() - 3_600_000
    items: List[Dict[str, Any]] = []
    for i in range(50):
        visitor_id = 1000 + i
        scenic_id = (i % 10) + 1
        ts = base_ts + i * 60_000
        items.append({
            "row_key": f"V{visitor_id:08d}",
            "visitor_id": str(visitor_id),
            "scenic_id": str(scenic_id),
            "action": "entry" if i % 2 == 0 else "exit",
            "ts": str(ts),
        })
    _SYN_DEMO = items
    return items


def _docker_available() -> bool:
    try:
        from services.docker_client import list_containers
        return bool(list_containers(all=False))
    except Exception:
        return False


def _parse_hbase_scan(out: str) -> List[Dict[str, Any]]:
    """Parse hbase shell `scan` output into [{row_key, cf, qualifier, value}]."""
    rows: List[Dict[str, Any]] = []
    for line in out.splitlines():
        line = line.strip()
        if not line or not line[0].isdigit() or " column=" not in line:
            continue
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
    return rows


# ----------------------------------------------------------------------
# Reviews: write + scan
# ----------------------------------------------------------------------
def write_review(scenic_id: str, visitor_id: str, rating: int, comment: str) -> Dict[str, Any]:
    """Write a review into scenic_reviews; create the table on first use."""
    ts = now_ms()
    row_key = f"{scenic_id}_{ts}"
    cmds = (
        'list_namespace_tables "default" 2>/dev/null\n'
        'exists "scenic_reviews"\n'
        f'put "scenic_reviews", "{row_key}", "cf:visitor_id", "{visitor_id}"\n'
        f'put "scenic_reviews", "{row_key}", "cf:rating", "{rating}"\n'
        f'put "scenic_reviews", "{row_key}", "cf:comment", "{comment}"\n'
    )
    out = hbase_shell(cmds)
    if "ERROR" in out and "exist" not in out.lower():
        create = (
            'create "scenic_reviews", "cf"\n'
            f'put "scenic_reviews", "{row_key}", "cf:visitor_id", "{visitor_id}"\n'
            f'put "scenic_reviews", "{row_key}", "cf:rating", "{rating}"\n'
            f'put "scenic_reviews", "{row_key}", "cf:comment", "{comment}"\n'
        )
        out = hbase_shell(create)
    return {"status": "ok", "row_key": row_key, "log": out[-500:]}


def list_reviews(scenic_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    cmds = f'scan "scenic_reviews", {{ROWPREFIXFILTER => "{scenic_id}", LIMIT => {limit}}}\n'
    out = hbase_shell(cmds)
    return _parse_hbase_scan(out)


# ----------------------------------------------------------------------
# HBase direct write API (used by demo-backend API; previously also by Kafka consumer before Kafka was removed)
# ----------------------------------------------------------------------
def put_review(visitor_id: str, attraction_id: str, rating: int, comment: str, ts: int = 0) -> Dict[str, Any]:
    """把一条评论消息写进 HBase scenic_reviews
    跟 write_review 一样，但 row_key 包含 visitor_id 便于前缀查询
    """
    if not ts:
        ts = now_ms()
    row_key = f"{attraction_id}_{ts}_{visitor_id}"
    create_check = (
        'exists "scenic_reviews"\n'
    )
    out = hbase_shell(create_check)
    if "does not exist" in out or "ERROR" in out:
        hbase_shell('create "scenic_reviews", "cf"\n')
    cmds = (
        f'put "scenic_reviews", "{row_key}", "cf:visitor_id", "{visitor_id}"\n'
        f'put "scenic_reviews", "{row_key}", "cf:attraction_id", "{attraction_id}"\n'
        f'put "scenic_reviews", "{row_key}", "cf:rating", "{rating}"\n'
        f'put "scenic_reviews", "{row_key}", "cf:comment", "{_escape(comment)}"\n'
        f'put "scenic_reviews", "{row_key}", "cf:ts", "{ts}"\n'
    )
    hbase_shell(cmds)
    return {"status": "ok", "row_key": row_key}


def put_realtime_event(visitor_id: str, attraction_id: str, event_type: str, ts: int = 0) -> Dict[str, Any]:
    """把一条实时事件写进 HBase scenic_realtime
    row_key: E{ts}_{visitor_id}_{attraction_id}（按时间倒序前缀查）
    """
    if not ts:
        ts = now_ms()
    row_key = f"E{ts}_{visitor_id}_{attraction_id}"
    create_check = 'exists "scenic_realtime"\n'
    out = hbase_shell(create_check)
    if "does not exist" in out or "ERROR" in out:
        hbase_shell('create "scenic_realtime", "cf"\n')
    cmds = (
        f'put "scenic_realtime", "{row_key}", "cf:visitor_id", "{visitor_id}"\n'
        f'put "scenic_realtime", "{row_key}", "cf:attraction_id", "{attraction_id}"\n'
        f'put "scenic_realtime", "{row_key}", "cf:event_type", "{event_type}"\n'
        f'put "scenic_realtime", "{row_key}", "cf:ts", "{ts}"\n'
        # 反向索引：按 attraction 查所有事件
        f'put "scenic_realtime", "A{attraction_id}_{ts}", "cf:event", "{event_type}:{visitor_id}"\n'
        f'put "scenic_realtime", "V{visitor_id}_{ts}", "cf:event", "{event_type}:{attraction_id}"\n'
    )
    hbase_shell(cmds)
    return {"status": "ok", "row_key": row_key}


def _escape(s: str) -> str:
    """Escape hbase shell string values."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


# ----------------------------------------------------------------------
# Realtime: visit-recent / visitor profile / attraction stat
# ----------------------------------------------------------------------
def recent_visits(limit: int = 20) -> List[Dict[str, Any]]:
    """Latest realtime events from scenic_realtime (key prefix V for visitor).

    Falls back to synthetic data when docker is unavailable.
    """
    if not _docker_available():
        return _ensure_syn()[:limit]
    cmds = (
        'exists "scenic_realtime"\n'
        f'scan "scenic_realtime", {{LIMIT => {limit * 5}}}\n'
    )
    out = hbase_shell(cmds)
    raw = _parse_hbase_scan(out)
    grouped: Dict[str, Dict[str, Any]] = {}
    for cell in raw:
        rk = cell["row_key"]
        grouped.setdefault(rk, {"row_key": rk})
        grouped[rk][cell["qualifier"]] = cell["value"]
    items: List[Dict[str, Any]] = []
    for rk, fields in grouped.items():
        items.append({
            "row_key": rk,
            "visitor_id": fields.get("visitor_id", ""),
            "scenic_id": fields.get("scenic_id", ""),
            "action": fields.get("action", ""),
            "ts": fields.get("ts", ""),
        })
    items.sort(key=lambda x: x["row_key"], reverse=True)
    return items[:limit]


def visitor_profile(visitor_id: int) -> Optional[Dict[str, Any]]:
    """All cells for a single visitor row key V{visitor_id}."""
    if not _docker_available():
        syn = [it for it in _ensure_syn() if it["visitor_id"] == str(visitor_id)]
        if not syn:
            return None
        return {
            "visitor_id": str(visitor_id),
            "total_visits": str(len(syn)),
            "last_attraction": syn[0]["scenic_id"],
            "last_visit_time": syn[0]["ts"],
            "recent_actions": syn[:5],
        }
    rk = f"V{visitor_id:08d}"
    cmds = f'scan "scenic_realtime", {{ROWPREFIXFILTER => "{rk}", LIMIT => 100}}\n'
    out = hbase_shell(cmds)
    cells = _parse_hbase_scan(out)
    if not cells:
        return None
    fields: Dict[str, Any] = {"visitor_id": str(visitor_id)}
    for c in cells:
        fields[c["qualifier"]] = c["value"]
    recent: List[Dict[str, Any]] = []
    for c in cells:
        if c["cf"] == "cf" and c["qualifier"].startswith("attr"):
            recent.append({"scenic_id": c["value"]})
        elif c["cf"] == "cf" and c["qualifier"].startswith("ts"):
            if recent:
                recent[-1]["ts"] = c["value"]
    return {
        "visitor_id": str(visitor_id),
        "total_visits": fields.get("total_visits", "0"),
        "last_attraction": fields.get("last_attr"),
        "last_visit_time": fields.get("last_ts"),
        "recent_actions": recent,
    }


def attraction_stat(scenic_id: int) -> Optional[Dict[str, Any]]:
    """Count visitors and recent activity for an attraction A{scenic_id}."""
    if not _docker_available():
        syn = [it for it in _ensure_syn() if it["scenic_id"] == str(scenic_id)]
        return {
            "scenic_id": str(scenic_id),
            "visitor_count": len({it["visitor_id"] for it in syn}),
            "last_24h_visits": min(len(syn), 24),
            "last_visit_time": syn[0]["ts"] if syn else None,
        }
    rk_prefix = f"A{scenic_id:04d}"
    cmds = (
        'exists "scenic_realtime"\n'
        f'scan "scenic_realtime", {{ROWPREFIXFILTER => "{rk_prefix}", LIMIT => 200}}\n'
    )
    out = hbase_shell(cmds)
    cells = _parse_hbase_scan(out)
    if not cells:
        return {
            "scenic_id": str(scenic_id),
            "visitor_count": 0,
            "last_24h_visits": 0,
            "last_visit_time": None,
            "_note": "no data in HBase",
        }
    visitor_ids = set()
    last_ts: Optional[str] = None
    for c in cells:
        if c["qualifier"] == "visitor_id":
            visitor_ids.add(c["value"])
        if c["qualifier"] == "last_ts" and (not last_ts or c["value"] > last_ts):
            last_ts = c["value"]
    return {
        "scenic_id": str(scenic_id),
        "visitor_count": len(visitor_ids),
        "last_24h_visits": min(len(visitor_ids), 24),
        "last_visit_time": last_ts,
    }


# ----------------------------------------------------------------------
# 一键初始化: 创建业务表 + 注入 demo 数据
# 由 demo-backend 的 on_startup 钩子调用, 保证 HBase 可用即可见
# ----------------------------------------------------------------------
REQUIRED_TABLES = ["scenic_realtime", "scenic_reviews"]


def init_tables() -> Dict[str, Any]:
    """确保 2 张业务表存在；不存在则创建。幂等。"""
    if not _docker_available():
        return {"status": "skipped", "reason": "docker not available"}
    created = []
    exists = []
    for table in REQUIRED_TABLES:
        out = hbase_shell(f'exists "{table}"')
        if "does exist" in out:
            exists.append(table)
        else:
            create_out = hbase_shell(f'create "{table}", "cf"')
            if "AlreadyExists" in create_out or "already exists" in create_out:
                exists.append(table)
            elif "ERROR" not in create_out and "Exception" not in create_out:
                created.append(table)
            else:
                log.warning("init_tables: table %s error: %s", table, create_out[:200])
                exists.append(table)
    return {"status": "ok", "created": created, "exists": exists}


# ----------------------------------------------------------------------
# Demo seeding: write a few rows so the page has something to show
# when HBase is empty.
# ----------------------------------------------------------------------
def clear_realtime_table() -> int:
    """清空 scenic_realtime 表的所有行，返回删除行数（演示用）"""
    if not _docker_available():
        return 0
    # 先 scan 出所有 row key
    scan_out = hbase_shell('scan "scenic_realtime"')
    cells = _parse_hbase_scan(scan_out)
    if not cells:
        return 0
    row_keys = sorted({c["row_key"] for c in cells})
    # 分批删除（避免命令过长）
    deleted = 0
    for rk in row_keys:
        try:
            hbase_shell(f'deleteall "scenic_realtime", "{rk}"')
            deleted += 1
        except Exception:
            pass
    return deleted


def seed_if_empty() -> bool:
    """If scenic_realtime is empty, write 30 demo rows so front-end isn't blank.

    Returns True if seeded, False if HBase already has data or is unavailable.
    """
    if not _docker_available():
        return False
    # Check if table exists first
    exists_out = hbase_shell('exists "scenic_realtime"')
    if "does not exist" in exists_out or "ERROR" in exists_out.lower():
        hbase_shell('create "scenic_realtime", "cf"')
    # Check if table already has data
    scan_out = hbase_shell('scan "scenic_realtime", {LIMIT => 1}')
    if "column=" in scan_out:
        return False  # already has data
    # Seed 30 demo rows
    seed_cmds = []
    for i in range(30):
        visitor_id = 1000 + i
        scenic_id = (i % 10) + 1
        ts = now_ms() - i * 60_000
        seed_cmds.append(f'put "scenic_realtime", "V{visitor_id:08d}", "cf:total_visits", "{i + 1}"')
        seed_cmds.append(f'put "scenic_realtime", "V{visitor_id:08d}", "cf:last_attr", "{scenic_id}"')
        seed_cmds.append(f'put "scenic_realtime", "V{visitor_id:08d}", "cf:last_ts", "{ts}"')
        seed_cmds.append(f'put "scenic_realtime", "A{scenic_id:04d}", "cf:visitor_id", "{visitor_id}"')
        seed_cmds.append(f'put "scenic_realtime", "A{scenic_id:04d}", "cf:last_ts", "{ts}"')
    hbase_shell("\n".join(seed_cmds))
    return True
