"""
/api/admin/* - 系统管理与操作触发

提供：
  GET  /api/admin/status              - 综合状态
  GET  /api/admin/containers          - 17 容器状态
  GET  /api/admin/models              - 已训练模型
  GET  /api/admin/datasets            - 数据集状态
  GET  /api/admin/hdfs                - HDFS 文件
  GET  /api/admin/jobs                - 异步任务列表
  GET  /api/admin/jobs/{id}           - 单个任务详情
  POST /api/admin/actions/{name}      - 触发单个操作
  POST /api/admin/pipeline            - 触发 pipeline
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional

import services.admin_service as admin

router = APIRouter(prefix="/api/admin", tags=["admin"])


@router.get("/status")
def status():
    """综合状态 - 前端 dashboard 一次拉全部"""
    return admin.get_system_status()


@router.get("/containers")
def containers():
    return {"containers": admin.get_containers_status()}


@router.get("/models")
def models():
    return admin.get_models_status()


@router.get("/datasets")
def datasets():
    return admin.get_datasets_status()


@router.get("/hdfs")
def hdfs():
    return admin.get_hdfs_status()


@router.get("/jobs")
def jobs(limit: int = 20):
    return {"jobs": [j.to_dict() for j in admin.list_jobs(limit=limit)]}


@router.get("/jobs/{job_id}")
def job_detail(job_id: str):
    j = admin.get_job(job_id)
    if j is None:
        raise HTTPException(404, f"job {job_id} not found")
    return j.to_dict()


@router.post("/actions/{name}")
def trigger_action(name: str):
    """触发一个操作（异步，立即返回 job_id）"""
    try:
        job = admin.trigger_action(name)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {
        "status": "accepted",
        "job_id": job.id,
        "name":   job.name,
        "kind":   job.kind,
        "poll_url": f"/api/admin/jobs/{job.id}",
    }


class PipelineIn(BaseModel):
    actions: List[str] = ["load_csv", "sqoop", "hive_ddl", "spark_train"]


@router.post("/pipeline")
def trigger_pipeline(req: PipelineIn):
    """触发一个 pipeline（多个操作顺序执行）"""
    if not req.actions:
        raise HTTPException(400, "actions must not be empty")
    try:
        job = admin.trigger_pipeline(req.actions)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {
        "status": "accepted",
        "job_id": job.id,
        "name":   job.name,
        "actions": req.actions,
        "poll_url": f"/api/admin/jobs/{job.id}",
    }


@router.get("/actions")
def list_actions():
    """列出所有可触发的操作"""
    return {
        "actions": [
            {"name": k, "label": v[0]}
            for k, v in admin.ACTIONS.items()
        ],
    }