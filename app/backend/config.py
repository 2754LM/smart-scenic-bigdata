"""
Backend configuration. All env-driven so the same code works
on local dev and remote VM.
"""
import os
from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # App
    APP_NAME: str = "Smart Scenic Backend API"
    APP_VERSION: str = "2.0.0"
    DEBUG: bool = False
    CORS_ORIGINS: list = ["*"]

    # MySQL
    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 13306
    MYSQL_USER: str = "root"
    MYSQL_PASS: str = "root123"
    MYSQL_DB:   str = "scenic"

    # HiveServer2 (Thrift)
    HIVE_HOST: str = "localhost"
    HIVE_PORT: int = 10000
    HIVE_DB:   str = "scenic_dw"

    # HBase Stargate REST (optional)
    HBASE_REST_URL: str = "http://localhost:8085"
    HBASE_TABLE_VISIT_RT: str = "scenic_visit_rt"
    HBASE_TABLE_VISITOR_PROFILE: str = "scenic_visitor_profile"
    HBASE_TABLE_ATTRACTION_STAT: str = "scenic_attraction_stat"

    # HDFS WebHDFS
    HDFS_NAMENODE: str = "localhost"
    HDFS_PORT: int = 9870
    HDFS_USER: str = "root"

    # ML model paths on HDFS (from P1)
    HDFS_MODEL_BASE: str = "/scenic/ml/models"
    HDFS_ML_PRED:    str = "/scenic/ml/predictions"

    # If True, services fall back to mock data when connection fails
    # (useful for local dev without full platform running)
    ENABLE_MOCK_FALLBACK: bool = True

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
