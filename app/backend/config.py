"""
Smart Scenic BigData - Backend Configuration.

All connection settings live here so routers / services stay free of env literals.
Override anything by exporting env vars or editing this file once.
"""
import os
from pathlib import Path

# ----------------------------------------------------------------------
# Project layout
# ----------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BACKEND_DIR.parent.parent
DATA_RAW_DIR = PROJECT_DIR / "data" / "raw_data"
JOBS_DIR = PROJECT_DIR / "app" / "jobs"

# ----------------------------------------------------------------------
# MySQL (business DB)
# ----------------------------------------------------------------------
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "13306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root123")
MYSQL_DB = os.getenv("MYSQL_DB", "scenic")

MYSQL_CONFIG = {
    "host": MYSQL_HOST,
    "port": MYSQL_PORT,
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "database": MYSQL_DB,
    "charset": "utf8mb4",
    "cursorclass": None,  # filled in by service
}

# ----------------------------------------------------------------------
# Kafka
# ----------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19095")
KAFKA_TOPIC_REVIEW = os.getenv("KAFKA_TOPIC_REVIEW", "scenic_reviews")
KAFKA_TOPIC_EVENTS = os.getenv("KAFKA_TOPIC_EVENTS", "scenic_events")

# ----------------------------------------------------------------------
# HBase (use docker exec since happybase protocol is broken in our env)
# ----------------------------------------------------------------------
HBASE_CONTAINER = os.getenv("HBASE_CONTAINER", "hbase-master")
HBASE_TABLE_REVIEW = "scenic_reviews"
HBASE_TABLE_PROFILE = "scenic_profiles"
HBASE_TABLE_REALTIME = "scenic_realtime"
HBASE_HBASE_OK_TIMEOUT = int(os.getenv("HBASE_HBASE_OK_TIMEOUT", "3"))

# ----------------------------------------------------------------------
# Hadoop (Sqoop / HDFS)
# ----------------------------------------------------------------------
HADOOP_CONTAINER = os.getenv("HADOOP_CONTAINER", "hadoop-namenode")
HDFS_SQOOP_BASE = "/scenic/sqoop"
HDFS_CLEANED_BASE = "/scenic/cleaned"
HDFS_MODELS_BASE = "/scenic/models"
SPARK_CONTAINER = os.getenv("SPARK_CONTAINER", "spark-master")

# ----------------------------------------------------------------------
# Hive
# ----------------------------------------------------------------------
HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT = int(os.getenv("HIVE_PORT", "11010"))
HIVE_DB = os.getenv("HIVE_DB", "scenic_ext")

# ----------------------------------------------------------------------
# ML models
# ----------------------------------------------------------------------
MODELS_DIR = JOBS_DIR / "ml" / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------
# Server
# ----------------------------------------------------------------------
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", "8000"))
APP_TITLE = "Smart Scenic BigData API"
APP_VERSION = "2.0.0"

# CORS - wide open by default; restrict in production
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")
