#!/bin/bash
# ============================================================
# Import all business tables from MySQL to HDFS via Sqoop.
# Schema: 4 tables with Chinese field names (matches 01-init-business.sql)
# Idempotent: re-imports into the same target-dir overwriting previous data.
#
# Run inside hadoop-namenode container:
#   docker exec hadoop-namenode bash /opt/jobs/sqoop-import-mysql.sh
# ============================================================
set -e

# Explicit PATH (non-login bash)
export JAVA_HOME=/opt/jdk8
export SQOOP_HOME=/opt/sqoop
export HADOOP_HOME=/opt/hadoop
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SQOOP_HOME/bin:$PATH

source /etc/profile.d/sqoop_env.sh 2>/dev/null || true

MYSQL_HOST="${MYSQL_HOST:-mysql}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_DB="${MYSQL_DB:-scenic}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASS="${MYSQL_PASS:-root123}"
HDFS_BASE="${HDFS_BASE:-/scenic/sqoop}"

# Tables to import (matches new 4-table schema)
TABLES=(
  "t_attraction"
  "t_visitor"
  "t_consumption"
  "t_visit_record"
)

echo "=== Pre-check: HDFS base dir ==="
hdfs dfs -mkdir -p "${HDFS_BASE}" 2>/dev/null || true
hdfs dfs -ls "${HDFS_BASE}/" 2>/dev/null | head -5 || true

# Clean previous output to keep idempotent
echo "=== Clean previous HDFS output ==="
for t in "${TABLES[@]}"; do
  hdfs dfs -rm -r -f "${HDFS_BASE}/${t}" 2>/dev/null || true
done

for t in "${TABLES[@]}"; do
  echo ""
  echo "=== Import ${t} ==="

  # Use single mapper (data sizes don't justify parallelism; keeps order)
  # --as-textfile so Spark can read with .csv format easily
  sqoop import \
    --connect "jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=UTC" \
    --username "${MYSQL_USER}" --password "${MYSQL_PASS}" \
    --table "${t}" \
    --target-dir "${HDFS_BASE}/${t}" \
    --num-mappers 1 \
    --fields-terminated-by "," \
    --lines-terminated-by "\n" \
    --input-null-string "\\N" \
    --input-null-non-string "\\N" \
    --null-string "\\N" \
    --null-non-string "\\N" \
    --delete-target-dir \
    --as-textfile \
    2>&1 | grep -E "(ERROR|Fatal|Error|records)" | head -5 || true

  # Verify
  ROWS=$(hdfs dfs -cat "${HDFS_BASE}/${t}/part-m-00000" 2>/dev/null | wc -l || echo 0)
  echo "  --> ${ROWS} rows written to ${HDFS_BASE}/${t}/"
done

echo ""
echo "=== HDFS Final List ==="
hdfs dfs -ls -R "${HDFS_BASE}/"
echo "=== Sqoop import done ==="
