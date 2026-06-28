#!/bin/bash
# Import all business tables from MySQL to HDFS via Sqoop.
# Idempotent: re-imports into the same target-dir overwriting previous data.
set -e

source /etc/profile.d/sqoop_env.sh 2>/dev/null || {
  export JAVA_HOME=/opt/jdk8
  export SQOOP_HOME=/opt/sqoop
  export HADOOP_HOME=/opt/hadoop
  export PATH=$JAVA_HOME/bin:$SQOOP_HOME/bin:$HADOOP_HOME/bin:$PATH
}

MYSQL_HOST="${MYSQL_HOST:-mysql}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_DB="${MYSQL_DB:-scenic}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASS="${MYSQL_PASS:-root123}"
HDFS_BASE="${HDFS_BASE:-/scenic/sqoop}"

hdfs dfs -mkdir -p "${HDFS_BASE}" 2>/dev/null || true

for t in t_scenic t_visitor t_consume t_visit t_review; do
  echo "=== Import ${t} ==="
  hdfs dfs -rm -r -f "${HDFS_BASE}/${t}" 2>/dev/null || true
  sqoop import \
    --connect "jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}" \
    --username "${MYSQL_USER}" --password "${MYSQL_PASS}" \
    --table "${t}" \
    --target-dir "${HDFS_BASE}/${t}" \
    --num-mappers 1 \
    --fields-terminated-by "," \
    --delete-target-dir \
    2>&1 | grep -E "(ERROR|records|Fatal|Error)" | head -3 || true
done

echo "=== HDFS Final List ==="
hdfs dfs -ls -R "${HDFS_BASE}/"