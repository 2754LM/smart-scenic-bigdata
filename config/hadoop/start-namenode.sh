#!/bin/bash
# Hadoop Namenode 首次启动：格式化 + 启动 NN + RM + HistoryServer

set -e

HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
NN_DIR="/tmp/hadoop-root/dfs/name"

echo "=========================================="
echo "  Initializing Hadoop Namenode"
echo "=========================================="

if [ ! -d "$NN_DIR" ] || [ -z "$(ls -A $NN_DIR 2>/dev/null)" ]; then
    echo "[1/4] Formatting namenode..."
    $HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive
else
    echo "[1/4] Namenode already formatted, skipping."
fi

echo "[2/4] Starting namenode..."
$HADOOP_HOME/bin/hdfs --daemon start namenode

echo "[3/4] Starting resourcemanager..."
$HADOOP_HOME/bin/yarn --daemon start resourcemanager

echo "[4/4] Starting mapred history server..."
$HADOOP_HOME/bin/mapred --daemon start historyserver

echo "=========================================="
echo "  Hadoop Namenode started!"
echo "=========================================="

# Keep container alive
tail -f $HADOOP_HOME/logs/*.log
