#!/bin/bash
# Start spark_server.py as a long-running daemon
cd /tmp/spark_jobs
export JAVA_HOME=/opt/java/openjdk
export PYSPARK_PYTHON=python3

# Kill any existing
pkill -9 -f spark_server.py 2>/dev/null
sleep 2

# Start with explicit nohup + disown
nohup python3 spark_server.py >> /tmp/srv.log 2>&1 &
PID=$!
disown $PID
echo "Started spark_server PID=$PID"
sleep 1
echo "Process status:"
ps -p $PID -o pid,state,cmd 2>&1 | head -3