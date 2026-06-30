#!/bin/bash
# Start hadoop-namenode + YARN + async Sqoop install.
# PID 1 runs the formater + starter + tail to keep container alive.
set +e

# Explicitly export PATH for non-login bash (PID 1 in container)
export JAVA_HOME=/opt/jdk8
export HADOOP_HOME=/opt/hadoop
export SQOOP_HOME=/opt/sqoop
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SQOOP_HOME/bin:$PATH

source /etc/profile.d/sqoop_env.sh 2>/dev/null || true

mkdir -p /data/dfs/name /var/log/hadoop
chmod -R o+rwx /data 2>/dev/null

# Clean up stale lock from previous crashes
rm -f /data/dfs/name/in_use.lock

if [ ! -f /data/dfs/name/.formatted ]; then
  hdfs namenode -format -force -nonInteractive && touch /data/dfs/name/.formatted
fi

# Launch namenode in background FIRST so container stays alive.
nohup hdfs namenode > /var/log/hadoop/namenode.log 2>&1 &
NAMENODE_PID=$!

# Wait briefly for namenode to bind port
sleep 5

# Launch YARN daemons in background.
if [ ! -f /var/run/hadoop-yarn/yarn-root-resourcemanager.pid ]; then
  yarn --daemon start resourcemanager 2>&1 || true
fi
if [ ! -f /var/run/hadoop-yarn/yarn-root-nodemanager.pid ]; then
  yarn --daemon start nodemanager 2>&1 || true
fi

# Run Sqoop install in background so container starts immediately.
if [ -x /opt/install-sqoop.sh ]; then
  nohup bash /opt/install-sqoop.sh > /var/log/hadoop/sqoop-install.log 2>&1 &
fi

# ==== HBase 预创建 /hbase 目录 ====
# 原因: HBase master 启动时调 MasterFileSystem.createInitialFileSystemLayout() 会在
#       HDFS 上创建 /hbase, 但如果 datanode 还没 join, setSafeMode() 会失败导致 master
#       进入 abort 循环. 显式预先 mkdir + chmod 777 可彻底规避.
echo "=== Pre-creating HDFS /hbase directory (waits for safe-mode off) ==="
for i in 1 2 3 4 5 6 7 8 9 10; do
  if hdfs dfsadmin -safemode wait 2>/dev/null; then
    if hdfs dfs -mkdir -p /hbase 2>/dev/null && hdfs dfs -chmod 777 /hbase 2>/dev/null; then
      echo "=== /hbase ready (attempt $i) ==="
      break
    fi
  fi
  echo "=== HDFS not ready, retry $i in 6s ==="
  sleep 6
done

# Keep PID 1 alive (NOT exec - we want to keep supervising)
echo "=== Starter finished, namenode PID=$NAMENODE_PID ==="
echo "=== Following namenode.log ==="
tail -F /var/log/hadoop/namenode.log