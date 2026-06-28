#!/bin/bash
# Start hadoop-namenode + YARN + async Sqoop install.
# Mounted into the container and executed as PID 1.
set +e

source /etc/profile.d/sqoop_env.sh 2>/dev/null || true

mkdir -p /data/dfs/name /var/log/hadoop
chmod -R o+rwx /data 2>/dev/null

if [ ! -f /data/dfs/name/.formatted ]; then
  hdfs namenode -format -force -nonInteractive && touch /data/dfs/name/.formatted
fi

# Launch namenode in background FIRST so container stays alive.
nohup hdfs namenode > /var/log/hadoop/namenode.log 2>&1 &

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

# Keep PID 1 alive so container stays running.
exec tail -F /var/log/hadoop/namenode.log