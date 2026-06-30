#!/bin/sh
# Spark Master container entrypoint.
# - Set up /shared volume with permissive perms for spark user (uid 185) to write models.
# - Start Spark Master as PID 1 (foreground) so container stays alive.

set +e

# /shared volume permissions
chmod -R 1777 /shared 2>&1 | head -3 || true

[ -d /shared/models ] || mkdir -p /shared/models/sklearn
chmod 1777 /shared/models /shared/models/sklearn 2>&1 || true

mkdir -p /tmp/spark-events
chmod 1777 /tmp/spark-events

ls -la /shared/

# Run Spark Master FOREGROUND. This replaces this shell with the Master JVM,
# keeping it as PID 1 so the container stays alive.
exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --host spark-master \
    --port 7077 \
    --webui-port 8080
