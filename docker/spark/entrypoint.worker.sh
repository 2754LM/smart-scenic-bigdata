#!/bin/sh
# Spark Worker container entrypoint.
set +e

chmod -R 1777 /shared 2>&1 | head -3 || true
mkdir -p /tmp/spark-events
chmod 1777 /tmp/spark-events

# Wait for master to be reachable before starting worker
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
    if bash -c "(echo > /dev/tcp/spark-master/7077)" 2>/dev/null; then
        echo "[worker] spark-master:7077 reachable"
        break
    fi
    echo "[worker] waiting for spark-master... $i"
    sleep 4
done

# Run Spark Worker FOREGROUND
exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    spark://spark-master:7077 \
    --webui-port 8081 \
    --cores 2 \
    --memory 1g
