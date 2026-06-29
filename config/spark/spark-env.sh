#!/bin/bash
export SPARK_MASTER_HOST=spark-master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export JAVA_HOME=/opt/java/openjdk
export PATH=$JAVA_HOME/bin:$PATH
# 注：apache/spark 镜像不允许在 *_OPTS 环境变量中设 -Xmx
# 内存通过 SPARK_WORKER_MEMORY 或 spark-defaults.conf 控制