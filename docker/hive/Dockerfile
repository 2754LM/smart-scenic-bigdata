# Custom Hive 3.1.3 image with MySQL JDBC driver baked in.
# Replaces the bind-mount approach (./lib/mysql-connector-java-8.0.33.jar).
FROM apache/hive:3.1.3

# MySQL JDBC driver - Hive connects to MySQL metastore
COPY mysql-connector-java-8.0.33.jar /opt/hive/lib/mysql-connector-java-8.0.33.jar

# Fix CRLF in Hive scripts (Windows Docker builds introduce \r in startup scripts)
USER root
RUN chmod 444 /opt/hive/lib/mysql-connector-java-8.0.33.jar && \
    find /opt/hive -name '*.sh' -type f -exec sh -c 'sed -i "s/\r$//" "$1"' _ {} \; 2>/dev/null || true
USER hive