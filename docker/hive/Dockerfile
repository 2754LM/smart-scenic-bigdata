# Custom Hive 3.1.3 image with PostgreSQL JDBC driver.
# Hive connects to a separate PostgreSQL container for the metastore
# (DataNucleus 4.2 + MySQL 8.0 SQL incompatibility — use Postgres instead).
FROM apache/hive:3.1.3

# PostgreSQL JDBC driver (matches the PostgreSQL-based metastore approach
# used by big-data-europe/docker-hive — the officially recommended setup
# for DataNucleus 4.2 in Hive 3.x).
COPY postgres-jdbc.jar /opt/hive/lib/postgres-jdbc.jar

# Fix CRLF in Hive scripts (Windows Docker builds introduce \r in startup scripts)
USER root
RUN chmod 444 /opt/hive/lib/postgres-jdbc.jar && \
    find /opt/hive -name '*.sh' -type f -exec sh -c 'sed -i "s/\r$//" "$1"' _ {} \; 2>/dev/null || true
USER hive
