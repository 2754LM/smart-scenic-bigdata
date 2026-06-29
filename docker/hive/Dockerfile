# Custom Hive 3.1.3 image with MySQL JDBC driver baked in.
# Replaces the bind-mount approach (./lib/mysql-connector-java-8.0.33.jar).
FROM apache/hive:3.1.3

# MySQL JDBC driver - Hive connects to MySQL metastore
COPY mysql-connector-java-8.0.33.jar /opt/hive/lib/mysql-connector-java-8.0.33.jar

USER root
RUN chmod 444 /opt/hive/lib/mysql-connector-java-8.0.33.jar
USER hive