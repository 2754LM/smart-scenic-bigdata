# Custom Hive 3.1.3 image with MySQL 5.x JDBC driver.
# ------------------------------------------------------------
# Hive ships with a stub JDBC driver that doesn't work for MySQL.
# We add mysql-connector-java 5.1.49 (matches Hive 3.1.3's tested matrix).
# MySQL 5.7 is used for both the business schema and the Hive Metastore DB
# because DataNucleus 4.2 emits DDL with the MySQL 5.x-compatible
# "DEFAULT CHARACTER SET charset_name" syntax which MySQL 8.0 rejects.
FROM apache/hive:3.1.3

# MySQL JDBC driver - Hive connects to MySQL 5.7 metastore
COPY mysql-connector-java-5.1.49.jar /opt/hive/lib/mysql-connector-java-5.1.49.jar

# Fix CRLF in Hive scripts (Windows Docker builds introduce \r in startup scripts)
# Also: write Hadoop core-site.xml + mapred-site.xml so MR jobs in the local
# runner can talk to the namenode (otherwise fs.defaultFS defaults to file:///
# and GROUP BY queries fail with "no HDFS data" / OOM in the reduce phase).
USER root
RUN chmod 444 /opt/hive/lib/mysql-connector-java-5.1.49.jar && \
    find /opt/hive -name '*.sh' -type f -exec sh -c 'sed -i "s/\r$//" "$1"' _ {} \; 2>/dev/null || true && \
    cat > /opt/hadoop/etc/hadoop/core-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hadoop-namenode:9000</value>
    </property>
</configuration>
EOF
    cat > /opt/hadoop/etc/hadoop/mapred-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>local</value>
    </property>
    <property>
        <name>mapreduce.jobtracker.address</name>
        <value>local</value>
    </property>
</configuration>
EOF
USER hive
