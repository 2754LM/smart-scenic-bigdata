# Custom Hadoop 3.3.6 image with Sqoop 1.4.7 + JDK 1.8 + starter scripts baked in.
# Replaces the bind-mount approach (./services, ./jobs, ./lib).
#
# This image is built on first `docker compose up`. To rebuild after script changes:
#   docker compose build hadoop-namenode
#
# Layers:
#   1. Base apache/hadoop:3.3.6 (CentOS 7 + JRE only)
#   2. Download + install JDK 1.8.0_202 (required by Sqoop to compile Java code)
#   3. Download + install Sqoop 1.4.7 (uses Huawei Cloud mirror for reliability)
#   4. Copy MySQL JDBC driver (for Sqoop to talk to MySQL)
#   5. Copy commons-lang 2.6 (fixes NoClassDefFoundError)
#   6. Copy starter + import scripts
#
# Build size: ~700MB (vs apache/hadoop:3.3.6 base ~600MB)

FROM apache/hadoop:3.3.6

USER root

# ---- Step 1: Install JDK 1.8 (apache/hadoop only ships JRE) ----
RUN cd /opt && \
    curl --connect-timeout 10 --max-time 600 -fsSL \
      -o jdk.tar.gz \
      "https://repo.huaweicloud.com/java/jdk/8u202-b08/jdk-8u202-linux-x64.tar.gz" && \
    tar -xzf jdk.tar.gz && \
    mv jdk1.8.0_202 jdk8 && \
    rm jdk.tar.gz

# ---- Step 2: Install Sqoop 1.4.7 ----
RUN cd /opt && \
    curl --connect-timeout 10 --max-time 300 -fsSL \
      -o sqoop.tar.gz \
      "https://repo.huaweicloud.com/apache/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz" && \
    tar -xzf sqoop.tar.gz && \
    ln -sf sqoop-1.4.7.bin__hadoop-2.6.0 sqoop && \
    rm sqoop.tar.gz

# ---- Step 3: Copy MySQL JDBC driver + commons-lang 2.6 (fixes NoClassDefFoundError) ----
COPY mysql-connector-java-8.0.33.jar /opt/sqoop/lib/mysql-connector-java-8.0.33.jar
COPY commons-lang-2.6.jar /opt/sqoop/lib/commons-lang-2.6.jar

# ---- Step 4: Copy scripts ----
COPY starter.sh /opt/starter.sh
COPY install-sqoop.sh /opt/install-sqoop.sh
COPY sqoop-import-mysql.sh /opt/jobs/sqoop-import-mysql.sh
RUN chmod +x /opt/starter.sh /opt/install-sqoop.sh /opt/jobs/sqoop-import-mysql.sh && \
    mkdir -p /opt/jobs

# ---- Step 5: Set up environment ----
RUN cat > /etc/profile.d/sqoop_env.sh <<'EOF'
export JAVA_HOME=/opt/jdk8
export SQOOP_HOME=/opt/sqoop
export HADOOP_HOME=/opt/hadoop
export PATH=$JAVA_HOME/bin:$SQOOP_HOME/bin:$HADOOP_HOME/bin:$PATH
EOF

USER hadoop