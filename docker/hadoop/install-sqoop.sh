#!/bin/bash
# Sqoop install script for apache/hadoop:3.3.6 image (CentOS 7 + JRE only)
# Idempotent: safe to re-run.
set -e

JAVA_ARCHIVE="/opt/jdk.tar.gz"
JDK_DIR="/opt/jdk8"
SQOOP_ARCHIVE="/opt/sqoop.tar.gz"
SQOOP_DIR="/opt/sqoop"
MYSQL_JAR_SRC="/tmp/mysql-connector-java-8.0.33.jar"
MYSQL_JAR_DST="${SQOOP_DIR}/lib/mysql-connector-java-8.0.33.jar"
COMMONS_LANG_SRC="/opt/hadoop/share/hadoop/yarn/timelineservice/lib/commons-lang-2.6.jar"
COMMONS_LANG_DST="${SQOOP_DIR}/lib/commons-lang-2.6.jar"

if [ -x "${JDK_DIR}/bin/javac" ]; then
  echo "[init] JDK already installed"
else
  if [ ! -f "${JAVA_ARCHIVE}" ]; then
    echo "[init] Downloading JDK 8u202 ..."
    curl --connect-timeout 10 --max-time 240 -fsSL \
      -o "${JAVA_ARCHIVE}" \
      "https://repo.huaweicloud.com/java/jdk/8u202-b08/jdk-8u202-linux-x64.tar.gz"
  fi
  echo "[init] Extracting JDK ..."
  cd /opt && tar -xzf "${JAVA_ARCHIVE}" && mv jdk1.8.0_* jdk8
fi

if [ -x "${SQOOP_DIR}/bin/sqoop" ]; then
  echo "[init] Sqoop already installed"
else
  if [ ! -f "${SQOOP_ARCHIVE}" ]; then
    echo "[init] Downloading Sqoop 1.4.7 ..."
    curl --connect-timeout 10 --max-time 240 -fsSL \
      -o "${SQOOP_ARCHIVE}" \
      "https://repo.huaweicloud.com/apache/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz"
  fi
  echo "[init] Extracting Sqoop ..."
  cd /opt && tar -xzf "${SQOOP_ARCHIVE}" && ln -sf sqoop-1.4.7.bin__hadoop-2.6.0 sqoop
fi

if [ ! -f "${MYSQL_JAR_DST}" ] && [ -f "${MYSQL_JAR_SRC}" ]; then
  echo "[init] Copying MySQL JDBC driver ..."
  cp "${MYSQL_JAR_SRC}" "${MYSQL_JAR_DST}"
fi

if [ ! -f "${COMMONS_LANG_DST}" ] && [ -f "${COMMONS_LANG_SRC}" ]; then
  echo "[init] Copying commons-lang 2.6 ..."
  cp "${COMMONS_LANG_SRC}" "${COMMONS_LANG_DST}"
fi

if ! grep -q "JAVA_HOME=/opt/jdk8" /etc/profile.d/sqoop_env.sh 2>/dev/null; then
  cat > /etc/profile.d/sqoop_env.sh <<'EOF'
export JAVA_HOME=/opt/jdk8
export SQOOP_HOME=/opt/sqoop
export HADOOP_HOME=/opt/hadoop
export PATH=$JAVA_HOME/bin:$SQOOP_HOME/bin:$HADOOP_HOME/bin:$PATH
EOF
  echo "[init] Created /etc/profile.d/sqoop_env.sh"
fi

echo "[init] DONE: JDK=$(test -x /opt/jdk8/bin/javac && echo OK || echo MISS) Sqoop=$(test -x /opt/sqoop/bin/sqoop && echo OK || echo MISS) MySQLJar=$(test -f ${MYSQL_JAR_DST} && echo OK || echo MISS) CommonsLang=$(test -f ${COMMONS_LANG_DST} && echo OK || echo MISS)"