@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM ============================================================
REM Smart Scenic BigData - Quick Health Check
REM Usage: scripts\full-check.bat
REM
REM Checks:
REM   1. All 16 containers running
REM   2. Custom image contents (JDK, Sqoop, JDBC)
REM   3. Key service endpoints reachable
REM ============================================================

echo ============================================================
echo   Smart Scenic BigData - Quick Health Check
echo ============================================================
echo.

set FAILED=0

REM ---- Level 1: Container status ----
echo [1/3] Checking all 16 containers ...
set CONTAINERS=
for /f "tokens=*" %%c in ('docker ps --format "{{.Names}}"') do set CONTAINERS=!CONTAINERS! %%c
set EXPECTED=mysql zookeeper-1 zookeeper-2 zookeeper-3 hadoop-namenode hadoop-datanode-1 hadoop-datanode-2 hbase-master hbase-regionserver-1 hbase-regionserver-2 kafka-1 kafka-2 spark-master spark-worker-1 hive-server-1 hive-server-2
set MISSING=0
for %%e in (%EXPECTED%) do (
    echo !CONTAINERS! | findstr /i " %%e " >nul 2>nul
    if errorlevel 1 (
        echo       [MISSING] %%e
        set /a MISSING+=1
    )
)
if !MISSING! gtr 0 (
    echo       !MISSING! containers missing
    echo       Fix: scripts\start.bat
    set /a FAILED+=1
) else (
    echo       All 16 containers OK
)
echo.

REM ---- Level 2: Custom image contents ----
echo [2/3] Checking custom image contents ...
docker exec hadoop-namenode sh -c "test -x /opt/jdk8/bin/javac && test -x /opt/sqoop/bin/sqoop && test -f /opt/sqoop/lib/mysql-connector-java-8.0.33.jar && test -f /opt/sqoop/lib/commons-lang-2.6.jar" >nul 2> nul
if errorlevel 1 (
    echo       [FAIL] hadoop-namenode image missing JDK/Sqoop/JDBC
    echo       Fix: docker compose build hadoop-namenode
    set /a FAILED+=1
) else (
    echo       hadoop-namenode: JDK 8 + Sqoop 1.4.7 + JDBC OK
)

docker exec hive-server-1 sh -c "test -f /opt/hive/lib/mysql-connector-java-8.0.33.jar" >nul 2> nul
if errorlevel 1 (
    echo       [FAIL] hive image missing MySQL JDBC driver
    echo       Fix: docker compose build hive-server-1 hive-server-2
    set /a FAILED+=1
) else (
    echo       hive-server: MySQL JDBC driver OK
)
echo.

REM ---- Level 3: Key endpoints ----
echo [3/3] Checking key service endpoints ...
set ENDPOINTS=MySQL-13306 ZK-12181 HDFS-19870 YARN-18088 HBase-11610 Spark-18080 Kafka-19092 Hive-11010
for %%e in (%ENDPOINTS%) do (
    for /f "tokens=1,2 delims=-" %%a in ("%%e") do (
        set PORT=%%b
        powershell -Command "exit (Test-NetConnection -ComputerName localhost -Port %%b -InformationLevel Quiet -WarningAction SilentlyContinue).TcpTestSucceeded" 1>nul 2>nul
        if !errorlevel! equ 0 (
            echo       %%a:%%b OK
        ) else (
            echo       [FAIL] %%a:%%b
            set /a FAILED+=1
        )
    )
)
echo.

REM ---- Summary ----
echo ============================================================
echo   Summary: %FAILED% failure(s)
echo ============================================================
if %FAILED% equ 0 (
    echo   All quick checks PASSED
    echo.
    echo   For full validation, run separately:
    echo     scripts\verify.bat      - 38 connectivity tests
    echo     scripts\test-e2e.bat   - 7 business scenarios
) else (
    echo   Fix the failures above, then re-run this script.
)
echo ============================================================
pause