@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM ============================================================
REM Smart Scenic BigData - One-click Start (Windows CMD)
REM Usage: scripts\start.bat
REM ============================================================

cd /d "%~dp0\.."

echo ==========================================
echo   Smart Scenic BigData Platform - Starting
echo ==========================================

REM ---- Check Docker ----
where docker >nul 2>nul
if errorlevel 1 (
    echo [ERROR] docker not found. Please install Docker Desktop.
    pause
    exit /b 1
)

docker info >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Docker is not running. Please start Docker Desktop.
    pause
    exit /b 1
)

REM ---- Check Docker memory ----
for /f "delims=" %%i in ('docker info --format "{{.MemTotal}}" 2^>nul') do set DOCKER_MEM=%%i
if "%DOCKER_MEM%"=="" set DOCKER_MEM=0
set /a DOCKER_MEM_GB=DOCKER_MEM/1073741824
echo [INFO] Docker memory: %DOCKER_MEM_GB% GB (recommended: 18 GB+)

REM ---- Clean up ----
echo [0/4] Cleaning up previous containers...
docker compose down --remove-orphans 2>nul
docker rm -f mysql zookeeper-1 zookeeper-2 zookeeper-3 hadoop-namenode hadoop-datanode-1 hadoop-datanode-2 hbase-master hbase-regionserver-1 hbase-regionserver-2 kafka-1 kafka-2 spark-master spark-worker-1 hive-server-1 hive-server-2 2>nul

REM ---- Stage 1: Core services ----
echo [1/4] Stage 1: Starting CORE services (mysql, zookeeper, hadoop, hive)...
docker compose up -d mysql zookeeper-1 zookeeper-2 zookeeper-3

echo [2/4] Waiting for MySQL to be ready...
set MYSQL_READY=0
for /l %%i in (1,1,30) do (
    docker exec mysql mysqladmin ping -uroot -proot123 >nul 2>nul
    if !errorlevel! equ 0 (
        echo       MySQL is ready [OK]
        set MYSQL_READY=1
        goto :mysql_ok
    )
    echo       Waiting for MySQL... [%%i/30]
    timeout /t 3 /nobreak >nul
)

:mysql_ok
if "%MYSQL_READY%"=="0" (
    echo [WARN] MySQL not ready after 30 retries. Continuing...
)

echo [3/4] Starting Hadoop + Hive...
docker compose up -d hadoop-namenode hadoop-datanode-1 hadoop-datanode-2
timeout /t 5 /nobreak >nul
docker compose up -d hive-server-1 hive-server-2

REM ---- Stage 2: Extension services (may fail on first run) ----
echo [4/4] Stage 2: Starting EXTENSION services (kafka, hbase, spark)...
docker compose up -d kafka-1 kafka-2 2>nul
if errorlevel 1 (
    echo [WARN] Kafka images not ready yet. Skip. Run later: docker compose up -d kafka-1 kafka-2
)
docker compose up -d hbase-master hbase-regionserver-1 hbase-regionserver-2 2>nul
if errorlevel 1 (
    echo [WARN] HBase images not ready yet. Skip. Run later: docker compose up -d hbase-master hbase-regionserver-1 hbase-regionserver-2
)
docker compose up -d spark-master spark-worker-1 2>nul
if errorlevel 1 (
    echo [WARN] Spark images not ready yet. Skip. Run later: docker compose up -d spark-master spark-worker-1
)

REM ---- Stage 3: Sqoop is pre-installed in custom hadoop image, just trigger import ----
echo [5/5] Triggering Sqoop MySQL - HDFS import for 5 business tables...
timeout /t 10 /nobreak >nul
docker exec hadoop-namenode bash /opt/jobs/sqoop-import-mysql.sh >nul 2> nul
if errorlevel 1 (
    echo [WARN] Sqoop import failed. Run later: docker exec hadoop-namenode bash /opt/jobs/sqoop-import-mysql.sh
)

echo.
echo ==========================================
echo   Startup complete
echo ==========================================
echo.
echo Run verify:  scripts\verify.bat
echo Run e2e:     scripts\test-e2e.bat
echo If some services are missing, run:
echo   docker compose up -d [service-name]
echo.
echo Status: docker compose ps
echo Logs:   docker compose logs -f [service-name]
echo ==========================================
pause