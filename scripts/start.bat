@echo off
REM ============================================================
REM Smart Scenic BigData - One-click Start
REM Double-click to run on Windows
REM ============================================================
chcp 65001 >nul
cd /d "%~dp0\.."

echo ==========================================
echo   Smart Scenic BigData Platform - Starting
echo ==========================================
echo.

REM Check Docker
where docker >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker not found. Install Docker Desktop first.
    echo Press any key to close...
    pause >nul
    exit /b 1
)

docker info >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not running. Start Docker Desktop.
    echo Press any key to close...
    pause >nul
    exit /b 1
)

echo [0/4] Cleaning up previous containers...
docker compose down --remove-orphans 2>nul

echo [1/4] Starting MySQL + ZooKeeper (3 nodes)...
docker compose up -d mysql zookeeper-1 zookeeper-2 zookeeper-3

echo.
echo [2/4] Waiting for MySQL init (90 seconds)...
echo       This is needed because MySQL init scripts run on first start.
echo.
timeout /t 90 /nobreak >nul

echo [3/4] Starting Hadoop + Hive...
docker compose up -d hadoop-namenode hadoop-datanode-1 hadoop-datanode-2 hive-server-1 hive-server-2
timeout /t 10 /nobreak >nul

echo [4/4] Starting Kafka + HBase + Spark...
docker compose up -d kafka-1 kafka-2 hbase-master hbase-regionserver-1 hbase-regionserver-2 spark-master spark-worker-1

echo.
echo ==========================================
echo   All containers started!
echo ==========================================
echo.
echo Next steps:
echo   1. Wait 1-2 minutes for HBase to initialize
echo   2. Copy 4 CSV files to data\raw_data\:
echo        copy "D:\Desktop\选题与数据相关资料\数据集\Topic 18\*.csv" data\raw_data\
echo   3. Run scripts\start-app.bat to start Web app
echo.
echo Or open browser:
echo   http://localhost:8000/docs     - API documentation
echo   http://localhost:8080          - Web dashboard
echo.

REM Auto-close after 10 seconds (or press any key)
timeout /t 10 >nul
exit /b 0