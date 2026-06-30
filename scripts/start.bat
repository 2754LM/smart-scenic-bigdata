@echo off
REM Smart Scenic BigData - Start Platform
REM Double-click to run on Windows
cd /d "%~dp0\.."

echo ==========================================
echo   Smart Scenic BigData Platform - Starting
echo ==========================================
echo.

where docker >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker not found. Install Docker Desktop first.
    pause
    exit /b 1
)

docker info >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not running. Start Docker Desktop.
    pause
    exit /b 1
)

echo [1/5] Cleaning up...
docker compose down --remove-orphans 2>nul

echo [2/5] Starting MySQL + ZooKeeper...
docker compose up -d mysql zookeeper-1 zookeeper-2 zookeeper-3

echo.
echo [3/5] Waiting 90 seconds for MySQL init...
echo       This is needed only on first start.
echo.
ping -n 90 127.0.0.1 >nul

echo [4/5] Starting all remaining services...
docker compose up -d hadoop-namenode hadoop-datanode-1 hadoop-datanode-2 hive-server-1 hive-server-2 kafka-1 kafka-2 hbase-master hbase-regionserver-1 hbase-regionserver-2 spark-master spark-worker-1 demo-backend

echo.
echo [5/5] Waiting 30s for HBase + auto-init tables...
ping -n 30 127.0.0.1 >nul

echo.
echo ==========================================
echo   All containers started!
echo ==========================================
echo.
echo Frontend: http://localhost:8080
echo API docs: http://localhost:8000/docs
echo HBase UI: http://localhost:11610
echo.
echo Next: open http://localhost:8080 in your browser
echo.
echo Quick verification:
echo   docker exec hbase-master bash -c "echo list ^| hbase shell -n"
echo.

ping -n 10 127.0.0.1 >nul