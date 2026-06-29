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

echo [1/4] Cleaning up...
docker compose down --remove-orphans 2>nul

echo [2/4] Starting MySQL + ZooKeeper...
docker compose up -d mysql zookeeper-1 zookeeper-2 zookeeper-3

echo.
echo [3/4] Waiting 90 seconds for MySQL init...
echo       This is needed only on first start.
echo.
ping -n 90 127.0.0.1 >nul

echo [4/4] Starting all remaining services...
docker compose up -d hadoop-namenode hadoop-datanode-1 hadoop-datanode-2 hive-server-1 hive-server-2 kafka-1 kafka-2 hbase-master hbase-regionserver-1 hbase-regionserver-2 spark-master spark-worker-1

echo.
echo ==========================================
echo   All containers started!
echo ==========================================
echo.
echo Next: double-click scripts\start-app.bat
echo Or: http://localhost:8000/docs
echo.

ping -n 15 127.0.0.1 >nul