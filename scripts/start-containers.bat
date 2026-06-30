@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

REM ============================================================
REM  Smart Scenic BigData Platform - Start Containers Only
REM  ----------------------------------------------------------
REM  Starts 15 Docker containers (Hadoop, Spark, HBase, Hive, MySQL)
REM  Does NOT load data, train models, or run pipelines.
REM  After this script exits:
REM    - Visit http://localhost:8080/manage.html -> System tab
REM      -> click "one-click init" to load data + train models
REM    - Or run scripts\start-app.bat to do it from CLI.
REM
REM  Doubles as a clean restart: docker compose down first so no
REM  leftover containers stay in the stack.
REM ============================================================

cd /d "%~dp0\.."

echo(
echo ==========================================================
echo   Smart Scenic BigData - Starting containers
echo ==========================================================
echo(

REM ---------- 0. Pre-flight checks ----------
where docker >nul 2>&1
if errorlevel 1 (
    echo [FAIL] Docker not found. Install Docker Desktop first.
    pause
    exit /b 1
)
docker info >nul 2>&1
if errorlevel 1 (
    echo [FAIL] Docker is not running. Start Docker Desktop.
    pause
    exit /b 1
)
echo [OK]   Docker ready.
echo(

REM ---------- 1. Clean previous run ----------
echo [1/4] Removing any leftover containers (data volumes preserved)...
docker compose down --remove-orphans 2>nul
echo(

REM ---------- 2. Start foundation (mysql + zookeeper) ----------
echo [2/4] Starting MySQL 5.7 + ZooKeeper (3-node)...
docker compose up -d mysql zookeeper-1 zookeeper-2 zookeeper-3
if errorlevel 1 (
    echo [FAIL] MySQL / ZK failed to start. Check: docker compose logs mysql
    pause
    exit /b 1
)
echo [OK]   MySQL + ZK starting...
echo(

REM ---------- 3. Start big-data + app stack ----------
echo [3/4] Starting Hadoop / Spark / HBase / Hive / demo-backend (11 services)...
docker compose up -d ^
    hadoop-namenode ^
    hadoop-datanode-1 hadoop-datanode-2 ^
    spark-master spark-worker-1 ^
    hbase-master hbase-regionserver-1 hbase-regionserver-2 ^
    hive-server-1 hive-server-2 ^
    demo-backend
if errorlevel 1 (
    echo [FAIL] Stack start failed. Run: docker compose logs
    pause
    exit /b 1
)
echo [OK]   All 15 containers up.
echo(

REM ---------- 4. Wait for all 15 containers to be Up ----------
echo [4/4] Waiting for all 15 containers to reach "Up" state (up to 4 min)...
set ALL_OK=0
for /l %%i in (1,1,120) do (
    set UP_COUNT=0
    REM Count "Up" containers via docker ps (faster than docker compose ps)
    for /f %%n in ('docker ps --filter "status=running" --format "{{.Names}}" 2^>nul') do (
        for %%c in (%%n) do set /a UP_COUNT+=1
    )
    if !UP_COUNT! geq 15 (
        set ALL_OK=1
        goto :containers_ready
    )
    set /a SECS=%%i*2
    <nul set /p "=        up=!UP_COUNT!/15  ...!SECS!s  " 2>nul
    timeout /t 2 /nobreak >nul
)
echo(
echo [WARN] Only !UP_COUNT!/15 containers reached Up. Check: docker compose ps
goto :after_ready

:containers_ready
echo(   15/15 containers up.
:after_ready
echo(

REM ---------- Final summary ----------
echo ==========================================================
echo   Container stack ready.
echo ==========================================================
echo(
echo   Frontend:        http://localhost:8080
echo   API docs:        http://localhost:8000/docs
echo   HBase Web UI:    http://localhost:11610
echo   Spark Web UI:    http://localhost:18080
echo   Hadoop NN UI:    http://localhost:19870
echo   HiveServer2 1:   http://localhost:11010  (beeline host)
echo   HiveServer2 2:   http://localhost:11011
echo(
echo   Container status:
for /f "tokens=*" %%c in ('docker compose ps --format "{{.Name}}\t{{.Status}}" 2^>nul') do (
    echo     %%c
)
echo(
echo Next steps:
echo   1. Open http://localhost:8080/manage.html in browser
echo   2. Go to "System" tab -^> click "one-click init"
echo      (CSV -^> MySQL -^> Sqoop -^> Spark clean -^> Hive DDL -^> train)
echo   3. Or run scripts\start-app.bat to do it from CLI.
echo(
echo To stop containers (data preserved): scripts\stop.bat
echo To reset all (delete volumes + local data):  scripts\reset.bat
echo(
pause
