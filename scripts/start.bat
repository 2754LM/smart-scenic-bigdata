@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

REM ============================================================
REM  Smart Scenic BigData Platform - One-Click Start
REM  19-container Docker stack (Hadoop HA + Spark + HBase + Kafka + Hive)
REM  Double-click on Windows. Auto-waits for each component ready.
REM ============================================================

cd /d "%~dp0\.."

echo(
echo ==========================================================
echo   Smart Scenic BigData - Starting
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

REM ---------- 1. Stop any leftover ----------
echo [1/6] Stopping any leftover containers (data preserved)...
docker compose down --remove-orphans 2>nul
echo(

REM ---------- 2. Start MySQL + ZK first (foundation) ----------
echo [2/6] Starting MySQL + ZooKeeper (3-node ensemble)...
docker compose up -d mysql zookeeper-1 zookeeper-2 zookeeper-3
if errorlevel 1 (
    echo [FAIL] Failed to start MySQL + ZK. Check 'docker compose logs mysql'.
    pause
    exit /b 1
)
echo [OK]   MySQL + ZK started.
echo(

REM ---------- 3. Wait for MySQL healthy ----------
echo [3/6] Waiting for MySQL (up to 90s for first-time init)...
set MYSQL_OK=0
for /l %%i in (1,1,45) do (
    for /f %%s in ('docker inspect --format="{{.State.Health.Status}}" mysql 2^>nul') do set STATUS=%%s
    if "!STATUS!"=="healthy" (
        set MYSQL_OK=1
        echo [OK]   MySQL healthy.
        goto :mysql_done
    )
    set /a WAIT=%%i*2
    <nul set /p "=        ...!WAIT!s / 90s  " 2>nul
    timeout /t 2 /nobreak >nul
)
echo(
echo [WARN] MySQL healthcheck timeout. Continuing anyway...
:mysql_done
echo(

REM ---------- 4. Start big-data + app stack (no Hive yet) ----------
echo [4/6] Starting 13 services (Hadoop, Spark, HBase, Kafka, Backend)...
echo        ^> hadoop-namenode, datanode x2
echo        ^> spark-master, spark-worker-1
echo        ^> hbase-master, regionserver x2
echo        ^> kafka-1, kafka-2
echo        ^> demo-backend (FastAPI on 8000)
docker compose up -d ^
    hadoop-namenode ^
    hadoop-datanode-1 hadoop-datanode-2 ^
    spark-master spark-worker-1 ^
    hbase-master hbase-regionserver-1 hbase-regionserver-2 ^
    kafka-1 kafka-2 ^
    demo-backend
if errorlevel 1 (
    echo [FAIL] Failed to start stack. Run 'docker compose logs'.
    pause
    exit /b 1
)
echo [OK]   Stack started.
echo(

REM ---------- 5. Start Hive stack (PG metastore + metastore thrift + 2 HS2) ----------
echo [5/6] Starting Hive (PostgreSQL metastore + 2 HiveServer2 instances)...
docker compose up -d ^
    hive-metastore-postgres ^
    hive-metastore ^
    hive-server-1 hive-server-2
if errorlevel 1 (
    echo [FAIL] Failed to start Hive stack. Run 'docker compose logs hive-metastore'.
    pause
    exit /b 1
)
echo [OK]   Hive stack started (schema init may take ~30s).
echo(

REM ---------- 6. Wait for key services ----------
echo [6/6] Waiting for services to become ready (up to 4 min)...
echo        ^> Hadoop namenode (port 9000)
echo        ^> HBase master (meta online)
echo        ^> demo-backend (port 8000)
echo(

set NAMENODE_OK=0
set HBASE_OK=0
set BACKEND_OK=0
set HIVE_OK=0
REM write HBase status file once
>  "%~dp0\.tmp-hbase-status.hbase" echo status
for /l %%i in (1,1,120) do (
    REM -- namenode port 9000 (write probe script to file to avoid shell quoting issues)
    >  "%TEMP%\namenode-probe.sh" echo ^(echo ^> /dev/tcp/hadoop-namenode/9000^)^ ^&^&^ echo ok
    docker cp "%TEMP%\namenode-probe.sh" hadoop-namenode:/tmp/probe.sh 1>nul 2>nul
    docker exec hadoop-namenode bash /tmp/probe.sh > "%TEMP%\nn-probe.out" 2>nul
    findstr "ok" "%TEMP%\nn-probe.out" >nul 2>nul
    if !errorlevel! equ 0 if !NAMENODE_OK! equ 0 (
        set NAMENODE_OK=1
        echo [OK]   HDFS namenode:9000 ready.
    )
    del "%TEMP%\nn-probe.out" 2>nul
    if !errorlevel! equ 0 if !NAMENODE_OK! equ 0 (
        set NAMENODE_OK=1
        echo [OK]   HDFS namenode:9000 ready.
    )

    REM -- demo-backend health
    for /f %%h in ('docker inspect --format="{{.State.Health.Status}}" demo-backend 2^>nul') do set BH=%%h
    if "!BH!"=="healthy" if !BACKEND_OK! equ 0 (
        set BACKEND_OK=1
        echo [OK]   demo-backend:8000 healthy.
    )

    REM -- HBase status
    if !HBASE_OK! equ 0 (
        docker cp "%~dp0\.tmp-hbase-status.hbase" hbase-master:/tmp/.tmp-hbase-status.hbase 1>nul 2>nul
        docker exec hbase-master bash -c "hbase shell /tmp/.tmp-hbase-status.hbase 2>/dev/null" > "%TEMP%\hbase-st.txt" 2>nul
        findstr /C:"2 servers" "%TEMP%\hbase-st.txt" >nul 2>nul
        if !errorlevel! equ 0 (
            findstr /C:"0 dead" "%TEMP%\hbase-st.txt" >nul 2>nul
            if !errorlevel! equ 0 (
                set HBASE_OK=1
                echo [OK]   HBase 1 master + 2 regionservers (no dead).
            )
        )
    )

    REM -- Hive HS2 port 10000 (hive-server-1)
    if !HIVE_OK! equ 0 (
        >  "%TEMP%\hive-probe.sh" echo ^(echo ^> /dev/tcp/hive-server-1/10000^)^ ^&^&^ echo ok
        docker cp "%TEMP%\hive-probe.sh" hive-server-1:/tmp/hive-probe.sh 1>nul 2>nul
        docker exec hive-server-1 bash /tmp/hive-probe.sh > "%TEMP%\hive-probe.out" 2>nul
        findstr "ok" "%TEMP%\hive-probe.out" >nul 2>nul
        if !errorlevel! equ 0 (
            set HIVE_OK=1
            echo [OK]   HiveServer2 :10000 ready (hive-server-1).
        )
    )

    REM -- all ready, exit for loop
    if !NAMENODE_OK! equ 1 if !BACKEND_OK! equ 1 if !HBASE_OK! equ 1 if !HIVE_OK! equ 1 goto :all_ready

    set /a SECS=%%i*2
    <nul set /p "=        ...!SECS!s / 240s  " 2>nul
    timeout /t 2 /nobreak >nul
)

REM loop ended without all-ready: warn
if !NAMENODE_OK! equ 0 echo [WARN] HDFS namenode not ready after 4 min.
if !HBASE_OK! equ 0 echo [WARN] HBase not ready after 4 min. Try: docker compose logs hbase-master
if !BACKEND_OK! equ 0 echo [WARN] demo-backend not healthy after 4 min.
if !HIVE_OK! equ 0 echo [WARN] HiveServer2 not ready after 4 min. Try: docker compose logs hive-server-1
goto :after_warn

:all_ready
echo [OK]   All key services ready.

:after_warn
del "%~dp0\.tmp-hbase-status.hbase" 2>nul

echo(
echo(

REM ---------- Frontend serve ----------
REM   Frontend is served by demo-backend (port 8080 inside container, mapped to host 8080)
echo ==========================================================
echo   All services up!
echo ==========================================================
echo(
echo   Frontend:        http://localhost:8080
echo   API docs:        http://localhost:8000/docs
echo   HBase Web UI:    http://localhost:11610
echo   Spark Web UI:    http://localhost:18080
echo   Hadoop NN UI:    http://localhost:19870
echo(
echo   Container status:
for /f "tokens=*" %%c in ('docker compose ps --format "{{.Name}}\t{{.Status}}" 2^>nul') do (
    echo     %%c
)
echo(
echo Next steps:
echo   1. Open http://localhost:8080 in browser
echo   2. Click "manage.html" -^> "System tab" -^> "one-click init" to load data
echo      (CSV -^> MySQL -^> Sqoop -^> Spark clean -^> train models, ~5-10 min)
echo(
echo To stop: run scripts\stop.bat
echo To reset: run scripts\reset.bat
echo(
pause

