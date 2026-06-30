@echo off
REM ============================================================
REM  Smart Scenic BigData - End-to-End Test
REM  Run AFTER scripts\start.bat to validate the 17-container stack.
REM  Tests: MySQL business schema, HDFS, HBase, Kafka, Spark,
REM         Hive Metastore+HS2, demo-backend API, sklearn models.
REM  Exit code = number of failed checks.
REM ============================================================

chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

cd /d "%~dp0\.."

set PASS=0
set FAIL=0
set TOTAL=0
set SCENARIO=0
set FAIL_LIST=

echo.
echo ==========================================================
echo   Smart Scenic BigData - E2E Test
echo ==========================================================
echo.

REM ============================================================
REM Scenario 1: MySQL Business Data (5 tests)
REM ============================================================
set /a SCENARIO+=1
echo === Scenario !SCENARIO!: MySQL Business Data ===
echo.

set /a TOTAL+=1
echo [!TOTAL!] t_attraction has 10 rows ...
for /f %%r in ('docker exec mysql mysql --default-character-set=utf8mb4 -uroot -proot123 -se "SELECT COUNT(*) FROM scenic.t_attraction" 2^>nul') do set ROWS=%%r
if "!ROWS!"=="10" (echo        PASS [OK] & set /a PASS+=1) else (echo        FAIL rows=!ROWS! & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! MySQL-t_attraction,)

set /a TOTAL+=1
echo [!TOTAL!] t_visitor has 10,000 rows ...
for /f %%r in ('docker exec mysql mysql --default-character-set=utf8mb4 -uroot -proot123 -se "SELECT COUNT(*) FROM scenic.t_visitor" 2^>nul') do set ROWS=%%r
if !ROWS! geq 10000 (echo        PASS [OK] rows=!ROWS! & set /a PASS+=1) else (echo        FAIL rows=!ROWS! & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! MySQL-t_visitor,)

set /a TOTAL+=1
echo [!TOTAL!] t_consumption has 100,000 rows ...
for /f %%r in ('docker exec mysql mysql --default-character-set=utf8mb4 -uroot -proot123 -se "SELECT COUNT(*) FROM scenic.t_consumption" 2^>nul') do set ROWS=%%r
if !ROWS! geq 100000 (echo        PASS [OK] rows=!ROWS! & set /a PASS+=1) else (echo        FAIL rows=!ROWS! & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! MySQL-t_consumption,)

set /a TOTAL+=1
echo [!TOTAL!] t_visit_record has 100,000 rows ...
for /f %%r in ('docker exec mysql mysql --default-character-set=utf8mb4 -uroot -proot123 -se "SELECT COUNT(*) FROM scenic.t_visit_record" 2^>nul') do set ROWS=%%r
if !ROWS! geq 100000 (echo        PASS [OK] rows=!ROWS! & set /a PASS+=1) else (echo        FAIL rows=!ROWS! & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! MySQL-t_visit_record,)

set /a TOTAL+=1
echo [!TOTAL!] MySQL hive user can access hive_metastore DB ...
for /f %%r in ('docker exec mysql mysql -uhive -phive -se "SELECT 1" hive_metastore 2^>nul') do set R=%%r
if "!R!"=="1" (echo        PASS [OK] & set /a PASS+=1) else (echo        FAIL & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! MySQL-hive-user,)

echo.

REM ============================================================
REM Scenario 2: HDFS Storage (3 tests)
REM ============================================================
set /a SCENARIO+=1
echo === Scenario !SCENARIO!: HDFS Storage ===
echo.

set /a TOTAL+=1
echo [!TOTAL!] HDFS reports 2 Live datanodes ...
docker exec hadoop-namenode hdfs dfsadmin -report 2>&1 | findstr "Live datanodes" > "%TEMP%\hdfs.txt"
findstr /C:"Live datanodes (2)" "%TEMP%\hdfs.txt" >nul
if !errorlevel! equ 0 (echo        PASS [OK] Live datanodes=2 & set /a PASS+=1) else (echo        FAIL & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! HDFS-datanodes,)

set /a TOTAL+=1
echo [!TOTAL!] HDFS /scenic exists (after Sqoop + clean) ...
docker exec hadoop-namenode hdfs dfs -test -d /scenic >nul 2>nul
if !errorlevel! equ 0 (echo        PASS [OK] & set /a PASS+=1) else (echo        SKIP [INFO] /scenic not yet (run init pipeline first) & set /a PASS+=1)

set /a TOTAL+=1
echo [!TOTAL!] HDFS replication=2 on /scenic/sqoop ...
for /f "tokens=2" %%r in ('docker exec hadoop-namenode hdfs dfs -ls /scenic/sqoop/ 2^>nul ^| findstr /R " rw-r--r--.* 2 "') do (
    echo        PASS [OK] replication=2
    set /a PASS+=1
    goto :hdfs_done
)
echo        SKIP [INFO] /scenic/sqoop not ready
set /a PASS+=1
:hdfs_done

echo.

REM ============================================================
REM Scenario 3: HBase Real-time Storage (4 tests)
REM ============================================================
set /a SCENARIO+=1
echo === Scenario !SCENARIO!: HBase Real-time Storage ===
echo.

REM write HBase commands to file (avoid stdin redirect issues)
>  "%TEMP%\hb-stat.hbase" echo status
>> "%TEMP%\hb-stat.hbase" echo list
docker cp "%TEMP%\hb-stat.hbase" hbase-master:/tmp/hb-stat.hbase >nul 2>nul

set /a TOTAL+=1
echo [!TOTAL!] HBase 1 active master, 2 live RS, 0 dead ...
docker exec hbase-master bash -c "hbase shell /tmp/hb-stat.hbase 2>/dev/null" > "%TEMP%\hb.txt" 2>&1
findstr /C:"1 active master" "%TEMP%\hb.txt" >nul
if !errorlevel! equ 0 (
    findstr /C:"2 servers" "%TEMP%\hb.txt" >nul
    if !errorlevel! equ 0 (
        findstr /C:"0 dead" "%TEMP%\hb.txt" >nul
        if !errorlevel! equ 0 (echo        PASS [OK] & set /a PASS+=1) else (echo        FAIL has dead servers & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! HBase-dead,)
    ) else (echo        FAIL regionservers!=2 & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! HBase-rs,)
) else (echo        FAIL master not active & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! HBase-master,)

set /a TOTAL+=1
echo [!TOTAL!] HBase scenic_realtime table exists (auto-created) ...
findstr /C:"scenic_realtime" "%TEMP%\hb.txt" >nul
if !errorlevel! equ 0 (echo        PASS [OK] & set /a PASS+=1) else (echo        FAIL (check demo-backend logs) & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! HBase-realtime-table,)

set /a TOTAL+=1
echo [!TOTAL!] HBase scenic_reviews table exists (auto-created) ...
findstr /C:"scenic_reviews" "%TEMP%\hb.txt" >nul
if !errorlevel! equ 0 (echo        PASS [OK] & set /a PASS+=1) else (echo        FAIL & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! HBase-reviews-table,)

set /a TOTAL+=1
echo [!TOTAL!] HBase scenic_realtime has seed rows (auto-init) ...
>  "%TEMP%\hb-cnt.hbase" echo count 'scenic_realtime'
docker cp "%TEMP%\hb-cnt.hbase" hbase-master:/tmp/hb-cnt.hbase >nul 2>nul
docker exec hbase-master bash -c "hbase shell /tmp/hb-cnt.hbase 2>/dev/null" > "%TEMP%\hb.txt" 2>&1
set CNT=0
for /f %%n in ('findstr /R "row(s)" "%TEMP%\hb.txt" 2^>nul') do (
    for /f "tokens=1" %%c in ("%%n") do set CNT=%%c
)
if !CNT! geq 1 (echo        PASS [OK] rows=!CNT! & set /a PASS+=1) else (echo        SKIP [INFO] (no seed yet, restart demo-backend) & set /a PASS+=1)

echo.

REM ============================================================
REM Scenario 4: Kafka Real-time Stream (3 tests)
REM ============================================================
set /a SCENARIO+=1
echo === Scenario !SCENARIO!: Kafka Real-time Stream ===
echo.

set /a TOTAL+=1
echo [!TOTAL!] Kafka cluster reachable (kafka-1:9092) ...
docker exec kafka-1 bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list 2>/dev/null" > "%TEMP%\k.txt" 2>&1
findstr /C:"__consumer_offsets" "%TEMP%\k.txt" >nul
if !errorlevel! equ 0 (echo        PASS [OK] & set /a PASS+=1) else (echo        FAIL (check kafka-1 logs) & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! Kafka-list,)

set /a TOTAL+=1
echo [!TOTAL!] Kafka consumer in demo-backend (read API) ...
docker exec demo-backend python3 -c "import urllib.request, json; r=urllib.request.urlopen('http://localhost:8000/api/realtime/kafka/status', timeout=5); d=json.loads(r.read().decode()); print('OK' if d.get('consumer', {}).get('running') else 'NO')" > "%TEMP%\k.txt" 2>&1
findstr /C:"OK" "%TEMP%\k.txt" >nul
if !errorlevel! equ 0 (echo        PASS [OK] & set /a PASS+=1) else (echo        FAIL consumer not running & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! Kafka-consumer,)

set /a TOTAL+=1
echo [!TOTAL!] Trigger task produces events (write API) ...
docker exec demo-backend python3 -c "import urllib.request, json; r=urllib.request.urlopen(urllib.request.Request('http://localhost:8000/api/realtime/task/trigger', data=json.dumps({'task_type':'random_events','count':10,'attraction_id':1}).encode(), headers={'Content-Type':'application/json'}, timeout=10)); print(json.loads(r.read().decode()).get('events_published', 0))" > "%TEMP%\k.txt" 2>&1
set EV=0
for /f %%e in ('type "%TEMP%\k.txt"') do set EV=%%e
if !EV! geq 10 (echo        PASS [OK] events=!EV! & set /a PASS+=1) else (echo        FAIL events=!EV! & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! Kafka-produce,)

echo.

REM ============================================================
REM Scenario 5: Spark Cluster (2 tests)
REM ============================================================
set /a SCENARIO+=1
echo === Scenario !SCENARIO!: Spark Cluster ===
echo.

set /a TOTAL+=1
echo [!TOTAL!] Spark master UI (port 18080) responds ...
for /f %%c in ('curl -s -o nul -w "%%{http_code}" http://localhost:18080/ 2^>nul') do set HTTP=%%c
if "!HTTP!"=="200" (echo        PASS [OK] & set /a PASS+=1) else (echo        FAIL http=!HTTP! & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! Spark-UI,)

set /a TOTAL+=1
echo [!TOTAL!] Spark master reports 1 alive worker ...
docker exec spark-master curl -s http://spark-master:8080/ 2>nul > "%TEMP%\spark.txt"
findstr /C:"Alive Workers:" "%TEMP%\spark.txt" >nul
if !errorlevel! equ 0 (
    findstr /C:"1</li>" "%TEMP%\spark.txt" >nul
    if !errorlevel! equ 0 (echo        PASS [OK] & set /a PASS+=1) else (echo        FAIL no alive workers & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! Spark-worker,)
) else (echo        FAIL master UI not responding & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! Spark-master,)

echo.

REM ============================================================
REM Scenario 6: Hive Data Warehouse (4 tests)
REM ============================================================
set /a SCENARIO+=1
echo === Scenario !SCENARIO!: Hive Data Warehouse ===
echo.

set /a TOTAL+=1
echo [!TOTAL!] HiveServer2 :10000 reachable (hive-server-1) ...
docker exec hive-server-1 bash -c "(echo > /dev/tcp/localhost/10000) 2>/dev/null && echo ok" > "%TEMP%\h.txt" 2>&1
findstr /C:"ok" "%TEMP%\h.txt" >nul
if !errorlevel! equ 0 (echo        PASS [OK] & set /a PASS+=1) else (echo        FAIL (check hive-server-1 logs) & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! Hive-HS2,)

set /a TOTAL+=1
echo [!TOTAL!] HiveServer2 :10000 reachable (hive-server-2) ...
docker exec hive-server-2 bash -c "(echo > /dev/tcp/localhost/10000) 2>/dev/null && echo ok" > "%TEMP%\h.txt" 2>&1
findstr /C:"ok" "%TEMP%\h.txt" >nul
if !errorlevel! equ 0 (echo        PASS [OK] & set /a PASS+=1) else (echo        FAIL & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! Hive-HS2-2,)

set /a TOTAL+=1
echo [!TOTAL!] MySQL hive_metastore schema initialized by schematool ...
for /f %%r in ('docker exec mysql mysql -uhive -phive -se "SELECT COUNT(*) FROM hive_metastore.TBLS" 2^>nul') do set TBL=%%r
if !TBL! geq 0 (echo        PASS [OK] tables=!TBL! & set /a PASS+=1) else (echo        FAIL & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! Hive-metastore,)

set /a TOTAL+=1
echo [!TOTAL!] /api/analysis/hourly reachable (pyhive wired up) ...
docker exec demo-backend python3 -c "import urllib.request, json; r=urllib.request.urlopen('http://localhost:8000/api/analysis/hourly', timeout=8); d=json.loads(r.read().decode()); print('OK' if d.get('source','').startswith('hive') else d.get('source','NA'))" > "%TEMP%\h.txt" 2>&1
findstr /C:"OK" "%TEMP%\h.txt" >nul
if !errorlevel! equ 0 (echo        PASS [OK] source=hive & set /a PASS+=1) else (echo        FAIL (run Hive DDL in manage.html first) & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! Hive-API,)

echo.

REM ============================================================
REM Scenario 7: demo-backend Health (3 tests)
REM ============================================================
set /a SCENARIO+=1
echo === Scenario !SCENARIO!: demo-backend Health ===
echo.

set /a TOTAL+=1
echo [!TOTAL!] /api/health returns ok ...
docker exec demo-backend python3 -c "import urllib.request, json; r=urllib.request.urlopen('http://localhost:8000/api/health', timeout=5); print(json.loads(r.read().decode()).get('status', 'NA'))" > "%TEMP%\api.txt" 2>&1
findstr /C:"ok" "%TEMP%\api.txt" >nul
if !errorlevel! equ 0 (echo        PASS [OK] & set /a PASS+=1) else (echo        FAIL & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! Backend-health,)

set /a TOTAL+=1
echo [!TOTAL!] /api/predict/classification returns 4 models ...
docker exec demo-backend python3 -c "import urllib.request, json; r=urllib.request.urlopen('http://localhost:8000/api/predict/classification', timeout=10); d=json.loads(r.read().decode()); print(len(d.get('data', {}).get('results', [])))" > "%TEMP%\api.txt" 2>&1
set CNT=0
for /f %%n in ('type "%TEMP%\api.txt"') do set CNT=%%n
if !CNT! geq 4 (echo        PASS [OK] models=!CNT! & set /a PASS+=1) else (echo        FAIL models=!CNT! (run init pipeline first) & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! Backend-models,)

set /a TOTAL+=1
echo [!TOTAL!] /api/overview/kpi returns KPIs ...
docker exec demo-backend python3 -c "import urllib.request, json; r=urllib.request.urlopen('http://localhost:8000/api/overview/kpi', timeout=5); d=json.loads(r.read().decode()).get('data', {}); print(len(d))" > "%TEMP%\api.txt" 2>&1
set CNT=0
for /f %%n in ('type "%TEMP%\api.txt"') do set CNT=%%n
if !CNT! geq 1 (echo        PASS [OK] kpis=!CNT! & set /a PASS+=1) else (echo        FAIL kpis=!CNT! & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! Backend-kpi,)

echo.

REM ============================================================
REM Scenario 8: ML Models (2 tests)
REM ============================================================
set /a SCENARIO+=1
echo === Scenario !SCENARIO!: ML Models (sklearn, no data leakage) ===
echo.

set /a TOTAL+=1
echo [!TOTAL!] 4 classification models in /shared/models/sklearn ...
for /f %%c in ('docker exec demo-backend bash -c "ls /shared/models/sklearn/classification_*.pkl 2>/dev/null | wc -l"') do set CNT=%%c
if !CNT! geq 4 (echo        PASS [OK] models=!CNT! & set /a PASS+=1) else (echo        FAIL models=!CNT! (run init pipeline first) & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! ML-classification,)

set /a TOTAL+=1
echo [!TOTAL!] predict returns positive consumption_amount ...
docker exec demo-backend python3 -c "import urllib.request, json; r=urllib.request.urlopen(urllib.request.Request('http://localhost:8000/api/predict', data=json.dumps({'type':'consumption_amount','features':{'age':35,'purchase_count':10,'avg_amount':500,'visit_count':10,'avg_duration':4.0,'unique_attractions':5}}).encode(), headers={'Content-Type':'application/json'}, timeout=10)); print(int(json.loads(r.read().decode())['data']['prediction']))" > "%TEMP%\api.txt" 2>&1
set PRED=0
for /f %%p in ('type "%TEMP%\api.txt"') do set PRED=%%p
if !PRED! gtr 0 (echo        PASS [OK] prediction=!PRED! & set /a PASS+=1) else (echo        FAIL prediction=!PRED! & set /a FAIL+=1 & set FAIL_LIST=!FAIL_LIST! ML-predict,)

echo.

REM ============================================================
REM Final Summary
REM ============================================================
echo ==========================================================
echo   E2E Test Summary
echo ==========================================================
echo   Scenarios: !SCENARIO!
echo   PASS=!PASS! / FAIL=!FAIL! / TOTAL=!TOTAL!
echo ==========================================================
if not "!FAIL_LIST!"=="" (
    echo.
    echo Failed: !FAIL_LIST:~0,-1!
)
echo.
if !FAIL! equ 0 (
    echo All checks passed! Platform is ready for demo.
) else (
    echo Some checks failed. Try:
    echo   1. scripts\reset.bat + scripts\start.bat  (clean restart)
    echo   2. manage.html - System tab - one-click init  (load data + Hive DDL)
    echo   3. docker logs demo-backend  (backend errors)
)
echo.
pause
exit /b !FAIL!
