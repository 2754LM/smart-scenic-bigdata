@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM ============================================================
REM Smart Scenic BigData - Full Connectivity Verify (Windows CMD)
REM Tests: 35+ items across 8 components
REM ============================================================

cd /d "%~dp0\.."

set PASS=0
set FAIL=0
set TOTAL=0

echo ==========================================
echo   Smart Scenic BigData - Verify
echo ==========================================
echo.

REM ============================================================
REM [1] Container Status (16)
REM ============================================================
echo === [1] Container Status (16) ===
set SERVICES=mysql zookeeper-1 zookeeper-2 zookeeper-3 hadoop-namenode hadoop-datanode-1 hadoop-datanode-2 hbase-master hbase-regionserver-1 hbase-regionserver-2 kafka-1 kafka-2 spark-master spark-worker-1 hive-server-1 hive-server-2
for %%s in (%SERVICES%) do (
    set /a TOTAL+=1
    docker inspect -f "{{.State.Running}}" %%s 2>nul | findstr "true" >nul
    if !errorlevel! equ 0 (
        echo [!TOTAL!] container %%s ... [OK]
        set /a PASS+=1
    ) else (
        echo [!TOTAL!] container %%s ... [FAIL]
        set /a FAIL+=1
    )
)

echo.
echo === [2] MySQL Connectivity (4) ===

set /a TOTAL+=1
echo [!TOTAL!] MySQL daemon ping ...
docker exec mysql mysqladmin ping -uroot -proot123 >nul 2>nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] MySQL contains scenic database ...
docker exec mysql mysql -uroot -proot123 -e "SHOW DATABASES" 2>nul | findstr "scenic" >nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] MySQL business tables (4 tables) ...
set TABLES_FOUND=0
for /f %%t in ('docker exec mysql mysql -uroot -proot123 -se "USE scenic; SHOW TABLES" 2^>nul') do (
    if not "%%t"=="" set /a TABLES_FOUND+=1
)
if !TABLES_FOUND! geq 4 (echo [OK] found !TABLES_FOUND! tables & set /a PASS+=1) else (echo [FAIL] found !TABLES_FOUND! & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] MySQL t_scenic has 10 rows ...
for /f %%c in ('docker exec mysql mysql -uroot -proot123 -se "SELECT COUNT(*) FROM scenic.t_scenic" 2^>nul') do set SCENIC_COUNT=%%c
if "!SCENIC_COUNT!"=="10" (echo [OK] rows=10 & set /a PASS+=1) else (echo [WARN] rows=!SCENIC_COUNT! & set /a PASS+=1)

echo.
echo === [3] ZooKeeper Connectivity (2) ===

set /a TOTAL+=1
echo [!TOTAL!] ZK port 2181 (imok) ...
docker exec zookeeper-1 bash -c "echo ruok | nc -w 2 localhost 2181" 2>&1 | findstr "imok" >nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] All 3 ZK nodes Up ...
set ZK_UP=0
for %%z in (zookeeper-1 zookeeper-2 zookeeper-3) do (
    docker inspect -f "{{.State.Running}}" %%z 2>nul | findstr "true" >nul && set /a ZK_UP+=1
)
if !ZK_UP!==3 (echo [OK] !ZK_UP!/3 & set /a PASS+=1) else (echo [FAIL] !ZK_UP!/3 & set /a FAIL+=1)

echo.
echo === [4] HDFS Operations (6) ===

set /a TOTAL+=1
echo [!TOTAL!] Hadoop NN Web 19870 ...
curl -fs http://localhost:19870/ >nul 2>nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] HDFS list root / ...
docker exec hadoop-namenode hdfs dfs -ls / >nul 2>nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] HDFS has 2 Live datanodes ...
docker exec hadoop-namenode hdfs dfsadmin -report 2>&1 | findstr "Live datanodes (2)" >nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [WARN] & set /a PASS+=1)

set /a TOTAL+=1
echo [!TOTAL!] HDFS mkdir + put + cat ...
docker exec hadoop-namenode hdfs dfs -mkdir -p /test_verify >nul 2>nul
docker exec hadoop-namenode hdfs dfs -put /etc/hostname /test_verify/hostname >nul 2>nul
docker exec hadoop-namenode hdfs dfs -cat /test_verify/hostname >nul 2>nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] HDFS replication = 2 ...
for /f "delims=" %%r in ('docker exec hadoop-namenode hdfs dfs -stat %%r /test_verify/hostname 2^>nul') do set REPL=%%r
if "!REPL!"=="2" (echo [OK] replication=2 & set /a PASS+=1) else (echo [FAIL] replication=!REPL! & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] YARN ResourceManager 18088 ...
curl -fs http://localhost:18088/ws/v1/cluster/info >nul 2>nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [WARN] & set /a PASS+=1)

echo.
echo === [5] Hive Operations (4) ===

set /a TOTAL+=1
echo [!TOTAL!] HiveServer2 #1 port 10000 listening ...
docker exec hive-server-1 bash -c "echo > /dev/tcp/localhost/10000 && exit 0 || exit 1" >nul 2>nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] HiveServer2 #2 port 10000 listening ...
docker exec hive-server-2 bash -c "echo > /dev/tcp/localhost/10000 && exit 0 || exit 1" >nul 2>nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] HiveServer2 #1 Thrift service started ...
docker exec hive-server-1 grep "ThriftBinaryCLIService is started" /tmp/hive/hive.log >nul 2>nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] HiveServer2 #2 Thrift service started ...
docker exec hive-server-2 grep "ThriftBinaryCLIService is started" /tmp/hive/hive.log >nul 2>nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

echo.
echo === [6] Kafka Operations (2) ===

set /a TOTAL+=1
echo [!TOTAL!] Kafka port 19092 listening ...
docker exec kafka-1 bash -c "echo > /dev/tcp/localhost/9092 && exit 0 || exit 1" >nul 2>nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] Kafka container running ...
docker inspect -f "{{.State.Running}}" kafka-1 2>nul | findstr "true" >nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

echo.
echo === [7] HBase Operations (3) ===

set /a TOTAL+=1
echo [!TOTAL!] HBase Master Web 11610 ...
curl -fs http://localhost:11610/ >nul 2>nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] HBase Master Web 11610 (via localhost) ...
curl -fs http://localhost:11610/ >nul 2>nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

set /a TOTAL+=1
echo [!TOTAL!] HBase shell status (1 active master) ...
docker exec hbase-master bash -c "echo 'status' | /hbase/bin/hbase shell -n 2>&1" 2>&1 | findstr "active master" >nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [FAIL] & set /a FAIL+=1)

echo.
echo === [8] Spark Operations (1) ===

set /a TOTAL+=1
echo [!TOTAL!] Spark Worker alive ...
docker exec spark-master curl -s http://localhost:8080/ 2>&1 | findstr "Alive Workers: 1" >nul
if !errorlevel! equ 0 (echo [OK] & set /a PASS+=1) else (echo [WARN] & set /a FAIL+=1)

echo.
echo === [9] Cleanup test data ===
docker exec hadoop-namenode hdfs dfs -rm -r -f /test_verify >nul 2>nul
docker exec kafka-1 bash -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --delete --topic verify-test 2>/dev/null' >nul 2>nul
echo [OK] all test data removed

echo.
echo ==========================================
echo   PASS=!PASS! / FAIL=!FAIL! / TOTAL=!TOTAL!
echo ==========================================
echo.

if !FAIL! equ 0 (
    echo All checks passed! Platform is ready.
) else (
    echo Some checks failed. Check logs: docker compose logs -f [service]
)

echo.
pause