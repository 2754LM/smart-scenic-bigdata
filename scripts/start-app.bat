@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

REM ============================================================
REM  Smart Scenic BigData Platform - Data Pipeline Runner
REM  ----------------------------------------------------------
REM  Runs the full data flow that powers every dashboard widget.
REM
REM  This bat is a thin wrapper. All the real work is done by
REM  the Python driver inside the demo-backend container. The
REM  driver is copied in via 'docker cp' from the host scripts/
REM  directory (mapped to /scripts/ via the volume mount below).
REM
REM  Prerequisites:
REM    - 4 CSVs already in data\raw_data\
REM    - start-containers.bat has been run (15 containers up)
REM ============================================================

cd /d "%~dp0\.."

echo(
echo ==========================================================
echo   Smart Scenic BigData - Running data pipeline
echo ==========================================================
echo(

REM ---------- 0. Pre-flight ----------
where docker >nul 2>&1
if errorlevel 1 (
    echo [FAIL] Docker not found. Run scripts\start-containers.bat first.
    pause
    exit /b 1
)
docker inspect --format="{{.State.Running}}" demo-backend 2>nul | findstr /c:"true" >nul
if errorlevel 1 (
    echo [FAIL] demo-backend not running. Run scripts\start-containers.bat first.
    pause
    exit /b 1
)
if not exist "data\raw_data\attractions.csv" (
    echo [FAIL] data\raw_data\attractions.csv not found.
    echo        Please put 4 CSVs ^(attractions / visitors / consumption / visit_records^)
    echo        in data\raw_data\ before running.
    pause
    exit /b 1
)
echo [OK]   Pre-flight OK: containers up, CSVs in data\raw_data\
echo(
echo NOTE: this runs the 4-step DATA pipeline only (load_csv, sqoop,
echo   spark_clean, hive_ddl). Models in /shared/models/sklearn/ are
echo   preserved. To (re)train: scripts\start-train.bat.
echo(

REM ---------- Run the Python driver inside demo-backend ----------
REM scripts/run_pipeline.py is mounted into the container at /app/scripts/
REM (see docker-compose.yml: ./scripts:/app/scripts:ro on demo-backend).
docker exec demo-backend python3 /app/scripts/run_pipeline.py
if errorlevel 1 (
    echo(
    echo [FAIL] Pipeline failed. Check: docker logs demo-backend
    pause
    exit /b 1
)

echo(
pause
