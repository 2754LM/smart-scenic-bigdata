@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

REM ============================================================
REM  Smart Scenic BigData - One-Click Start (Everything)
REM  ----------------------------------------------------------
REM  1. Starts 15 Docker containers
REM  2. Runs data pipeline (CSV -> MySQL -> Sqoop -> Spark -> Hive)
REM  3. Trains ML models (spark_train -> fpgrowth -> apriori)
REM
REM  For granular control, use:
REM    start-containers.bat  (containers only)
REM    start-app.bat         (data pipeline only)
REM    start-train.bat       (training only)
REM ============================================================

cd /d "%~dp0\.."

echo.
echo ==========================================================
echo   Smart Scenic BigData - One-Click Start
echo ==========================================================
echo.

REM --- Phase 0: Check Docker ---
docker info >nul 2>&1
if errorlevel 1 (
    echo [FAIL] Docker not running. Start Docker Desktop first.
    pause
    exit /b 1
)
echo [OK] Docker ready.

REM --- Phase 1: Containers ---
echo.
echo [1/3] Starting 15 containers...
call scripts\start-containers.bat
if errorlevel 1 (
    echo [FAIL] Containers did not start.
    pause
    exit /b 1
)

REM --- Phase 2: Data Pipeline ---
echo.
echo [2/3] Running data pipeline...
docker exec demo-backend python3 /app/scripts/run.py --mode pipeline
if errorlevel 1 (
    echo [WARN] Pipeline had errors. You can retry with scripts\start-app.bat
)

REM --- Phase 3: Training ---
echo.
echo [3/3] Training models...
docker exec demo-backend python3 /app/scripts/run.py --mode train
if errorlevel 1 (
    echo [WARN] Training had errors. You can retry with scripts\start-train.bat
)

echo.
echo ==========================================================
echo   All done! Platform is ready.
echo ==========================================================
echo.
echo   Frontend:   http://localhost:8080
echo   API docs:   http://localhost:8000/docs
echo   Spark UI:   http://localhost:18080
echo.
pause
