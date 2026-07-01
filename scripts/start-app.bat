@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

REM ============================================================
REM  Smart Scenic BigData - Data Pipeline Only
REM  ----------------------------------------------------------
REM  Runs: CSV -> MySQL -> Sqoop -> Spark clean -> Hive DDL
REM  Does NOT train models. Use start-train.bat for training.
REM  Or use start.bat for everything in one go.
REM ============================================================

cd /d "%~dp0\.."

echo.
echo ==========================================================
echo   Smart Scenic BigData - Data Pipeline
echo ==========================================================
echo.

docker info >nul 2>&1
if errorlevel 1 (
    echo [FAIL] Docker not running.
    pause
    exit /b 1
)
echo [OK] Docker ready.

docker exec demo-backend python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health', timeout=5)" >nul 2>&1
if errorlevel 1 (
    echo [FAIL] demo-backend not reachable. Run scripts\start-containers.bat first.
    pause
    exit /b 1
)
echo [OK] demo-backend reachable.

echo.
docker exec demo-backend python3 /app/scripts/run.py --mode pipeline

echo.
pause
