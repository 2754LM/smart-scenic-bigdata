@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

REM ============================================================
REM  Smart Scenic BigData Platform - Model Trainer
REM  ----------------------------------------------------------
REM  Runs spark_train (9 sklearn .pkl) + fpgrowth (5010 rules).
REM
REM  This is the on-demand training entry point. By default,
REM  start-app.bat (the data pipeline) does NOT re-train models,
REM  to avoid wasting 5+ minutes on every demo re-run.
REM
REM  Prerequisites:
REM    - 15 containers running (start-containers.bat)
REM    - 4 CSVs in data\raw_data\
REM    - existing models in /shared/models/sklearn/ (optional;
REM      this script overwrites them)
REM ============================================================

cd /d "%~dp0\.."

echo(
echo ==========================================================
echo   Smart Scenic BigData - Model Training
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
echo [OK]   Pre-flight OK
echo(

REM ---------- Run the trainer inside demo-backend ----------
REM scripts/run_train.py is mounted into the container at /app/scripts/
docker exec demo-backend python3 /app/scripts/run_train.py
if errorlevel 1 (
    echo(
    echo [FAIL] Training failed. Check: docker logs demo-backend
    pause
    exit /b 1
)

echo(
pause
