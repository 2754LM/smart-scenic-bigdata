@echo off
REM ============================================================
REM Smart Scenic BigData - Demo App Smart Starter
REM 智能启动：自动检测 venv / docker / 训练状态
REM Usage: scripts\start-app.bat
REM ============================================================

setlocal enabledelayedexpansion
cd /d "%~dp0.."

echo ==========================================
echo   Smart Scenic BigData - Demo App
echo ==========================================
echo.

REM ---------- Step 0: 选择启动模式 ----------
echo [0/5] Choose startup mode:
echo       1) Local Python (faster, hot reload, requires Python deps)
echo       2) Docker demo-backend (cleaner, no Python setup needed)
echo.
set /p MODE="Enter mode [1/2] (default=1): "
if "%MODE%"=="" set MODE=1
echo.

REM ---------- Step 1: 检查大数据平台是否在跑 ----------
echo [1/5] Checking big data platform...
docker ps --format "{{.Names}}" 2>nul | findstr /C:"hadoop-namenode" >nul
if errorlevel 1 (
    echo        [WARN] Big data platform NOT running. Demo may not work.
    echo        Run: scripts\start.bat first
    echo.
    set /p CONTINUE="Continue anyway? [y/N]: "
    if /i not "!CONTINUE!"=="y" exit /b 1
) else (
    echo        Big data platform: RUNNING
)
echo.

REM ---------- Step 2: 准备后端 ----------
if "%MODE%"=="1" (
    REM === 本地 Python 模式 ===
    echo [2/5] Setting up local Python venv...
    if not exist "app\backend\.venv\Scripts\python.exe" (
        echo        No venv found. Installing...
        call scripts\install-deps.bat
        if errorlevel 1 exit /b 1
    ) else (
        echo        venv ready at app\backend\.venv
    )
    set BACKEND_PY=app\backend\.venv\Scripts\python.exe
    set BACKEND_DIR=app\backend
) else (
    REM === Docker 模式 ===
    echo [2/5] Starting demo-backend in Docker...
    docker compose up -d demo-backend
    timeout /t 5 /nobreak >nul
    echo        demo-backend container started
    goto :start_frontend
)
echo.

REM ---------- Step 3: 检查 PySpark 训练状态 ----------
echo [3/5] Checking PySpark training status...
docker exec spark-master ls /shared/models/ >nul 2>&1
if errorlevel 1 (
    echo        [INFO] No PySpark models yet. Will auto-train in background.
    echo               Backend starts now (uses sklearn fallback).
    echo               Training will take 5-10 min. Check /api/predict/_engine
) else (
    docker exec spark-master ls /shared/models/ 2>nul | findstr /v "^$" >nul
    if errorlevel 1 (
        echo        [INFO] /shared/models empty. Will auto-train in background.
    ) else (
        echo        [OK] PySpark models found. Will load on backend startup.
    )
)
echo.

REM ---------- Step 4: 启动后端 ----------
echo [4/5] Starting FastAPI backend on port 8000...
cd /d "%BACKEND_DIR%"
start "smart-scenic-backend" /B "%BACKEND_PY%" main.py > "C:\Users\kano\AppData\Local\Temp\backend.log" 2>&1
timeout /t 4 /nobreak >nul
echo        Backend log: C:\Users\kano\AppData\Local\Temp\backend.log
cd /d "%~dp0.."

:start_frontend

REM ---------- Step 5: 启动前端 ----------
echo [5/5] Starting frontend HTTP server on port 8080...
cd /d "%~dp0..\app\frontend"
start "smart-scenic-frontend" /B python -m http.server 8080 > "C:\Users\kano\AppData\Local\Temp\frontend.log" 2>&1
timeout /t 2 /nobreak >nul
cd /d "%~dp0.."

echo.
echo ==========================================
echo   Ready!
echo     Frontend:  http://localhost:8080
echo     API docs:  http://localhost:8000/docs
echo     Engine:    http://localhost:8000/api/predict/_engine
echo ==========================================
echo.
echo Press any key to stop all servers...
pause >nul

echo.
echo Stopping...
taskkill /FI "WINDOWTITLE eq smart-scenic-backend*" /T /F >nul 2>nul
taskkill /FI "WINDOWTITLE eq smart-scenic-frontend*" /T /F >nul 2>nul
if "%MODE%"=="2" docker compose stop demo-backend >nul 2>&1
echo Done.
endlocal