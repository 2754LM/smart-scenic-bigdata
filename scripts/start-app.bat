@echo off
REM ============================================================
REM Smart Scenic BigData - Web App Starter
REM Double-click to run. Auto-detects environment + installs deps.
REM ============================================================
chcp 65001 >nul
setlocal enabledelayedexpansion
cd /d "%~dp0.."

echo ==========================================
echo   Smart Scenic BigData - Web App
echo ==========================================
echo.

REM ---------- Step 1: Check big data platform ----------
echo [1/5] Checking big data platform...
docker ps --format "{{.Names}}" 2>nul | findstr /C:"hadoop-namenode" >nul
if errorlevel 1 (
    echo        [WARN] Big data platform NOT running!
    echo        Please run scripts\start.bat first.
    echo.
    echo Press any key to continue anyway, or close this window...
    pause >nul
)

REM ---------- Step 2: Detect mode (check Docker demo-backend) ----------
echo [2/5] Detecting mode...
set MODE=1
docker ps --format "{{.Names}}" 2>nul | findstr /C:"demo-backend" >nul
if not errorlevel 1 (
    echo        Docker demo-backend is running - using that mode
    set MODE=2
) else (
    echo        Using Local Python mode (will install deps if needed)
)

if "%MODE%"=="1" (
    REM === Local Python mode ===
    cd /d "%~dp0..\app\backend"

    if not exist ".venv\Scripts\python.exe" (
        echo [Setup] No venv found, creating...
        python -m venv .venv
        if errorlevel 1 (
            echo [ERROR] Failed to create venv. Is Python 3.10+ installed?
            pause >nul
            exit /b 1
        )
    )

    echo [Setup] Installing/verifying dependencies...
    ".venv\Scripts\python.exe" -m pip install --upgrade pip -q -i https://pypi.tuna.tsinghua.edu.cn/simple 2>nul
    ".venv\Scripts\python.exe" -m pip install -q -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple 2>nul
    if errorlevel 1 (
        echo [WARN] Some packages failed (likely PySpark). Continuing with sklearn fallback.
    )

    set BACKEND_PY=app\backend\.venv\Scripts\python.exe
    cd /d "%~dp0.."
) else (
    REM === Docker mode ===
    echo [Docker] demo-backend already running
)

echo.
echo [3/5] Checking PySpark training status...
docker exec spark-master ls /shared/models/ >nul 2>&1
if errorlevel 1 (
    echo        No models yet - will auto-train in background after startup.
) else (
    echo        PySpark models found - will load on backend startup.
)

echo.
echo [4/5] Starting FastAPI backend on port 8000...
if "%MODE%"=="1" (
    cd /d "%~dp0..\app\backend"
    start "smart-scenic-backend" /B "%BACKEND_PY%" main.py > "%TEMP%\backend.log" 2>&1
) else (
    docker compose logs -f demo-backend > "%TEMP%\backend.log" 2>&1
)
timeout /t 3 /nobreak >nul
echo        Backend log: %TEMP%\backend.log

:start_frontend
echo [5/5] Starting frontend HTTP server on port 8080...
cd /d "%~dp0..\app\frontend"
start "smart-scenic-frontend" /B python -m http.server 8080 > "%TEMP%\frontend.log" 2>&1
timeout /t 2 /nobreak >nul
cd /d "%~dp0.."

echo.
echo ==========================================
echo   Ready! Open in browser:
echo ==========================================
echo     Frontend:    http://localhost:8080
echo     API docs:    http://localhost:8000/docs
echo     System admin: http://localhost:8080/manage.html  (^> System tab)
echo ==========================================
echo.
echo Press Ctrl+C in the new window to stop servers.
echo Or run scripts\stop.bat
echo.
echo This window will close in 30 seconds (or press any key)...
timeout /t 30 >nul
exit /b 0