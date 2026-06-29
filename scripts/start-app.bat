@echo off
REM ============================================================
REM Smart Scenic BigData - Web App Starter
REM 自动检测环境 + 装依赖 + 启动后端 + 前端
REM Usage: scripts\start-app.bat
REM ============================================================

setlocal enabledelayedexpansion
cd /d "%~dp0.."

echo ==========================================
echo   Smart Scenic BigData - Web App
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

REM ---------- Step 2: 准备后端（自动装依赖） ----------
if "%MODE%"=="1" (
    REM === Local Python 模式 ===
    echo [2/5] Setting up local Python venv...
    cd /d "%~dp0..\app\backend"

    if not exist ".venv\Scripts\python.exe" (
        echo        No venv found. Installing dependencies...
        call :install_python_deps
        if errorlevel 1 exit /b 1
    ) else (
        echo        venv ready at app\backend\.venv
        REM 检查 requirements.txt 是否变了（简化：总是 pip install --upgrade）
        ".venv\Scripts\python.exe" -m pip install -q --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple >nul 2>&1
        ".venv\Scripts\python.exe" -m pip install -q -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple >nul 2>&1
        if errorlevel 1 (
            echo        [WARN] Some packages failed to update, using existing
        ) else (
            echo        All dependencies up-to-date
        )
    )
    set BACKEND_PY=app\backend\.venv\Scripts\python.exe
    cd /d "%~dp0.."
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
cd /d "%~dp0..\app\backend"
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
echo     Admin:     http://localhost:8080/manage.html  (^> System tab)
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
goto :eof


REM ============================================================
REM 内部函数：自动装 Python 依赖
REM ============================================================
:install_python_deps
setlocal enabledelayedexpansion
cd /d "%~dp0..\app\backend"

echo.
echo    --- Installing Python dependencies ---

REM 1. 检查 Python
where python >nul 2>&1
if errorlevel 1 (
    echo    [ERROR] Python not found in PATH.
    echo    Install Python 3.10+ from https://www.python.org/downloads/
    echo    Make sure to check "Add Python to PATH" during installation.
    exit /b 1
)
echo    Python: && python --version

REM 2. 创建 venv
echo    Creating venv...
python -m venv .venv
if errorlevel 1 (
    echo    [ERROR] Failed to create venv
    exit /b 1
)

REM 3. 装 pip + requirements
echo    Installing requirements from requirements.txt...
".venv\Scripts\python.exe" -m pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple
".venv\Scripts\python.exe" -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
if errorlevel 1 (
    echo    [WARN] Some packages failed to install (likely PySpark or Java related)
    echo    Backend can still run with sklearn fallback.
)

REM 4. 验证
echo.
echo    Verifying installation:
".venv\Scripts\python.exe" -c "
import sys
print('   Python:', sys.version.split()[0])
for mod in ['fastapi', 'pymysql', 'kafka', 'sklearn', 'pandas']:
    try:
        __import__(mod)
        print(f'   {mod:12s} OK')
    except ImportError:
        print(f'   {mod:12s} MISSING')
try:
    import pyspark
    print('   pyspark     OK')
except ImportError:
    print('   pyspark     MISSING (fallback sklearn)')
"
echo.
echo    --- Done ---
endlocal & exit /b 0