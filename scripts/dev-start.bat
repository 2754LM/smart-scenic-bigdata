@echo off
REM Smart Scenic BigData - Development Server (Hot Reload)
REM Backend auto-restarts on .py changes. Frontend auto-refreshes.
REM Double-click to run. Close window to stop both servers.
cd /d "%~dp0.."

echo ==========================================
echo   Smart Scenic BigData - DEV MODE
echo   Backend  : uvicorn --reload (port 8000)
echo   Frontend : livereload (port 8080)
echo ==========================================
echo.

REM Check platform
docker inspect hadoop-namenode >nul 2>&1
if errorlevel 1 (
    echo [WARNING] Platform not running. Run scripts\start.bat first.
    pause
)

REM Check venv
cd /d "%~dp0..\app\backend"
if not exist ".venv\Scripts\python.exe" (
    echo Creating venv...
    python -m venv .venv
    ".venv\Scripts\python.exe" -m pip install --upgrade pip -q --timeout 30 -i https://pypi.tuna.tsinghua.edu.cn/simple
    ".venv\Scripts\python.exe" -m pip install -q -r requirements.txt --timeout 120 -i https://pypi.tuna.tsinghua.edu.cn/simple
    if errorlevel 1 (
        echo [WARNING] Some packages failed. Using fallback.
    )
)
cd /d "%~dp0.."

REM Start backend with hot reload
echo [1/2] Starting backend (--reload)...
cd /d "%~dp0..\app\backend"
start "smart-scenic-backend" /MIN ".venv\Scripts\python.exe" -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
timeout /t 5 /nobreak >nul
cd /d "%~dp0.."

REM Start frontend with livereload
echo [2/2] Starting frontend (livereload)...
start "smart-scenic-frontend" /MIN ".venv\Scripts\python.exe" "%~dp0..\scripts\dev-frontend.py"
timeout /t 3 /nobreak >nul

echo.
echo ==========================================
echo   DEV MODE READY
echo ==========================================
echo     Frontend:  http://localhost:8080
echo     API docs:  http://localhost:8000/docs
echo.
echo   Backend auto-restarts on .py changes
echo   Frontend auto-refreshes on .html/.js/.css changes
echo.
echo   Close this window to stop both servers.
echo ==========================================
echo.

ping -n 999999 127.0.0.1 >nul