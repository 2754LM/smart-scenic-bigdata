@echo off
REM Smart Scenic BigData - Start Web Application
REM Double-click to run. Auto-installs Python deps.
cd /d "%~dp0.."

echo ==========================================
echo   Smart Scenic BigData - Web Application
echo ==========================================
echo.

REM Check big data platform
echo [1/5] Checking platform...
docker inspect hadoop-namenode >nul 2>&1
if errorlevel 1 (
    echo [WARNING] Platform not running. Run scripts\start.bat first.
    pause
)

REM Setup venv if needed
echo [2/5] Checking Python venv...
cd /d "%~dp0..\app\backend"
if not exist ".venv\Scripts\python.exe" (
    echo Creating venv...
    python -m venv .venv
    if errorlevel 1 (
        echo [ERROR] Python 3.10+ required.
        pause
        exit /b 1
    )
    echo Installing deps...
    ".venv\Scripts\python.exe" -m pip install --upgrade pip -q --timeout 30 -i https://pypi.tuna.tsinghua.edu.cn/simple
    ".venv\Scripts\python.exe" -m pip install -q -r requirements.txt --timeout 120 -i https://pypi.tuna.tsinghua.edu.cn/simple
    if errorlevel 1 (
        echo [WARNING] Some pip packages failed. Using fallback.
    )
    echo venv ready.
) else (
    echo venv exists at .venv
)
cd /d "%~dp0.."

REM Start backend
echo [3/5] Starting FastAPI backend...
cd /d "%~dp0..\app\backend"
start "smart-scenic-backend" /MIN ".venv\Scripts\python.exe" -m uvicorn main:app --host 0.0.0.0 --port 8000
timeout /t 5 /nobreak >nul
cd /d "%~dp0.."

REM Start frontend
echo [4/5] Starting frontend...
cd /d "%~dp0..\app\frontend"
start "smart-scenic-frontend" /MIN python -m http.server 8080
timeout /t 2 /nobreak >nul
cd /d "%~dp0.."

REM Verify
echo [5/5] Verifying...
docker inspect hadoop-namenode >nul 2>&1
if errorlevel 1 (
    echo [WARNING] Platform not running. Some features may not work.
)
echo.
echo ==========================================
echo   Ready!
echo     Frontend:  http://localhost:8080
echo     API docs:  http://localhost:8000/docs
echo     Admin:     http://localhost:8080/manage.html
echo ==========================================
echo.
echo Close this window to stop all servers.
echo Or press Ctrl+C to stop, then close.
echo.
ping -n 999999 127.0.0.1 >nul