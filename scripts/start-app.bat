@echo off
REM ============================================================
REM Smart Scenic BigData - Demo App Starter
REM Starts FastAPI backend (port 8000) + frontend HTTP server (8080)
REM Usage: scripts\start-app.bat
REM ============================================================

echo ==========================================
echo   Smart Scenic BigData - Demo App
echo ==========================================

REM Start backend
echo [1/3] Starting FastAPI backend on port 8000...
cd /d "%~dp0..\app\backend"
start "smart-scenic-backend" /B python main.py > "C:\Users\kano\AppData\Local\Temp\backend.log" 2>&1
timeout /t 4 /nobreak >nul

REM Start frontend HTTP server
echo [2/3] Starting frontend HTTP server on port 8080...
cd /d "%~dp0..\app\frontend"
start "smart-scenic-frontend" /B python -m http.server 8080 > "C:\Users\kano\AppData\Local\Temp\frontend.log" 2>&1
timeout /t 2 /nobreak >nul

echo [3/3] Done.
echo.
echo ==========================================
echo   Open in browser:
echo     Frontend:  http://localhost:8080
echo     API docs:  http://localhost:8000/docs
echo ==========================================
echo.
echo Press any key to stop both servers...
pause >nul

echo Stopping...
taskkill /FI "WINDOWTITLE eq smart-scenic-backend*" /T /F >nul 2>nul
taskkill /FI "WINDOWTITLE eq smart-scenic-frontend*" /T /F >nul 2>nul
echo Done.