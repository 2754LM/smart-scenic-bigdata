@echo off
REM ============================================================
REM Smart Scenic BigData - Stop all services
REM Double-click to run. Data is preserved.
REM ============================================================
chcp 65001 >nul
cd /d "%~dp0.."

echo ==========================================
echo   Smart Scenic BigData - Stopping
echo ==========================================
echo.

REM Stop frontend/backend processes
taskkill /FI "WINDOWTITLE eq smart-scenic-backend*" /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq smart-scenic-frontend*" /T /F >nul 2>&1

echo Stopping all containers...
docker compose down

echo.
echo ==========================================
echo   Stopped. Data is preserved in volumes.
echo ==========================================
echo.
echo Full cleanup (deletes ALL data): scripts\reset.bat
echo Restart: scripts\start.bat + scripts\start-app.bat
echo.
echo This window will close in 5 seconds...
timeout /t 5 >nul
exit /b 0