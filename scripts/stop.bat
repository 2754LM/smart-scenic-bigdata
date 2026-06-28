@echo off
chcp 65001 >nul
REM ============================================================
REM Smart Scenic BigData - Stop all services (Windows CMD)
REM ============================================================

cd /d "%~dp0\.."

echo Stopping all containers...
docker compose down

echo.
echo Stopped. Data is preserved in volumes.
echo Full cleanup: scripts\reset.bat
pause