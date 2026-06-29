@echo off
REM Smart Scenic BigData - Stop All Services
REM Double-click to run. Data is preserved.
cd /d "%~dp0.."

echo ==========================================
echo   Smart Scenic BigData - Stopping
echo ==========================================
echo.

taskkill /FI "WINDOWTITLE eq smart-scenic-backend*" /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq smart-scenic-frontend*" /T /F >nul 2>&1

echo Stopping all containers...
docker compose down

echo.
echo ==========================================
echo   Stopped. Data preserved in volumes.
echo ==========================================
echo.
echo To restart: double-click scripts\start.bat
echo Full reset: double-click scripts\reset.bat
echo.

ping -n 5 127.0.0.1 >nul