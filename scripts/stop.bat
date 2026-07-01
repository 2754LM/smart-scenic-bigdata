@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

REM ============================================================
REM  Smart Scenic BigData - Stop All Containers
REM  Data is preserved in named volumes.
REM ============================================================

cd /d "%~dp0\.."

echo.
echo ==========================================================
echo   Smart Scenic BigData - Stopping
echo ==========================================================
echo.

docker info >nul 2>&1
if errorlevel 1 (
    echo [WARN] Docker not running. Nothing to stop.
    timeout /t 3 >nul
    exit /b 0
)

echo Stopping 15 containers (data preserved)...
docker compose down
if errorlevel 1 (
    echo [WARN] docker compose down returned non-zero.
)

echo.
echo ==========================================================
echo   Stopped. Data preserved in volumes.
echo ==========================================================
echo.
echo To restart:  double-click scripts\start.bat
echo Full reset:  double-click scripts\reset.bat
echo.

timeout /t 5 >nul
