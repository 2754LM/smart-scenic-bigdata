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

echo Stopping 17 containers (data preserved)...
docker compose down
if errorlevel 1 (
    echo [WARN] docker compose down returned non-zero. Some containers may still be running.
)
if errorlevel 1 (
    echo [WARN] docker compose down returned non-zero. Some containers may still be running.
)

echo.
echo ==========================================================
echo   Stopped. Data preserved in volumes.
echo ==========================================================
echo.
echo To restart:  double-click scripts\start.bat
echo Full reset:   double-click scripts\reset.bat
echo.

timeout /t 5 >nul
