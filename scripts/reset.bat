@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

REM ============================================================
REM  Smart Scenic BigData - Full Reset
REM  DESTRUCTIVE: Deletes ALL containers + volumes + local data.
REM ============================================================

cd /d "%~dp0\.."

echo.
echo ==========================================================
echo   WARNING: Full Reset
echo   ALL containers, volumes, and local data will be deleted!
echo ==========================================================
echo.

set /p CONFIRM=Type yes to continue:
if /i not "%CONFIRM%"=="yes" (
    echo Cancelled.
    pause
    exit /b 0
)

echo.
echo [1/4] Removing containers and volumes...
docker compose down -v --remove-orphans

echo [2/4] Cleaning local data and logs...
if exist data rmdir /s /q data >nul 2>&1
if exist logs rmdir /s /q logs >nul 2>&1
mkdir data\raw_data logs >nul 2>&1
echo   - Removed data/ and logs/; created empty data/raw_data/

echo [3/4] Pruning Docker system (containers, networks, dangling images)...
docker system prune -f

echo [4/4] Done.
echo.
echo ==========================================================
echo   Reset complete. Next: copy 4 CSV files to data\raw_data\
echo   then double-click scripts\start.bat to start fresh.
echo ==========================================================
echo.
pause
