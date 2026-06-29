@echo off
REM Smart Scenic BigData - Full Reset
REM DANGER: Deletes ALL persistent data!
cd /d "%~dp0.."

echo ==========================================
echo   WARNING: Full Reset
echo   All containers and data will be deleted!
echo ==========================================
echo.

set /p CONFIRM=Type yes to continue:
if /i not "%CONFIRM%"=="yes" (
    echo Cancelled.
    pause
    exit /b 0
)

echo [1/3] Removing containers and volumes...
docker compose down -v --remove-orphans

echo [2/3] Cleaning local data...
if exist data rmdir /s /q data >nul 2>&1
if exist logs rmdir /s /q logs >nul 2>&1
mkdir data logs >nul 2>&1

echo [3/3] Cleaning Docker resources...
docker system prune -f

echo.
echo Done. Double-click scripts\start.bat to restart.
pause