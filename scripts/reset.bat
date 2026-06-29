@echo off
chcp 65001 >nul
REM ============================================================
REM Smart Scenic BigData - Full Reset (Windows CMD)
REM DANGER: deletes all persistent data!
REM ============================================================

cd /d "%~dp0\.."

echo WARNING: All containers and volumes will be deleted.
set /p CONFIRM=Type 'yes' to continue:
if /i not "%CONFIRM%"=="yes" (
    echo Cancelled.
    pause
    exit /b 0
)

echo [1/3] Removing containers and volumes...
docker compose down -v --remove-orphans

echo [2/3] Cleaning local data dirs...
if exist data rmdir /s /q data
if exist logs rmdir /s /q logs
mkdir data logs

echo [3/3] Cleaning Docker resources...
docker system prune -f

echo.
echo Reset complete.
echo Restart: scripts\start.bat
pause