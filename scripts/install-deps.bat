@echo off
REM ============================================================
REM Smart Scenic BigData - Python Dependency Installer
REM Creates app\backend\.venv and installs requirements
REM Usage: scripts\install-deps.bat
REM ============================================================

setlocal enabledelayedexpansion
cd /d "%~dp0..\app\backend"

echo ==========================================
echo   Smart Scenic BigData - Python Deps
echo ==========================================
echo.

REM ---------- Step 1: Check Python ----------
echo [1/5] Checking Python...
where python >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found in PATH.
    echo Please install Python 3.10+ from https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during installation.
    exit /b 1
)
python --version
echo.

REM ---------- Step 2: Check Java (needed for PySpark on host) ----------
echo [2/5] Checking Java (for PySpark)...
where java >nul 2>&1
if errorlevel 1 (
    echo [WARN] Java not found. PySpark may not work on host.
    echo         Backend will fallback to sklearn.
    echo         Install JDK 1.8 / 11 / 17 to enable PySpark.
    echo.
) else (
    java -version 2>&1
    echo.
)

REM ---------- Step 3: Create venv ----------
echo [3/5] Creating virtual environment...
if exist ".venv\Scripts\python.exe" (
    echo        .venv already exists, skip create
) else (
    python -m venv .venv
    if errorlevel 1 (
        echo [ERROR] Failed to create venv
        exit /b 1
    )
    echo        .venv created
)
echo.

REM ---------- Step 4: Install requirements ----------
echo [4/5] Installing requirements...
".venv\Scripts\python.exe" -m pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple >nul 2>&1
".venv\Scripts\python.exe" -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
if errorlevel 1 (
    echo [WARN] Some packages failed to install (probably PySpark + Java issues)
    echo         Backend can still run with sklearn fallback.
    echo.
)
echo.

REM ---------- Step 5: Verify ----------
echo [5/5] Verifying installation...
".venv\Scripts\python.exe" -c "
import sys
print('Python:', sys.version.split()[0])
try:
    import fastapi; print('fastapi:    OK')
except: print('fastapi:    MISSING')
try:
    import pymysql; print('pymysql:    OK')
except: print('pymysql:    MISSING')
try:
    import kafka; print('kafka-python: OK')
except: print('kafka-python: MISSING')
try:
    import sklearn; print('scikit-learn: OK')
except: print('scikit-learn: MISSING')
try:
    import pyspark; print('pyspark:    OK')
except: print('pyspark:    MISSING (fallback sklearn)')
try:
    import pandas; print('pandas:     OK')
except: print('pandas:     MISSING')
"

echo.
echo ==========================================
echo   Done. venv at: %CD%\.venv
echo ==========================================
echo.
echo Next step:
echo   scripts\start-app.bat
echo.
endlocal