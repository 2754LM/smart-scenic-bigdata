@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

REM ============================================================
REM  Install Python deps for local IDE / pytest only.
REM  Does NOT start any service (demo-backend is in Docker).
REM  Use scripts\start.bat for the full platform.
REM ============================================================

cd /d "%~dp0\..\app\backend"

echo.
echo ==========================================================
echo   Installing Python deps (local venv) for IDE
echo ==========================================================
echo.

if not exist ".venv\Scripts\python.exe" (
    echo [1/3] Creating venv at .venv\ ...
    python -m venv .venv
    if errorlevel 1 (
        echo [FAIL] Python 3.10+ required. Install from python.org.
        pause
        exit /b 1
    )
) else (
    echo [1/3] venv already exists.
)

echo [2/3] Upgrading pip ...
".venv\Scripts\python.exe" -m pip install --upgrade pip -q --timeout 30 -i https://pypi.tuna.tsinghua.edu.cn/simple
if errorlevel 1 (
    echo [WARN] pip upgrade failed (using cached). Continuing.
)

echo [3/3] Installing requirements.txt ...
".venv\Scripts\python.exe" -m pip install -q -r requirements.txt --timeout 120 -i https://pypi.tuna.tsinghua.edu.cn/simple
if errorlevel 1 (
    echo [WARN] Some packages failed to install. Try: .venv\Scripts\activate ^&^& pip install -r requirements.txt
) else (
    echo [OK]   All dependencies installed.
)

echo.
echo ==========================================================
echo   Done. You can now use the .venv in your IDE.
echo   The full platform runs in Docker via scripts\start.bat
echo ==========================================================
echo.
pause
