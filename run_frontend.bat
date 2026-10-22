@echo off
title IR-IDS Cyber Defense Dashboard Launcher
color 0b
echo ============================================================
echo   Intelligent Real-Time IDS (IR-IDS) - Frontend Dashboard
echo ============================================================
echo.
echo [1/2] Checking Python and Uvicorn...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python is not installed or not in PATH!
    pause
    exit /b 1
)

echo [2/2] Launching Defense Operations Console on http://localhost:8050 ...
start "" "http://localhost:8050"

python -m uvicorn services.dashboard.app:app --host 0.0.0.0 --port 8050
pause
