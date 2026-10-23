@echo off
title IR-IDS Cyber Defense Platform - All-in-One Launcher
color 0a

echo ======================================================================
echo   IR-IDS: Intelligent Real-Time Intrusion Detection System
echo   Launching Complete Infrastructure + Real-Time Frontend
echo ======================================================================
echo.

echo [1/3] Checking Docker daemon status...
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker is not running! Please start Docker Desktop first.
    pause
    exit /b 1
)

echo [2/3] Starting all Docker microservices (Kafka, Ingestion, Preprocessing,
echo       Causal Feature Selection, Shapley Classifier, KAN-CVAE, Simulator)...
echo.
docker compose -f docker/docker-compose.yml --profile simulator up -d --build

if %errorlevel% neq 0 (
    echo [ERROR] Failed to start Docker services!
    pause
    exit /b 1
)

echo.
echo [3/3] Launching Defense Operations Console on http://localhost:8050 ...
start "" "http://localhost:8050"

python -m uvicorn services.dashboard.app:app --host 0.0.0.0 --port 8050
pause
