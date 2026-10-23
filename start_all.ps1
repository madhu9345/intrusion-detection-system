<#
.SYNOPSIS
    All-in-One Launcher: Starts all Docker microservices and the Frontend Defense Dashboard.
#>

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  IR-IDS: Intelligent Real-Time Intrusion Detection System            " -ForegroundColor White
Write-Host "  Launching Complete Infrastructure + Real-Time Frontend              " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

# 1. Verify Docker is running
Write-Host "[1/3] Checking Docker daemon status..." -ForegroundColor Gray
docker info > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Docker is not running! Please start Docker Desktop first." -ForegroundColor Red
    exit 1
}

# 2. Start all backend Docker services
Write-Host "[2/3] Starting all Docker microservices (Kafka, Ingestion, Preprocessing, Feature Selection, Classifier, KAN-CVAE, Simulator)..." -ForegroundColor Yellow
docker compose -f docker/docker-compose.yml --profile simulator up -d --build

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to start Docker services!" -ForegroundColor Red
    exit 1
}

# 3. Launch the Frontend Dashboard
Write-Host ""
Write-Host "[3/3] Opening Defense Operations Console at http://localhost:8050 ..." -ForegroundColor Green
Start-Process "http://localhost:8050"

# Run Uvicorn
python -m uvicorn services.dashboard.app:app --host 0.0.0.0 --port 8050
