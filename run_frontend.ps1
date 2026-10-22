<#
.SYNOPSIS
    Launches the IR-IDS Defense Operations Console Dashboard.
#>

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Intelligent Real-Time IDS (IR-IDS) - Frontend Dashboard   " -ForegroundColor White
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

Write-Host "[1/2] Connecting to local Kafka streaming broker at localhost:9092..." -ForegroundColor Gray
Write-Host "[2/2] Starting Uvicorn Web Server on http://localhost:8050 ..." -ForegroundColor Green
Write-Host ""

# Open browser automatically
Start-Process "http://localhost:8050"

# Run Uvicorn
python -m uvicorn services.dashboard.app:app --host 0.0.0.0 --port 8050
