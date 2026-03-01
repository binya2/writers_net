Write-Host "================================" -ForegroundColor Cyan
Write-Host "   Services Health Check (PS)" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan

# Helper function to print health status
function Show-Status {
    param($Service, $IsUp)
    $Color = if ($IsUp) { "Green" } else { "Red" }
    $Status = if ($IsUp) { "UP" } else { "DOWN" }
    Write-Host "$($Service.PadRight(15)): " -NoNewline
    Write-Host $Status -ForegroundColor $Color
}

# MongoDB Check
try {
    $mongoRes = docker exec mongo mongosh --quiet --eval "db.adminCommand('ping').ok" 2>$null
    Show-Status "MongoDB" ($mongoRes -match "1")
} catch {
    Show-Status "MongoDB" $false
}

# Elasticsearch Check
try {
    $esRes = Invoke-RestMethod -Uri "http://localhost:9200/_cluster/health" -ErrorAction SilentlyContinue
    Show-Status "Elasticsearch" ($esRes.status -in @("green", "yellow"))
} catch {
    Show-Status "Elasticsearch" $false
}

# Kafka Check
try {
    docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >$null 2>&1
    Show-Status "Kafka" ($LASTEXITCODE -eq 0)
} catch {
    Show-Status "Kafka" $false
}

# API Service Check
try {
    $apiRes = Invoke-WebRequest -Uri "http://localhost:8000/docs" -Method Get -ErrorAction SilentlyContinue
    Show-Status "API Service" ($apiRes.StatusCode -eq 200)
} catch {
    Show-Status "API Service" $false
}

# Dashboard Service Check
try {
    $dashRes = Invoke-WebRequest -Uri "http://localhost:8501/_stcore/health" -Method Get -ErrorAction SilentlyContinue
    Show-Status "Dashboard UI" ($dashRes.Content -match "ok")
} catch {
    Show-Status "Dashboard UI" $false
}

Write-Host "================================" -ForegroundColor Cyan
