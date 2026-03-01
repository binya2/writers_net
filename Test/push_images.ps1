$DockerUser = "beni2"
$Tag = "latest"

$Services = @(
    @{ Name = "api-service"; Dockerfile = "API_app/Dockerfile" },
    @{ Name = "ocr-service"; Dockerfile = "OCREngine/Dockerfile" },
    @{ Name = "clean-service"; Dockerfile = "CleanService/Dockerfile" },
    @{ Name = "analytics-service"; Dockerfile = "AnalyticsService/Dockerfile" },
    @{ Name = "indexer-service"; Dockerfile = "IndexerService/Dockerfile" },
    @{ Name = "dashboard-service"; Dockerfile = "DashboardService/Dockerfile" }
)

Write-Host "Logging into Docker Hub..." -ForegroundColor Cyan
docker login

foreach ($Service in $Services) {
    $ImageName = "$DockerUser/writers_net-$($Service.Name):$Tag"
    Write-Host "------------------------------------------------" -ForegroundColor Yellow
    Write-Host "Building $($Service.Name)..."
    Write-Host "Image: $ImageName"
    
    docker build -t $ImageName -f $Service.Dockerfile .
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Pushing $ImageName to Docker Hub..." -ForegroundColor Green
        docker push $ImageName
    } else {
        Write-Host "Failed to build $($Service.Name). Skipping push." -ForegroundColor Red
    }
}

Write-Host "Done!" -ForegroundColor Cyan
