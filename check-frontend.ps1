Write-Host "Checking Docker containers..." -ForegroundColor Cyan
docker ps -a | Select-String "frontend"

Write-Host "`nChecking docker-compose services..." -ForegroundColor Cyan
docker-compose ps

Write-Host "`nChecking frontend logs (last 30 lines)..." -ForegroundColor Cyan
docker-compose logs --tail=30 frontend

Write-Host "`nChecking if port 3000 is in use..." -ForegroundColor Cyan
netstat -ano | Select-String ":3000"


