# PowerShell script to add servers to pgAdmin via REST API
# Run this from your host machine after pgAdmin is running

$pgAdminUrl = "http://localhost:5050"
$email = "admin@admin.com"
$password = "admin"

Write-Host "Waiting for pgAdmin to be ready..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# Wait for pgAdmin
$maxRetries = 30
$ready = $false
for ($i = 1; $i -le $maxRetries; $i++) {
    try {
        $response = Invoke-WebRequest -Uri "$pgAdminUrl/misc/ping" -Method GET -TimeoutSec 2 -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            Write-Host "pgAdmin is ready!" -ForegroundColor Green
            $ready = $true
            break
        }
    } catch {
        # Continue waiting
    }
    Start-Sleep -Seconds 1
}

if (-not $ready) {
    Write-Host "pgAdmin is not ready. Please ensure it's running." -ForegroundColor Red
    exit 1
}

# Get CSRF token first
Write-Host "Getting CSRF token..." -ForegroundColor Yellow
$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession

try {
    $browserPage = Invoke-WebRequest -Uri "$pgAdminUrl/browser/" `
        -WebSession $session `
        -ErrorAction Stop
    
    # Extract CSRF token from the page (it's usually in a meta tag or script)
    $csrfToken = $null
    if ($browserPage.Content -match 'csrf_token["\s:]+["'']([^"'']+)["'']') {
        $csrfToken = $matches[1]
    } elseif ($browserPage.Content -match 'name=["'']csrf_token["'']\s+value=["'']([^"'']+)["'']') {
        $csrfToken = $matches[1]
    }
    
    if (-not $csrfToken) {
        Write-Host "Could not extract CSRF token. Trying alternative method..." -ForegroundColor Yellow
    }
} catch {
    Write-Host "Warning: Could not get browser page: $_" -ForegroundColor Yellow
}

# Login
Write-Host "Logging in to pgAdmin..." -ForegroundColor Yellow

$loginBody = @{
    email = $email
    password = $password
}

# Add CSRF token if we found it
if ($csrfToken) {
    $loginBody['csrf_token'] = $csrfToken
}

try {
    $loginResponse = Invoke-WebRequest -Uri "$pgAdminUrl/authenticate/login" `
        -Method POST `
        -Body $loginBody `
        -ContentType "application/x-www-form-urlencoded" `
        -WebSession $session `
        -ErrorAction Stop
    
    # Check if login was successful
    $loginResult = $loginResponse.Content | ConvertFrom-Json
    if ($loginResult.success -eq 1 -or $loginResult.access_token) {
        Write-Host "Login successful!" -ForegroundColor Green
    } else {
        Write-Host "Login failed: $($loginResult.errormsg)" -ForegroundColor Red
        exit 1
    }
} catch {
    $errorDetails = $_.Exception.Response
    if ($errorDetails) {
        $reader = New-Object System.IO.StreamReader($errorDetails.GetResponseStream())
        $responseBody = $reader.ReadToEnd()
        Write-Host "Login failed: $responseBody" -ForegroundColor Red
    } else {
        Write-Host "Login failed: $_" -ForegroundColor Red
    }
    Write-Host "`nTrying alternative login method..." -ForegroundColor Yellow
    
    # Try with session-based login
    try {
        $loginResponse2 = Invoke-RestMethod -Uri "$pgAdminUrl/authenticate/login" `
            -Method POST `
            -Body $loginBody `
            -ContentType "application/x-www-form-urlencoded" `
            -WebSession $session `
            -ErrorAction Stop
        
        if ($loginResponse2.success -eq 1 -or $loginResponse2.access_token) {
            Write-Host "Login successful (alternative method)!" -ForegroundColor Green
        } else {
            Write-Host "Login still failed. Please add servers manually in pgAdmin UI." -ForegroundColor Red
            exit 1
        }
    } catch {
        Write-Host "All login methods failed. Please add servers manually:" -ForegroundColor Red
        Write-Host "  1. Open http://localhost:5050" -ForegroundColor Cyan
        Write-Host "  2. Right-click 'Servers' -> 'Register' -> 'Server'" -ForegroundColor Cyan
        Write-Host "  3. Host: postgres, Port: 5432, Database: airflow or dag_data" -ForegroundColor Cyan
        exit 1
    }
}

# Servers to add
$servers = @(
    @{
        name = "Local PostgreSQL - Airflow"
        host = "postgres"
        port = 5432
        maintenance_db = "airflow"
        username = "airflow"
        password = "airflow"
        ssl_mode = "prefer"
        comment = "Airflow metadata database"
        servergroup = 1
    },
    @{
        name = "Local PostgreSQL - DAG Data"
        host = "postgres"
        port = 5432
        maintenance_db = "dag_data"
        username = "airflow"
        password = "airflow"
        ssl_mode = "prefer"
        comment = "DAG data storage database"
        servergroup = 1
    }
)

# Add each server
Write-Host "`nAdding servers..." -ForegroundColor Yellow
$successCount = 0

foreach ($server in $servers) {
    try {
        $jsonBody = $server | ConvertTo-Json
        $response = Invoke-WebRequest -Uri "$pgAdminUrl/browser/server/obj/" `
            -Method POST `
            -Body $jsonBody `
            -ContentType "application/json" `
            -WebSession $session `
            -ErrorAction Stop
        
        Write-Host "  ✓ Added: $($server.name)" -ForegroundColor Green
        $successCount++
    } catch {
        if ($_.Exception.Response.StatusCode -eq 400) {
            Write-Host "  ⚠ Server '$($server.name)' may already exist" -ForegroundColor Yellow
        } else {
            Write-Host "  ✗ Failed to add '$($server.name)': $_" -ForegroundColor Red
        }
    }
}

Write-Host "`n✓ Successfully added $successCount/$($servers.Count) server(s)" -ForegroundColor Green
Write-Host "`nPlease refresh your pgAdmin browser to see the servers!" -ForegroundColor Cyan

