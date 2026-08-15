<#
.SYNOPSIS
    Pipeline health check. Exits non-zero if anything is unhealthy.

.DESCRIPTION
    Runs the monitoring views from sql/10_production_hardening.sql and prints
    a status line per check. Designed to be run either by hand each morning or
    on a schedule with the -Quiet flag, where it only speaks when something is
    wrong.

    This is deliberately a script and not a monitoring platform. It closes the
    gap between "no observability at all" and "a real observability stack",
    which is the gap that matters when the alternative is nobody noticing the
    pipeline died overnight.

.EXAMPLE
    .\healthcheck.ps1
    .\healthcheck.ps1 -Quiet        # only output on failure
#>

[CmdletBinding()]
param(
    [string]$Container = "iot-postgres",
    [string]$User      = "iot_user",
    [string]$Database  = "iot_dashboard",
    [switch]$Quiet
)

$ErrorActionPreference = "Stop"
$failed = @()

function Write-Status {
    param([string]$Name, [string]$Status, [string]$Detail)

    $colour = switch ($Status) {
        "HEALTHY"     { "Green" }
        "OK"          { "Green" }
        "DEGRADED"    { "Yellow" }
        "OPEN"        { "Yellow" }
        default       { "Red" }
    }
    if (-not $Quiet -or $colour -ne "Green") {
        $label = $Status.PadRight(12)
        Write-Host "  [" -NoNewline
        Write-Host $label -ForegroundColor $colour -NoNewline
        Write-Host "] $Name" -NoNewline
        if ($Detail) { Write-Host "  -- $Detail" -ForegroundColor DarkGray }
        else { Write-Host "" }
    }
}

# --- 1. Containers -------------------------------------------------------
if (-not $Quiet) { Write-Host "`nInfrastructure" -ForegroundColor Cyan }

$expected = @("iot-zookeeper", "iot-kafka", "iot-postgres", "iot-adminer")
$running  = docker ps --format "{{.Names}}" 2>$null

foreach ($name in $expected) {
    if ($running -contains $name) {
        Write-Status $name "OK" ""
    } else {
        Write-Status $name "DOWN" "container not running"
        $failed += $name
    }
}

if ($failed.Count -gt 0) {
    Write-Host "`nContainers are down. Start them before checking the pipeline:" -ForegroundColor Red
    Write-Host "  docker compose up -d`n"
    exit 1
}

# --- 2. Pipeline ---------------------------------------------------------
if (-not $Quiet) { Write-Host "`nPipeline" -ForegroundColor Cyan }

$query = "SELECT check_name || '|' || status || '|' || detail FROM mart.v_health_dashboard;"
$raw = docker exec $Container psql -U $User -d $Database -t -A -c $query 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Host "  Could not query the database. Has 10_production_hardening.sql been applied?" -ForegroundColor Red
    Write-Host "  $raw" -ForegroundColor DarkGray
    exit 1
}

foreach ($line in $raw) {
    if (-not $line.Trim()) { continue }
    $parts = $line -split '\|', 3
    if ($parts.Count -lt 2) { continue }

    Write-Status $parts[0].Trim() $parts[1].Trim() $(if ($parts.Count -gt 2) { $parts[2].Trim() })
    if ($parts[1].Trim() -notin @("HEALTHY", "OK", "NO DATA")) {
        $failed += $parts[0].Trim()
    }
}

# --- 3. Recent failures --------------------------------------------------
$failQuery = @"
SELECT batch_id || ' at ' || TO_CHAR(completed_at, 'HH24:MI') || ': ' ||
       LEFT(COALESCE(error_detail, 'no detail'), 90)
FROM mart.etl_batch_log
WHERE status = 'FAILED' AND started_at > NOW() - INTERVAL '24 hours'
ORDER BY completed_at DESC LIMIT 5;
"@
$failures = docker exec $Container psql -U $User -d $Database -t -A -c $failQuery 2>$null

if ($failures -and ($failures | Where-Object { $_.Trim() })) {
    Write-Host "`nRecent batch failures" -ForegroundColor Yellow
    foreach ($f in $failures) {
        if ($f.Trim()) { Write-Host "  $($f.Trim())" -ForegroundColor DarkGray }
    }
}

# --- 4. Verdict ----------------------------------------------------------
if ($failed.Count -gt 0) {
    Write-Host "`n$($failed.Count) check(s) need attention: $($failed -join ', ')" -ForegroundColor Red
    Write-Host "Playbooks: docs/OPERATIONS_RUNBOOK.md`n"
    exit 1
}

if (-not $Quiet) { Write-Host "`nAll checks healthy.`n" -ForegroundColor Green }
exit 0
