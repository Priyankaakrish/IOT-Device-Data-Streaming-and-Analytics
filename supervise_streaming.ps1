<#
.SYNOPSIS
    Keeps the Spark streaming job running, restarting it if it exits.

.DESCRIPTION
    The job is checkpointed, so restarting resumes from the last committed
    offset without data loss or duplication -- the sink is idempotent. What
    was missing was anything to notice the process had died.

    This is a supervisor, not an orchestrator. It handles "the process
    stopped"; it does not handle dependencies, backfills, scheduling or
    alerting. For that, see docs/PRODUCTION_READINESS.md.

    Register as a scheduled task to start at boot:

      $action  = New-ScheduledTaskAction -Execute "powershell.exe" `
                   -Argument "-NoProfile -ExecutionPolicy Bypass -File C:\path\to\supervise_streaming.ps1"
      $trigger = New-ScheduledTaskTrigger -AtStartup
      Register-ScheduledTask -TaskName "IoT Streaming" -Action $action `
                   -Trigger $trigger -RunLevel Highest

.EXAMPLE
    .\supervise_streaming.ps1
    .\supervise_streaming.ps1 -MaxRestarts 20
#>

[CmdletBinding()]
param(
    [int]$MaxRestarts   = 10,
    [int]$BackoffSeconds = 30,
    [string]$LogDir     = "logs"
)

$ErrorActionPreference = "Stop"

# The script lives at the project root, so PSScriptRoot is the root.
Set-Location $PSScriptRoot

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

$env:PYSPARK_SUBMIT_ARGS = "--packages " +
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," +
    "org.postgresql:postgresql:42.7.3 pyspark-shell"

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $line = "{0} | {1,-7} | {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level, $Message
    Write-Host $line
    Add-Content -Path (Join-Path $LogDir "supervisor.log") -Value $line
}

# A host that suspends kills the JDBC socket, and the job dies with a read
# timeout roughly 45 minutes later -- long enough after the cause that it
# looks like an unrelated crash. Check before starting rather than
# diagnosing it again.
$standby = (powercfg /query SCHEME_CURRENT SUB_SLEEP STANDBYIDLE 2>$null |
            Select-String "Current AC Power Setting Index").ToString()
if ($standby -and $standby -notmatch "0x00000000") {
    Write-Log "Host sleep is enabled on AC power. The job will die with 'Read timed out'." "WARN"
    Write-Log "Fix with: powercfg /change standby-timeout-ac 0" "WARN"
}

$restarts = 0

while ($restarts -le $MaxRestarts) {
    if ($restarts -gt 0) {
        # Linear backoff. If the broker is down, hammering it does not help.
        $wait = $BackoffSeconds * [Math]::Min($restarts, 5)
        Write-Log "Restart $restarts of $MaxRestarts in ${wait}s" "WARN"
        Start-Sleep -Seconds $wait
    }

    # Do not start against a dead broker; it just burns a restart budget.
    $kafkaUp = (docker ps --format "{{.Names}}" 2>$null) -contains "iot-kafka"
    if (-not $kafkaUp) {
        Write-Log "iot-kafka is not running. Attempting docker compose up -d" "WARN"
        docker compose up -d 2>&1 | Out-Null
        Start-Sleep -Seconds 20
    }

    Write-Log "Starting streaming_job_star.py"
    $started = Get-Date

    & py -3.11 spark\streaming_job_star.py 2>&1 |
        Tee-Object -FilePath (Join-Path $LogDir "streaming.log") -Append

    $exit = $LASTEXITCODE
    $ranFor = (New-TimeSpan -Start $started -End (Get-Date)).TotalMinutes

    if ($exit -eq 0) {
        Write-Log "Job exited cleanly after $([math]::Round($ranFor,1)) minutes. Not restarting."
        break
    }

    Write-Log "Job exited with code $exit after $([math]::Round($ranFor,1)) minutes" "ERROR"

    # A job that dies within a minute is failing to start, not crashing under
    # load. Restarting will not fix a bad config or a missing driver.
    if ($ranFor -lt 1 -and $restarts -ge 2) {
        Write-Log "Three rapid failures. This is a startup fault, not a transient one." "ERROR"
        Write-Log "Check logs/streaming.log and docs/OPERATIONS_RUNBOOK.md" "ERROR"
        exit 1
    }

    $restarts++
}

if ($restarts -gt $MaxRestarts) {
    Write-Log "Restart limit reached. Giving up." "ERROR"
    exit 1
}
