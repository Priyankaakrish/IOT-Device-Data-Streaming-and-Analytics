<#
.SYNOPSIS
    Runs the periodic batch jobs that sit alongside the streaming pipeline.

.DESCRIPTION
    The streaming job is continuous and supervised. This script covers the
    other half of the platform: the finite, periodic work that genuinely has
    schedules and dependencies.

        snowflake-refresh   daily      snow.refresh_from_star()
        scd2-detect         daily      dim_machine attribute drift
        health-report       hourly     mart.v_health_dashboard
        dlq-report          hourly     unreplayed dead letters
        retention           monthly    drop sensor partitions

    Every run is written to mart.etl_batch_log with the same job_name /
    status / error_detail contract the streaming job uses, so lineage for
    batch and streaming lives in one table rather than two.

    This is what an orchestrator would call. It is deliberately not an
    orchestrator: five periodic SQL calls do not justify a scheduler, a
    webserver and a metadata database. If those jobs later acquire real
    dependencies or backfill requirements, each -Job here becomes one
    Airflow task and the SQL does not change.

.PARAMETER Job
    Which job to run. 'daily', 'hourly' and 'monthly' run a set;
    'all' runs everything except retention, which is destructive.

.PARAMETER DryRun
    Print the SQL that would run without executing it.

.EXAMPLE
    .\run_batch_jobs.ps1 -Job hourly
    .\run_batch_jobs.ps1 -Job daily
    .\run_batch_jobs.ps1 -Job retention -RetainMonths 6
    .\run_batch_jobs.ps1 -Job snowflake-refresh -DryRun
#>

[CmdletBinding()]
param(
    [ValidateSet("all", "hourly", "daily", "monthly",
                 "snowflake-refresh", "scd2-detect", "health-report",
                 "dlq-report", "retention")]
    [string]$Job = "all",

    [int]$RetainMonths = 6,
    [string]$Container = "iot-postgres",
    [string]$User      = "iot_user",
    [string]$Database  = "iot_dashboard",
    [switch]$DryRun,
    [switch]$Quiet
)

$ErrorActionPreference = "Stop"
$script:failures = 0

function Write-Line {
    param([string]$Text, [string]$Colour = "Gray")
    if (-not $Quiet) { Write-Host $Text -ForegroundColor $Colour }
}

function Invoke-Sql {
    <#
        Runs SQL and returns its output. ON_ERROR_STOP=1 matters: without it
        psql reports success even when a statement failed, which would let
        this script log SUCCESS for a job that did nothing.
    #>
    param([string]$Sql, [switch]$Tuples)

    $args = @("-U", $User, "-d", $Database, "-v", "ON_ERROR_STOP=1")
    if ($Tuples) { $args += @("-t", "-A") }
    $args += @("-c", $Sql)

    $output = docker exec $Container psql @args 2>&1
    if ($LASTEXITCODE -ne 0) { throw ($output | Out-String).Trim() }
    return $output
}

function Invoke-BatchJob {
    <#
        Wraps a job with lineage. The log row is written before the work
        starts so a job that crashes the host still leaves a RUNNING row --
        an absent row and a failed row mean different things.
    #>
    param(
        [string]$Name,
        [string]$Sql,
        [string]$Description
    )

    Write-Line "`n> $Name  --  $Description" "Cyan"

    if ($DryRun) {
        Write-Line $Sql.Trim() "DarkGray"
        return
    }

    $batchId = (Get-Date -Format "yyyyMMddHHmmss")

    try {
        Invoke-Sql "INSERT INTO mart.etl_batch_log (job_name, batch_id, status)
                    VALUES ('$Name', '$batchId', 'RUNNING');" | Out-Null

        $result = Invoke-Sql -Sql $Sql -Tuples

        Invoke-Sql "UPDATE mart.etl_batch_log
                    SET completed_at = NOW(), status = 'SUCCESS'
                    WHERE job_name = '$Name' AND batch_id = '$batchId';" | Out-Null

        foreach ($line in $result) {
            if ($line.ToString().Trim()) { Write-Line "  $($line.ToString().Trim())" }
        }
        Write-Line "  OK" "Green"
    }
    catch {
        $script:failures++
        # Doubling single quotes: Postgres error text routinely contains them,
        # and an unescaped literal turns a logged failure into a second one.
        $detail = ($_.Exception.Message -replace "'", "''")
        if ($detail.Length -gt 2000) { $detail = $detail.Substring(0, 2000) }

        try {
            Invoke-Sql "UPDATE mart.etl_batch_log
                        SET completed_at = NOW(), status = 'FAILED',
                            error_detail = '$detail'
                        WHERE job_name = '$Name' AND batch_id = '$batchId';" | Out-Null
        } catch { }

        Write-Line "  FAILED: $($_.Exception.Message)" "Red"
    }
}

# ---------------------------------------------------------------------------
# Job definitions
# ---------------------------------------------------------------------------

function Job-SnowflakeRefresh {
    # snow is a derived copy, not a second source of truth. It drifts unless
    # refreshed, and because fact_production_hourly accumulates in place the
    # drift is invisible to a row-count check -- values change while the
    # count stays flat. refresh_from_star() returns an IN SYNC / DRIFT verdict.
    Invoke-BatchJob -Name "batch_snowflake_refresh" `
        -Description "Refresh snow.* from mart.* and report drift" `
        -Sql "SELECT table_name || ': ' || row_count || ' (' || verdict || ')'
              FROM snow.refresh_from_star();"
}

function Job-Scd2Detect {
    # dim_machine carries row_hash for change detection, but nothing compares
    # it automatically. This surfaces machines whose source attributes have
    # moved away from the current SCD2 version, which is the trigger for
    # inserting a new version.
    Invoke-BatchJob -Name "batch_scd2_detect" `
        -Description "Report dim_machine attribute drift against source" `
        -Sql @"
SELECT COALESCE(
    STRING_AGG(m.machine_id || ' (' || m.machine_name || ')', ', '),
    'no drift detected'
)
FROM mart.dim_machine m
JOIN public.machines s ON s.machine_id = m.machine_id
WHERE m.is_current
  AND (m.machine_name IS DISTINCT FROM s.machine_name
    OR m.machine_type IS DISTINCT FROM s.machine_type);
"@
}

function Job-HealthReport {
    Invoke-BatchJob -Name "batch_health_report" `
        -Description "Pipeline health checks" `
        -Sql "SELECT RPAD(check_name, 26) || RPAD(status, 14) || detail
              FROM mart.v_health_dashboard;"
}

function Job-DlqReport {
    Invoke-BatchJob -Name "batch_dlq_report" `
        -Description "Dead letters awaiting replay" `
        -Sql @"
SELECT COALESCE(
    STRING_AGG(failure_reason || ': ' || n, ' | '),
    'no unreplayed dead letters'
)
FROM (
    SELECT failure_reason, COUNT(*) AS n
    FROM mart.dlq_sensor_reading
    WHERE replayed_at IS NULL
    GROUP BY failure_reason
) t;
"@
}

function Job-Retention {
    # Destructive and not reversible, so it is excluded from -Job all and
    # requires explicit confirmation.
    Write-Line "`n> batch_retention  --  Drop sensor partitions older than $RetainMonths months" "Yellow"

    if (-not $DryRun) {
        $preview = Invoke-Sql -Tuples -Sql @"
SELECT c.relname
FROM pg_class c
JOIN pg_inherits i   ON i.inhrelid = c.oid
JOIN pg_class parent ON parent.oid = i.inhparent
WHERE parent.relname = 'fact_sensor_reading'
  AND c.relname ~ '^fact_sensor_reading_[0-9]{6}$'
  AND TO_DATE(RIGHT(c.relname, 6), 'YYYYMM')
      < DATE_TRUNC('month', NOW()) - INTERVAL '$RetainMonths months';
"@
        $targets = @($preview | Where-Object { $_.ToString().Trim() })

        if ($targets.Count -eq 0) {
            Write-Line "  Nothing older than $RetainMonths months. Nothing to do." "Green"
            return
        }

        Write-Line "  Would drop: $($targets -join ', ')" "Yellow"
        $answer = Read-Host "  Type DROP to confirm"
        if ($answer -ne "DROP") {
            Write-Line "  Cancelled." "Yellow"
            return
        }
    }

    Invoke-BatchJob -Name "batch_retention" `
        -Description "Drop partitions older than $RetainMonths months" `
        -Sql "SELECT * FROM mart.drop_sensor_partitions_older_than($RetainMonths);"
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

if (-not (docker ps --format "{{.Names}}" 2>$null | Select-String -Quiet $Container)) {
    Write-Host "$Container is not running. Start it with: docker compose up -d" -ForegroundColor Red
    exit 1
}

Write-Line "Batch run: $Job    $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" "White"

switch ($Job) {
    "hourly" {
        Job-HealthReport
        Job-DlqReport
    }
    "daily" {
        # Refresh before detection: SCD2 drift is easier to read against a
        # snowflake copy that matches the star.
        Job-SnowflakeRefresh
        Job-Scd2Detect
        Job-HealthReport
    }
    "monthly" {
        Job-Retention
    }
    "all" {
        Job-SnowflakeRefresh
        Job-Scd2Detect
        Job-HealthReport
        Job-DlqReport
    }
    "snowflake-refresh" { Job-SnowflakeRefresh }
    "scd2-detect"       { Job-Scd2Detect }
    "health-report"     { Job-HealthReport }
    "dlq-report"        { Job-DlqReport }
    "retention"         { Job-Retention }
}

if ($script:failures -gt 0) {
    Write-Line "`n$($script:failures) job(s) failed. Detail in mart.etl_batch_log." "Red"
    exit 1
}

Write-Line "`nDone." "Green"
exit 0
