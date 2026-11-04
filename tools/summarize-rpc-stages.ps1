[CmdletBinding()]
param([Parameter(Mandatory)][string]$ExecutionLog, [Parameter(Mandatory)][string]$OutputPath)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$culture = [Globalization.CultureInfo]::InvariantCulture
$rows = @(
    foreach ($line in Get-Content -LiteralPath $ExecutionLog) {
        if ($line -notmatch 'Backend started: pid=(\d+);.*data=(.*tls-bench-r(\d+)-c(\d+)-(\w+)-(json|protobuf)\.json)') { continue }
        $backendId = $Matches[1]; $dataPath = $Matches[2]; $round = [int]$Matches[3]; $concurrency = [int]$Matches[4]; $operation = $Matches[5]; $protocol = $Matches[6]
        $logPath = Join-Path (Split-Path -Parent $dataPath) "backend-$backendId.stdout.log"
        $ordinal = 0
        foreach ($record in Get-Content -LiteralPath $logPath) {
            if ($record -notmatch 'RPC_STAGES trace=(\S+) elapsed_ms=([\d.]+) stages=(.*?) process_gc0_delta=(\d+) process_gc1_delta=(\d+) process_gc2_delta=(\d+) pending=(\d+)') { continue }
            $trace = $Matches[1]; $elapsed = [double]::Parse($Matches[2], $culture); $tags = $Matches[3]
            $gc0 = [int]$Matches[4]; $gc1 = [int]$Matches[5]; $gc2 = [int]$Matches[6]; $pending = [long]$Matches[7]
            $stages = [ordered]@{}
            foreach ($tag in $tags.Split(';')) {
                if ($tag -match '^diag\.([a-z_]+_ms)=([\d.]+)$') { $stages[$Matches[1]] = [double]::Parse($Matches[2], $culture) }
            }
            [pscustomobject]@{
                round = $round; concurrency = $concurrency; operation = $operation; protocol = $protocol
                ordinal = $ordinal; warmup = $ordinal -lt 20; trace = $trace; log = $logPath; elapsedMs = $elapsed
                processGc0Delta = $gc0; processGc1Delta = $gc1; processGc2Delta = $gc2; pendingAtEnd = $pending; stages = $stages
            }
            $ordinal++
        }
        if ($ordinal -ne 220) { throw "Expected 220 diagnostic records, found $ordinal in $logPath" }
    }
)
if ($rows.Count -ne 7920) { throw "Expected 7920 records, found $($rows.Count)" }
$measured = @($rows | Where-Object { -not $_.warmup })
$slow = @($measured | Sort-Object elapsedMs -Descending | Select-Object -First 30)
[ordered]@{
    scope = 'Separate instrumented run; inclusive wall-clock stages, nested stages cannot be added; process GC deltas do not attribute GC causally to one request. First 20 records per instance are warmups. Raw stdout remains authoritative.'
    totalRecords = $rows.Count; measuredRecords = $measured.Count
    executionLogSha256 = (Get-FileHash -LiteralPath $ExecutionLog -Algorithm SHA256).Hash
    slowest = $slow; records = $rows
} | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $OutputPath -Encoding utf8
Write-Host "Parsed $($rows.Count) diagnostic requests; 7200 measured; inclusive stage report saved."
