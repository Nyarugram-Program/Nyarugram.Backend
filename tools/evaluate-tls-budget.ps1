[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$BenchmarkPath,
    [Parameter(Mandatory)][string]$OutputPath
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$source = Get-Content -LiteralPath $BenchmarkPath -Raw | ConvertFrom-Json
$samplesPath = Join-Path (Split-Path -Parent $BenchmarkPath) 'benchmark-tls-samples.json'
$samples = Get-Content -LiteralPath $samplesPath -Raw | ConvertFrom-Json
if ($source.rounds -ne 3 -or $source.measuredRequests -ne 7200 -or $source.warmupRequests -ne 720) {
    throw 'Expected the original three-round 7200 measured / 720 warmup TLS benchmark.'
}
$pairs = @(
    foreach ($round in 0..2) {
        foreach ($concurrency in 1, 2) {
            foreach ($operation in 'GetState', 'GetHistory', 'SendMessage') {
                $cells = @($source.results | Where-Object { $_.round -eq $round -and $_.concurrency -eq $concurrency -and $_.operation -eq $operation })
                $json = @($cells | Where-Object protocol -eq 'json')
                $pb = @($cells | Where-Object protocol -eq 'protobuf')
                if ($json.Count -ne 1 -or $pb.Count -ne 1) { throw "Missing/duplicate pair: $round/$concurrency/$operation" }
                $json = $json[0]; $pb = $pb[0]
                if ($json.measured -ne 200 -or $pb.measured -ne 200 -or $json.warmup -ne 20 -or $pb.warmup -ne 20) { throw 'Unexpected cell size' }
                if ($json.p95Ms -le 0 -or $json.cpuMsPerRequest -le 0 -or $json.maxPrivateMemoryBytes -le 0) { throw 'Invalid denominator' }
                $details = @(
                    foreach ($protocol in 'json', 'protobuf') {
                        $requests = @($samples | Where-Object { $_.PSObject.Properties['requests'] -and -not $_.warmup -and $_.round -eq $round -and $_.concurrency -eq $concurrency -and $_.operation -eq $operation -and $_.protocol -eq $protocol } | ForEach-Object { $_.requests })
                        if ($requests.Count -ne 200) { throw 'Raw request count differs from summary' }
                        $times = @($requests.httpMilliseconds | Sort-Object)
                        $cell = if ($protocol -eq 'json') { $json } else { $pb }
                        [pscustomobject]@{
                            protocol = $protocol; requestCount = $requests.Count
                            errors = @($requests | Where-Object status -ne 200).Count
                            p50Ms = ($times[99] + $times[100]) / 2; p95Ms = $times[189]; p99Ms = $times[197]; maxMs = $times[199]
                            cpuMilliseconds = $cell.cpuMilliseconds; cpuMsPerRequest = $cell.cpuMsPerRequest
                            sampledPrivateBytes = $cell.maxPrivateMemoryBytes; maxSampleGapMs = $cell.maxSampleGapMs
                            requestBodyBytes = $cell.meanRequestBodyBytes; responseBodyBytes = $cell.meanResponseBodyBytes
                            cpuCyclesPerRequest = $(if ($cell.PSObject.Properties['cpuCyclesPerRequest']) { $cell.cpuCyclesPerRequest } else { $null })
                            lifetimePeakPagedMemoryBytes = $(if ($cell.PSObject.Properties['lifetimePeakPagedMemoryBytes']) { $cell.lifetimePeakPagedMemoryBytes } else { $null })
                        }
                    }
                )
                [pscustomobject]@{
                    round = $round; concurrency = $concurrency; operation = $operation
                    jsonP95Ms = $json.p95Ms; protobufP95Ms = $pb.p95Ms
                    latencyRatio = $pb.p95Ms / $json.p95Ms
                    latencyPass = $pb.p95Ms -le 1.10 * $json.p95Ms
                    cpuRatio = $pb.cpuMsPerRequest / $json.cpuMsPerRequest
                    cpuPass = $pb.cpuMsPerRequest -le 1.10 * $json.cpuMsPerRequest
                    sampledPrivateRatio = $pb.maxPrivateMemoryBytes / $json.maxPrivateMemoryBytes
                    sampledPrivatePass = $pb.maxPrivateMemoryBytes -le 1.10 * $json.maxPrivateMemoryBytes
                    cpuCycleRatio = $(if ($json.PSObject.Properties['cpuCyclesPerRequest'] -and $json.cpuCyclesPerRequest -gt 0) { $pb.cpuCyclesPerRequest / $json.cpuCyclesPerRequest } else { $null })
                    details = $details
                }
            }
        }
    }
)
$latencyFailures = @($pairs | Where-Object { -not $_.latencyPass }).Count
$cpuFailures = @($pairs | Where-Object { -not $_.cpuPass }).Count
$errorCount = ($pairs | ForEach-Object { $_.details } | Measure-Object -Property errors -Sum).Sum
$report = [ordered]@{
    benchmarkSha256 = (Get-FileHash -LiteralPath $BenchmarkPath -Algorithm SHA256).Hash
    rawSamplesSha256 = (Get-FileHash -LiteralPath $samplesPath -Algorithm SHA256).Hash
    backendSha256 = $source.backendSha256
    executionOrder = @($source.results | ForEach-Object { "r$($_.round)/c$($_.concurrency)/$($_.operation)/$($_.protocol)" })
    budget = 1.10; pairCount = $pairs.Count
    latencyFailures = $latencyFailures; cpuFailures = $cpuFailures
    measuredErrorCount = $errorCount
    latencyStatus = $(if ($latencyFailures) { 'FAIL' } else { 'PASS' })
    cpuStatus = $(if ($cpuFailures) { 'FAIL' } else { 'PASS' })
    exactPeakPrivateStatus = 'INCONCLUSIVE'
    caveat = 'Sampled memory is a lower bound. This report does not certify Android, TLS security, error rate or joint P5/P6 acceptance. No sample trimming or pooling.'
    pairs = $pairs
}
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $OutputPath -Encoding utf8
Write-Host "18 pairs; latency failures=$latencyFailures; CPU failures=$cpuFailures; exact memory peak INCONCLUSIVE"
if ($latencyFailures -or $cpuFailures -or $errorCount) { exit 1 }
