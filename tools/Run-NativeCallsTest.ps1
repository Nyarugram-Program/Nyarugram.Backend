param(
    [ValidateSet('start','run','stop','status')][string]$Action = 'status',
    [string]$Root = 'D:\BotgramBuild\native-calls-test'
)
$ErrorActionPreference = 'Stop'
$Root = [IO.Path]::GetFullPath($Root).TrimEnd('\')
if (!$Root.StartsWith('D:\', [StringComparison]::OrdinalIgnoreCase)) { throw 'Test runtime must be on D:' }
$manifestPath = Join-Path $Root 'process.json'
$deployment = Get-Content -LiteralPath (Join-Path $Root 'deployment.json') -Raw | ConvertFrom-Json
$scriptPath = $PSCommandPath
function Get-OwnedProcess($IdToCheck, $Executable) {
    if (!$IdToCheck) { return $null }
    $candidate = Get-CimInstance Win32_Process -Filter "ProcessId=$IdToCheck" -ErrorAction SilentlyContinue
    if ($candidate -and $candidate.ExecutablePath -eq $Executable) { return $candidate }
    return $null
}
if ($Action -eq 'status') {
    if (Test-Path -LiteralPath $manifestPath) { Get-Content -LiteralPath $manifestPath }
    else { Write-Output 'Not started' }
    return
}
if ($Action -eq 'stop') {
    if (!(Test-Path -LiteralPath $manifestPath)) { return }
    $state = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    $monitor = Get-CimInstance Win32_Process -Filter "ProcessId=$($state.monitorPid)" -ErrorAction SilentlyContinue
    if ($monitor -and $monitor.CommandLine.Contains($scriptPath) -and $monitor.CommandLine.Contains($Root)) {
        Stop-Process -Id $monitor.ProcessId -Force
    } elseif ($monitor) { throw 'Monitor identity mismatch; refusing to stop' }
    foreach ($child in @(@{Id=$state.backendPid;Exe=$deployment.backendExecutable}, @{Id=$state.turnPid;Exe=$deployment.turnExecutable})) {
        $owned = Get-OwnedProcess $child.Id $child.Exe
        if ($owned) { Stop-Process -Id $owned.ProcessId -Force }
    }
    $state.status = 'stopped'
    $state | ConvertTo-Json | Set-Content -LiteralPath $manifestPath -Encoding UTF8
    return
}
if ($Action -eq 'start') {
    if (([Uri]$deployment.url).Port -ne 18524) { throw 'The local test backend port is fixed at 18524; update deployment.json and appsettings before starting.' }
    if (Test-Path -LiteralPath $manifestPath) {
        $state = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
        $existing = Get-CimInstance Win32_Process -Filter "ProcessId=$($state.monitorPid)" -ErrorAction SilentlyContinue
        if ($existing -and $existing.CommandLine.Contains($scriptPath) -and $existing.CommandLine.Contains($Root)) {
            Write-Output 'Test supervisor already running'
            return
        }
    }
    # WMI detaches the hidden supervisor from the launching terminal's job object.
    $shell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
    $command = '"{0}" -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "{1}" -Action run -Root "{2}"' -f $shell,$scriptPath,$Root
    $result = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{CommandLine=$command;CurrentDirectory=$Root}
    if ($result.ReturnValue -ne 0) { throw "Cannot launch supervisor: $($result.ReturnValue)" }
    Write-Output "Started test supervisor: $($result.ProcessId)"
    return
}
$env:DOTNET_CLI_HOME = 'D:\BotgramBuild\dotnet'
if (([Uri]$deployment.url).Port -ne 18524) { throw 'The local test backend port is fixed at 18524.' }
$runtimeSettings = Get-Content -LiteralPath (Join-Path $deployment.backendDirectory 'appsettings.json') -Raw | ConvertFrom-Json
foreach ($listenUrl in $runtimeSettings.Urls.Split(';')) {
    if (([Uri]$listenUrl).Port -ne 18524) { throw 'Every test backend listener must use port 18524.' }
}
$env:TEMP = Join-Path $Root 'tmp'
$env:TMP = $env:TEMP
$backend = $null
$turn = $null
while ($true) {
    foreach ($kind in @('backend','turn')) {
        $process = Get-Variable -Name $kind -ValueOnly
        if (!$process -or $process.HasExited) {
            $stamp = Get-Date -Format 'yyyyMMdd-HHmmss-fff'
            if ($kind -eq 'backend') {
                $executable = $deployment.backendExecutable
                $arguments = @('--contentRoot', ('"{0}"' -f $deployment.backendDirectory))
                $directory = $deployment.backendDirectory
            } else {
                $executable = $deployment.turnExecutable
                $arguments = @('-config', ('"{0}"' -f (Join-Path $Root 'private\turn.json')))
                $directory = $Root
            }
            $process = Start-Process -FilePath $executable -ArgumentList $arguments -WorkingDirectory $directory -WindowStyle Hidden -PassThru `
                -RedirectStandardOutput (Join-Path $Root "logs\$kind-$stamp.stdout.log") `
                -RedirectStandardError (Join-Path $Root "logs\$kind-$stamp.stderr.log")
            Set-Variable -Name $kind -Value $process
        }
    }
    $state = [ordered]@{status='running';monitorPid=$PID;backendPid=$backend.Id;turnPid=$turn.Id;url=$deployment.url;updatedAt=[DateTimeOffset]::Now.ToString('o')}
    $temporary = "$manifestPath.tmp"
    $state | ConvertTo-Json | Set-Content -LiteralPath $temporary -Encoding UTF8
    Move-Item -LiteralPath $temporary -Destination $manifestPath -Force
    Start-Sleep -Seconds 3
}
