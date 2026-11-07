[CmdletBinding()]
param([switch]$SkipToolchainCheck)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (-not $SkipToolchainCheck) {
    & (Join-Path $PSScriptRoot 'protobuf.ps1') -Action Check
    return
}

$root = 'D:\BotgramBuild\protobuf'
$repository = Split-Path $PSScriptRoot -Parent
$lock = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'protobuf-toolchain.json') -Raw | ConvertFrom-Json
$downloads = Join-Path $root 'downloads'
$compiler = Join-Path $root "protoc-$($lock.protocVersion)\bin\protoc.exe"
$compat = Join-Path $root 'compat'
$run = Join-Path $root ('tests\' + [Guid]::NewGuid().ToString('N'))
$classes = Join-Path $run 'classes'
$javaFixtures = Join-Path $run 'java-fixtures'
$csharpFixtures = Join-Path $run 'csharp-fixtures'
$env:DOTNET_CLI_HOME = Join-Path $root 'dotnet'
$env:NUGET_PACKAGES = Join-Path $root 'nuget\packages'
$env:NUGET_HTTP_CACHE_PATH = Join-Path $root 'nuget\http'
$env:NUGET_PLUGINS_CACHE_PATH = Join-Path $root 'nuget\plugins'
$env:TEMP = Join-Path $root 'tmp'
$env:TMP = $env:TEMP
$env:DOTNET_CLI_TELEMETRY_OPTOUT = '1'
foreach ($directory in @((Join-Path $compat 'java'), (Join-Path $compat 'csharp'), $classes, $javaFixtures, $csharpFixtures, $env:TEMP)) {
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
}
# Check also verifies these hashes. Recheck here when called directly with SkipToolchainCheck.
foreach ($artifact in $lock.artifacts) {
    $actual = (Get-FileHash -LiteralPath (Join-Path $downloads $artifact.name) -Algorithm SHA256).Hash
    if ($actual -ne $artifact.sha256) { throw "SHA256 mismatch for $($artifact.name)." }
}
$testSources = Join-Path $repository 'Protocol.Tests'
# global.json in the repository root pins SDK 8.0.204. Run every dotnet command
# from there so an unrelated caller's working directory cannot select SDK 9.
Push-Location $repository
try {
& $compiler "--proto_path=$testSources" "--java_out=lite:$(Join-Path $compat 'java')" "--csharp_out=$(Join-Path $compat 'csharp')" (Join-Path $testSources 'compat_v1.proto') (Join-Path $testSources 'compat_v2.proto') (Join-Path $testSources 'compat_backend_v1.proto')
if ($LASTEXITCODE -ne 0) { throw "Compatibility generation failed ($LASTEXITCODE)." }

$javac = if ($env:JAVA_HOME) { Join-Path $env:JAVA_HOME 'bin\javac.exe' } else { (Get-Command javac -ErrorAction Stop).Source }
$java = Join-Path (Split-Path $javac -Parent) 'java.exe'
$javaRuntime = Join-Path $downloads "protobuf-javalite-$($lock.runtimeVersion).jar"
$sources = @(Get-ChildItem -LiteralPath (Join-Path $root 'generated\java'), (Join-Path $compat 'java') -Recurse -Filter '*.java' -File | ForEach-Object FullName)
$sources += Join-Path $testSources 'ProtoGolden.java'
$sourceList = Join-Path $run 'java-sources.txt'
$sources | ForEach-Object { '"' + $_.Replace('\', '/') + '"' } | Set-Content -LiteralPath $sourceList -Encoding utf8
& $javac --release 17 -encoding UTF-8 -cp $javaRuntime -d $classes "@$sourceList"
if ($LASTEXITCODE -ne 0) { throw "Java fixture compilation failed ($LASTEXITCODE)." }
& $java -cp "$classes;$javaRuntime" ProtoGolden write $javaFixtures
if ($LASTEXITCODE -ne 0) { throw "Java fixture generation failed ($LASTEXITCODE)." }

$project = Join-Path $testSources 'Protocol.Tests.csproj'
& dotnet restore $project --source $downloads --packages $env:NUGET_PACKAGES --nologo -p:NuGetAudit=false "-p:ProtocolTestRunRoot=$run"
if ($LASTEXITCODE -ne 0) { throw "Protocol.Tests restore failed ($LASTEXITCODE)." }
& dotnet build $project --no-restore --configuration Release "-p:ProtocolTestRunRoot=$run" --nologo
if ($LASTEXITCODE -ne 0) { throw "Protocol.Tests build failed ($LASTEXITCODE)." }
$testDll = Join-Path $run 'bin\Protocol.Tests\Release\net8.0\Protocol.Tests.dll'
& dotnet $testDll $javaFixtures $csharpFixtures
if ($LASTEXITCODE -ne 0) { throw "C# interop tests failed ($LASTEXITCODE)." }
& $java -cp "$classes;$javaRuntime" ProtoGolden verify $csharpFixtures
if ($LASTEXITCODE -ne 0) { throw "Java verification failed ($LASTEXITCODE)." }
Write-Host "Binary Java lite ↔ C# interop passed. Fixtures: $run"
} finally { Pop-Location }
