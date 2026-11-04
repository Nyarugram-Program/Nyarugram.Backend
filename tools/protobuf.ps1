[CmdletBinding()]
param(
    [ValidateSet('Prepare', 'Generate', 'Check')]
    [string]$Action = 'Check',
    [string]$SchemaPath = (Join-Path (Split-Path $PSScriptRoot -Parent) 'Protocol\botgram_rpc.proto')
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$root = 'D:\BotgramBuild\protobuf'
$repository = Split-Path $PSScriptRoot -Parent
$schema = [System.IO.Path]::GetFullPath($SchemaPath)
$lock = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'protobuf-toolchain.json') -Raw | ConvertFrom-Json
$downloads = Join-Path $root 'downloads'
$compilerRoot = Join-Path $root ("compiler-runs\protoc-$($lock.protocVersion)-" + [Guid]::NewGuid().ToString('N'))
$compiler = Join-Path $compilerRoot 'bin\protoc.exe'
$generated = Join-Path $root 'generated'
$env:DOTNET_CLI_HOME = Join-Path $root 'dotnet'
$env:NUGET_PACKAGES = Join-Path $root 'nuget\packages'
$env:NUGET_HTTP_CACHE_PATH = Join-Path $root 'nuget\http'
$env:NUGET_PLUGINS_CACHE_PATH = Join-Path $root 'nuget\plugins'
$env:TEMP = Join-Path $root 'tmp'
$env:TMP = $env:TEMP
$env:DOTNET_CLI_TELEMETRY_OPTOUT = '1'

function Assert-Artifact($artifact, [string]$path) {
    $actual = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash
    if ($actual -ne $artifact.sha256) {
        throw "SHA256 mismatch for $($artifact.name): expected $($artifact.sha256), got $actual. Remove the invalid cached file and rerun Prepare."
    }
}

function Prepare-Toolchain {
    foreach ($directory in @($downloads, $env:DOTNET_CLI_HOME, $env:NUGET_PACKAGES, $env:NUGET_HTTP_CACHE_PATH, $env:NUGET_PLUGINS_CACHE_PATH, $env:TEMP)) {
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
    }
    foreach ($artifact in $lock.artifacts) {
        $path = Join-Path $downloads $artifact.name
        if (-not (Test-Path -LiteralPath $path)) {
            $partial = "$path.partial"
            Invoke-WebRequest -Uri $artifact.url -OutFile $partial
            Assert-Artifact $artifact $partial
            Move-Item -LiteralPath $partial -Destination $path -Force
        }
        Assert-Artifact $artifact $path
        Write-Host "SHA256 verified: $($artifact.name)"
    }
    # Re-extract the verified archive so an altered extracted executable is not trusted.
    $archive = Join-Path $downloads "protoc-$($lock.protocVersion)-win64.zip"
    Expand-Archive -LiteralPath $archive -DestinationPath $compilerRoot -Force
    $version = & $compiler --version
    if ($LASTEXITCODE -ne 0 -or $version -ne "libprotoc $($lock.protocVersion)") {
        throw "Unexpected protoc version: $version"
    }
    Write-Host $version
}

function Generate-Protocol([string]$destination) {
    if (-not (Test-Path -LiteralPath $schema)) {
        throw "Schema is not ready: $schema"
    }
    $java = Join-Path $destination 'java'
    $csharp = Join-Path $destination 'csharp'
    New-Item -ItemType Directory -Force -Path $java, $csharp | Out-Null
    & $compiler "--proto_path=$(Split-Path $schema -Parent)" "--java_out=lite:$java" "--csharp_out=$csharp" $schema
    if ($LASTEXITCODE -ne 0) { throw "protoc generation failed ($LASTEXITCODE)." }
    Write-Host "Generated Java lite and C# in $destination"
}

function Get-GeneratedHashes([string]$directory) {
    @(Get-ChildItem -LiteralPath $directory -Recurse -File | Sort-Object FullName | ForEach-Object {
        $relative = $_.FullName.Substring($directory.Length).TrimStart('\', '/').Replace('\', '/')
        "$relative $((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash)"
    })
}

Prepare-Toolchain
if ($Action -eq 'Prepare') { return }
if ($Action -eq 'Generate') {
    Generate-Protocol $generated
    return
}
if (-not (Test-Path -LiteralPath $generated)) {
    throw 'Generated files are missing; run tools/protobuf.ps1 -Action Generate first.'
}

$checkRoot = Join-Path 'D:\BotgramBuild\tmp' ('protobuf-check-' + [Guid]::NewGuid().ToString('N'))
Generate-Protocol $checkRoot
$first = Get-GeneratedHashes $generated
$second = Get-GeneratedHashes $checkRoot
if ($first.Count -eq 0 -or @(Compare-Object $first $second).Count -ne 0) {
    throw 'Repeated generation differs, or stale generated files exist.'
}
$trackedCsharp = Join-Path $repository 'Protocol\BotgramRpc.cs'
if (Test-Path -LiteralPath $trackedCsharp) {
    if ((Get-FileHash -LiteralPath $trackedCsharp -Algorithm SHA256).Hash -ne (Get-FileHash -LiteralPath (Join-Path $checkRoot 'csharp\BotgramRpc.cs') -Algorithm SHA256).Hash) {
        throw 'Checked-in C# protocol differs from the pinned generated schema.'
    }
}
Write-Host 'Repeated generation and checked-in C# are byte-for-byte identical.'

$javac = if ($env:JAVA_HOME) { Join-Path $env:JAVA_HOME 'bin\javac.exe' } else { (Get-Command javac -ErrorAction Stop).Source }
$classes = Join-Path $checkRoot 'classes'
New-Item -ItemType Directory -Force -Path $classes | Out-Null
$sources = @(Get-ChildItem -LiteralPath (Join-Path $generated 'java') -Recurse -Filter '*.java' -File | ForEach-Object FullName)
$sourceList = Join-Path $checkRoot 'java-sources.txt'
$sources | ForEach-Object { '"' + $_.Replace('\', '/') + '"' } | Set-Content -LiteralPath $sourceList -Encoding utf8
$javaRuntime = Join-Path $downloads "protobuf-javalite-$($lock.runtimeVersion).jar"
& $javac --release 8 -encoding UTF-8 -cp $javaRuntime -d $classes "@$sourceList"
if ($LASTEXITCODE -ne 0) { throw "Java lite compilation failed ($LASTEXITCODE)." }
Write-Host 'Java lite compilation passed (Java 8 target).'

& (Join-Path $PSScriptRoot 'test-protobuf.ps1') -SkipToolchainCheck
Write-Host 'Protocol toolchain checks passed.'
