param(
    [string]$RuntimeRoot = 'D:\BotgramBuild\p7',
    [string]$Distribution = 'Ubuntu-24.04',
    [ValidateSet('start', 'stop')][string]$Action = 'start'
)
$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath($RuntimeRoot)
if (!$root.StartsWith('D:\', [StringComparison]::OrdinalIgnoreCase)) { throw 'P7 runtime must remain on D:' }
if (!(Test-Path -LiteralPath (Join-Path $root 'runtime-manifest.json'))) { throw 'Prepare the isolated P7 runtime and private manifest first.' }
$script = Join-Path $PSScriptRoot 'p7-process.py'
$linuxScript = & wsl -d $Distribution --exec wslpath -u $script
if ($LASTEXITCODE) { throw 'Cannot resolve the P7 helper path in WSL' }
$linuxRoot = & wsl -d $Distribution --exec wslpath -u $root
if ($LASTEXITCODE) { throw 'Cannot resolve the P7 data path in WSL' }
& wsl -d $Distribution -u root --exec python3 $linuxScript.Trim() $Action $linuxRoot.Trim()
if ($LASTEXITCODE) { throw "P7 $Action failed. Inspect the task-owned D-drive logs." }
