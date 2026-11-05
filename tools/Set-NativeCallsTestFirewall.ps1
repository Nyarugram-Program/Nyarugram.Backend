param([string]$Root = 'D:\BotgramBuild\native-calls-test')
$ErrorActionPreference = 'Stop'
$Root = [IO.Path]::GetFullPath($Root).TrimEnd('\')
if (!$Root.StartsWith('D:\', [StringComparison]::OrdinalIgnoreCase)) { throw 'Test root must be on D:' }
$deployment = Get-Content -LiteralPath (Join-Path $Root 'deployment.json') -Raw | ConvertFrom-Json
if (([Uri]$deployment.url).Port -ne 18524) { throw 'The local test backend port is fixed at 18524.' }
foreach ($program in @($deployment.backendExecutable, $deployment.turnExecutable)) {
    if (![IO.Path]::GetFullPath($program).StartsWith($Root+'\', [StringComparison]::OrdinalIgnoreCase)) { throw 'Program outside test root' }
}
try {
    $network = Get-NetIPAddress -AddressFamily IPv4 -IPAddress 192.168.2.200
    if (!$network) { throw 'Test LAN address is not assigned to this host' }
    foreach ($rule in @(
        @{Name='BotgramNativeCallsTest-HTTP-18524';Protocol='TCP';Ports=@('18524');Program=$deployment.backendExecutable},
        @{Name='BotgramNativeCallsTest-TURN-UDP';Protocol='UDP';Ports=@('3478','49160-49200');Program=$deployment.turnExecutable}
    )) {
        if (!(Get-NetFirewallRule -Name $rule.Name -ErrorAction SilentlyContinue)) {
            New-NetFirewallRule -Name $rule.Name -DisplayName $rule.Name -Direction Inbound -Action Allow `
                -Profile Private -InterfaceAlias $network.InterfaceAlias -LocalAddress 192.168.2.200 -RemoteAddress 192.168.2.0/24 `
                -Protocol $rule.Protocol -LocalPort $rule.Ports -Program $rule.Program -ErrorAction Stop | Out-Null
        } else {
            Set-NetFirewallRule -Name $rule.Name -Direction Inbound -Action Allow -Enabled True `
                -Profile Private -InterfaceAlias $network.InterfaceAlias -LocalAddress 192.168.2.200 -RemoteAddress 192.168.2.0/24 -ErrorAction Stop | Out-Null
            Get-NetFirewallRule -Name $rule.Name | Get-NetFirewallApplicationFilter | Set-NetFirewallApplicationFilter -Program $rule.Program -ErrorAction Stop | Out-Null
            Get-NetFirewallRule -Name $rule.Name | Get-NetFirewallPortFilter | Set-NetFirewallPortFilter -Protocol $rule.Protocol -LocalPort $rule.Ports -ErrorAction Stop | Out-Null
        }
    }
    if (Get-NetFirewallRule -Name 'BotgramNativeCallsTest-HTTP-18521' -ErrorAction SilentlyContinue) {
        Disable-NetFirewallRule -Name 'BotgramNativeCallsTest-HTTP-18521' -ErrorAction Stop | Out-Null
    }
    'PASS' | Set-Content -LiteralPath (Join-Path $Root 'firewall-status.txt')
} catch {
    'FAIL: '+$_.Exception.Message | Set-Content -LiteralPath (Join-Path $Root 'firewall-status.txt')
    throw
}
