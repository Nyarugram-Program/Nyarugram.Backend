# 本机通话测试 TURN/STUN

Windows 原生 UDP 服务，使用固定的 `github.com/pion/turn/v4 v4.1.4`，依赖校验值见 `go.sum`。它避免 WSL NAT 对局域网 UDP 中继的影响。仅用于独立局域网测试，不提供公网映射、TURN/TCP、TLS 或 TURN REST 临时凭据。

当前测试部署位于 `D:\BotgramBuild\native-calls-test`：

- 后端：`http://192.168.2.200:18521`，使用独立 JSON 数据与 DPAPI 密钥环。
- RPC 和事件通道只启用 Protobuf；REST 注册、登录和能力协商继续使用 JSON。
- STUN：`stun:192.168.2.200:3478`。
- TURN：`turn:192.168.2.200:3478?transport=udp`；中继端口 UDP `49160–49200`。
- 测试后端设置 `Rtc:P2p=false`，强制验证中继路径。
- TURN 账号随机密码保存在受限的 `private\turn.json`，同时配置到该测试后端；不在仓库或交付记录中保存凭据。手机从通话 RPC 获取凭据。
- 防火墙只放行拥有 `192.168.2.200` 的私有网卡，来源限制为 `192.168.2.0/24`。TURN 分配和 peer permission 也限制到该子网。

隐藏监控由 WMI 启动，任一子进程退出后约三秒自动拉起。未安装系统服务或登录启动项；重启电脑后手动执行 start。

```powershell
$root = 'D:\BotgramBuild\native-calls-test'
& "$root\Run-NativeCallsTest.ps1" -Action status
& "$root\Run-NativeCallsTest.ps1" -Action start
& "$root\Run-NativeCallsTest.ps1" -Action stop
& "$root\bin\botgram-turn.exe" -config "$root\private\turn.json" -probe
```

维护脚本源码为 `../Run-NativeCallsTest.ps1` 和 `../Set-NativeCallsTestFirewall.ps1`；部署目录保留各自副本。防火墙脚本需要管理员运行。日志位于 `logs`，进程号在 `process.json`，发布身份在 `deployment.json`。关闭测试服务不删除数据库或防火墙规则。

构建所用 Go 为官方 `go1.27.3.windows-amd64.zip`，SHA-256 为 `f26081f438319757f8a1b46d7d0b2c5ec7583d4038e485e0aa725cfde7885fff`。工具链、包缓存、构建缓存均在 D 盘。

```powershell
# 在本目录运行；工具链已准备到 D 盘。
$env:GOPATH = 'D:\BotgramBuild\go'
$env:GOMODCACHE = 'D:\BotgramBuild\go\pkg\mod'
$env:GOCACHE = 'D:\BotgramBuild\go\cache'
$env:GOTMPDIR = 'D:\BotgramBuild\tmp'
$env:CGO_ENABLED = '0'
& D:\BotgramBuild\tools\go1.27.3\go\bin\go.exe build -trimpath -o D:\BotgramBuild\native-calls-test\bin\botgram-turn.exe .
```

`-probe` 实测 STUN binding、正确凭据分配、双向 UDP 中继和错误密码拒绝。上述本机探针与服务端通话状态机验证不代表手机的语音/视频已经接通；还需两台同子网手机安装现有 APK、新登录该地址，验证语音、视频及前后台行为。
