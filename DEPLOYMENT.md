# 本机部署

当前部署位于 `D:\Botgram\Backend`，使用已经发布并通过集成测试的 Release 程序。

- 手机连接同一局域网时，App 登录页填写 `http://192.168.2.200:5080`。
- 本机地址为 `http://127.0.0.1:5080`。
- 健康检查为 `GET /health`，未登录访问账号接口返回 401。
- 正式数据在 `D:\Botgram\Backend\data\botgram.json`，只在实际写入时创建；此文件应备份。
- 日志在 `D:\Botgram\Backend\logs`；`process.json` 记录当前后台进程与实际监听地址。
- `deployment.json` 记录部署版本、程序目录、网卡和端口。

服务由当前 Windows 用户后台运行，并通过用户启动目录的快捷方式在登录 Windows 后启动。不是系统级 Windows Service；无人登录或用户注销时不保证运行。后台监控在进程异常退出后等待 5 秒重启。

```powershell
# 停止服务及后台监控
powershell -NoProfile -ExecutionPolicy Bypass -File D:\Botgram\Backend\Stop-BotgramBackend.ps1

# 重新后台启动
powershell -NoProfile -ExecutionPolicy Bypass -File D:\Botgram\Backend\Start-BotgramBackend.ps1
```

防火墙规则 `BotgramBackend-LAN-5080` 仅允许“以太网 5”私有网络的本地子网访问 TCP 5080。未配置公网映射或外网域名。服务启动时读取此网卡的 IPv4 地址；如果电脑地址变化，请根据 `process.json` 更新手机中的地址。

部署版已经通过隔离数据与端口的账号、TL 聊天、同步、持久化、权限、重启等集成验证。测试数据位于 `validation`，没有在正式数据库中创建测试账号。手机到服务器的实际连通性尚需在手机上确认。

本地开发网络使用 HTTP。原生通话、附件等未支持功能的范围见同级客户端中的 `../Botgram/Botgram.Android.md`。更新程序前应先停止服务并备份正式数据；不要删除 `data` 来更新程序。
