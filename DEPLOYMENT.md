# 公网 HTTPS 部署（Ubuntu 22.04）

## 验证器密钥的持久化

TOTP 账号密钥由 ASP.NET Core Data Protection 加密保存到账号隐私状态。LegacyOnly 默认将密钥环保存在实际 `DataPath` 加 `.keys` 的目录。PostgreSQL 部署应显式设置 `Privacy:KeyDirectory` 为持久化的受限目录，所有处理同一数据库的实例使用同一密钥环和应用标识。Windows 密钥环使用当前运行用户的 DPAPI 保护。Linux 应通过部署的文件权限和数据卷访问控制保护密钥环。

备份与迁移必须同时保留账号数据和密钥环。仅搬迁 PostgreSQL 隐私状态不会搬迁解密密钥。更换 Windows 运行用户时需要事先安排密钥保护迁移。新恢复码为 10 个八位数字，只保存带版本前缀的 HMAC-SHA256 校验值，使用受保护的 TOTP 密钥计算。既有 SHA-256 格式恢复码继续可用，每个码成功使用后在同一事务中移除。明文只在生成时返回，邮件找回服务当前未配置。

公网业务入口为 `https://nyarugram.moe.autos:8521`，事件连接为 `wss://nyarugram.moe.autos:8521/api/ws`。客户端地址未填写端口时默认补齐 **8521**，不使用 HTTPS 的常规默认端口 443。DNS 指向 `43.142.58.20`。

## 端口与代理

- TCP 8521：Caddy TLS 终止、HTTP RPC 与 WebSocket 共用。仅允许 TLS 1.2 / 1.3；使用公共受信任 ACME 证书，不跳过证书或主机名校验。
- TCP 80：ACME HTTP-01 验证及跳转到 `https://nyarugram.moe.autos:8521`；不提供业务 API。自动续期需要持续允许 CA 访问 80。
- 此主机原有的 443、6543 和其他网站继续保留，但不是 Botgram 客户端入口。无需为 Botgram 新开放 443 或 UDP 8521。
- 后端 HTTP 8521 仅在 Docker 网络中使用，没有宿主机端口映射。Caddy 和后端位于 `botgram_edge`，代理地址 `172.28.52.2`，后端 `172.28.52.3`。
- `ReverseProxy:KnownProxies` 仅信任代理地址，转发头在限流前处理；Caddy 覆盖客户端提交的转发头。不要扩大信任范围。

仓库的 `compose.https.yaml` 提供后端部署定义，`Caddyfile.https` 是追加到现有 Caddyfile 的站点片段，不能覆盖已有网站。服务器现有代理项目位于 `/opt/qslink/releases/d720b78`，其 `compose.ip.yaml` 增量加入 `8521:8521` 及外部网络；后续迁移目录时应以 Docker Compose 实际配置为准。

## 运行、更新与回退

后端部署根目录为 `/opt/botgram`；正式数据目录为 `/opt/botgram/data`，隔离测试数据保留在 `/opt/botgram/validation/data`。正式库不创建测试账号，JSON 文件首次实际写入时才创建。镜像及数据目录由该目录的 `.env` 指定，使用 `compose.https.yaml` 启动。发布程序位于独立 `releases/<hash>`。后端采用非 root 用户、只读根文件系统及可写数据卷；数据目录必须允许 UID 1654 写入。

```bash
# 查看实际镜像、数据挂载及运行状态（不要公开 .env 或数据库内容）
docker compose --project-directory /opt/botgram -f /opt/botgram/compose.https.yaml ps
docker inspect botgram-backend-1 --format '{{json .Mounts}}'
curl --fail https://nyarugram.moe.autos:8521/health

# 更新镜像或数据目录配置后，只重建后端
# 先停止服务、备份实际数据卷及 .env，再修改 BOTGRAM_IMAGE
docker compose --project-directory /opt/botgram -f /opt/botgram/compose.https.yaml up -d --no-deps backend
```

更新前使用独立数据目录验证镜像，不能向正式数据库创建测试账号。停止后端并确认释放数据库后，再备份实际数据目录和配置；保留原镜像、发布目录及备份。回退时停止新后端、恢复旧镜像配置；若数据格式不兼容，应在停止状态下从升级前快照恢复，不能用旧快照覆盖仍在写入的数据。

Caddy 证书保存在其既有持久卷中，重建代理不能删除卷。配置更新先执行 `docker exec qslink-proxy-1 caddy validate --config /etc/caddy/Caddyfile`，仅配置变化可使用 Caddy reload；新增发布端口需要只重建 proxy，可能短暂影响已有网站。不要对整个既有站点执行 `down` 或删除卷。备份代理配置位于 `/opt/botgram/backups`。

公网隔离测试必须使用测试数据目录，并显式设置 `BOTGRAM_HTTPS_TEST_ISOLATED=1`。服务端测试与 TLS 握手通过不等于 Android 真机安装、前后台切换、定位或端到端页面操作已经验证。

# 本机 Windows 部署（保留）

当前部署位于 `D:\Botgram\Backend`，使用已经发布并通过集成测试的 Release 程序。

- 手机连接同一局域网时，App 登录页填写 `http://192.168.2.200:8521`。
- 本机地址为 `http://127.0.0.1:8521`。
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

防火墙规则 `BotgramBackend-LAN-8521` 仅允许“以太网 5”私有网络的本地子网访问 TCP 8521。未配置公网映射或外网域名。服务启动时读取此网卡的 IPv4 地址；如果电脑地址变化，请根据 `process.json` 更新手机中的地址。

部署版已经通过隔离数据与端口的账号、TL 聊天、同步、持久化、权限、重启等集成验证。测试数据位于 `validation`，没有在正式数据库中创建测试账号。手机到服务器的实际连通性尚需在手机上确认。

本地开发网络使用 HTTP。原生通话、附件等未支持功能的范围见同级客户端中的 `../Botgram/Botgram.Android.md`。更新程序前应先停止服务并备份正式数据；不要删除 `data` 来更新程序。
