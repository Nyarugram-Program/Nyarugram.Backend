# Botgram.Backend 开发指引

本文件适用于独立的 `Botgram.Backend` 仓库及其子目录，用户当前任务中的明确要求优先。当前电脑上后端位于 `W:\Work\Botgram.Backend`，客户端位于同级 `W:\Work\Botgram`，但该布局不是后端构建或部署的前提。只 Clone 后端仓库即可编译、发布和运行。涉及客户端契约且客户端可用时再阅读其指引；后端独立工作不要求客户端文件存在。

## 架构与入口

后端使用 ASP.NET Core 8，当前没有第三方 NuGet 运行依赖。优先沿用现有标准库与框架能力，确有需求再引入依赖；所有新下载及缓存仍须放在 D 盘。

- `Program.cs`：HTTP 路由、认证中间件、限流、WebSocket 与业务入口。
- `Models.cs`：持久化模型、请求模型、事件及稳定 TL 数字身份映射。
- `Store.cs`：单进程数据库租约、串行写入、原子快照与失败回滚。
- `Auth.cs`：密码/令牌摘要、会话、公开用户 DTO、成员权限检查。
- `TelegramGateway.cs`：原 App TL 对象的服务端兼容层。
- `Gids.cs`：M49 映射、GID 分配、修改和旧数据迁移。
- `tests/`：隔离运行的真实 HTTP/WebSocket 集成测试及原 TL 编码验证。

实现面向现有 Telegram App 的后端行为；不要为了省略服务端逻辑把聊天模型重写到客户端。阅读相关路由及调用方后再改变契约。

## 数据与身份不变量

- 所有持久化修改通过 `Store.Write`，读取通过 `Store.Read`；不把可变数据库引用带出锁外后继续修改。
- 保持进程租约、原子替换及失败回滚。写入接口成功必须代表状态已持久化；不要启动第二个进程打开同一数据文件。
- 迁移兼容已有 JSON 字段缺省；已有 UUID、会话、成员关系、消息、事件及 `TelegramIds` 映射保持稳定。拒绝不可恢复的非法数据，不以清空数据库作为修复方式。
- GID 以十一位数字规范存储、以 `+030 12345678` 对外展示。完整 GID 唯一；三位 M49 地理区域码后接八位非全零随机数字。
- 注册依据 `locationCountry` 查共享映射；缺省或无法映射用 `001`。随机数使用密码学 RNG，分配、查重、保存与创建会话在同一个写事务中完成。
- 已有无 GID 用户只补发一次持久化的 World GID。手动修改禁止 `001`、国家码、未知区域、非八位/全零后缀；当前用户保留自己的 GID允许成功，其他用户碰撞返回 409 `gid_taken`。
- 后端地理数据固定在 `Resources/M49/`，嵌入本仓库的 `botgram-m49.json`，编译、发布及启动均不得读取客户端仓库或联网下载 M49。MSBuild 校验其 SHA-256；维护规则见该目录 `README.md`。
- `source.json` 是联合国官方数据快照，`policy.json` 明确允许的 29 个区域及兼容项，`manifest.json` 记录应用快照版本和来源/政策/数据哈希。通过 `tools/update-m49.mjs --version YYYY-MM-DD` 主动更新，检查后同时提交数据与校验值；不能静默自动更新或伪称快照日期是官方发布版本。
- 客户端自行持有对应数据与清单，通过相同版本及 `dataSha256` 保持一致。同步复制是维护操作，不能重新引入跨仓库构建路径依赖。

## API、安全与同步契约

- 保持公开接口为 camelCase；REST 用户通过 `Auth.PublicUser` 返回，不能直接返回包含密码盐、密码摘要的持久化 `User`。
- 除健康检查、注册和登录外，API 继续要求有效 Bearer。数据库仅存令牌摘要；密码继续使用加盐 PBKDF2。不要记录完整 Authorization、密码、令牌或登录请求体。
- 在每个会话、消息、回执及通话操作中验证成员/参与者权限。事件只返回给其接收人，不因增加 GID 或查重功能泄漏其他用户数据。
- 保持请求大小、分页和限流约束；参数错误、权限错误、冲突等返回明确状态，不伪装成成功或空结果。
- 消息幂等键及 `random_id` 重试不能产生重复消息；已读游标只前进。稳定身份、同会话回复、实体及发送选项应在映射中保留。
- 事件 ID、消息 ID、各用户的 TL pts 是不同游标。初始 pts 为 1；新增事件类型时同步检查 `Stream`、`Pts`、差分分页与状态推进，不能重新排序已有事件或破坏重放。
- GID 变更发出 `user.gid_changed`，通过原 `TL_updateUserPhone` 更新客户端缓存；TL `phone` 是兼容字段，不是实际电话号码。
- `$type` 使用本仓库真实 Java 类名。多数类在 `org.telegram.tgnet.TLRPC$…`，更新类如 `TL_updateUserPhone` 在 `org.telegram.tgnet.tl.TL_update$…`；以源代码校验，不能凭旧 Telegram 版本猜测。
- 不支持的 TL 方法明确返回 501 `BOTGRAM_RPC_UNSUPPORTED:<method>`。当前未实现 MTProto、原生 `phone.*`、附件与后台推送；独立 REST WebRTC 信令与原 Telegram 通话尚未打通，不得混淆。

## 本地构建与测试

在本目录运行 .NET 命令，使用 `global.json` 的 SDK 选择（8.0.204，允许 latestFeature 滚动），避免从仓库根目录误用其他 SDK。先配置 D 盘缓存：

```powershell
$env:DOTNET_CLI_HOME = 'D:\BotgramBuild\dotnet'
$env:NUGET_PACKAGES = 'D:\BotgramBuild\nuget'
$env:TEMP = 'D:\BotgramBuild\tmp'
$env:TMP = $env:TEMP
$env:DOTNET_CLI_TELEMETRY_OPTOUT = '1'
dotnet build --nologo --ignore-failed-sources -p:NuGetAudit=false
node tests/m49.mjs
node tests/gid.mjs
node tests/smoke.mjs
```

测试使用 Node.js 22+，自动启动独立端口并创建 `obj/` 中的临时数据，不能指向正式 `DataPath`。可用 `BOTGRAM_TEST_ROOT` 指定隔离测试目录、`BOTGRAM_TEST_DLL` 指定待验证的发布 DLL；不要把生产令牌或生产账号用于测试。

- `m49.mjs`：离线验证官方快照、政策、版本、SHA-256、区域集合及国家映射，不依赖客户端。
- `gid.mjs`：注册区域与回退、号码验证、并发碰撞、TL 更新、重启、旧数据迁移与身份保持。
- `smoke.mjs`：认证、权限隔离、文字单聊/群聊、幂等、历史、已读、差分、事件重放、WebSocket、REST 通话信令与会话失效。
- `tl-schema.mjs`：可选的客户端契约集成检查（`node tests/tl-schema.mjs`），需要 Android 源码；不属于独立编译和启动的依赖。新增 TL 类型时维护相应分类。
- 变更 TL 响应、flags、实体或共享身份时，使用客户端仓库 `../Botgram/Tools/Verify-BotgramTL.ps1` 及 GID fixture 参数，验证真实 Android TL 类能解码、序列化及重新解析。先生成测试样本并编译 Android Java 类；宿主替身不能进入 APK。`tl-schema.mjs` 默认读取同级 `Botgram`，可通过 `BOTGRAM_ANDROID_ROOT` 指定其他客户端根目录。

按修改范围运行相关检查；业务或存储修改应执行集成回归。文档修改不需要重新构建客户端或部署。失败后修复并重跑相关检查，不修改断言来掩盖产品错误。

## 发布与本机升级

当前服务在 `D:\Botgram\Backend`，由用户登录启动的隐藏 PowerShell 监控进程维护，不是系统级 Windows Service。以 `deployment.json`、`process.json` 为准获取实际路径、版本和地址，不硬编码 PID 或历史版本目录。

仅在当前任务授权发布/部署时升级运行中的服务。准备及验证交付物后再切换：

```powershell
# 在 Botgram.Backend 目录，已设置上述 D 盘环境变量
dotnet publish -c Release -o artifacts/publish --nologo --ignore-failed-sources -p:NuGetAudit=false
$env:BOTGRAM_TEST_DLL = (Resolve-Path artifacts/publish/Botgram.Backend.dll).Path
node tests/gid.mjs
node tests/smoke.mjs

# 发布物通过测试后，按目标主机的部署方式启动服务
# dotnet artifacts/publish/Botgram.Backend.dll --urls http://127.0.0.1:5080
```

- 陌生主机只需后端仓库和兼容的 .NET 8 SDK即可构建；运行已发布程序需要对应 .NET 8 ASP.NET Core Runtime。Node 仅用于测试和数据维护。Docker 从后端根目录执行 `docker build -t botgram-backend .`，上下文不得包含其他仓库。
- 当前电脑的既有 D 盘部署仍可选用客户端目录的部署脚本；它们不是后端独立运行的必要条件。使用这些脚本时，显式发布到它们读取的 `../Botgram/artifacts/backend`。不要反复调用初装脚本覆盖现有部署。
- 升级前停止服务及监控，等待旧进程释放数据库，再备份数据和配置；备份继承数据目录的受限 ACL。新程序置于独立 `releases/<hash>`，保留旧版本和快照，切换后检查健康状态与迁移结果。
- 保留 `data/botgram.json`、备份和用户会话；测试样本不能进入正式库。失败时使用现有回退机制，不自动覆盖或删除用户数据。
- 不扩大监听地址、防火墙、网络配置或公网暴露范围来解决功能问题。现有端口与私有局域网规则继续沿用，除非任务要求调整。
- `../Botgram/artifacts/Botgram.Backend.zip` 与 `../Botgram/artifacts/build-info.json` 必须对应实际发布物及校验结果。明确区分后端测试、宿主 TL 验证和真机验证，不能将服务端通过等同于手机功能已验证。
