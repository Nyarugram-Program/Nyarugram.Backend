# Botgram.Backend

基于当前 Telegram Android 仓库需求建立的独立聊天后端，使用 ASP.NET Core 8，无第三方 NuGet 依赖。

**Android 使用原 Telegram 页面，后端增加了针对本仓库 TL 模型的兼容层，参见 [原页面接入说明](../Botgram/Botgram.Android.md)。未实现 MTProto，客户端仅修改认证和传输入口；不能通过更换 Telegram 数据中心 IP 接入。**

## 已实现范围

- 原 App TL 模型兼容层：数字身份、文字单聊／基础群聊、会话／历史、已读、差分同步。入口为 `/api/telegram/me` 和 `/api/telegram/rpc`。不支持的 TL 请求返回 501。
  普通文字支持 Android 原发送器附带的自身 `send_as`，拒绝冒用其他用户；首次打开单聊的历史响应包含双方资料，并提供 `messages.getPeerSettings`。当前没有在线状态跟踪，空在线状态可能在原页面显示为“很久前上线”，不能据此判断对方实际在线情况。

- 用户名／密码注册登录，PBKDF2-SHA256 密码摘要，30 天随机 Bearer 会话；数据库仅保存令牌摘要；退出使会话失效。
- 按完整用户名查询用户；单聊及固定成员群聊；重复创建同一双方的单聊返回已有会话。
- 原 TL `contacts.importContacts` 按 GID 添加联系人，`contacts.resolvePhone` 兼容字段按 GID 查询，`contacts.getContacts` 返回持久化列表。备注姓名按添加者隔离，重复添加更新已有关系，改 GID 后关系保留；不写入手机通讯录。不支持联系人笔记。
- 文字消息、历史分页、客户端请求 ID 幂等发送、已读游标、未读计数。
- 持久化事件游标、HTTP 补拉、WebSocket 实时推送；多设备通过同一事件流同步。
- 一对一 WebRTC 通话信令：呼叫、忙线检查、接听、拒绝、挂断、SDP／ICE 转发。
- 会话成员／通话参与者权限检查、请求大小限制、登录限流及用户 API 限流。
- 单进程 JSON 快照持久化，写入串行化，原子替换，保存失败回滚；进程锁阻止同时打开同一数据文件。

尚未实现附件、语音消息、群成员变更、编辑撤回、推送唤醒、音视频媒体服务器、群通话、端到端加密、Telegram 账号或网络互通。

## 运行

安装 .NET 8 SDK，然后：

```powershell
cd Botgram.Backend
dotnet build
dotnet run
```

默认监听 `http://127.0.0.1:5080`；健康检查 `GET /health`。数据位于工作目录下 `data/botgram.json`。可用环境变量 `DataPath` 指定绝对路径。

Android 模拟器访问宿主机时使用 `http://10.0.2.2:5080`。真机联调需通过 `dotnet run -- --urls http://0.0.0.0:5080` 开放局域网监听，并配置防火墙。开发版客户端若使用 HTTP，需单独配置 Android 的明文网络策略。对外部署使用 HTTPS／WSS 反向代理。

```powershell
dotnet build
node tests/m49.mjs
node tests/gid.mjs
node tests/contacts.mjs
node tests/smoke.mjs
```

集成测试要求 Node.js 22+，自动启动独立端口及临时数据文件，覆盖账号、越权、群聊、并发去重、分页、已读、事件隔离、WebSocket、通话状态权限、重启持久化和退出失效。测试数据位于忽略的 `obj/smoke-*`，不使用正常服务数据。

**后端可独立 Clone、编译和运行，不需要客户端仓库。** 使用兼容的 .NET 8 SDK（由 `global.json` 选择 8.0.204 或更新的 8.x feature band）；服务运行不需要 Node.js。Node.js 22+ 只用于测试与数据更新。可选的 `tests/tl-schema.mjs` 和原 TL 二进制兼容性测试需要客户端源码或编译产物，不是独立构建的前提。

M49 固定快照、29 个区域规则、兼容项和校验清单随本仓库保存于 [`Resources/M49/`](Resources/M49/README.md)。编译时校验数据 SHA-256，并将数据嵌入程序集；编译和启动不访问地理数据网络接口。通过维护脚本主动向联合国官方来源更新，客户端与后端各自携带同版本和校验值的数据。

独立发布（运行主机需安装 .NET 8 ASP.NET Core Runtime）：

```sh
dotnet publish -c Release -o artifacts/publish
dotnet artifacts/publish/Botgram.Backend.dll --urls http://127.0.0.1:5080 --DataPath ./data/botgram.json
```

将 `artifacts/publish` 整个目录复制到目标主机，使用固定工作目录启动，保留持久化数据目录。局域网/公网访问需自行配置监听、防火墙和 HTTPS；`GET /health` 用于检查启动状态。

Docker（从后端仓库根目录构建，数据挂载必须保留）：

```sh
docker build -t botgram-backend .
docker run --rm -p 5080:8080 -v botgram-data:/data botgram-backend
```

## HTTP 契约

除注册、登录及健康检查外，所有请求携带 `Authorization: Bearer <token>`。字段为 camelCase，时间为 UTC ISO 8601，ID 为不透明字符串；消息和事件 ID 为递增 64 位整数。Android 使用 `long`，不要将用户字符串 ID 转换为数值。

| 方法 | 路径 | 请求／行为 |
|---|---|---|
| POST | `/api/auth/register` | `{username,displayName,password}`，返回 `{token,expiresAt,user}` |
| POST | `/api/auth/login` | `{username,password}`，返回同上 |
| POST | `/api/auth/logout` | 撤销当前令牌，204 |
| GET | `/api/me` | 当前用户公开资料 |
| GET | `/api/users?username=bob` | 按完整用户名查询，返回公开用户数组 |
| POST | `/api/conversations` | `{kind:"direct",memberIds:["对方ID"]}` 或 `{kind:"group",title,memberIds:[...]}` |
| GET | `/api/conversations` | 返回包含 conversation、lastMessage、unreadCount、receipts、users 的数组 |
| GET | `/api/conversations/{id}/messages?before=123&limit=50` | `before` 不包含自身；默认 50、最多 100，页内由旧到新 |
| POST | `/api/conversations/{id}/messages` | `{clientMessageId:"UUID",text:"你好"}`，返回规范消息 |
| POST | `/api/conversations/{id}/read` | `{messageId:123}`，已读只前进，ID 必须属于此会话 |
| GET | `/api/events?after=0&limit=100` | 返回 `{events:[{id,type,payload,createdAt}],cursor}`，最多 200 条 |
| GET | `/api/ws?after=0` | 带 Bearer header 升级 WebSocket，推送与 `/events` 相同的批次结构 |
| GET | `/api/rtc/config` | 返回 `{iceServers:[{urls,username,credential}]}` |
| POST | `/api/calls` | `{conversationId,calleeId}`，只支持单聊，返回 Call |
| GET | `/api/calls` | 当前用户的 ringing／active 通话数组，用于前台恢复来电状态 |
| GET | `/api/calls/{id}` | 查询当前状态 |
| POST | `/api/calls/{id}/accept` | 仅被叫，ringing → active |
| POST | `/api/calls/{id}/reject` | 仅被叫，ringing → rejected |
| POST | `/api/calls/{id}/end` | 任一参与者，ringing／active → ended |
| POST | `/api/calls/{id}/signals` | `{type:"offer"或"answer"或"ice",data:"SDP或候选JSON字符串"}`，202 |

用户名为 3–32 位 ASCII 字母、数字或下划线，大小写归一；显示名称最多 80 字符；密码 10–256 字符。群聊最多 100 人。文字消息最多 16000 字符。无效参数通常返回 400，未认证 401，权限不足 403，不可见资源 404，冲突 409，限流 429。业务错误体为 `{error:"错误码"}`；框架绑定错误和限流可能没有该结构。

发送消息时客户端先生成 `clientMessageId` 并持久化，超时重试沿用原 ID。同一发送人、会话及 ID 返回相同消息，若文字不同则返回 409。HTTP 成功表示服务端已落盘，不代表对方已收到；`receipt.updated` 才表示对方已读。未提供独立的送达回执。

示例（PowerShell）：

```powershell
$alice = Invoke-RestMethod http://127.0.0.1:5080/api/auth/register -Method Post -ContentType application/json -Body '{"username":"alice","displayName":"Alice","password":"change-this-password"}'
$headers = @{ Authorization = "Bearer $($alice.token)" }
Invoke-RestMethod http://127.0.0.1:5080/api/me -Headers $headers
```

## 同步与 WebSocket

事件类型为 `conversation.created`、`message.created`、`receipt.updated`、`call.updated`、`call.signal`，payload 分别为对应对象。事件只向其授权接收人返回。

1. 首次安装从 `after=0` 拉取，直到没有新事件；普通聊天事件目前不裁剪。
2. 将一批事件及其 `cursor` 一起提交至客户端数据库。接收失败时从最近已提交的 cursor 重放，按事件／消息 ID 去重。
3. 连接 `/api/ws?after=<cursor>`；服务器约每 500ms 检查新事件。使用服务端返回的 cursor，因为它可能跳过对其他用户可见的事件。
4. 断线后重连同一路径；可用 HTTP `/events` 备用补拉。socket 为单向事件流，写操作通过 HTTP。Android OkHttp WebSocket 握手可设置 Authorization header；未实现浏览器的 query-token 认证。
5. 登录过期或退出时 socket 会关闭，客户端应停止重连并重新认证。

这是游标重放机制，不保证 exactly-once。客户端必须去重。历史记录使用消息 ID 分页；事件同步使用事件 ID，两者不可混用。

## 独立 WebRTC 信令（未接入原 Telegram 通话）

客户端需实现 WebRTC PeerConnection 和音频采集播放。推荐过程：主叫创建 Call → 被叫收到 `call.updated` 并接听 → 主叫提交 offer → 被叫收到 `call.signal` 并提交 answer → 双方交换 ICE → 建立音频连接。只允许主叫提交 offer、被叫提交 answer；ICE 双向发送。信令仅转给另一参与者。

SDP 与 ICE 作为不透明字符串转发；客户端负责协议校验、协商次序及候选排队。信令事件 5 分钟后不再返回，并在下一次写入时从快照清除；它们并非端到端加密，磁盘备份中也可能保留。`ringing` 60 秒超时自动结束；active 最长 2 小时，后台每 5 秒检查一次。媒体断线后应由客户端主动挂断。离线推送尚未实现，关闭应用时不保证来电提醒。

配置示例，将以下内容加入本地 appsettings 或环境配置（不要提交真实凭据）：

```json
{
  "Rtc": {
    "IceServers": [
      { "Urls": ["stun:your-turn.example.com:3478"] },
      { "Urls": ["turn:your-turn.example.com:3478?transport=udp"], "Username": "user", "Credential": "secret" }
    ]
  }
}
```

默认 ICE 列表为空，仅适合基础本地实验；跨 NAT 通话通常需要运行并配置 TURN 服务。本项目不包含 TURN、SFU 或音频中继。当前返回配置中的固定凭据，面向真实用户部署前应改为按用户签发短期 TURN 凭据。

## 接入当前 Android 仓库

| 现有类 | 当前处理方式 |
|---|---|
| `LoginActivity` / `UserConfig` | 替换认证表单，沿用原登录完成和账号导航流程 |
| `ConnectionsManager` | 为绑定账号接入通用 TL 对象 HTTP 传输 |
| `MessagesController` / `SendMessagesHelper` | 保留原实现；后端提供原 TL 响应、random_id 去重及 pts 差分 |
| `MessagesStorage` / `MessageObject` | 保留原实现；稳定的数字身份由服务端分配 |
| `ChatActivity` / `DialogsActivity` | 保留原页面及交互，不再另建聊天界面 |
| 通话控制与通话界面 | 保留原实现；服务端尚未兼容 phone.*，现有 REST 信令未与其对接 |
| `NotificationsController` | 保留本地消息处理；自建后端的后台云推送尚未接入 |

原 `ConnectionsManager` 默认将 TL 对象交给 native MTProto。现为绑定自建后端的账号增加通用 TL 对象 HTTP 传输，后端负责对象语义与模型兼容；页面、消息控制器、消息存储、搜索及群聊创建仍使用原实现。此前的独立 Activity 和聊天界面已移除。具体支持范围与构建说明见 [原页面接入说明](../Botgram/Botgram.Android.md)。

## 部署边界

此版本用于单机原型与客户端接入验证。每次写入都会重写完整 JSON，事件和消息持续增长，不适合大规模或多实例部署。扩展时迁移 PostgreSQL 事务／唯一约束和持久事件表，使用共享消息分发、分页会话列表、事件保留策略及资源配额。

数据目录含用户和消息资料，应限制文件权限、加密磁盘及备份；当前没有静态加密。注册默认开放。公网部署前应根据产品要求增加注册控制／验证、账号恢复、推送与监控。默认按 TCP 来源 IP 对登录限流；若加反向代理，需仅信任明确代理并正确配置转发头。公开接口应使用 HTTPS／WSS。沿用本仓库客户端代码分发时需遵守其 GPL 及组件许可。
