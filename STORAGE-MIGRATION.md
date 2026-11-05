# 存储迁移：M0 审计与一致性决策

**容量锁任务的最终保留版见第 13 节；新授权的完整迁移/P7 实施进展见第 14 节**。当前已接入 HTTP 的真实关系化入口仍只有 GetState/GetHistory/SendMessage，且仅限隔离 Validation；默认仍为 LegacyOnly。第 14 节的独立仓储测试不能当作全部入口已接线。下列 M0–M2 描述及第 7–12 节测量保留为各阶段历史，不能作为本轮结果或全部 RPC 已关系化的证据。

早期阶段状态：M0 静态审计完成；用户四步中的第一步（消息 ID、pts、事件游标、幂等、历史投影与 Outbox/消费重放契约 v1）已冻结，见第 10 节；更大范围的 M1 仍未完成（完整账户认证、会话管理及通话等存储契约不在本轮范围）。M2 已实现隔离 PostgreSQL 消息接受事务核心，当时尚未注册到业务入口。本文不是迁移完成或生产切换许可。未访问生产数据库，未更改既有协议开关。后续阶段必须分别通过真实数据库回归、故障注入、性能和影子验收。

## 1. 实际模块和旧依赖

```text
Program（REST、认证、能力广告、WS） ─┐
ProtobufRpcEndpoint → ProtobufRpcMapper ├→ Database 参数化领域服务 → Store
TelegramGateway（TL JSON） ───────────┘                            ↓
Auth / CallExpiryService ─────────────────────────────── 全局锁 → JSON 文件
```

- `Store.cs:17-30` 读取 DataPath、独占 `.lock` 租约、解析全库、迁移缺失 GID。不能打开正式库做影子读取：必须使用受控停写后取得的备份。
- `Store.cs:33-105` Read/Write 共用一把进程锁。每次 Write 清理会话/临时事件、结束超时通话，再业务回调、全库序列化、Flush(true)、文件替换；失败从已提交文件恢复。Read 可以返回可变引用，数据库驱动不能沿用这一接口。
- `Program.cs` 注册/登录、GID、用户查询、会话、历史、已读、事件、REST 通话都依赖 Store；认证中间件也依赖 Store。能力广告读取协议配置及会话信息，没有独立业务广告模型。健康端点不验证四组件就绪。
- `Auth.cs` PBKDF2 210000 次、会话令牌只存摘要；登录读取用户后锁外验密，之后写会话。注册、GID查重和分配、会话创建在同一个 Write 中。
- `ConversationMessageService.cs` 创建会话、发送、建群服务消息、历史、已读；`ConversationQueryService`、`UserQueryService`、`ContactService` 与 `IdentityService` 均直接使用 Database，不是独立存储库。
- `ProtobufRpcEndpoint.cs:55-77` 在同一 Write 内重新认证、执行类型化 Mapper、编码 PB、检查 8 MiB 限制和取消，然后提交。响应网络写在提交后；取消/断线不能证明未提交，重试必须幂等。不能改为 PB→JSON 往返。
- `TelegramGateway` 共用领域服务；部分查询分配 TL ID，历史可创建 direct/saved 会话。不能把所有查询标成只读事务。
- `CallExpiryService` 每5秒检查，实际超时状态仍通过 Store.Write 更新。
- WS 每500ms读取事件，持续复核会话；PB只发送cursor提示，JSON发送事件，无客户端ACK。差分恢复才是可靠路径，不能以WS成功取代持久化。

## 2. 模型、读写路径与目标映射

| 当前模型/键 | 目标权威与约束 | 读取/迁移注意 |
|---|---|---|
| Users.Id | PostgreSQL users.user_id text PK | 稳定字符串；不得数值转换、重发 UUID |
| Username、Gid | PostgreSQL 唯一索引 | GID text(11位)可变，M49政策保持；用户名大小写与现有注册正规化一致 |
| Salt、PasswordHash、Sessions.TokenHash | PostgreSQL 认证/会话 | 保留摘要、盐、过期时间；Redis不是会话吊销唯一依据 |
| Conversations/Members | PostgreSQL 会话及成员关联外键 | direct/saved唯一性需规范化成员键；当前无真实角色模型 |
| Contacts | PostgreSQL (owner_id,user_id) | 私有备注保持，双向关系不能臆造 |
| Receipts | PostgreSQL (conversation_id,user_id) | 游标只前进，与差分事件在同事务 |
| Messages | PostgreSQL接收日志→Scylla桶化历史 | 保留long Id及完整文本、实体、回复、静音和服务消息 |
| Events/Recipients | PostgreSQL可靠差分日志与接收人索引 | 事件ID≠消息ID≠pts；不是仅Kafka通知 |
| Calls | PostgreSQL 通话/短期信令日志 | 仅既有REST信令，不代表原phone.*可用 |
| TelegramIds/NextTelegramId | PostgreSQL 唯一映射/序列 | 保存完整映射键和long值，迁移序列不得回退 |
| NextMessageId/NextEventId | PostgreSQL 序列及提交排序控制 | 保留已分配ID；不能用Kafka offset代替 |
| 在线/网关/输入中/缓存/限流 | Redis，前缀 botgram:v1:<环境>:... | 全部有TTL，可重建；不得唯一去重或持久历史 |
| 已提交投递任务 | PostgreSQL outbox → Kafka | 版本化事件，按conversation_id分区 |

不存在的当前功能：S2S信任/投递持久模型、附件媒体、频道、封禁/角色、消息编辑撤回。远域 UserRef 当前被拒绝。目标表可规划，不能宣称已经迁移这些功能。

## 3. 必须保留的顺序语义

消息ID当前全库递增；事件ID另行全库递增；`TelegramIds`又是独立计数。发送去重键是 (conversation_id,sender_id,client_message_id)；冲突载荷返回409，命中不新增消息/事件。回复必须属于同会话。

`SyncService` 的 pts = 1 + 收到的 message.created / receipt.updated / user.gid_changed 数量；seq/qts目前为0。每页100，超前pts返回TooLong。REST事件页满时cursor取末项，否则推进到全局高水位；不可见/已过期事件也影响高水位。消息、已读、GID事件不可按临时通知TTL删除后再重算pts。

新架构须在事务内锁定所涉及会话、按稳定顺序锁定接收人的同步行，更新每用户pts和持久差分记录。大群仍有每用户扇出写放大，不在本阶段伪称解决。PG sequence自身不保证提交顺序：不能直接把nextval当作WS全局已提交高水位，否则慢事务会被跳过。兼容全局cursor须引入短提交栅栏或提交后有序日志；该热点必须实测，不能删除全局锁后就宣称无串行点。新消息ID允许定义并验证的空洞，不改变历史排序。

## 4. ADR：采用 A，拒绝无恢复双写

| 维度 | A：PG接收日志+outbox | B：Scylla权威+可恢复发布 |
|---|---|---|
| 正确性 | 接收、去重、权限、pts、差分/outbox同PG事务 | Scylla内还需同分区发布标记、扫描检查点和PG元数据协调 |
| 写放大 | PG短期载荷+Scylla投影+事件 | Scylla载荷/发布日志+PG元数据，不能省去可靠日志 |
| 延迟 | 成功仅等待PG提交；投影异步 | 等Scylla quorum，仍需解决PG权限/pts部分失败 |
| 吞吐 | PG WAL、接收人扇出及提交栅栏可能热点 | 可分区高吞吐，但现有同步模型跨用户协调更复杂 |
| 运维/恢复 | outbox和接收日志可检查、重放 | 需可靠发现未发布分区，跨库恢复成本更高 |
| p95/p99 | 尚未测量，不承诺提升 | 尚未测量，不承诺提升 |

选择A匹配现有成功即持久化、单次操作原子、PB编码失败回滚语义。详细提交协议：

1. 在PG事务重新检查会话、成员、幂等键/完整载荷；按会话和用户顺序锁行；分配稳定消息ID、每用户pts；持久接收载荷、差分、去重和outbox。
2. 在提交前生成并校验协议响应；仅PG durable commit成功后向客户端成功响应。超时导致结果未知时，用原幂等键查询/重试，不无限重试。
3. 工作者按会话顺序将载荷异步幂等写入Scylla，再更新PG投影完成标记；写Scylla后崩溃只会重复相同主键写。PG丢失连接时不得虚报成功。
4. Kafka producer启用幂等、acks=all；成功broker确认后再标记outbox。确认后标记前崩溃允许重复。消费者以持久event_id去重，完成副作用/可恢复任务后再提交offset。WS只作唤醒，重复提示无用户可见重复消息。
5. Kafka/Scylla不可用时接收日志保留，积压达容量上限拒绝新写503而不是无限增长。Redis故障只影响缓存/实时提示；认证与幂等始终可回源。
6. 历史合并Scylla已投影数据与PG待投影接收记录，按原消息ID去重排序；以连续投影水位避免分页缺失。跨桶游标有界扫描，不ALLOW FILTERING。桶按会话内独立序号每10000条，旧message_id不变；该序号不是TL ID或pts。
7. 只有Scylla quorum成功、历史一致性验证通过且备份/恢复窗口满足策略时，才可清理PG临时消息载荷；永久去重键和差分状态不能随之删除。恢复过程中不得清理。
8. 编辑/撤回目前不实现；未来须引入同主键单调版本和版本化outbox，不能先删Scylla再写PG。

单节点LOCAL_QUORUM不能被称作HA。真实复制集故障切换、PG synchronous_commit/复制策略、Kafka min.insync.replicas和Scylla副本策略必须在多节点验收中明确。

## 5. 性能风险与阶段门槛

现有：全局锁包含维护、LINQ全表/排序、PB编码及同步刷盘；pts每次扫描Events，历史全会话排序，JSON全量序列化导致CPU/GC/WAL替代前的磁盘压力。PBKDF2是必要认证成本，不得通过弱化密码散列提升数据。

保留基准：历史P5/P6 7200正式请求、18组，其中8组p95、3组CPU超110%，0非预期错误。2913断言、八套回归和199真实TL样本是历史记录，不是本次已重跑结果。此次架构设计不改变原性能未通过结论。

M1类型化契约不能被解释成已接入新存储；M2-5依次需要真实驱动和集成证据，M6需要完整有界解析/引用摘要/可恢复检查点，M7需崩溃及断网故障矩阵，M8需同机同数据性能证据，M9需隔离影子/回滚演练。

## 6. 迁移、运行模式及回滚边界

- 默认LegacyOnly，拒绝拼错或数值模式。新驱动未就绪时其他模式启动失败，且必须在构造Store/创建数据目录前失败，避免误用新模式却实际写旧库。
- Shadow：旧库唯一权威，只从固定备份导入隔离目标，不产生Kafka真实通知/联邦发送。当前尚未实现。
- Validation：专用环境完整新库读写，不读取正式DataPath；当前尚未实现。
- Primary：生产切换另行授权，本阶段硬性拒绝；不能靠环境变量绕开。
- 迁移工具需独立获取停写快照，原文件不覆盖删除。SHA-256绑定源版本，逐类有界批次、检查点、唯一键重跑，保留原字符串ID、所有TL映射及序列高水位。当前没有在线增量CDC，不能声称边写边迁移；首次验收使用明确停写窗口。
- 验证不止计数：引用完整性、主键集合、规范化内容摘要、pts、cursor、分页和随机请求对比均需执行。拒绝坏数据，不通过清空目标掩盖错误。
- LegacyOnly/只读快照Shadow的生产回滚RPO=0（前提旧库一直权威且没有新库独占写）；RTO未演练，无量化承诺。
- 新库产生独占写后不允许直接拨回开关。必须冻结写、导出增量、恢复关联/序列/差分、全量校验再启动旧库；工具未实现前不存在可声明的无损Primary回滚。禁止退役或删除旧JSON。

## 7. 本阶段实际验证（2026-11-03）

- `dotnet build --no-restore`：成功，0警告、0错误，无新增下载。
- `node tests/storage-policy.mjs`：21项断言通过，Primary/Shadow/Validation/数值/错误大小写/未知/空值均在创建旧数据目录前拒绝。
- `node tests/rpc-boundaries.mjs`：通过，156次RPC调用、131个成功TL fixture；本次没有执行Java真实TL样本回读，不能将fixture生成等同于回读。
- `Protocol.HttpTests`：2913项断言通过、全部21操作覆盖；其中WS回归599项通过，不覆盖TLS或真机。
- 首次HTTP套件构建遇到既有测试进程占用默认输出DLL；未终止该进程，改用 `-p:OutputPath=D:/BotgramBuild/tmp/storage-m1-http/bin/` 编译并运行通过。完整测试数据在 `D:/BotgramBuild/tmp/p6-http-runs-4c84d3e23f4c4155826625f7cd7a9ef9`。
- 未跑四组件集成/故障测试、八套全量Node回归、性能比较或Android真机。M2–M9未完成。

## 8. 环境与尚未解决风险

本机有dotnet/node/wsl命令；docker和podman未找到。未安装容器环境，未访问生产主机或数据库；新下载/缓存必须在D盘。不能以Fake测试替代四组件真实验收。

尚未解决：持久全局cursor发布栅栏吞吐、超大群fanout/锁热点、PG暂存清理的可证明连续水位、长时间积压的容量阈值、成员变更与接收人快照、故障切换RPO、历史多桶锚点/日期/负偏移等旧查询语义，以及独立认证/REST/通话到类型化存储的完整迁移。以上是后续实现门槛，不是已具备能力。

## 9. PostgreSQL 最小事务核心（本轮实现与证据）

- 固定 Npgsql 8.0.8，锁文件记录依赖；新增 `PostgresMessageAcceptance`，仅实现 `IMessageAcceptancePersistence`，没有把未实现的历史查询或已读操作伪装成可用能力。未注册业务入口，运行模式仍仅允许 LegacyOnly。
- 显式初始化独立 `botgram_intake_v1` schema；users/sessions 仅为接受事务所需的身份引用与会话摘要，不是完整账户、认证或迁移实现，不读取旧 JSON 或正式数据库。
- 消息 ID 为全局 bigint identity，允许回滚空洞；会话内 sequence 在事务锁内递增。去重唯一键为 `(conversation_id, sender_id, client_key)`，永久保留接受记录；REST 只比较文本，Telegram 比较完整实体/回复/静音字段，保持现有规则。
- 每用户 pts 从 1 起，按 ordinal 用户顺序更新并锁定；消息、每用户差分关联、事件及 Outbox 同一 ReadCommitted 事务提交，显式 `synchronous_commit=on`。事件 cursor 通过事务 singleton 行递增，持锁至提交，不能把序列分配顺序误认为提交顺序。该全局串行点和用户扇出成本尚未性能验证。
- Outbox UUID 与 event_id 持久且唯一，事件 schemaVersion=1，创建消息 projectionVersion=1，分区键为 conversation_id。发布重试必须重用同一 UUID/event_id，不分配新消息或 pts；投影重复版本幂等、旧版本不可覆盖新版本。Kafka 发布状态、消费者持久去重和 Scylla 水位仍未实现，当前 Outbox 不得清理。
- 响应准备在 commit 前执行；异常/取消回滚。commit 不确定异常原样传播，不自动重试、不虚报成功，调用者用原幂等键恢复。内部 Message JSONB 是持久化映射，不改变 Protobuf RPC 编码链路。
- 新增 `Storage.Tests` 真库套件：只接受 loopback 且入口库名称为 `botgram_test_*`，创建唯一隔离测试库并最终删除，覆盖同键并发、冲突、原子回滚、已提交后模拟丢响应重试、重建 DataSource 去重、权限和失效会话。重建连接池不等于数据库重启；尚无真实断线发生在 COMMIT 或故障切换的证据。
- 实际执行：依赖恢复成功；最新后端和 Storage.Tests 编译成功，0 警告/0 错误；storage-policy 21 项断言通过；rpc-boundaries 156 次 RPC、131 个 TL fixture 通过，未执行 Java 回读。storage-policy 首次调用失败且无可用日志，重新独立采集日志后通过。
- **真库最小闭环已通过（2026-11-03）**：用户下载 `postgresql-16.15-5-windows-x64-binaries.zip` 后，ZIP 21799 条目 CRC/路径检查通过，实际运行 PostgreSQL 16.15 64-bit。文件 SHA256 为 `43BB45F173A6F08CF1D29A97A6D8DEB119E8E8093A24C00D2D1001A0CCAA8281`，仅本地完整性记录，未与官方哈希核验。工具、临时数据及缓存均在 D 盘。
- 隔离实例仅监听 loopback，随机密码、SCRAM-SHA-256；Storage.Tests 真库套件输出 `PASS: PostgreSQL acceptance integration tests`，退出码 0，非 SKIP。restore/build 均成功、0 警告/0 错误；附加 storage-policy 21 项及 rpc-boundaries 156 次 RPC/131 个 fixture 再次通过。日志目录 `D:/BotgramBuild/tmp/pg-acceptance-20261103-b43e7c28`。
- 测试子库已删除、自建 PostgreSQL 实例正常停止，无遗留监听及 PID 文件，临时密码文件已删除。原下载 HTTP 403 阻塞已解除，但本次只证明隔离接受事务最小闭环；完整 M1/M2、真实 COMMIT 断线、数据库重启/故障切换及后续投影仍未完成验收。
- 本轮未运行性能比较、完整八套 Node 回归、数据库崩溃恢复或真机验证；未改变历史性能未通过结论，未部署或生产切换。

## 10. 第一步冻结：消息存储契约 v1（2026-11-03）

本节与 `StorageContracts.cs` 共同构成后续实现的约束；第 9 节的最小适配器只满足其中接受事务子集。新接口没有注册、没有 Kafka/Scylla/Redis 实现，没有改变 REST/PB/TL 的公开字段或旧历史请求格式。完整认证、会话创建/撤销、联系人、会话管理、通话存储迁移不是本次“第一步完成”的含义。

### 10.1 身份、顺序与幂等

| 值 | 冻结定义 | 不允许的替代 |
|---|---|---|
| message_id | 全局正 signed bigint；保留迁移旧值，序列从旧最大值之后开始，回滚允许空洞；历史按此值降序 | Kafka offset、会话 sequence、重新生成旧消息 ID |
| conversation_sequence | 每会话从 1 开始，持会话锁递增并随事务回滚；已提交消息连续，服务消息也占一位；不因清理重编号 | 全局 message_id 的差值、接收人 pts |
| 用户 pts | signed int，初值 1；message.created/receipt.updated/user.gid_changed 每接收人各加 1，其他类型不加；唯一 `(user_id,pts)` | 临时事件数量、全局事件 ID |
| event_id / cursor | 正 signed bigint，事务 singleton 栅栏形成全局提交顺序；已提交高水位在同一 PG 快照读取 | nextval 分配顺序、broker offset、最大消息 ID |
| 幂等键 | `MessageDeduplicationKey(conversation,sender,clientKey)`；永久唯一，不含会话令牌/传输类型/时间窗口 | Redis TTL 锁、进程内字典、Kafka producer 去重 |
| outbox_id | 接受事务内一次生成 UUID，event_id 一对一唯一；重发/恢复/手动重放不变 | 每次 publish 生成 UUID |

- 用户/会话 ID 原样保留字符串；GID 与 TL 身份不参与上述编号。bigint 不缩窄为 int；TL 边界仍须显式验证可表示范围。任何序列/pts 溢出都须在提交前明确失败并回滚，不能环绕或重置。
- 会话内 message_id 顺序与 sequence 顺序一致，但不同会话的消息 ID 不保证提交先后；不能把全局最大 message_id 当同步高水位。
- 权限与会话有效性先于幂等命中；撤销后不能凭旧键读取消息。保持当前校验次序，实体与同会话回复先检查。REST 比较 text；Telegram 比较 text、按序实体全部字段、reply、silent。冲突分别 409 idempotency_conflict / RANDOM_ID_DUPLICATE。
- 命中返回原 Message 与发送人的**当前** pts，不能产生新 message/event/outbox/pts。commit 响应丢失只能原键重试；取消或 commit 异常不证明失败，不以新键自动重发。永久去重记录须保存足以恢复原响应和比较载荷的字段，不能只保留一个 hash。
- 固定锁顺序：会话/身份权限锁在前，接收人同步行按 ordinal user ID 加锁，event 栅栏最后获取，之后不再获取其他业务锁。成员修改必须与发送争用同一会话锁；接收人及各自 pts 是提交时快照，发布时不可按新成员列表重算。
- Difference 保持旧语义：每页 100，pts<=0 从起点开始，超前为 TooLong；Slice 返回页末 pts。REST Events 满页推进至末项，否则至同快照全局高水位（不小于 after），不可见/过期通知可跨过。持久三类差分不能按通知 TTL 删除；seq/qts 仍为 0。

### 10.2 历史快照、游标与投影版本

- 三种版本严格分开：事件 envelope schema=1、历史布局 format=1、单条 MessageVersion=1。当前只有创建，不实现编辑/撤回。ProjectionWork 必须包含 OutboxId、EventId、完整 Message、会话 sequence、MessageVersion、format、目标 generation。
- Scylla 逻辑键为 `(generation,conversation_id,bucket,message_id)`，bucket=`(sequence-1)/10000`。相同版本相同内容是 no-op，相同版本不同内容是损坏必须报错；旧版本不能覆盖新版本。写入必须在 quorum 持久成功后返回，不能用客户端时间戳碰运气保证版本条件。
- `ProjectionCheckpoint.ThroughSequence=W` 表示该 generation 从 1 至 W **每条**均已持久投影，不能用已见最大 sequence/offset 推进。完成 N+1 而 N 缺失时只记完成标记，水位不越过 N。Scylla 写后、PG 完成前崩溃只导致同键重写。
- 首次历史页在一致 PG 快照内取得已提交会话 sequence=S、活跃 generation、format 和 W（W<=S）。读取该 generation 中 sequence<=W 的 Scylla 行，加 PG 中 W<sequence<=S 的已提交载荷；按 message_id 去重、降序后执行旧查询。不能先在各源分别 offset/limit 再合并。
- 旧查询次序冻结：OffsetId 非零优先，以首个 id<OffsetId 为 anchor；否则 OffsetDate 非零取首个 unix-second<OffsetDate；否则 anchor=0。未找到取长度；先对完整降序列表应用 clamp(anchor+AddOffset)，再过滤严格 MinId/MaxId，再取 clamp(Limit,1,100)。负 AddOffset、稀疏全局 ID、同秒时间戳和服务消息均不可改变该次序。
- `HistoryCursor` 是内部类型，公开传输只能用认证的 opaque token，绑定用户、会话、完整原始 query、S/W、generation/format、最后返回 ID 和过期时间。续页保持原 S/W，只按 last ID 向后继续，**不再次应用 anchor/AddOffset**；每页重新验证权限。新增消息不进入旧快照。旧公开接口不自动新增分页字段，新页调用未提供游标时仍是新快照。
- 游标 format=1，初始有效期固定 15 分钟，续页不延长。签名/参数不匹配返回 400 invalid_history_cursor；到期或其 generation 已不可用返回 410 history_cursor_expired，要求重开查询；不能悄悄换新快照。空结果不得生成无进展游标。
- 为保证 W 固定的旧游标仍能从 PG 取 W 以上数据，v1 **禁止清理 PG 消息载荷**。Scylla 不可用时只允许由完整 PG 数据等价回读，否则明确 503，不可返回缺页并推进游标。跨桶执行须有界扫描与可解释索引，不使用 ALLOW FILTERING；超资源预算明确失败而非返回不完整成功。

### 10.3 Outbox 状态机与发布

- `DeliveryEvent` 为不可变 envelope：固定 UUID/event/schema、partitionKey、type、类型化 payload、提交时 recipients（各自可选 pts）、创建/过期时间、可选会话 sequence/消息版本/投影 format。message.created 三个投影字段必须存在，其余非消息事件为空；前三类同步事件 recipients 必须有 pts，其余为空。过期通知仍可重放 envelope，但下游不得当作新鲜通知再次展示。
- 分区键冻结：有会话的事件用 conversation_id；无会话 GID 事件用 `user:<user_id>`。不能以瞬时成员列表派生。接收人数组按 ordinal 排序且无重复，载荷 type 必须与具体类型一致；未知 schema/type 不得伪装成功。
- 状态为 Pending→Leased→Published；可重试失败 Leased→Pending（保存 retryAt），不可处理失败→Blocked，保留数据并报警。禁止以“超过重试次数”删除事件。Blocked 和活跃租约阻塞同键后继；每键仅可 claim 最早未发布 event_id，不要求不同键的 Kafka 到达顺序一致。
- 每次 claim 生成新的 LeaseToken/attempt，时间以 PG 时钟为准；renew/confirm/fail 仅当前未过期 token 可执行。进程崩溃后租约到期可重新 claim；过期 worker 不能更新 PG 状态。PG 租约不能撤销已经发往 broker 的旧请求，因此消费者仍必须容忍迟到重复及乱序。
- Publisher 启用幂等 producer 和 acks=all；min.insync.replicas/副本数部署验收时验证。返回 BrokerAcknowledgement(topic,partition,offset) 后才可持 token 标记 Published；超时为未知，不标记成功。确认后、标记前崩溃必须重发同一 envelope。
- Published/Blocked 可显式 Replay→Pending，重新经过相同 claim 流程，不改 UUID/event/创建时间/接收人、不加 pts、不生成用户消息。重放旧事件不会撤销下游持久去重；历史重建使用新 generation。v1 不提供 outbox/inbox/差分清理接口，Published 不代表可以删除恢复源。

### 10.4 消费、追赶与重建

- 选择“先持久 inbox+任务，后副作用”的方案，避免 PG/Scylla 双写事务。StageAsync 在一个 PG 事务内校验 envelope，写去重键 `(consumer_name,generation,outbox_id)` 及唯一 `(consumer_name,generation,event_id)`，并保存可恢复投影任务。相同键不同 envelope 必须失败，不把污染当 Duplicate。
- Stage 只有 PG durable commit 成功才返回 Staged/Duplicate；消费者此后可提交 Kafka offset（实际提交该 offset+1）。每分区必须全部前序都已 Stage 才能推进提交点，不能因并行任务先完成而跨过未落盘前序。rebalance/丢响应允许重复 Stage。毒事件阻塞该分区、记录告警，不静默跳过或提交越过。
- Kafka offset 只是恢复扫描位置，不是全局 cursor。不同 topic/partition、重放和租约迟到不保证全局 event 顺序。PG 差分不由消费顺序生成；投影按 sequence 完成标记推进连续水位，任何重复都不得新增 pts。
- Projection worker 独立领取带 fencing token 的持久任务，Scylla quorum upsert 后再 CompleteAsync；Complete 和连续 checkpoint 推进同 PG 事务。写失败只 Retry，租约过期重领。Stage 成功但 Scylla 停机时任务仍在 PG，不依赖 Kafka offset 回退来恢复。
- 正常追赶扫描未完成持久任务；Kafka 保留窗口已过而尚未 Stage 的事件从 PG 不变 outbox/envelope 补送。v1 不删除这些恢复数据；Kafka 不是唯一重建来源。
- `IProjectionSource.ReadAsync` 按 `(afterSequence,throughSequence]` 返回有界、按 sequence 升序的原始 envelope；直接 PG 恢复/回填的 Stage 使用空 BrokerPosition，不伪造 Kafka offset。BeginRebuild 在会话锁内创建独立 generation 并捕获权威 S；从完整 PG 接收日志有界回填到 S，同时为新 generation 追赶之后提交的消息。consumer 去重包含 generation，因此新代可重新投影，但不能重新生成业务事件/消息/pts。基线回填沿用原 envelope 身份，不能伪造新 UUID。
- Activate 必须持会话锁比较 ExpectedActiveGeneration，并检查新代 format、内容摘要/主键集合及连续水位覆盖**当时最新** sequence；有缺口或别的重建已切代则返回 false，继续追赶或明确冲突。只更新 PG 活跃代指针，不做跨库原子切换；写入后续任务必须读取该指针。
- 活跃代切换不改变已有游标的 generation；旧代至少保留切换后 15 分钟以覆盖存量游标。重建未完成不得删源。v1 继续保留 PG 载荷和所有代数据；后续若引入清理，需要独立冻结租约/保留策略并完成故障验证，不能凭 Published 或 max(sequence) 清理。

### 10.5 实现验收边界

第一步是契约完成，不是适配器完成。后续必须实现并验证：租约 fencing 与迟到 publish、ack 丢失重放、inbox/offset/rebalance 崩溃点、Scylla quorum/版本冲突、缺口水位、负 offset/日期/多桶历史、固定游标跨切代及过期、重建追赶与并发切代。容量上限、吞吐、HA/RPO 与部署副本参数仍须测量和单独验收，未在本轮承诺数值。

本轮实际检查：

- `dotnet build Storage.Tests/Storage.Tests.csproj --no-restore --nologo` 成功，后端及原 PG 接受测试均编译通过，0 警告/0 错误，无新增依赖。
- `node tests/rpc-boundaries.mjs` 通过：156 次 RPC、131 个成功 TL fixture；未执行 Java 回读。
- `node tests/storage-policy.mjs` 本轮未通过：子进程非零退出但未捕获到诊断文本，断言期望 Storage:Mode 等文本而实际为空；日志在 `obj/storage-freeze-policy.log`。直接运行无效模式也以 -532462766 退出且无输出，尚不能确定原因。未修改断言或存储开关来掩盖失败；第 9 节此前通过记录不代表本轮重跑通过。
- 此处记录为第 10 节契约阶段的检查；后续适配器实现与重新验证见第 11 节，不能把两个阶段的结果混用。

## 11. M3/M4 适配器实现与隔离验证（2026-11-03）

### 11.1 已实现，未接入生产

- PostgreSQL 持久 outbox/inbox、broker 坐标、projection task、租约 fencing、连续 checkpoint、重放及重建代管理。初始活跃代在会话行锁内原子分配为非空 UUID；Stage 拒绝空、未知、跨会话或已失效代，只接受当前活跃代或基于当前活跃代分配的重建代。容量统计由事务 advisory lock 串行化，查重先于容量拒绝。
- Kafka publisher 使用幂等生产及 all ack；consumer 禁用自动提交，PG Stage 成功后才提交 offset，Stage/Commit 失败后必须重开实例。实时消费可解析当前活跃代，重建协调器固定代并按 PG 原始 envelope 有界回填。
- OutboxPublisherWorker 和 ProjectionWorker 提供有界批次、领取后续租、指数退避；发布达到上限后阻塞原事件，人工重放保留原身份。Scylla 成功回读/写入之后才确认 PG 任务；外部成功但确认丢失时允许幂等重试，不伪称跨库事务或 exactly-once。
- Scylla 适配器实现分桶、代隔离、LocalQuorum/LocalSerial、重复与内容冲突检查及源回读校验。当前 ScyllaSourceValidator **只比较权威源覆盖的桶**，不检测范围外额外桶，空代也没有完整键集合检查；因此尚不满足第 10 节的完整集合验收，不应据此授权生产切代。
- Redis 只缓存已提交消息，带环境及代命名空间和有限 TTL。每次命中仍调用权威授权；缓存缺失、损坏、超时和连接错误回源，权威读取错误继续失败，不以缓存替代权限、会话、幂等键或 pts。
- 以上为独立适配器和批处理组件，未注册为生产后台链路；应用仍强制 LegacyOnly，没有删除 JSON、迁移正式数据或部署服务。

### 11.2 实际结果

- WSL Linux .NET SDK 8.0.408 构建 Storage.Tests 成功，0 警告、0 错误。依赖与组件数据位于 D 盘。
- 真实隔离 PostgreSQL 16：接受事务、并发幂等、回滚、租约 fencing、重放、checkpoint 缺口、代分配及并发容量测试通过。12 路容量测试只剩 3 个名额时成功 3 次、拒绝 9 次；消息、事件、outbox 与序号保持原子性。激活外部 verifier 在这组测试中是替身，不是 Scylla 验证。
- 真实隔离 Redis 7.4.2：写入/命中、TTL、命名空间 key 删除后重建、关闭原客户端连接后的回源通过。另有 fake transport 超时/损坏/取消测试。**没有执行 Redis 服务停机或真实 PG 停机**；PG 读取失败为注入错误。
- 真实 PG + fake publisher/projection 的 worker 编排通过：模糊成功不误确认、有限发布重试及阻塞、原 envelope 重放、无伪造 broker 坐标的回填、幂等投影与连续水位。不能把它算作真实 Kafka/Scylla 持久性验证。
- 真实 Kafka 3.9.0 启动及 API 检查曾通过，但本次联测失败：初始 offset commit 遇协调器错误，清理 topic 时 D 盘 drvfs 目录重命名发生 AccessDeniedException，broker 将日志目录置为离线并退出；后续连接拒绝。**Kafka 端到端、offset reopen 与崩溃点验证未通过**，不修改断言掩盖环境失败。
- Scylla 6.2.3 镜像未下载完整，没有启动，真实 quorum、冲突、重建切代测试均未执行。
- 不带 Kafka/Scylla 环境变量的 PG + Redis 完整回归退出 0，并明确打印两项 SKIP。
- storage-policy 在 Linux 运行通过 21 项启动边界断言，弥补第 10 节 Windows 本轮重跑失败的覆盖，但未解释 Windows 无输出退出的根因。rpc-boundaries 通过 156 次 RPC、131 个成功 TL fixture；使用现有 Linux Node 20.12.2，未新增下载，尚未按项目要求在 Node 22+ 重跑。未执行 Android Java TL 回读、真机、性能、HA/RPO 或多副本故障验证。

下一步仍需补全真实联测、完整投影键集合验证与故障矩阵；未达到 M3/M4 完整验收或生产切换条件。Kafka 挂载修复结果见下节，不覆盖上述首次联测失败记录。

### 11.3 Kafka 隔离目录挂载修复（2026-11-03）

- 避开 NTFS/WSL drvfs 的目录重命名限制：在 `D:\BotgramBuild\tmp\m34-kafka-ext4.img` 创建 8 GiB 稀疏 ext4 映像，loop 挂载至 `/mnt/botgram-m34-kafka`，将旧数据复制至其 `data` 子目录。原 `D:\BotgramBuild\tmp\m34-kafka-data` 完整保留，未重新 format Kafka 元数据。
- 隔离配置 `D:\BotgramBuild\tmp\m34-kafka.properties` 的 `log.dirs` 改为 `/mnt/botgram-m34-kafka/data`；仍仅监听 loopback 19092/19093。启动脚本 `D:\BotgramBuild\tmp\m34-kafka-start.sh` 检查 ext4 挂载、loop backing file 及既有 meta.properties，未挂载时先挂载映像，避免向挂载点下的 WSL 根文件系统写数据。使用前台进程保持 WSL 会话，不注册自动启动或修改系统 fstab。
- 执行 `wsl -d Ubuntu-24.04 -u root -- bash /mnt/d/BotgramBuild/tmp/m34-kafka-start.sh` 启动该隔离实例；首次及 WSL 重启后的挂载需要 root。映像最大容量固定为 8 GiB，仅适用于当前隔离测试，不是生产容量配置。
- 真实验证通过：唯一 topic 创建、写入及读回；删除 topic 后确认 partition 及删除临时目录消失，broker 仍健康；另一个 topic 写入后 TERM 优雅停止、重新启动，再读回同一记录，随后删除并确认目录清理。本轮检查新增日志 372,968 字节，未见 AccessDeniedException 或 KafkaStorageException。
- 测试脚本及状态保存在 D 盘临时目录 `m34-kafka-mount-verify.py`、`m34-kafka-mount-verify-state.json`。本轮没有重新运行 PG→Kafka→PG Inbox 集成测试，不代表此前的 offset 协调器问题及全链路验收已通过。
- 测试结束后已优雅停止 broker、同步并卸载 ext4 挂载，19092/19093 无监听。卸载及定向 detach 均返回成功，但 `/dev/loop0` 仍显示关联且 AUTOCLEAR=1；未强制操作其他设备或重启 WSL，不能声称 loop 关联已经完全释放。再次使用前应核对该映像与 loop 状态。未触碰生产服务或扩大网络监听范围。

### 11.4 Scylla 就绪及完整现有测试续跑（2026-11-03）

- Scylla 6.2.3 镜像下载完成并实际运行，镜像 digest 为 `sha256:a9d904089abe9a4f8b5b893ebb5b5bf8b5a1bd0dc6658921cf05f89d3712289c`。数据使用 D 盘 12 GiB 稀疏 ext4 映像 `m34-scylla-ext4.img`，单 shard、开发模式，实际内存参数 1 GiB；1536 MiB 与 2 GiB 曾因容器可分配内存约 1.4 GiB 而启动失败。仅适用于隔离功能测试，不是生产容量配置。
- 首次真实 Scylla 联测暴露 `LWT is not yet supported with tablets`。投影 keyspace 创建现在显式设置 `tablets = {'enabled': false}`，保留 LocalQuorum/LocalSerial 与条件写入，不以普通覆盖写掩盖一致性要求。此设置仅作用于新建 keyspace，不迁移已有 tablets keyspace。测试增加首次直接 upsert，使驱动异常能直接暴露，并覆盖随后 worker 的同键重试。
- 修复后 Storage.Tests 构建 0 警告、0 错误，完整真实 PostgreSQL + Redis + Kafka + Scylla 回归 exit 0，无 Kafka/Scylla SKIP：包括 Kafka 发布、PG durable Stage、offset commit/reopen，以及 Scylla 条件投影、重复内容、同版本冲突、源回读验证和重建代激活。PG 接受事务、fencing、重放、连续水位及容量测试同时通过。构建使用 `UseAppHost=false`，避免既有输出目录与 Linux apphost 同名冲突。
- Node 22.14.0 下所有 11 项 `tests/*.mjs` 均 exit 0（HTTPS 使用本轮此前的隔离 TLS 成功记录）。10 项续跑日志统计 1,403 次 Node 断言调用，不含 HTTPS、内嵌 C# 与协议断言。Java/C# 协议双向 66 个 fixture 验证通过。
- Protocol.HttpTests 首次失败于按操作系统猜测目录写入异常类型。测试改为在同一路径以同样 FileStream 参数探测实际 IOException/UnauthorizedAccessException，再精确检查相应错误码；500、磁盘快照不变、重试幂等断言均保留。重新构建与完整测试 exit 0，报告 2,913 assertions，WebSocket 子阶段报告 599 assertions，不将两者擅自相加。
- 本轮新生成的 40 + 2 + 26 + 131 = 199 个 TL 响应均由真实 Android Java TL 类解码、序列化、重新解析通过。Windows 验证脚本创建 D 盘输出目录遭 AccessDenied，改用 D 盘 Linux JDK 与等效宿主命令；不是 Android 真机验证。
- 其余测试命令、历史失败及修复后记录保留于 D 盘 `backend-regression-20261103-2334/results.tsv`，HTTP 最终日志为 `protocol-http-tests-fixed.log`。四组件最终成功命令为 `dotnet build Storage.Tests/Storage.Tests.csproj --no-restore --nologo -p:UseAppHost=false` 后执行 D 盘 `m34-run-storage-tests.sh all`；首次失败日志 `m34-storage-all.log` 保留，不冒充最终成功日志。
- 收尾已优雅停止本轮 Scylla、Redis、Kafka、PostgreSQL 及专用 Docker daemon，sync 后成功卸载两个 ext4 映像；`losetup -l` 无关联，隔离组件端口不再监听。镜像、数据和日志保留在 D 盘；系统 Docker、生产部署及数据库均未操作。
- **边界不变**：以上是现有功能测试全绿，不是 M3/M4 完整验收。第 11.1 节投影额外桶/空代完整集合验证缺口仍在；未测多副本 HA/RPO、性能、真机、Redis 服务停机或真实 PG 停机、完整 broker/rebalance/崩溃矩阵。typed-persistence 的 Windows 专用原子替换分支及 Flush(true) 失败注入未覆盖。保持 LegacyOnly，未授权或执行生产切换。

## 12. 全键集合、恢复与隔离业务对照续验（2026-11-03）

本节是后续结果，不覆盖前文历史失败。默认仍为 LegacyOnly，NewStorageShadow / NewStoragePrimary 继续拒绝启动；没有生产部署或正式库访问。

- Scylla 验证改为有界全表物理键审计（预算+1，前后两遍），比较 generation、conversation、bucket、message_id 与载荷，并验证源 sequence 连续、消息 ID 唯一和内容完整。真实组件覆盖空代、额外桶、错误物理键/代、其他会话污染、保留旧代、重建追赶与并发激活 fencing。允许保留的旧代仍消耗审计预算；这是离线有界审计，不是在线全库强一致快照，也没有自动删除旧代。
- 恢复矩阵新增真实 PG 协议代理：收到 COMMIT 的已提交 ReadyForQuery(I) 后吞掉回复并断开客户端，调用方报告失败，原幂等键恢复后只有一份 message/event/outbox，pts 不重复增长。Kafka 在 PG Stage 已成功、offset 尚未确认时注入失败，重开 consumer 重放，inbox/task/position 不重复。后者是精确故障点异常注入，不是操作系统 kill、rebalance 或多副本故障切换。
- NewStorageValidation 通过 Development + ValidationAcknowledged=true + 显式 loopback botgram_test_* PG 连接启用；不创建旧 DataPath 或文件租约，不在 PG 失败时回落 JSON。PostgresValidationStore 采用独立 schema 的全库 JSONB 快照、FOR UPDATE 串行事务及 synchronous_commit=on；COMMIT 不确定时不自动重试。它是兼容验证桥，不是 botgram_intake_v1 关系化业务入口，不代表目标存储架构已完成。
- Protocol.HttpTests --storage-parity 从相同隔离 seed 对全部 21 个 PB RPC 比较响应语义及 25 个完整持久化状态检查点，覆盖身份、消息 ID、pts、幂等及进程重开重试；最终连同延迟采样共 1,845 项断言通过。旧端先提交，之后离线隔离重放，绝不让影子结果参与旧请求成功判定或对正式库双写。尚未实现生产在线影子队列/增量捕获。
- Scylla 影子读只在 conversation/generation/format 和连续 checkpoint 等于采样已提交水位时比较；滞后或不同代返回 Deferred，不误报语义差异。调用方须在权威会话锁或停写窗口内采样。真实测试验证滞后 Deferred、代不匹配 Deferred、相同水位 Match；这不是在线读路由。
- 最新 Storage.Tests 全四组件运行 exit 0，无 Kafka/Scylla SKIP；构建 0 警告/0 错误。Node 22.14.0 storage-policy 33 项门禁断言通过；完整 Protocol.HttpTests 2,913 项断言通过（WS 子阶段 599，不相加）。本轮未重跑 Android 或 Java TL 验证。

### 12.1 诊断性能对照，非验收结论

相同快照、PB loopback、并发 1；每操作/存储 5 次预热及 40 次正式采样，交替部分先后顺序，保存所有排序样本。PG 测试连接 Pooling=false，计入建连、认证/快照事务等成本；不是优化后关系化 intake 的性能，不能据此推出生产吞吐或扩展性。40 个样本的 p99 实际接近最大值。

| 操作 | Legacy p95 ms | PG 快照桥 p95 ms |
|---|---:|---:|
| GetState | 10.84 | 150.47 |
| GetHistory | 11.14 | 147.30 |
| SendMessage | 11.29 | 136.68 |

本次快照桥显著更慢，没有性能通过结论。最终记录为 D:/BotgramBuild/tmp/p6-http-runs-441470da6b0a4b1497a1569b124e1f63/storage-parity.json。初次采样复用了 seed 中已有 long.MaxValue 幂等键而触发冲突，已改用独立键并完整重跑；没有放宽成功断言。此前 HTTP 启动被环境代理误转发到宿主，最终测试仅在子进程清除代理并直连 loopback，未修改系统网络设置。

仍未完成：全关系化 21 RPC 业务迁移、在线影子链路、持续负载/并发性能门槛、多副本 HA/RPO、真实 PG 停机与完整跨组件 kill/rebalance 矩阵。禁止将兼容桥对照通过解释成这些目标完成或生产切换许可。

本轮收尾：优雅停止隔离 Scylla、Redis、Kafka、PostgreSQL 和专用 Docker daemon；sync 后两个 ext4 映像卸载成功，losetup 无关联，15433/16379/19092/9042 等隔离监听均消失，保留 D 盘数据及日志。未停止系统 Docker 或其他应用。Windows git diff --check 通过（仅既有换行提示）；构建不创建提交。

### 12.2 三操作真实关系化 PG 与连接池重测（2026-11-03）

新增显式 `Storage:ValidationBackend=RelationalThreeRpc`，仅允许 Development + NewStorageValidation + acknowledgement + loopback `botgram_test_*`。默认仍为 SnapshotBridge / LegacyOnly；不解析旧 Store、不启动通话维护，不支持的路由和 RPC 明确拒绝。只支持 GetState、GetHistory、SendMessage 和预建会话，不支持账户/会话元数据修改或完整 21 RPC，不能作为生产切换开关。

关系化路径复用 PG acceptance 事务，身份/会话/成员/回执/pts 分表查询；消息逐行 JSONB 保存，历史使用会话/消息 ID 索引和 SQL 分页，不读写整个 Database 快照。单例 NpgsqlDataSource 强制 Pooling=true，本次池 Min=0、Max=20。隔离 fixture 仅空库导入一次；三类旧同步事件用于初始化 pts，只有匹配现存消息的 message.created 导入 intake event，其他类型不伪造成消息事件。未启动 delivery，不宣称这是完整投影恢复库。

同一 seed、PB HTTP loopback、并发 1，每组 5 次预热 + 40 次正式请求，分开恢复 seed。以下均为本轮实际 p95（ms）：

| 操作 | LegacyOnly | PG 快照桥 + 池 | 关系化 PG + 池 |
|---|---:|---:|---:|
| GetState | 13.96 | 9.46 | 1.75 |
| GetHistory | 10.62 | 7.79 | 3.67 |
| SendMessage | 10.78 | 10.22 | 21.22 |

关系化读取明显降低本轮延迟，但发送仍比 Legacy 慢，不能宣称整体性能达标。池化快照桥已由历史约 140 ms 降到约 8–10 ms；前后不同运行轮次且无分阶段计时，不能把全部差值严格归因于某个内部阶段。关系化发送包含多个 SQL 往返、权限/幂等/pts/event/outbox durable commit，具体热点尚未采集分阶段证据。40 样本及单并发不能代表吞吐或生产 p99。

实际验证：编译 0 warning / 0 error；完整协议 2913 断言（含 WS 599）、启动门禁 42 断言通过；存储测试 4779 断言通过，其中 21 RPC 是快照桥语义对照，三 RPC 是关系化逐请求规范化 PB 对照（仅归一化时钟字段）、历史分页、非成员拒绝、不支持操作拒绝、进程重启后原键重试/冲突及 pts/消息不重复。未验证数据库重启、真机或持续并发。报告：`D:/BotgramBuild/tmp/p6-http-runs-c26ed282deb645ea85f08589bae1e09d/storage-parity.json`。完整协议日志：`D:/BotgramBuild/tmp/p6-http-runs-3a840d8a025a45d5b1dbc627816daf9a`。

测试中发现并修复测试宿主漏传 ValidationBackend、重启时重复传一次性 seed；失败轮次不计入上表。WSL 在调用间退出会停止其后台数据库，因此最终同一 WSL 调用内启动、执行及优雅停止隔离 PG。子库已由 finally 删除，15433 无监听；保留测试日志，未访问正式数据、部署或提交。

### 12.3 SendMessage 阶段诊断（2026-11-03）

仅增加现有 `Protocol:DiagnosticStages` 下的 Activity 阶段数据，不改变 SQL、schema、事务隔离或 synchronous_commit=on。`BOTGRAM_SEND_DIAGNOSTIC=1` 只测关系化发送：每轮独立同构 seed、池 Min=0 Max=20、并发1、10次预热+100次正式样本。此模式只验证发送 HTTP/协议信封，不执行三存储逐响应对照及关系化重启检查；仍执行入口已有的快照桥回归，每轮1632断言通过。最终编译0 warning/0 error。

| 采样方式 | HTTP median ms | HTTP p95 ms | HTTP p99 ms |
|---|---:|---:|---:|
| 阶段关闭、SQL日志关闭 | 17.07 | 20.58 | 21.68 |
| 阶段开启、SQL日志关闭 | 16.57 | 18.51 | 19.56 |
| 阶段开启、SQL全量日志开启 | 33.02 | 36.58 | 41.51 |

原21.22ms与关闭阶段后的20.58ms接近，但不同轮次/样本数不能逐毫秒分解旧p95；阶段开关差异不能解释为性能提升。全量SQL日志明显扰动，只用于核对调用和数据库执行，不作为性能基线。

以下来自无SQL日志、阶段开启轮，均为100样本的均值与独立p95（ms），包含关系不能相加：

| 阶段 | mean | p95 |
|---|---:|---:|
| 完整HTTP（客户端读完响应） | 16.8461 | 18.5149 |
| 服务端中间件总计 | 16.2091 | 17.9415 |
| 三次连接池获取合计均值 | 0.0325 | 不相加 |
| 入口认证查询 | 0.4893 | 0.6615 |
| 读取/解码 | 0.0592 | 0.1078 |
| 元数据事务鉴权 | 0.4321 | 0.5046 |
| 元数据五次查询/读取 | 2.0472 | 2.3490 |
| 元数据事务提交 | 0.1957 | 0.2394 |
| acceptance初始化（含首次实际发送BEGIN） | 0.2522 | 0.3254 |
| 权限/幂等 | 1.6292 | 2.0397 |
| 容量/advisory锁及backlog查询 | 0.8217 | 0.9856 |
| 消息sequence/INSERT/body更新 | 1.3537 | 1.6653 |
| pts/event/recipients/outbox | 3.1141 | 3.7013 |
| 响应映射+PB编码 | 0.1132 | 0.1684 |
| acceptance COMMIT | 5.3720 | 5.8457 |
| 响应写出API | 0.0480 | 0.0587 |
| acceptance 20次SQL调用合计（包含于上述阶段） | 6.9325 | 7.9788 |

BEGIN API是驱动延迟发送，API时间不能当数据库BEGIN往返；实际初始化计入首个SQL。成功发送包含入口认证1次、元数据鉴权1次、元数据5次以及acceptance20次，共27次串行命令调用，另两次COMMIT；BEGIN可能随首命令发送，不按API次数猜网络包数。三个池获取均值分别0.01489/0.00798/0.00965ms，未见池等待为主因。

完整HTTP p95对应同一条样本（正式第69条、含暖机第79条、trace尾号50）：HTTP18.5149、服务端17.9056；acceptance SQL合计7.1851、COMMIT6.9100、元数据2.1576、入口认证0.4333、元数据鉴权0.4345、元数据提交0.1850、响应准备0.0883ms。此处是同请求分解，不是把不同请求的独立p95相加。

SQL跟踪轮按backend PID1495核对，排初始化事务与10次暖机后，100次发送恰有2000 Parse、2000 Bind、2000 Execute、100 COMMIT；每事务服务端Parse/Bind/Execute均值0.55049/0.94441/1.28589ms，COMMIT5.48893ms。该轮客户端20次SQL均值17.85158ms，扣除服务端Parse/Bind/Execute后仍差15.07079ms，包含驱动、协议往返、调度、结果读取及日志扰动，不能全部称为网络。不能将该轮服务端SQL时间直接从另一轮无日志时间扣除。

结论：主要可测成本是串行数据库调用链与durable COMMIT，应用映射/编码和池获取不是主因。COMMIT约占无SQL日志服务端均值33%，它是单个最大阶段；不能单凭COMMIT墙钟证明全部是WAL fsync。SQL跟踪以log_lock_waits=on、deadlock_timeout=1ms启动，未见锁等待记录；只支持本次单并发没有观测到明显锁争用，不排除短等待及高并发争用。虽然开启track_io_timing/track_wal_io_timing，本次未采集逐请求WAL IO/等待事件或EXPLAIN BUFFERS，因此WAL刷盘、OS调度及更细索引维护成本尚未严格拆分。未降低持久性作对照、未优化或重构。

证据目录：`D:/BotgramBuild/tmp/p6-http-runs-d13ca4a32a3d4f67851b936afbf85563`（阶段开启、backend-1392.stdout.log）、`p6-http-runs-0ecc28df9f4c4315b37f864804c3bea9`（阶段关闭）、`p6-http-runs-1cc9e03741004b63868a2a3b764d657c`（SQL跟踪、backend-1466.stdout.log），各有storage-parity.json；PG日志为`D:/BotgramBuild/tmp/m34-pg-meta/send-diag.log`。该批requestId重复为http_test-1，按单并发trace与样本顺序关联；后续代码已为诊断样本生成唯一send_diag_NNN（编译通过，未另重跑，仅改变诊断关联及报告文字）。旧JSON的通用scope文字不适用于诊断模式，以上实际模式为准，代码已纠正latencyScope与控制台范围说明。所有子库finally清理，PG优雅停止，15433未发现监听；未访问生产或部署。

### 12.4 接收人 pts 集合更新（2026-11-03）

本轮只改 `PostgresMessageAcceptance` 的 pts 循环：已按 .NET Ordinal 排序的接收人以 text[] 输入，用 WITH ORDINALITY 保持更新顺序，一条 INSERT SELECT / ON CONFLICT DO UPDATE / RETURNING 返回每人的 pts。首次插入为2，已有值加1；返回结果以 user_id 关联，不依赖 RETURNING 顺序。event_recipients 仍逐人写入，鉴权、幂等、容量锁、最后获取事件游标、响应准备及编码成功后提交均未改变。未修改 schema、fsync 或 synchronous_commit。

优化前后分别构建到 `D:/BotgramBuild/tmp/pts-before-bin`、`pts-after-bin`；两目录使用相同新测试 DLL，后端 DLL 独立保留。后端 SHA-256 分别为 `11CA931D16DD421900B3760D3B371DA7C123A83C50DDE44D7784E054D5B7A0A13`、`C7E33054D743CB5BD959BC287FD775487775F0CE1150373DF08D8D01BAEE7060`。

独立阶段轮（每版10暖机+100样本，阶段开启但SQL详细日志关闭）实际观测每次acceptance命令数20→17，上游7条不变，即27→24，另两次COMMIT未减少。pts/event/outbox阶段均值3.0126→2.1892ms。COMMIT均值5.3815→4.7983ms；p50/p95/p99分别为5.1364/6.2286/8.8734→4.6874/5.6345/5.8811ms。这只是独立两轮观察，不能把COMMIT差值归因为本次优化或WAL刷盘改善。证据为 `D:/BotgramBuild/tmp/pts-stages-before.log`、`pts-stages-after.log`；关联后端日志分别位于 `p6-http-runs-3fa0cc9126fd4a75a9c789634a969560/backend-3493.stdout.log` 与 `p6-http-runs-f561f952c8b74c188be0b97501b26868/backend-3745.stdout.log`。

正式性能轮关闭阶段及SQL详细日志，以 before→after→after→before 交错。每轮 LegacyOnly / PG-relational、small / grown（预填充150条）、并发1/2/8，共12组；每组10暖机、96样本。以下是同会话 PG 两轮各自分位数的范围（不是合并样本分位数，单位ms）：

| 数据/并发 | before p50 / p95 / p99 | after p50 / p95 / p99 |
|---|---|---|
| small/1 | 15.01–15.68 / 17.37–17.61 / 19.57–23.13 | 14.22–14.78 / 16.88–20.32 / 17.42–29.45 |
| small/2 | 25.50–28.61 / 29.93–47.28 / 126.32–130.30 | 22.52–23.00 / 31.43–37.04 / 117.96–126.57 |
| small/8 | 63.05–71.36 / 326.44–449.48 / 416.11–560.33 | 54.86–65.47 / 298.95–519.18 / 386.83–649.79 |
| grown/1 | 13.15–14.31 / 15.59–20.93 / 17.25–68.87 | 12.57–13.32 / 14.13–21.13 / 14.83–36.27 |
| grown/2 | 21.74–22.42 / 24.52–26.74 / 126.15–162.70 | 21.09–26.31 / 38.29–49.66 / 111.45–150.99 |
| grown/8 | 58.22–64.63 / 338.69–340.18 / 434.80–435.21 | 55.25–62.63 / 312.80–314.60 / 398.25–404.18 |

结论仅为SQL调用数确定减少、目标阶段均值降低；HTTP中位数有改善趋势但非所有组一致，grown/2的p95两轮均变差，small/8尾延迟仍波动，不能宣称稳定全面提速。四轮各9990 assertions通过，证据为 `D:/BotgramBuild/tmp/pts-matrix-v2-{before1,after1,after2,before2}.log` 及其中关联的 storage-parity.json（含原始样本与Legacy对照）。早期1000条HTTP预填充触发300次/分钟限流，失败轮不用于结论；最终改150条，不放宽产品限流。

正确性：Storage.Tests真实PG通过，包括并发幂等、权限/会话过期、响应准备失败、COMMIT成功丢响应后重试、同接收人不同会话并发锁顺序、消息/事件/pts/outbox不重复；新增Outbox触发器注入数据库异常，验证完整业务快照回滚且失败幂等键可重试。完整HTTP存储语义回归4779 assertions通过（21 RPC及三存储延迟采样），存储策略42通过，编译及diff whitespace检查通过。

验收边界：不同会话只有功能并发测试，没有HTTP性能矩阵；150条不是大数据索引验证，未执行EXPLAIN BUFFERS或大规模数据测试；没有逐请求WAL同步写分解，也未额外运行Android/真机测试。本轮未部署、未提交git、未访问正式数据库。隔离PG已优雅停止。

### 12.5 事件接收人集合写入（2026-11-03）

在12.4的独立17命令产物基础上，仅把 event_recipients 的逐人 INSERT 改为一条 INSERT SELECT FROM unnest(text[], integer[])；两数组来自同一个 recipientPts 列表，以相同顺序投影，保持 user_id 与 pts 对应。不合并权限检查、不改变事务及锁顺序、不改变schema或同步提交。双人新消息acceptance为16条命令，上游7条不变（共23条，另两次COMMIT）。成员锁及排序、pts更新、事件游标与outbox依赖仍保留；消息INSERT后body回写依赖数据库生成ID，本轮不混入其他优化。

独立产物为 `D:/BotgramBuild/tmp/pts-after-bin`（before）与 `recipients-after-bin`（after）。正式轮关闭阶段及SQL日志，使用原有same-conversation矩阵（Legacy/PG、small/grown150、并发1/2/8，每组10暖机96样本）。`recipients-before1.log`、`recipients-after1.log`、`recipients-after2.log`各9990 assertions通过。首次`recipients-before2.log`未完成：DROP DATABASE触发的checkpoint等待超过命令超时，PG日志显示checkpoint总25.847秒而write/sync仅0.058/0.029秒；该失败轮不纳入完整性能结论，不调整产品限流或持久性来掩盖。PG证据为 `D:/BotgramBuild/tmp/m34-pg-meta/recipients.log`。

前三轮中small/1 p50 before15.17、after13.67/14.56ms，small/2 p50 before24.43、after33.92/36.26ms；grown/8 after第二轮p95达到532.33ms。目标SQL调用减少是确定事实，端到端尾延迟并未稳定改善。原始样本分别在 `p6-http-runs-ccdad3554b46473fb1b95e515d7a20f1`、`p6-http-runs-b59ed2d662b5491ca071dd146ba90adf`、`p6-http-runs-3461c2148b2e41f7bc8d6056972e72eb` 的 storage-parity.json，均位于D盘隔离测试根目录。

本轮实际before产物SHA-256为 `124B2602D86245A8BE4E86C48A6B228BC192BD655C072695CE5E180347FF89E1`，after为 `C438A41F0833FF11F5879575ECC1638C188AB2ACEFBEB72E1841E6FD0A492B6F`；before目录当前哈希不同于12.4当时记录，不能宣称二者为逐字节相同产物。本轮HTTP产物及新增Storage.Tests诊断产物均编译通过，后者0警告0错误；当前隔离DLL的存储启动边界42项通过。最初误用Windows默认旧DLL因8521占用失败，不算通过，随后显式指定本轮DLL重跑成功，未停止原服务。

新增三接收人 user_id/pts映射及第二成员INSERT触发异常整事务回滚/重试测试，在WSL恢复后的完整Storage.Tests中实际通过（见12.6）。第四轮补跑 `recipients-before2-restored.log` 9990项通过，原始样本为D盘隔离根目录 `p6-http-runs-63006d3ab5254cf58a1954254d9edf24/storage-parity.json`。该轮与前三轮之间有WSL恢复间隔，不是无间断交错实验。完整HTTP三存储parity在 `parity-restored.log` 4779项通过，原始输出目录 `p6-http-runs-07717608e9c240a68372ca907ab5fa32`。存储启动边界42项与git diff --check通过。

完整四轮PG关系存储区间如下（每侧两轮，不合并成单个百分位）：

|场景/并发|before p50 ms|after p50 ms|before p95 ms|after p95 ms|
|---|---|---|---|---|
|small/1|15.17–19.32|13.67–14.56|20.72–29.69|15.93–16.19|
|small/2|24.43–24.53|33.92–36.26|29.99–45.89|45.40–46.06|
|small/8|62.06–62.42|58.54–60.78|346.53–412.70|302.77–308.05|
|grown/1|13.91–16.06|12.68–13.32|14.96–27.68|16.99–21.98|
|grown/2|21.16–22.73|21.80–22.24|26.47–26.52|25.53–27.16|
|grown/8|52.86–56.45|53.23–57.62|327.04–355.16|304.39–532.33|

结论仍为减少一条SQL且语义/回滚验证通过，不满足端到端稳定提速验收，small/2中位数回退不能忽略。尚未做大数据EXPLAIN BUFFERS、不同会话HTTP性能或真机验证；12.6仅覆盖不同会话的存储适配器诊断，不替代HTTP性能。

### 12.6 COMMIT/WAL 独立采样设施及阻塞（2026-11-03）

Storage.Tests 设置 `BOTGRAM_COMMIT_DIAGNOSTIC=1` 后，在临时测试库运行独立诊断：并发1/2/8，每worker10次暖机、每组96次新消息；独立会话、接收人及conversation，仍保留全局容量advisory锁。采集每请求阶段（含COMMIT、提交前SQL及锁等待所在阶段）、writer PID、独立observer的pg_stat_activity等待事件；另采集cluster级pg_stat_wal的records/bytes/write/sync/time与WAL LSN增量，顺序SELECT 1作为往返参照。每组暖机与正式writer池分别关闭，确认backend退出后读稳定统计，以避免writer本地统计尚未发布；不改变fsync、synchronous_commit或schema。每请求断言16条acceptance SQL，输出JSON包含实际DLL哈希。

边界：统计为cluster级，可能含后台WAL；等待采样并非精确1ms周期，可能漏掉短等待；独立SELECT 1不能代替并发请求网络时延；不能以COMMIT减去平均WAL时间推导逐请求网络或fsync时间。下述为组级实测归因，不是逐请求fsync或网络时间的精确拆分。

历史阻塞（已恢复）：隔离PG通过pg_ctl fast正常停止后，WSL曾出现 `getpwnam(root) failed 5`、`Wsl/Service/CreateInstance/E_FAIL` 和 `Wsl/Service/E_UNEXPECTED`。用户恢复WSL后继续测量。首次检查发现track_wal_io_timing实际为off，诊断正确拒绝运行；使用本次pg_ctl启动参数显式开启track_wal_io_timing/track_io_timing，仅增加观测，不降低fsync/synchronous_commit。又修正诊断初始化使用MaxPoolSize=1引发的嵌套初始化连接池耗尽：setup单独使用2连接池，采样writer池大小仍保持并发数，不改生产代码。

实测证据：`D:/BotgramBuild/tmp/commit-wal-diagnostic-20261103-144501-652ec2316da14c65a95206930a374ce7.json`，实际后端SHA-256与12.5 after一致；288次调用全部16条SQL，fsync/synchronous_commit均on，wal_sync_method=fdatasync。

|并发|COMMIT p50/p95/p99 ms|容量阶段 p50/p95 ms|WAL write/sync次数|WAL write/sync总ms|WAL records/bytes/LSN增量bytes|SELECT 1 p50/p95 ms|
|---|---|---|---|---|---|---|
|1|5.6435 / 11.7707 / 20.6400|0.7778 / 1.4948|96 / 96|37.891 / 543.088|3203 / 280938 / 287080|0.2243 / 0.4685|
|2|5.2488 / 6.4107 / 29.4896|7.8117 / 10.0116|98 / 98|27.808 / 496.964|3209 / 287815 / 293960|0.2041 / 0.2726|
|8|5.3734 / 8.1771 / 10.3965|65.7809 / 137.7330|97 / 97|33.543 / 479.144|3192 / 293677 / 299776|0.2049 / 0.2509|

等待采样：并发1/2/8的IO/WALSync计数141/121/127；Lock/advisory计数0/188/1679。WAL同步是COMMIT的重要成本；并发增长时COMMIT中位数基本稳定，而容量阶段及advisory等待显著增长。该阶段包括advisory锁等待及backlog COUNT，并非纯锁耗时，但等待事件佐证全局容量锁序列化不同会话写入。每96请求仍约96次sync，没有观察到显著group commit摊薄，不能只优化COMMIT或通过关闭同步提交解决。下一笔优化需要另行设计容量守恒及并发回归，不在本轮贸然移除全局锁。

`D:/BotgramBuild/tmp/storage-restored.log`完整PostgreSQL存储回归通过，包含新增三接收人映射、集合INSERT异常回滚、提交响应丢失代理重试、delivery/容量/worker等；外部publisher/projection仍为FAKE，真实Redis/Kafka/Scylla未配置而跳过，不能视作这些服务验证。未部署、未提交git、未访问生产数据。

## 13. SendMessage 容量锁临界区（2026-11-04，本轮独立证据）

### 13.1 接手基线与范围

本节是本轮结果，前面各节的断言数和性能均为历史，不能与本轮累计。初始独立工作树只有提交 `67d5806a72a8e9f143bb92fb151e3a232d28f563`，缺少全部新存储实现；没有在旧版本重写。经授权导入源工作区的未提交源码 overlay，73 文件（14 tracked 修改、59 untracked、删除 0），HEAD 相同。ZIP SHA-256 `a67e3ab5f03364a5f441049755c2756e5abe1160e0dd1429d64c7cfa9f332a52`，manifest SHA-256 `320aed8ed739a0201b47e1fa4d6d5191078fb7ea07e30b85a267849db2636d54`。导入前工作树干净，校验每个成员路径、重复/符号链接、长度及 SHA-256，导入后 73 文件逐一复核；未导入 `.git`、索引、refs 或源检出元数据。该 manifest 是用户既有修改基线，不应把相对 HEAD 的全部差异称作本轮新增。

基线 manifest 在 `C:\Users\idk Ma\.copilot\session-state\8c862192-7af2-46cc-9592-e000d0d07614\files\source-overlay-9b19e0daf09c4694a65d34a8c78272ee\manifest.json`。源码证据：`PostgresRelationalValidation.DispatchAsync` 仅允许 GetState/GetHistory/SendMessage，其他关系化 RPC 返回 501；`PostgresValidationStore` 是全状态快照桥，不是其余 18 RPC 的关系实现。`StoragePolicy.Load` 仍默认 LegacyOnly，拒绝 Primary/Shadow，RelationalThreeRpc 只在显式承认的 Development、loopback、`botgram_test_*` 环境可用。

新消息（无回复）acceptance **16 个数据库命令**：SET 1、session 锁及时钟检查 2、conversation 锁 1、members 1、幂等查找 1、容量锁及 COUNT 2、sequence/消息 INSERT/body 回写 3、集合 pts 1、event_cursor 1、event/recipients/outbox INSERT 3。接收人数不改变调用数；有 reply 另加 1。入口认证 1、视图事务认证 1、元数据 5，整个 HTTP 新发送 23 个命令，另两个 COMMIT；BEGIN 的延迟发送不能算作独立网络包。旧的 20→17 和 17→16 是历史不同阶段，不能再次累计。

### 13.2 锁、容量和其他写者的逐项审计

| 约束/锁 | 当前源码及含义 |
|---|---|
| 容量范围 | `PostgresMessageAcceptance.AcceptAsync` 的 maximumBacklog 默认 100000，隔离 HTTP 入口使用该默认值；它是适配器参数，不是持久数据库配置，多个写者须使用同一上限。计算当前数据库 `botgram_intake_v1` **所有会话**的 Outbox 中，未 Published **或**不存在活动 generation 的 Complete 投影任务的条数；一个 outbox 只算一次。不是用户配额、会话配额、永久消息数或接收人数上限。多数据库各有独立限额，不宣称跨数据库/联邦全局配额。 |
| advisory key | 单参数 signed bigint `7459241304231`，不是按用户/会话 hash。所有发送（除已鉴权的幂等命中）共用一个数据库内的排他事务锁，数据库中其他 schema 使用同 key 也会争用。诊断只输出 `backlog-global-v1`，不输出会话令牌/业务标识。 |
| 原持锁区间 | session→conversation→members→校验/幂等→容量锁→COUNT→消息写入→接收人 pts→event_cursor→event/recipients/outbox→响应准备→COMMIT；advisory 锁不能在事务提交前释放。原来不同用户/会话也在消息写入和 pts 上全局排队。 |
| 消息 ID / 历史 | identity 负责全局唯一 message_id，允许回滚空洞；conversation 行锁负责会话 sequence。advisory 不是 ID 分配器。没有永久消息/去重清理实现；不能通过删除消息、事件或已投影记录解决容量。Seed fixture 的旧历史不创建 outbox，因此本计数也不是全部导入历史的投影/存储配额；不能把 fixture seed 当成完整生产历史迁移。 |
| 接收人 pts | session 的 FOR UPDATE 还串行化同一令牌；conversation 的 FOR UPDATE 串行化同会话。members FOR SHARE 与会话锁共同固定接收人。所有 sync 行按 .NET Ordinal 排序后经 WITH ORDINALITY UPSERT；重叠接收人的不同会话仍会在 sync 争用。 |
| 事件游标 | pts 之后更新 event_cursor singleton，持行锁直到 COMMIT，使事件 ID 与提交顺序一致；这是独立的、仍保留的全局串行点。不能以 nextval/Kafka offset 代替，也不能将容量等待消失误判为没有全局瓶颈。 |
| 幂等 | session/权限、文本/实体/同会话回复验证在幂等之前；命中在所有试写/容量锁前返回旧消息和当前 pts，满容量也可恢复，不分配新 ID/sequence/pts/event/outbox。永久唯一约束未变。 |
| 发布/投影工作者 | Claim/Fail/Renew 的 Pending/Leased/Blocked 状态本来都计入 backlog；ConfirmPublished、Complete 只可能保持或降低 backlog。Stage/BeginRebuild 不修改活动 head；Activate 只有新 generation 完整通过 PG 及实际读回校验才切换，不能使已完成源记录重新变成未投影。上述代码没有持容量锁后再请求 sync/conversation 的路径。 |
| 手动重放 | 原 ReplayAsync 将 Published 改 Pending，可能让已发布且已投影的记录重新消耗容量，却没有参与容量锁。这是关联竞态；本轮补齐同 key，先锁容量、后改 outbox。恢复重放仍允许在满容量时执行，**限额是新发送准入限制，不是拒绝恢复操作的硬存量不变量**。重放可使 backlog 超限，此后新发送必须拒绝；未将恢复语义偷偷改为配额限制。 |
| 初始化/导入 | Initialize 是显式 DDL。原 SeedAsync 先取容量再锁表、写 sync，不可当作在线写者。最终版本先 pg_try_advisory_xact_lock，后 ACCESS EXCLUSIVE NOWAIT（包括 sync），也严格遵循容量→sync；任何活跃表锁/容量锁都明确失败并回滚，既不等待容量，也不持容量等待在线写者的 session/conversation/表锁。仍只允许空目标，不是在线迁移。 |

### 13.3 最小改动与正确性边界

**最终保留原 SendMessage 容量检查位置，两个性能候选均未采纳。** 原 key、COUNT 谓词和全数据库容量范围不变，不引入计数表/迁移/Redis 预留。最终工作树顺序为 session→conversation/members→幂等→**容量/COUNT→message/sequence→有序 sync→event_cursor**→event/recipients/outbox→响应准备→同步 COMMIT。16 条命令不变，没有扩大连接池或降低持久性。保留诊断、测试及可分离的 Replay/Seed 一致性修复；不能把失败的实验默认启用。

曾实际实施并测量的首候选将消息及 pts 前移为 sync→容量：独立会话吞吐提高，但同群 c8 p95 两轮约 112–114ms，基线约 45–57ms，并与旧容量→sync 相反。第二候选只前移三条消息写入，pts 保持在容量后，没有引入这一反转；仍复现同群长尾及部分增长场景退化。**两者性能均 FAIL，均未采纳**。首候选源码 `early-sync-candidate.cs`、第二候选精确源码和全部原始轮均保留；独立实验补丁与最终基线差异分开交付，不删除失败轮，也不再发明第三候选。

准入证明：每个新发送持相同排他锁；获取锁后用**下一条独立 SQL**在 ReadCommitted 的新快照执行 COUNT，才能看到前一持锁事务的提交。不能把锁和 COUNT 合并成同一语句而使用等待前的快照。该事务尚未写自己的 outbox，COUNT 小于上限后才分配 sequence/消息并写入一条 outbox；持锁至 COMMIT，下一发送能看到这一条。数据库异常、取消或响应准备异常时所有业务修改回滚，允许正常的 identity 回滚空洞。最终保留版容量拒绝仍发生在消息写入之前。Replay 的容量增加也串行化；发布/完成的并发减少最多造成保守拒绝，不会多批准新发送。消息、所有接收人 pts、cursor、event、recipients、outbox 和响应准备仍同事务，返回成功仍在 durable COMMIT 之后。

实验代价边界：仅被拒的前移候选在满容量时先做三条消息试写再回滚，会增加 WAL 和 identity 空洞；最终保留版不承担这一额外拒绝开销。幂等命中均在容量检查之前。本轮只验证少量临界容量和故障请求的正确性，不覆盖长期满容量拒绝洪峰或最大 bigint/TL 边界寿命评估。

最终锁图中所有在线接受事务仍是容量→有序 sync→event_cursor；HTTP 关系入口复用同一 AcceptAsync，没有相反顺序的第二套实现。同会话在会话锁处串行，其他发送仍在全局容量锁排队；本轮没有启用消息准备并行化，也没有消除全局串行点。delivery 的 conversation→generation/task 顺序未变，replay 只取容量→outbox，不请求 sync/会话。Seed 最终先 try-capacity、后包括 sync 的 NOWAIT 表锁，离线入口也严格保持容量→sync；两次保护均非等待，排除活动写者，不引入 session/conversation 与容量的等待环。该修复与消息前移独立，最终保留版已再次完成交叉并发及 HTTP/存储回归。

**不支持新旧写入器混跑或滚动替换**。最终保留版恢复原发送锁序，但旧 Replay 仍没有容量协调，旧 Seed 仍可能阻塞等待；本轮没有混版本端到端验收。首个实验候选更存在显式 sync/capacity 反转，绝不能与旧写者混跑。回滚必须先停完所有隔离写者，再换回旧程序集。本适配器目前仅显式隔离验证，未授权滚动生产升级；将来在线混版本需要独立设计和验证。隔离回归通过不代表生产迁移安全验收通过。

诊断新增 session/conversation 获取、有序 pts、capacity 获取/COUNT/持锁区间、event_cursor 阶段，默认不开启；持锁测量是客户端从收到锁成功到 COMMIT API 返回的区间，不是服务器精确释放时刻。父子阶段是包含关系，不可直接相加；未采用的首候选 sync/event/outbox 总阶段还包含容量等待，不能与最终候选相同名称的阶段直接比较。

最终产品修改为 `PostgresMessageAcceptance.cs`（诊断及复用的锁 SQL，**没有移动准入位置**）、`PostgresDeliveryPersistence.cs`（修复未协调的 Replay 容量增加）、`PostgresRelationalValidation.cs`（离线 Seed 的非等待排他保护）。测试新增 `Storage.Tests/CapacityDiagnostic.cs`、`CapacityTests.cs`，接入现有 Program，复用 CommitDiagnostic 辅助；本文随实际结果更新。无 schema、协议字段、默认模式、旧兼容路径修改；不提交 Git。

### 13.4 隔离条件及最终回归

本轮单独初始化 PostgreSQL 16.15 Windows 实例，仅 `127.0.0.1:15471`；依次使用 `D:\BotgramBuild\tmp\capacity-lock-62741f4e` 下新建的 `pgdata`、`pgdata-ordered`、`pgdata-retained`，每次先关闭前一个，不修改已有数据目录。三者均已关闭；随机 SCRAM 凭据不进入报告。未触碰生产、18524、其他 PG/WSL/Kafka 数据；不恢复或重建旧实例。fsync=on、synchronous_commit=on、track_wal_io_timing=on，SQL 详细日志关闭。这里是原生 Windows PG，历史 WSL 数值不作为本轮对照。

证据根目录为 `D:\BotgramBuild\tmp\capacity-lock-62741f4e`，以下文件均相对它；本轮重新执行，不引用历史断言数冒充通过。

| 状态 | 本轮实际执行及证据 |
|---|---|
| PASS | 独立构建，0 警告/错误，.NET 8.0.204、Node 24.15.0、Npgsql 8.0.8。初次无 assets 的 build 失败后才 restore；HTTP 工程固定旧缓存路径需覆盖为本轮 D 盘缓存和独立输出，未覆盖既有测试 DLL。依赖及锁文件未变。 |
| PASS | 恢复原发送顺序后完整 Storage.Tests，最终 `storage-retained.log`：原有权限/冲突/重开池去重/三接收人映射/数据库触发异常回滚、delivery/activation/worker，以及真实 PG 协议代理丢弃已提交 COMMIT 回包后的原键恢复。 |
| PASS | 最终保留版新增 **47** 项容量断言，`capacity-faults-retained.json`：c=1/2/8 的多会话临界容量精确准入、并发同 random_id、满容量幂等恢复；重复请求不分配 identity、不推进 pts/sequence/event/outbox；真实容量锁等待取消和驱动超时；响应准备失败时持锁事务回滚，其他会话继续发送，失败键可重试。 |
| PASS | 同版交叉入口：Replay 等待同一 key；Seed 分别与活动 capacity/sync 锁交叉时明确失败且无残留。等待同时匹配 pg_stat_activity 和 pg_locks 的实际 bigint key（高/低 32 位及 objsubid=1），不是仅由耗时猜测。取消/超时/回滚后业务快照不变，允许 identity 回滚空洞。整个隔离服务器日志未出现 `deadlock detected`，不等于任意负载下的无死锁证明。 |
| PASS | 最终保留版 `http-retained.log`：2913 断言，21 操作及身份引导，内含 WS 子集 599；`http-parity-retained.log`：4779 断言、25 全状态检查点。21 操作的全状态桥对照不等于全部关系化；真实关系路径仍只有三项。 |
| PASS | 最终保留版 `node-storage-policy-retained.log`：42 启动边界断言；`node-{m49,gid,smoke}-retained.log` 全部通过。 |
| PASS | 实验二 HTTP ABBA 四轮 `http-ordered-matrix-{before1,candidate1,candidate2,before2}.log`，**每轮** 9990 功能断言；性能 FAIL、候选未采纳。不是最终保留版的新性能矩阵，不能将 9990 写成性能预算通过。 |
| NOT RUN | 真实 Redis/Kafka/Scylla：未配置独立服务连接变量，套件明确 SKIP；未挪用现有实例。fake publisher/projection/validator 不证明外部持久性。 |
| NOT RUN | 本轮 Android 真机、TLS/WSS、HA、生产迁移、长期满容量拒绝洪峰和大规模数据；WS 回归不是 TLS 或手机验收。历史协议 JSON/PB 110% 预算 FAIL、历史真机 BLOCKED 均未改为 PASS。 |

诊断 SQL 数是 acceptance 尝试执行的命令数，不含 BEGIN/COMMIT；最终保留版新成功 16、幂等命中 7、容量拒绝 8、锁等待取消/超时 7。前移实验对应拒绝 11、取消/超时 10，不能混用。Seed/Replay 的 fault trace 未接 acceptance SQL 计数器，值为 null（未知），不能解释为零 SQL。错误保留 503 容量拒绝、OperationCanceledException、NpgsqlException、响应准备异常及 Seed busy/55P03 类别；故障少量样本只作正确性证据，不宣称故障 p99。

所有随机 runner 数据库已在 finally 删除；每次关闭前仅剩本轮 admin 库、其他连接数为 0。已正常停止本轮三个先后使用 15471 的实例，确认监听和各 postmaster.pid 消失，删除本轮临时连接凭据文件；最终 `cleanup-retained.log`、`stop-retained.log` 及先前 cleanup/stop 日志保存结果。原始测试证据保留，不清理其他服务。

### 13.5 最终性能对照与交付判定

**两候选稳定性能门槛：FAIL，均未采纳；最终工作树保留原锁设计。** 实验二独立会话 c=2/8 诊断锁等待/持有中位数缩短有实测，但不能覆盖热点和增长退化；CPU 改善归因 INCONCLUSIVE。保留原设计是允许的工程结论，不修改门槛把 FAIL 变 PASS，不继续下一组 RPC。最终保留版新的 ABBA 优化收益验收为 NOT RUN，未声称它产生发送性能提升。

**最终保留版**是 `retained-bin` / `retained-http`（backend DLL SHA-256 均为 `e045da407781b5ca58eb52443cd5fb720752c1761958669d64fc331d68204b9e`），正确性证据见 13.4。以下实验对照使用旧准入行为、相同诊断的 `ordered-before-bin` / `ordered-before-http`（`1458a817311b4a7e5027b04769cbfdf3a6085b6a9edb37c62d63184c7fd22cd7`）；**被拒实验二**为 `ordered-bin` / `ordered-http`（`cd3cb7aa6ebbd3433a08bdd4e8826249cbac5f3302f7fed97c29746494a8fad6`）。`conservative-bin` 的 `d60ce26b54969c04764c266f49a24e9d550623164d7ee6d98ba678c61207ad69` 是实验二收紧 Seed 前的中间构建；`final-bin` / `final-http` 也是旧中间产物，不能按目录名当成最终交付。

直接 acceptance 实验矩阵：5 拓扑 × prefill 0/150 × c=1/2/8，共 30 组；same-session 共用令牌，same-user-same-chat 使用同用户不同令牌，same-user-cross-chat 为同用户不同会话，independent 为不同用户不同会话，group 为同群 8 个不同发送用户/令牌、8 人接收集合（不是共享发送者）。每组重建相同确定性合成图、10 次串行预热、96 次测量；MinPool=0/MaxPool=20，提前开 20 连接且两侧不变。诊断开启和关闭各做独立 ABBA：旧1→实验轮1→实验轮2→旧2。实验二最后构建比较共 **23040 次成功测量请求**，拒绝/意外错误均 0。实验一及实验二中间构建各另有 23040 次，均保留，未混进下表。

实验二原始最后轮：`ordered-{before,candidate}-{diagnostic,formal}{1,2}.json` 及同名日志；`safe-*` 是中间构建旧轮，保留供审计。formal 关闭每请求 Activity/观察器，SQL 详细日志关闭；服务器仍开启超过 100ms 的锁等待日志，两侧一致但可能扰动长尾。diagnostic 请求约 1ms 观察（非精确间隔），保存原始 pg_stat_activity 等待、脱敏 key、阶段、COMMIT、SQL 次数、资源和 WAL。最初 fixture 的多 SQL/位置参数触发 42601，已拆成独立命令后重跑；失败初始化没有测量样本，不计为通过轮。打包时另发现 SHA 大小写比较断言，已按大小写无关的十六进制值核验，八轮程序集实际完全匹配；不是业务测试失败。

下表仅为**被拒实验二**的 formal 两轮分别计算的分位数范围，不是最终保留版的性能收益，也不是合并样本重新算分位数；p50/p95/p99 单位 ms，吞吐为每组实际成功数/墙钟秒。每轮 n=96 的 p99 近乎最大值，两轮不足以建立生产统计置信度。

| 拓扑 | prefill | c | 旧 p50/p95/p99 | 候选 p50/p95/p99 | 吞吐 /s：旧 -> 候选 |
|---|---:|---:|---|---|---|
| same-session | 0 | 1 | 3.84-3.88/7.49-7.56/9.83-10.36 | 3.72-7.06/7.45-11.01/10.61-18.31 | 218.28-223.70 -> 131.41-225.82 |
| same-session | 0 | 2 | 6.01-6.85/11.77-13.07/14.14-29.24 | 5.98-13.87/12.58-24.78/13.25-59.36 | 254.78-269.20 -> 130.95-279.06 |
| same-session | 0 | 8 | 22.96-24.87/46.54-60.06/52.65-72.05 | 23.43-23.72/48.44-51.43/53.61-69.41 | 258.45-284.00 -> 267.93-278.98 |
| same-user-same-chat | 0 | 1 | 3.30-3.30/6.92-7.46/7.42-20.62 | 3.26-3.35/6.56-7.00/7.47-7.95 | 240.27-256.63 -> 252.84-258.10 |
| same-user-same-chat | 0 | 2 | 5.29-5.88/11.00-13.48/12.07-33.72 | 5.50-6.24/10.46-11.79/11.99-12.91 | 284.67-292.97 -> 275.80-311.26 |
| same-user-same-chat | 0 | 8 | 21.36-27.09/43.02-55.32/45.06-67.65 | 11.47-12.46/78.11-94.55/111.45-115.45 | 240.44-312.03 -> 298.76-310.99 |
| same-user-cross-chat | 0 | 1 | 3.15-3.41/6.95-7.25/7.55-20.61 | 3.16-3.57/6.39-7.98/7.20-20.63 | 249.39-254.38 -> 224.61-261.91 |
| same-user-cross-chat | 0 | 2 | 5.03-14.73/10.82-15.71/25.66-29.53 | 3.54-3.70/7.43-7.73/11.79-29.46 | 146.64-313.04 -> 420.48-454.82 |
| same-user-cross-chat | 0 | 8 | 19.20-19.29/36.12-37.96/39.01-42.36 | 14.13-15.45/28.95-30.79/34.14-36.20 | 336.83-354.05 -> 437.70-465.97 |
| independent | 0 | 1 | 3.50-3.58/7.23-7.53/10.93-20.67 | 3.20-6.70/6.63-12.03/7.14-31.29 | 222.37-230.19 -> 145.98-262.53 |
| independent | 0 | 2 | 4.61-5.34/8.86-10.84/10.59-29.99 | 3.50-3.62/6.87-7.75/23.78-29.53 | 280.87-367.42 -> 419.78-445.03 |
| independent | 0 | 8 | 18.25-19.39/36.08-37.03/38.46-55.06 | 13.47-15.44/28.64-43.74/32.23-50.41 | 327.66-365.63 -> 444.63-445.01 |
| group | 0 | 1 | 3.31-3.69/7.14-8.36/11.19-20.64 | 3.43-7.06/7.19-10.60/10.87-20.62 | 224.30-238.27 -> 136.00-221.90 |
| group | 0 | 2 | 6.78-13.50/13.45-15.12/25.77-27.01 | 5.78-6.70/11.31-12.29/13.15-14.63 | 184.23-239.29 -> 253.43-290.70 |
| group | 0 | 8 | 22.41-27.00/43.69-45.77/45.91-63.04 | 11.52-12.34/108.76-116.68/124.30-129.97 | 251.11-298.66 -> 273.24-279.70 |
| same-session | 150 | 1 | 3.48-3.61/8.83-9.10/20.60-30.27 | 3.20-3.27/6.76-6.88/7.92-20.63 | 217.67-217.76 -> 249.81-252.81 |
| same-session | 150 | 2 | 5.81-5.96/11.93-12.06/29.39-29.60 | 5.83-5.92/12.04-12.12/29.81-31.24 | 267.80-274.54 -> 269.15-272.02 |
| same-session | 150 | 8 | 24.12-24.39/49.71-61.14/50.93-61.73 | 23.54-23.54/47.56-48.86/64.66-65.03 | 266.77-267.60 -> 266.93-271.19 |
| same-user-same-chat | 150 | 1 | 3.26-3.89/6.49-7.00/7.84-20.67 | 3.16-3.17/6.67-6.80/7.46-20.59 | 222.24-259.08 -> 252.96-262.21 |
| same-user-same-chat | 150 | 2 | 5.25-13.46/10.61-15.17/25.72-29.71 | 5.34-13.86/10.40-14.33/11.64-29.94 | 172.49-299.65 -> 139.88-316.00 |
| same-user-same-chat | 150 | 8 | 22.26-32.77/41.66-54.74/46.10-66.92 | 10.53-10.87/74.52-98.58/124.25-124.98 | 226.11-307.92 -> 292.76-292.93 |
| same-user-cross-chat | 150 | 1 | 3.22-3.23/6.77-6.98/7.40-20.66 | 3.28-7.51/7.17-7.73/8.11-18.01 | 250.81-258.50 -> 132.72-224.69 |
| same-user-cross-chat | 150 | 2 | 5.14-5.16/10.18-20.37/29.63-30.34 | 3.43-3.53/6.78-9.17/24.25-29.16 | 274.72-296.24 -> 431.83-448.68 |
| same-user-cross-chat | 150 | 8 | 20.45-24.08/35.46-43.74/38.67-57.02 | 13.16-13.24/26.61-44.33/31.17-51.02 | 282.59-335.49 -> 451.21-495.72 |
| independent | 150 | 1 | 3.27-3.32/7.54-7.69/8.57-20.65 | 3.19-3.27/6.68-6.72/7.65-20.66 | 237.09-250.04 -> 251.72-257.61 |
| independent | 150 | 2 | 4.71-4.76/9.44-9.66/11.45-12.18 | 3.29-3.50/6.57-7.82/10.37-24.84 | 355.25-359.81 -> 431.08-507.07 |
| independent | 150 | 8 | 18.15-20.83/48.87-50.71/57.25-60.69 | 14.04-55.07/42.12-113.51/52.58-115.97 | 303.16-343.13 -> 166.12-431.23 |
| group | 150 | 1 | 3.61-4.59/7.69-8.80/18.10-20.65 | 3.33-3.37/6.69-6.89/9.33-20.56 | 172.78-224.03 -> 242.78-245.45 |
| group | 150 | 2 | 5.92-7.78/11.98-16.59/28.04-29.32 | 6.17-13.73/11.56-14.92/18.19-29.83 | 217.81-270.46 -> 180.49-265.86 |
| group | 150 | 8 | 23.19-26.93/51.91-52.65/57.07-63.65 | 10.57-14.89/97.97-111.14/138.52-152.97 | 250.46-276.72 -> 264.92-296.71 |

退化不能隐去：小数据同用户同会话 c8 的 p95 从 43.02-55.32 增至 78.11-94.55ms、p99 增至 111.45-115.45ms；同群 c8 的 p95 从 43.69-45.77 增至 108.76-116.68ms。增长 independent/c8 候选还有一轮 p50/p95/p99=55.07/113.51/115.97ms、吞吐 166.12/s，不能用另一轮 431.23/s 掩盖。会话/令牌串行点仍在，无法仅凭客户端分位数证明每个尾部离群值的服务器原因。共享 Windows 主机的调度和 I/O 未受控，禁止把全部变化归因于三条 SQL 前移，亦禁止只引用 independent 的最佳轮。

同群退化的**阶段位置已证实，底层致因仍 INCONCLUSIVE**。`CapacityDiagnostic.Input/Seed` 的 group/c8 是 8 个不同发送用户、各自不同 session、共享 chat-0；不存在共享发送者/令牌造成的 session 串行化。small/c8 的 diagnostic 阶段 p95 如下（ms，分别列出两次重复，不可相加）：

| 阶段 | 旧轮1/轮2 | 实验二轮1/轮2 |
|---|---|---|
| session 获取 | 0.80 / 0.76 | 0.88 / 0.71 |
| conversation 获取 | 40.62 / 40.25 | 91.09 / 97.17 |
| capacity 等待 | 0.17 / 0.23 | 0.18 / 0.18 |
| capacity 持有（含后续阶段） | 5.18 / 4.88 | 3.94 / 3.75 |
| sync/pts | 0.43 / 0.47 | 0.45 / 0.47 |
| event_cursor | 0.31 / 0.33 | 0.30 / 0.31 |
| COMMIT | 0.47 / 0.48 | 2.23 / 0.47 |

实验二逐请求最慢样本总计 117.92ms / 142.13ms，其中 conversation 命令 114.12ms / 138.21ms，COMMIT 仅 0.25ms / 0.30ms。数据库采样观察到 tuple/transactionid 等待，该组没有采到 advisory 等待。测得的是客户端 conversation 锁命令整个 await 区间，包含服务端等待、网络及调度，不能全部叫作服务器精确锁等待时间。

关闭阶段/观察器仍复现：formal small/c8 p95 旧 45.77/43.69→实验 116.68/108.76ms；prefill150 为旧 52.65/51.91→实验 111.14/97.97ms。diagnostic small 总 p95 旧 47.59/46.54→实验 98.03/100.98ms。两个 ABBA 重复方向一致，不能归咎于只在诊断开启时才有的扰动；但持锁者交接、blocking PID 时间线、重新获锁公平性及 OS 调度尚未采集到足以判因的证据。锁车队/饥饿/调度变化均是待验证假设，**不是已证明的机制**。

diagnostic 的 independent 证据如下（两轮范围；ms；advisory 样本数是采样命中数，不是请求数/精确等待时长）：

| prefill/c | 锁等待 p50：旧 -> 候选 | 持锁 p50：旧 -> 候选 | COMMIT p50：旧 -> 候选 | advisory 采样：旧 -> 候选 |
|---|---|---|---|---|
| 0/2 | 1.47-1.50 -> 0.21-0.26 | 2.42-2.43 -> 1.78-1.81 | 0.24-0.24 -> 0.24-0.29 | 82-87 -> 12-12 |
| 0/8 | 15.50-15.76 -> 11.39-12.12 | 2.37-2.38 -> 1.82-2.08 | 0.23-0.24 -> 0.25-0.27 | 1015-1016 -> 679-1493 |
| 150/2 | 1.43-1.51 -> 0.22-0.25 | 2.28-2.44 -> 1.65-1.76 | 0.24-0.24 -> 0.24-0.24 | 82-88 -> 10-11 |
| 150/8 | 16.05-17.83 -> 10.54-13.24 | 2.48-2.71 -> 1.67-2.23 | 0.23-0.27 -> 0.23-0.27 | 1034-1113 -> 758-776 |

两侧成功调用均 16 条 acceptance SQL，同一脱敏 key；全局锁未消失。c=1 和其他拓扑的完整 wait/held/commit p50/p95/p99、session/conversation/pts/event_cursor 阶段、资源与逐请求原始值均在上述 JSON 中。0/c8 的候选 advisory 采样一轮反而 1493，另一轮 679，不能宣称采样等待次数一致降低；COMMIT 也未一致改善。先前 `safe-*` 的增长 c2 COMMIT 离群轮仍保留。WAL 统计为实例级，不能精确拆给单请求；原生 Windows 的 write/sync 统计含义不能照搬历史 WSL fsync 结论。

CPU 没有一致改善：formal independent/c8，prefill0 的两轮应用 CPU 旧 140.625/281.25ms、候选 171.875/93.75ms，PG writer CPU 旧 437.5/250ms、候选 375/312.5ms；prefill150 的应用旧 187.5/250ms、候选 234.375/171.875ms，PG writer 旧 234.375/453.125ms、候选 359.375/453.125ms。测量是各组进程 CPU 差值，PG 不含后台/WAL 进程；这些组约 89-92MB 应用工作集、415-420MB 的 20 个 writer 工作集合计是组后读数，含共享页重复计算，不是峰值物理内存。

另做完整 HTTP 同一 alice→bob 会话 ABBA。每轮独立 seed，图/条数一致，但 UUID/时钟新生成，不是字节相同 fixture；池和预热/样本/并发不变。关系路径 p50/p95/p99 如下，单位 ms，列顺序保留实测轮序：

| 数据/c | 旧轮1 | 实验二轮1 | 实验二轮2 | 旧轮2 |
|---|---|---|---|---|
| small/1 | 7.67/13.59/27.60 | 5.80/8.09/8.63 | 5.48/7.24/8.70 | 5.04/5.88/6.91 |
| small/2 | 7.80/15.27/60.44 | 8.26/14.29/44.16 | 7.71/11.70/35.54 | 8.78/16.13/36.34 |
| small/8 | 21.85/74.29/100.10 | 21.15/89.29/116.71 | 20.77/71.50/95.63 | 20.24/75.76/100.39 |
| grown150/1 | 5.05/6.14/8.48 | 5.20/7.49/9.49 | 6.64/7.92/23.90 | 5.06/7.90/11.90 |
| grown150/2 | 7.71/10.12/34.22 | 7.31/9.63/27.40 | 7.48/9.88/33.71 | 7.72/11.69/38.65 |
| grown150/8 | 25.00/151.08/182.61 | 19.37/68.20/86.98 | 20.11/69.36/94.62 | 18.50/70.12/93.18 |

grown150/c1 的候选 p50 两轮均高于两轮基线，small/c8 的实验轮1 p95/p99 也退化，没有统一端到端收益；旧轮1的 grown/c8 长尾本身也波动很大。该既有 HTTP harness 不采集各组吞吐/CPU，不从分位数倒推吞吐；资源和真实吞吐来自直接矩阵。实验二最后四轮完整 `storage-parity.json` 位于 `regressions-ordered\p6-http-runs-{67d2972479ac4c87b60831f32561cd77,628aff6eb1604dc89b628a47bc8bbea4,4025a62689284ba4a51f41cd01c284a5,68cd61a7ffd8400d9ae18d690f9775ff}`，同时保存 Legacy 对照和原始 HTTP 样本。先前 `regressions-safe` 的四轮也保留，不合并或隐藏。

最终交付只包含相对 overlay 的 **6 个既有文件修改 + 2 个新增测试**；其余 67 个 overlay 文件逐字节不变。会话 artifacts 中 `capacity-lock-delta.patch` 为最终保留版审阅用 LF 补丁；`capacity-lock-delta.zip` 保存这 8 文件的字节精确 before/after（新增文件没有 before），`capacity-lock-delta-manifest.json` 记录原 ZIP/manifest 哈希及全部 73 文件复核、各修改前后 SHA-256。

实验独立交付：`capacity-lock-experiment-message-writes.patch`、`capacity-lock-experiment-early-sync.patch` 均以最终保留版的 acceptance 文件为 before，分别只表示实验二/实验一；`capacity-lock-experimental-source.zip` 保存两个实验的字节精确 acceptance 源码，`capacity-lock-experiments.json` 标明基线、文件哈希、实验 DLL 和未采纳状态。**不把实验补丁自动应用到工作树。** `capacity-lock-evidence.zip` / `capacity-lock-evidence-summary.json` 保存全部原始轮、最终保留版回归、程序集哈希和索引，不含连接凭据。所有汇总只是证据索引，不另建平行报告；报告权威仍是本文。没有应用到原工作区，也未提交/暂存 Git。

下一阶段若另获授权，首先需要更长尾样本/重复、受控主机的 blocking PID 与持锁者交接时间线、队列公平性/调度证据及拒绝洪峰成本，才能评估新的临界区方案；这些本轮 NOT RUN，不承诺一定能优化成功。本轮范围到审计、可分离一致性修复、失败实验验证和可信交付为止，不追加第三候选或下一组 RPC。

### 13.6 剩余 18 RPC 的实施分组（仅准备，不擅自扩展实现）

以当前 `ProtobufRpcMapper.Adapter.Execute` 的 21 个业务分支减去已接入 3 个为准，不把 transport-only ResolveIdentities 算进去：

| 建议次序 | RPC（合计 18） | 必须先保留的真实语义及独立验收 |
|---|---|---|
| A：身份/联系人读取（7） | GetUsers、ResolveUsername、ResolveGid、SearchContacts、GetContacts、GetFullUser、GetPeerSettings | UserQueryService.Profile、ContactService 及 PeerSettings 复用私有备注/双向联系人/成员权限；Profile/FindUser/PeerId 会分配稳定 TL ID，不能简单标只读。先提取事务性唯一 TL 映射及公开用户视图，保留 UUID/GID/TL ID 分离、大小写及明确拒绝；逐项 Legacy 比较和重启验证。 |
| B：会话/消息读取（7） | GetDialogs、GetPinnedDialogs、GetPeerDialogs、GetMessages、GetFullGroup、GetGroups、GetNotifySettings | ConversationQueryService 中未读/回执、消息过滤、成员列表顺序、分页及当前空 pinned/folder 行为；GetFullGroup 还返回 Related(contacts=true)，不能只查群成员。peer 查询仍可能分配身份；不能因 GetNotifySettings 当前值静态而省略 peer 权限校验。 |
| C：同步与已读（2） | GetDifference、ReadHistory | 关系 events 目前只接受 message.created；先有 receipt.updated/user.gid_changed 的持久类型、每用户 pts 和差分分页支持，需要容量协调的变更必须先容量、再有序 sync、最后 event 栅栏。ReadThrough 只前进且同事务，三游标不可混用。 |
| D：独立变更（2） | ImportContacts、CreateGroup | 联系人 upsert、备注验证、未知/自身 GID 跳过与稳定 TL ID；建群的会话/成员、conversation.created 和 service message.created 必须原子，服务消息占 sequence/pts/容量，不能直接复用只有普通消息的 schema 假装完成。 |

A/B 有些查询会产生身份分配副作用，不能仅凭 RPC 名称选择只读隔离级别。直接/收藏会话的唯一创建也须补齐（当前关系三操作只接受预种会话）。每组仍须功能/Legacy/权限隔离/重启/持久化/性能单独验收，不将 snapshot bridge 的 21 项覆盖当作迁移完成。认证、注册/登录/撤销与关系业务的同一权威及撤销竞态是各组上线前的独立前置门槛，不能把当前只读 fixture 用户/会话表当作完整认证实现。

本轮先完成容量安全和性能评估，不实施上述下一组 RPC；也不触及投影完整键验证、在线重建 HA、生产增量迁移、正式切流、Android 真机等后续验收。

## 14. 新授权：完整关系化迁移与隔离 P7（进行中，未完成验收）

### 14.1 范围、基线与有副作用的入口

本节是上一容量锁任务之后的新任务，授权完整业务实现及隔离切换，不授权正式切流。继续保留第 13 节最终版本，不恢复两个失败候选。新起点在当前会话 artifacts 的 `full-migration-start.zip` / `full-migration-start-manifest.json`：101 个 tracked/nonignored 文件，ZIP SHA-256 `87e2ebccce5dcf7f70a25b56d813f75c935fd237022b5a54092f06a1bc95a2f6`。最终增量须相对此起点，而非 Git HEAD 或原 73 文件 overlay。未提交、暂存、访问原检出或操作生产数据。

| 入口与源码 | 权威读写及容易遗漏的副作用 | 新任务状态 |
|---|---|---|
| Program 注册/登录；Auth 鉴权/退出；WSS 会话重查 | PBKDF2、GID唯一分配、账户+会话原子提交；撤销必须与在途操作争用同一 session 行，等锁后重新检查到期时间 | PG 仓储基础已实现并独立验证；HTTP/WSS 接线未完成 |
| UserQueryService、ContactService、IdentityService；7 个 A 组 RPC | 公开用户、私有姓名、双向联系人；查询可能分配持久 TL ID；GetUsers/FindUser 遍历中的分配不能误称无副作用 | 共享 Profile 映射及单用户稳定映射已实现；完整 A 组尚未完成 |
| ConversationQueryService；7 个 B 组 RPC | 成员可见性、对话排序/分页/回执/未读、群成员和 Related(true)；静态通知设置也要校验 peer | 未实现完整关系化 |
| GetHistory/SendMessage、ConversationMessageService | 非预种 direct/saved 的唯一创建；同会话回复、永久幂等、消息 ID/sequence/pts/event/outbox；PB 建群另含服务消息 | 原三 RPC 路径保留；动态会话/建群未完成 |
| GetDifference/ReadHistory、SyncService | 仅 message.created/receipt.updated/user.gid_changed 推进 pts；事件游标与 pts 分离；已读只前进 | 完整事件 schema/执行未完成 |
| ImportContacts/CreateGroup；REST 会话创建、消息/已读 | 联系人私有备注 upsert；建群的成员、conversation.created 和服务消息原子；REST 与 PB 差异不得消失 | 未完成 |
| resolve_identities | 自身必须已有映射；UUID/GID/TL 身份不得互换；群映射须成员授权 | 未完成，不算入 21 个业务 RPC |
| `/me`、`/users`、`/telegram/me`、`/me/gid`、`/gid/regions` | 用户视图、TL ID 分配、GID 唯一修改及接收人事件；区域清单本身无持久副作用 | `/me` 仓储已验证；路由仍旧模式 |
| `/events`、`/ws`、Store.Maintain、CallExpiryService | 可见/过期事件分页、持续认证；GET/维护可能结束超时通话并发事件，不能遗漏后台写 | 未迁移 |
| `/calls` 创建/列表/详情/action/signals；`/rtc/config` | 参与者权限、状态机、信令事件及过期；RTC 配置不等于通话持久化 | 未迁移 |
| PG Outbox/Kafka/持久 Inbox/Scylla/Redis | 批次驱动已有但未全部 Hosted 接线；需真实重启、重复、乱序、连续水位和缓存降级证据 | 四组件验收 NOT RUN |
| 离线 JSON 导入/校验/重跑/回滚 | 完整账户/会话/身份/业务/事件和计数；仅合成数据，不能使用生产 JSON | NOT RUN |

### 14.2 第一切片：账户、会话与稳定身份基础

新增 `PostgresBackendSchema.ApplyAsync` 为显式离线 schema 操作，不在仓储构造或请求内自动迁移；保留 intake users/sessions/sync 作为同一权威，新增账户详情、唯一 username/GID、联系人与唯一 TL 映射表。没有 JSON 快照表，也没有 Store fallback。后续业务 schema 前向扩展及完整导入尚未实施。

新增 `PostgresBackend`：注册在同一持久事务写用户、GID、sync 和摘要会话，唯一 username 失败回滚全部记录；GID 冲突使用约束内重试。登录使用共享 PBKDF2 校验。鉴权不缓存令牌有效性，每个仓储操作再次锁 session，再用 `clock_timestamp()` 检查到期；退出删除同一行，不能越过持锁的在途操作。COMMIT 使用 `synchronous_commit=on`；失败不回退旧 Store。

`Auth` 提取注册输入/密码校验和强类型 session 结果供旧/新路径复用；`Gids` 提取密码学候选生成；`UserQueryService.Profile` 提取纯映射。旧 HTTP 路由仅改为调用这些相同规则，尚未切到新仓储。TL 映射使用唯一键和序列，不重复覆盖已有值；已存在映射直接读取，避免无意义 UPDATE。多实体及跨组锁顺序仍须随后续批次验证，不能由单实体测试外推。

当前真实结果：**PASS 30 项**账户/会话/身份 PostgreSQL 断言。涵盖地区与 World 回退、公开 DTO 无密码字段、仅持久化令牌摘要、大小写用户名冲突、8 路并发注册原子回滚、8 个登录会话并发懒分配稳定 ID、重开数据源保留身份、profile 不推进 pts，以及与现有 message acceptance 共用认证权威。撤销测试通过测试专用触发器/锁门闩观测 `pg_locks` 与 `pg_stat_activity` 的实际等待，证明在途身份写先完成、退出后后续操作和发送拒绝。

环境为本轮新建的 Windows PG16、loopback `15472`，目录 `D:\BotgramBuild\tmp\full-migration-62741f4e`；每次 runner 创建并删除自己的随机测试库，不读取生产库或 `18524`。命令为 `BOTGRAM_FULL_BACKEND_TESTS=1` 下运行 `Storage.Tests.dll`；证据 `foundation-tests.log`。首次测试遇到测试查询读取 `regclass` 的 Npgsql 类型错误，改为显式 `::text` 后重跑通过，未放宽业务断言。缓存 fake tests 同时通过，真实 Redis 明确 SKIP。构建 0 警告/错误。以上不是完整 HTTP 回归、完整 RPC 迁移或四组件验收。

### 14.3 P7 验收边界与未完成项

用户要求“开启运行 P7”在本轮仅适用于新建隔离实例。最终必须实际配置 `LegacyJsonRpcEnabled=false`、`LegacyJsonWssEnabled=false`、`ProtobufRolloutPercent=100`，保留回滚开关和兼容代码，不禁用 JSON 认证/GID/能力 REST。当前尚未开启或留下新的 P7 服务，状态 **NOT RUN**。

客户端只读源码信息是契约线索，不是其 Git 基线/构建/真机证据：HTTP pin 与加密会话绑定，旧 `tl-json-v1` pin 遇 410 应明确拒绝，不能清 pin 自动重试写入；隔离 P7 使用新登录重新协商。WSS 独立选择 protobuf subprotocol；EventHint 仅唤醒 getDifference，不替代 pts。必须覆盖旧 pin 拒绝、新登录 PB、WSS 只选 PB、握手/认证/恢复以及恢复两个开关后的回滚。将来客户端 Mapping 验证的 BackendRoot 必须指向本独立工作树。

完整 RPC/REST/WSS/后台、四组件连通和故障恢复、合成 JSON 导入/回滚、完整 P7 运行验收均尚未完成；不得标 PASS。正式切流未授权，客户端快照仍受其权限阻塞，真机 NOT RUN。第 13 节性能 FAIL 和历史协议 110% 预算 FAIL 原样保留。

### 14.4 后续已实现切片与新增 Premium 契约

以下进度晚于 14.2 的第一切片，不能将旧计数当作最终全迁移结果：

- `PostgresBackend.DispatchAsync` 目前实际执行 12 个业务 RPC：GetState、A 组全部 7 个、GetFullGroup/GetGroups/GetNotifySettings、ImportContacts；另有 transport-only ResolveIdentities。每个派发均重新认证，并在同一事务内执行响应准备。它尚未注册为新的完整 HTTP 模式，缺失操作明确拒绝，不转入 Store。
- 联系人校验与公开 Profile/群映射复用旧服务的纯函数。导入私有姓名、互为联系人、重导入顺序、8 路并发、PB 字节级 Legacy 对照和响应准备失败回滚已测试。GetUsers 保留旧遍历触发身份分配的副作用；批量 TL 映射按键顺序取得唯一约束，bootstrap 只读取已有映射，缺少自身映射明确拒绝。
- 显式 schema 版本 2 增加会话元数据/成员顺序/回执/通话表，并扩展业务事件种类；不是已实现全部这些业务。REST 会话创建仓储支持同一 direct 对的唯一复用及事务事件；并发创建与反向复用已测试。完整消息/建群服务消息及 HTTP 接线仍待实现。
- 一般事件现在持久化 payload/expiry、可空的非 stream pts；GID 修改与 Difference/Events 的仓储查询已实现。分页先固定提交上界，再读取该上界内不可变事件，避免 READ COMMITTED 下读取后再取全局游标跳过并发新事件。Difference 不把通知过期等同于丢弃 pts。
- Outbox Source/Inbox 扩展至已有冻结 DeliveryEvent v1 支持的非消息类型；消息仍生成真实投影任务，非消息只在 PG Inbox/消费位置持久化后设置 consumed，不伪造 Scylla 任务或消息水位。非消息占用全局容量，须 Published **且** consumed 才释放；消息仍须 Published 且当前 generation 的投影完成。Replay 再次计入 backlog，原全局容量锁和发送中容量先于消息写的顺序保留。第 13 节的旧性能数据不能证明此次扩展性能。
- 新增两个不与原 bigint 容量键混用的双 int advisory namespace：`(2147000001, hashtext(direct_key))` 串行化同一直接/收藏会话的创建，hash 碰撞仅降低并发；`(2147000002,0)` 在账户行锁前串行化 GID 修改，避免交换 GID 的反向 UNIQUE 等待。后者不在普通 SendMessage 路径内。多操作最终死锁/性能矩阵仍待完整实现后重跑。

本阶段实际证据：`identity-group-premium-tests.log` **PASS 116 项** PG 断言（含互换 GID 冲突不死锁）；`storage-regression.log` 为一般事件扩展后的完整既有 PG 回归，包含 47 项容量断言、真实 PG COMMIT 响应丢失代理验证及 fake 外部适配器编排。真实 Kafka/Scylla/Redis 明确 SKIP。共享服务提取后的旧 HTTP 曾实际重跑 2913 项；新增 Premium REST/TL bit28/PB presence 断言后 `premium-http-tests.log` 实际通过 **2949 项**。原始目录均为 `D:\BotgramBuild\tmp\full-migration-62741f4e`，这些是分阶段结果，不代表未运行的完整新 HTTP 模式。

用户随后明确新增 Nyarugram 自有 Premium、Gram ID UI、壁纸及用户可达 Telegram 接口范围。Gram ID 原 wire 契约继续为 `PUT /api/me/gid` 的 `regionCode` + `number`；保留 `gid`/TL phone 兼容字段，显示文本改名由客户端负责。原端点已经支持完整八位数字；客户端需复核实际界面故障，不重复造一个改号接口。

Premium 已实现独立本人接口：`GET /api/me/premium`、`POST /api/me/premium/activate`（空对象），返回 `{ "active": false/true }`。旧 JSON 用户缺省 false，PG 显式 schema 版本 3 增加默认 false 字段。激活按本人 session 重新验证、持久、重复幂等，不修改其他用户，不调用收费/Stars/Fragment 服务，不制造支付成功。当前已接入旧模式 HTTP，PG 仓储已验证，新完整模式路由仍待接线。

为让登录/刷新后的 Mapper 不覆盖已激活状态，协调会话依据用户新增功能确认了兼容扩展：`UserSummary` 新增 **`optional bool nyarugram_premium = 11`**。保留 major 1、既有字段号和 21+resolve 请求 tag；absence 代表旧服务没有该信息，不能当作明确 false；本服务已知状态明确发送 presence 与 false/true。TL JSON 根据客户端提供的真实 `TL_user` 构造器 `0xB1B8CC83` 源码证据设置 `premium` 和 flags bit 28；这仅表示 Nyarugram 自有权益，不是 Telegram 付费状态证明。

锁定 protoc 25.8、SHA 校验过的既有工具生成 C#/Java lite，未写共享生成目录或客户端检出：

- `Protocol/botgram_rpc.proto` SHA-256 `4bdb6ea49cf76407006e8e5c742521642dd3d65edac855cc3c904ee6628e3f93`
- `Protocol/BotgramRpc.cs` SHA-256 `0e9b90fbe15229e77d3def919805a5a15f6f5e91fcd46d8f906b88b7759917ae`
- 离线字段注册检查 PASS；Android 最新源对照仍 NOT RUN。
- `protocol/csharp-interop.log` 与 `protocol/java-interop.log`：**71 个双向 Java lite/C# 二进制 fixture PASS**，包括 Premium absence/false/true，以及保留旧 UserSummary 1–10 字段的旧 schema 编解码器未知字段往返。首次独立输出目录缺 assets，按报错从已校验的 D 盘缓存恢复后完成；没有下载新的依赖版本。这是宿主协议验证，不是 Android APK/真机通过。

壁纸尚未实现，不能用空 Updates/Bool 声称支持。客户端已指出需区分 `messages.setChatWallPaper`（peer、wallpaper/id/settings、for_both/revert）、全局 account.getWallPapers/installWallPaper/saveWallPaper，以及依赖文件上传的 uploadWallPaper；本地 OverrideWallpaperInfo 也不能全当作服务端 RPC。等待最新客户端调用/字段复核后逐项实现与真实存储验证。原 `phone.*`、附件/推送及全部未覆盖 TL 方法不因 21 RPC 清单而变成支持，最终覆盖矩阵须列清具体原因。

### 14.5 全部既有 RPC 仓储与 REST/通话切片（尚非完整运行接线）

14.4 的 12 个是此前阶段计数。当前 `PostgresBackend.DispatchAsync` 已有全部 **21 个既有业务 RPC + resolve_identities** 的真实关系化执行；不是快照桥，不构造整个 Database 作为生产执行视图。新增 `PostgresBackend.Messages.cs`、`PostgresBackend.Dialogs.cs` 处理 history/messages/difference/dialogs/read，`PostgresBackend.Rest.cs` 处理同一权威上的 REST 查询/发送/回执。

- 提取原 `PostgresMessageAcceptance` 内核为调用方持有事务的内部入口，独立 AcceptAsync 仍自行提交；原全局容量锁、锁后新 READ COMMITTED 快照、消息写入前容量检查不变。内部入口不提交，外层响应准备与取消检查成功后才提交。锁持有诊断对象保留至外层提交结束。新建 direct/saved、建群服务消息及两个 Outbox 记录不拆事务；第二个事件容量不足时第一个也回滚。没有恢复性能失败候选。
- 历史锚点 SQL 与既有三 RPC 验证器共用；PB 消息映射提取纯函数，与旧 Mapper 共用实体/回复/静音/服务消息规则。GetHistory 隐式创建 saved/direct 仍是写操作，测试其响应失败回滚及八路唯一创建。ReadHistory 保持只前进、同会话裁剪、重复不产生事件；GetMessages 权限筛选不泄漏不属于用户的消息。
- Difference 固定 pts 上界，100 项分页并验证连续性；Dialog 携带的 pts 在依赖数据读取前采样，后续并发提交仍可被差分重放。三个 Dialog RPC 保留原分页、排序和当前空 pinned/folder 行为，不伪造新的 pin 功能。
- `PostgresBackend.Calls.cs` 实现独立 REST 通话状态/信令/超时仓储；`CallService` 的校验/转换与旧路由共用。新增 `(2147000003,0)` 仅串行化通话操作，防止同一参与者被并发创建多个活动通话；在容量/事件锁前取得。后台维护删除过期 session 使用 `FOR UPDATE SKIP LOCKED`，不反向等待持有 session 行且等待通话锁的在途请求。过期通知在读取时过滤；不删除权威 stream/Outbox 源。它仍不是 Telegram 原生 phone.* 或已托管后台运行。

本阶段真实结果（同一 `D:\BotgramBuild\tmp\full-migration-62741f4e` 证据根目录）：

| 状态 | 验证范围 | 证据 |
|---|---|---|
| PASS | 当前 PG 仓储 **230 项**：21 RPC、105 消息差分、字节对照、8 路重试/已读/隐式建会话/通话竞争、响应失败/取消/容量回滚、权限、REST 历史/回执、通话信令及过期维护 | `full-rpc-rest-tests.log` |
| PASS | 内核提取后的既有 PG 接纳/投递回归、47 容量断言、真实 COMMIT 响应丢失代理 | `storage-regression-core.log` |
| PASS | 共享 Mapper/CallService 修改后的旧 HTTP **2949 项**，含 WS 599 项；不是新 PG HTTP 实例 | `rpc-http-regression.log`，`rpc-regression\p6-http-runs-01b456bf6d4344d7aa0083406a91e02b` |
| PASS | 既有快照桥 21 RPC + 三 RPC 验证器 **4779 项**，不作为新仓储 21 RPC HTTP 证明 | `rpc-storage-parity.log`，`rpc-regression\p6-http-runs-a6c5348c9bc34898ad74ce7c5d3c2d63` |
| PASS | 旧 REST/WS/通话 smoke、启动边界 **42 项** | `rest-smoke.log`、`startup-regression.log` |
| NOT RUN | 完整新 HTTP/WSS/后台运行接线、四组件真实链路、合成完整导入/回滚、新存储 P7 常驻实例、客户端 APK/真机 | 不可将前述仓储或旧 HTTP 结果替代 |

测试初次引用内部 EventPayloadTypes 导致测试项目编译失败，改为测试自己的显式 fixture 类型分派后重跑通过；初次 Node smoke 的隔离父目录不存在导致 ENOENT，仅创建本轮测试目录后重跑通过。没有放宽产品断言。旧 parity 命令附带的单轮 c=1 延迟仅保留诊断，未据此宣称性能改善，历史性能 FAIL 不变。

仅进行了边界明确的外部组件只读检查：D 盘已有 Kafka/Java/PG 工具；本轮检查的 Ubuntu PATH 无 Redis/Scylla，已知 Scylla 安装目录不存在。未停止既有 WSL 监听进程、未清理任何外部数据。下一步需补齐真实组件环境与运行接线，缺组件不能用 fake 替代就绪。

客户端当前唯一实施者已恢复为 17dc 会话；最新快照确认 tag11 无冲突，但其正式生成/APK仍有权限阻塞。用户新增壁纸需 typed PB 扩展，不能绕过 P7 临时走 REST；正在依据真实 TL 字段协调新请求 tags，未把尚未定义/实现的壁纸标为支持。

### 14.6 应用重启后：壁纸与独立 PG HTTP/WSS 接线源码（运行验收 BLOCKED）

用户已授权恢复，并随后明确不做 Android 测试。未重新导入源码、未提交 Git、未启动数据库或服务、未触碰生产/18524。应用重启后的真实沙箱拒绝仍阻断 D 盘隔离 PG 数据/工具和 Git 的运行时配置文件；未换工具绕过。下列新增源码不能继承 14.5 的 230/2949/4779/42 项 PASS。

| 项目 | 本轮状态及边界 |
|---|---|
| 壁纸 typed 合同 | PASS（离线注册表检查）：原 21 + bootstrap + 4 个请求/响应，26 tags、89 messages；并非正式跨语言运行联调 |
| 客户端/后端 schema 静态比较 | PASS：字符串安全的 440 条字段/声明摘要与逐顶层声明 token 一致；源文件及声明顺序不同，不代表生成物或运行兼容 |
| 离线 fingerprint 与冻结兼容源码 | PASS：7 个 Node 测试，覆盖 fingerprint 和归档旧 Kafka/Outbox 关键源码哈希；冻结旧版 message body 与任务内原始快照逐字相同（仅归一换行）。C#/Java/PG/Kafka 旧版运行 fixtures 尚未执行 |
| 启动边界测试 JS | PASS：`node --check tests\storage-policy.mjs`；仅语法检查，不是运行启动案例 |
| 壁纸 PG/schema4/事件版本2、完整 PG HTTP/WSS 源码 | 已落盘；编译、PG/HTTP/WSS 回归 BLOCKED |
| 旧客户端/旧服务/旧消费者兼容运行、C#/Java 新 fixtures | NOT RUN；新源码 fixtures 不能替代运行证据 |
| Git diff 检查 | BLOCKED：Git runtime `etc\gitconfig` 路径沙箱拒绝；未禁用系统配置来规避 |
| 四组件、导入/回滚、长期 P7 | NOT RUN / 未完成；不能由 PG 单库 host 推断就绪 |
| Android Studio/模拟器/真机 | NOT RUN，按用户本轮指令不执行 |

实际本轮检查：`node tools\verify-protobuf-contract.mjs` exit0，明确输出 Android comparison SKIP；启动测试 JS syntax exit0。完整 hash 重新读取一致：

- `Protocol\botgram_rpc.proto`：`53dc4ec9b795d4148b1b5255a7b6fdf7562a249e76b5a1bbce518e3fec2215d4`
- 正式生成的 `Protocol\BotgramRpc.cs`：`15894ff220ed9d9907800f52f34f9e91215a69700fea0b8f3eba0882ac551c95`
- 先前正式生成 Java-lite 的 hash 仍为历史记录 `fe4e8916265644f3a54be02df9c1ba1eb6f4525040716aef8fbd6ec90278f20f`；本轮未重读被拒绝的 D 盘 Java 文件。

#### 壁纸状态与兼容边界

`WallpaperService.cs`、`PostgresBackend.Wallpapers.cs`、`PostgresBackendSchema.cs` 实施独立账户 revision、全局 saved/installed、会话 shared 和个人 override。schema4 是显式离线迁移，HTTP 启动仅校验版本，不迁移或 seed。NoFile ID 保留 signed64；黑色0与缺失区分；第二色/rotation 同 presence；第三/第四色依赖前色，非空 emoticon/file/slug/upload/采用服务消息仍明确拒绝，不称“所有壁纸完成”。

个人 clear 写入显式 null override，屏蔽 shared；revert 删除请求者 override，重新显示 shared；direct for_both 修改 shared 并删除双方 override，群/saved for_both 拒绝，群仅请求者 view。全状态缺失代表旧服务未知，不代表 clear。完全相同操作不重复推进 revision；save 列表上限100，溢出409。拒绝、响应准备失败、容量不足必须回滚状态/revision/Outbox；相关 PG fixture 已加入但尚未执行。

请求/响应122–125分别为 get/save/install/set-chat wallpaper；UserFullResult.wallpaper10、GroupFullResult.wallpaper8、Update.peer_wallpaper_changed6、EventHint.wallpaper_revision3。成功的 tag125 即时响应包括恰好一条目标 peer 的 kind6，update.state 与顶层 state 一致；输入 Self 归一为本人 UserRef。个人 clear、revert、direct for_both、群个人 view 和无变化重试均遵循此形状，非法操作拒绝。**无新持久服务消息、不分配聊天 message ID/sequence、不推进 pts、不污染旧 Difference**；新增响应场景 fixture 仍待运行。`wallpaper.changed` 显式内部 schema_version2，旧事件保持1，分区 user:UUID、接收者仅该账户、无 projection/pts；Inbox 消费走非投影路径。当前 codec 源码验证版本形状；冻结旧消费者拒绝且不确认新版的实际运行仍未完成，不能声称已证明滚动混部安全。

GetWallpapers 可持久查询 revision/hash；hash hit 也返回 revision 和 installed。新 WSS 接线连接时发送显式 revision（含0），以后 cursor 或 revision 变化再发送。客户端必须在登录/重连/前台时查询 revision 并刷新相关 full，补拉失败不推进 watermark；提示不是唯一恢复渠道。客户端已回报相应源码，但未正式生成/编译联调。

客户端真实源码补充：基本群 `TL_chatFull` 二进制没有 wallpaper/flags2 字段；PB GroupFullResult 的 tag8 不代表原 TL 磁盘序列化具备该字段。客户端用已有壁纸缓存另存 NoFile，按账户和 own TL ID 隔离并在 fullInfo 磁盘加载后补回；后端继续返回既定显式状态，不伪造 TL flags。客户端手写 schema 的源码 hash 与后端不相同，不可称为同一字节文件；结构比较如下。

两端各自在各自允许的工作区独立比较：客户端 Shared proto 原始 SHA-256 为 `8f2d8109cf393ea7f2dc0d35770573aff47dd82ea3a94cf95b3d9d554aa5f317`，后端是上列 `53dc...15d4`。忽略字符串外注释/空白但保留字符串及转义后，两端 89 message、0 enum 和 syntax/package/options 相同；包含字段名称、tag、类型、presence label 与 oneof 的 440 条排序记录 SHA-256 都是 `deb2ef4326da933fdf619cf966996935c4c18cfdd0a0b4f4f81dfa9f685f0a31`。双方逐顶层声明 token 哈希的无序汇总均为 `5ffac55196c5737f3783b53b664367dc1198d74fdf0c5d113c8b61fd3058c40f`；有序 token 哈希不同是因为客户端将壁纸声明置于 GetStateRequest 后、后端将其置于文件末尾，且这些声明相对顺序不同。即静态 wire 声明一致，但源码并非逐字相同；不能据此认定正式生成器、Java/C# 互操作或 Android 构建通过。`tools\proto-fingerprint.cjs` 的字符串/注释、字段变更和声明顺序检查由 `node --test tests\proto-fingerprint.cjs` 实际通过 4 项。

`Protocol.Tests\compat_backend_v1.proto` 冻结自本会话 `full-migration-start.zip`，原始归档 proto SHA-256 `295935768e4285332b3e6a0fe63703035e57a545716621873763641b640251f1`；仅改变 package 和生成语言命名空间，message body 与归档原文比较通过。`tools\test-protobuf.ps1` 已加入该 schema，C#/Java fixture 源码覆盖旧版未知 122–125 请求/响应、旧服务缺失与显式 false Premium、full 壁纸缺失/clear/present、revision 缺失/零及旧版未知 Update kind 往返；generator 脚本语法已检查，但生成、编译、实际旧服务拒绝和跨语言执行仍 **NOT RUN**。已有 endpoint 源码对未识别 operation 返回 501，不能将这项源码阅读冒称旧服务运行验证。

#### 冻结旧 Outbox/消费者的版本边界（运行 NOT RUN）

`Storage.Tests\FrozenKafkaDeliveryV1.cs` 保留任务起始快照中的 Kafka publisher、consumer 和 v1 payload 类型分派，仅改变命名空间；`Storage.Tests\FrozenOutboxV1.cs` 冻结同一快照的 Source 查询和 Claim 方法，依赖当前共享模型，不是已部署旧程序二进制。`tests\frozen-kafka-source.cjs` 比较换行归一后的归档 SHA-256：Kafka 源码 `41a476b797ef03c3ed9835898656ed840ff820a2c55fad0cbebb60f298b12c22`、类型分派 `3cb85955f41ab896fae19b1fbd67154b67612baaac52064f93960c556aa0a50c`、旧 Source `49e3dda749f142f71079d1d46b1cb3aed013f89062daa145357749ef13a10fc0`、旧 Claim `196afe3bc79e005bbd27d046c1163524d9fb890b3b4eb698a05bb71a834b30d3`。`node --test tests\frozen-kafka-source.cjs tests\proto-fingerprint.cjs` 实际通过 7 项；这仅证明源码冻结与静态合同，不证明部署兼容。

原始输出保存在本会话 `C:\Users\idk Ma\.copilot\session-state\62741f4e-0218-4a72-8dff-0cb874d0f038\files\offline-frozen-source-check.log`（SHA-256 `850ca4647877b6f8e6b304e5b4ff8a8f68e7d3e0c8d10a9853c93dc796d3ffaf`）和 `offline-protobuf-contract-check.log`（SHA-256 `b2568cc02fdd4e8e1fc481770215d1f826c83c231b9961f6592750a1224ce43b`）；后者明确 SKIP Android。文件仅是隔离静态检查记录，不是服务端或客户端运行验收。

**源码风险，非运行结论：**该旧快照的 Outbox Source 对 `messages` 做内连接，而新版 `wallpaper.changed` 的 `message_id` 为 null；旧 Claim 在同一事务内先更新租约，再对 Source 结果调用 `.Single()`，随后才 COMMIT。据此推断，选中新版事件时可能抛异常并回滚整个批次（包括此前已更新的旧消息租约），持续轮询则可能停滞。旧 Kafka 版本先解码再执行 Inbox Stage 和 offset Commit；其旧类型分派不认识 `wallpaper.changed`，源码显示它会拒绝该记录并要求重开，不能据此称消息已被处理。`Storage.Tests\FrozenOutboxTests.cs` 已设计隔离随机测试库：先真实创建 v1 消息并验证旧 Claim 可接收，再生成 v2 壁纸事件、重复旧 Claim 并检查整批租约和 attempt 回滚，最后由新版 Claim 接收两条；`BrokerProjectionTests.cs` 还加入真实 loopback Kafka 的旧发布者/消费者拒绝及未确认 offset fixture。二者**尚未编译或运行**，旧部署版本也可能不同，不能替代实际版本确认或滚动升级测试。

默认 `LegacyOnly` 不变；显式 `RelationalBackend` 仅为隔离 PG 验证、不是 P7。当前没有可证明旧 worker 全部退出或自动协商能力的运行时升级门禁，因此**除专用隔离兼容测试外，禁止将新 v2 写入与任何可能使用该旧快照的 Outbox/消费者混部，混版部署和 P7 readiness 保持 BLOCKED**。在授权的隔离环境验证旧/新完整处理、明确升级顺序并增加或验证可执行的 fail-closed 版本门禁前，不得部署启用新版事件；不能依靠 Kafka 的 Protobuf/JSON wire 可解析性或 transient hint 宣称兼容。

#### 独立 PG 验证 host（不是完整四组件 P7）

新增 `Storage:ValidationBackend=RelationalBackend`，仍限定 Development、显式 `Storage:ValidationAcknowledged=true`、loopback `botgram_test_*` 数据库，并要求 JSON RPC/WSS=false、PB rollout=100。`LegacyOnly` 默认未变，Primary/Shadow 继续拒绝。启用该验证分支时不注册/解析 `Store`，不构造 SnapshotBridge，也不自动执行迁移或快照导入。

`Program.cs`、`ProtobufRpcEndpoint.cs`、`RelationalHttpRoutes.cs` 接到 PG authentication、原21+bootstrap+4壁纸 typed RPC、Premium/GID、REST 会话/消息/回执/事件、REST 通话/信令和后台维护。PB 编码仍在权威事务 COMMIT 前；WSS 初始及每次查询重新检查 session，注销关闭连接；接收/发送/关闭有取消和超时边界，数据库异常记录脱敏类型并结束连接。`TelegramGateway.User(UserProfile)` 仅公开现有纯映射供 `/telegram/me` 使用，不重建可写 Database。

该 host 尚无 Kafka/Scylla/Redis 后台接线，health 明示 `storage=isolated-postgresql-validation`、`fullStackReady=false`，不冒充 P7 readiness。`PostgresBackend.VerifySchemaAsync` 仅检查已执行的显式版本1–4；`EventHintAsync` 在事件 cursor 后读取账户 revision，避免竞争提交被错误跳过。

WSS cursor 源码已改为与 `/api/events` 共用接收人过滤与 100 条分页边界：页满时停在最后一条可见事件，否则可前进到采样的全局上界，不能直接跳过超过一页的可见事件。`Protocol.HttpTests\RelationalBackend.cs` 加入 105 条消息、两页 cursor 对照和断线 cursor 恢复 fixture；这些变更尚未编译或运行。

新增 `Protocol.HttpTests\RelationalBackend.cs`、入口 `--relational-backend`：创建独立随机测试库、显式离线迁移、真实 HTTP 原21+bootstrap+4壁纸、REST/PB共用消息、通话权限/信令、JSON拒绝、WSS revision/session注销、进程重启后幂等/revision/full恢复、未创建 Store/快照桥断言。新增启动守卫案例拒绝错误 mode/JSON配置。**均是待运行 fixture，不计通过数量**；恢复权限后先编译修复，再执行该入口与既有 PG/Legacy 回归。仅测试自己创建的随机数据库，不使用 FORCE 删除其他会话。

原始容量锁优化结论不变：保留数据库全局 `7459241304231`，两个性能候选均撤回；壁纸另用 `(2147000004,0)` 协调其状态，不能用其替代全局容量。其余待完成：冻结旧版本兼容运行、四组件真实处理/故障恢复、Scylla/Redis读接线、完整离线导入及回滚演练、自定义图片上传下载链路、同版本 P7验收和最终常驻实例。本轮没有缩小这些要求。

## 15. 2026-11-04 四组件隔离 P7 续接

本节记录后续实际完成的本机四组件工作。自定义文件壁纸及未覆盖 Telegram API 的功能范围仍见客户端与接口覆盖文档。

### 15.1 运行接线

`Storage:ValidationBackend=FourComponent` 复用完整关系化后端，增加 `Storage:FullStackAcknowledged=true`。原 Development、显式验证确认、loopback `botgram_test_*` 数据库、JSON RPC/WSS 关闭和 PB rollout 100 守卫继续执行。Kafka topic/group、Scylla keyspace 和 Redis namespace 也要求独立 `botgram_test_*` 名称及 loopback 服务。

`FourComponentRuntime` 运行真实 Outbox 发布、Kafka 消费和 PG Inbox/offset 提交、Scylla 投影及连续 checkpoint 推进。工作进程持有独立 PG 会话租约 `7459241304238`，数据库重启后重新取得租约。离线准备在执行 schema 操作前取得相同的排斥锁。运行合同固定事件 schema 2 和各组件命名空间，缺失合同或初始化 offset 时启动失败。topic 的起始 offset 仅在显式离线准备空 topic 时初始化，运行期间缺失 offset 会拒绝启动。

历史页和按 ID 读取先在 PG 验证会话、成员资格及消息元数据。已完成 watermark 内的消息从 Scylla 读取，未完成部分读取已提交的 PG 尾部。Redis 缓存按环境、代际、会话及消息 ID 隔离，命中也重新验证权限。缓存故障可回退到持久读取，已确认投影不可用时返回 `503 projection_unavailable`。PG 不可用返回 `503 storage_unavailable`。`/health` 实际检查四组件、后台循环、阻塞及积压计数。

### 15.2 离线迁移及回退

`Storage.Admin` 提供 `prepare <private-config.json>`、`import <private-config.json> <snapshot.json>` 和 `export <private-config.json> <new-snapshot.json>`。配置通过相同隔离策略验证，私有连接配置保存在 D 盘环境目录。

导入覆盖当前旧 `Database` 模型的账户和 Premium、会话摘要、联系人、会话与成员、消息及回复/实体/幂等键、回执、通话/信令事件、TL identities、pts 和三个分配游标。消息原始事件需匹配，事件沿用原 ID 并建立可恢复 Outbox。UUID、TL ID 和聊天关联保留。缺失 GID 使用现有迁移逻辑补发一次 World GID，重复导入复核同一已提交状态。

导入在同一事务内校验规范化的全模型指纹，失败回滚。非空目标、不同源或已经发生业务变化的目标会拒绝重新导入。原文件保持只读。时间统一为 UTC 微秒精度，以匹配 PostgreSQL timestamptz；集合的无语义排序差异统一后比较。

导出在离线租约下取得一致业务状态，通过同目录临时文件和原子重命名创建新文件，已存在的目标文件保持原内容。合成验收已经验证导出由真实旧 `Store` 读取，并保留导入后新增消息的幂等键。旧格式无法表达独立壁纸状态，因此已产生新版壁纸状态时明确拒绝旧格式导出，回退需使用切换前备份及明确的数据恢复流程。

### 15.3 实际证据和环境

- 原真实四组件套件通过，覆盖 Redis 回填和连接丢失、Kafka 发布/Inbox/offset 重开、冻结旧消费者拒绝 v2、Scylla 实体内容审计及代际激活，以及 47 项 PG 容量与 COMMIT 回包丢失回归。
- 最终 Release 的 HTTP/WS 联合验收通过 738 项断言，HTTPS/WSS 通过 745 项，含投影读取、缓存命中、迟到 Kafka 重复消息、离线重建、JSON 拒绝及重启。HTTPS/WSS 使用临时 localhost 证书精确 pin，验证了主机名、过期和错误证书拒绝。该证据的范围为宿主测试，临时密钥已清理。
- 快照库级迁移通过 22 项断言，包含成员顺序不同的旧单聊只读 RPC。实际交付包的 CLI 与导入后完整四组件处理通过 30 项断言。
- 启动边界通过 60 项断言。旧 GID、基础 HTTP/WSS smoke 及客户端严格 proto 字节一致检查通过。
- 真实 Redis、Kafka、Scylla、PostgreSQL 停机恢复已执行，包含缓存回退、PG 尾部读取、待投影补齐、持久会话及幂等键、PG 工作进程租约重建。逐轮实际数量与最终实例状态记录在客户端仓库 `artifacts/migration-resume-20261104/p7-report.md`。

本机运行根目录为 `D:\BotgramBuild\p7`，Docker 数据置于该目录内的 ext4 镜像。独立 Docker/containerd 使用专用 socket 和命名空间，服务均为 loopback。版本固定为 Kafka 3.9.0、Scylla 6.2.3、Redis 7.4.2 和本机既有 PostgreSQL 17.11 镜像。镜像 digest、本地 image ID 及来源记录在 `tools/p7-images.json`。该实例使用单节点复制配置，验证范围为本机持久性与恢复。

运行状态从 D 盘的 `runtime-manifest.json` 和 `process.json` 获取。源码中的生命周期工具验证镜像、数据挂载和进程命令后操作当前任务实例，保留数据库卷及镜像文件。

```powershell
./tools/Start-P7Validation.ps1
./tools/Start-P7Validation.ps1 -Action stop
```

准备好的实例使用 `http://127.0.0.1:18525`，正式环境切流、真实用户库迁移、设备操作及长期高负载验收需要各自的证据。此前性能候选的 FAIL 结论保留。

## 16. 指定既有隔离数据库的离线 schema v7→v8 升级（2026-11-05）

`Storage.Admin` 新增 `inspect` 与 `upgrade`。它们只操作显式配置指定的、回环地址上的 `botgram_test_*` 数据库，不启动服务、不读取旧 JSON、不连接正式库，也不打印连接字符串或凭据。实际支持的起始版本是精确的 `{1,2,3,4,5,6,7}`，目标是 `{1,2,3,4,5,6,7,8}`；已经是 v8 时报告 no-op，缺版本、含未知版本或其他集合明确拒绝。

`BOTGRAM_MIGRATION_TESTS=1` 仍只是快照导入导出的回归入口：测试程序创建随机 `botgram_test_intake_*` 临时库并在结束时删除，不能升级调用方指定的既有数据库。两者用途不能互相替代。

维护配置保存在 D 盘受限目录，不要提交到仓库。回环主机和 `botgram_test_*` 库名在加载时再次校验：

```json
{
  "Storage": {
    "Mode": "NewStorageValidation",
    "ValidationBackend": "RelationalBackend",
    "ValidationAcknowledged": "true",
    "ValidationConnectionString": "Host=127.0.0.1;Port=55432;Database=botgram_test_schema_fixture;Username=<limited-user>;Password=<secret>"
  }
}
```

`FourComponent` 目标使用同一入口，配置中的后端名可写 `FourComponent`。示例命令全部在 PowerShell 下执行，路径按实际仓库位置调整；已发布的维护程序位于 `artifacts/storage-admin`，也可按下面命令从源码构建：

```powershell
# 0. 先停止后端、四组件工作进程及任何读写者，等待数据库连接完全释放。
# 1. 备份目标数据库、部署配置以及 Privacy:KeyDirectory 密钥环；缺少密钥环会导致验证器不可解密。
# 2. 只读预检查：打印主机、数据库、用户、服务器版本、活动客户端数和真实 schema 版本。
dotnet build Storage.Admin/Storage.Admin.csproj -c Release
dotnet Storage.Admin/bin/Release/net8.0/Storage.Admin.dll inspect D:\BotgramBuild\private\schema-upgrade.json

# 3. 应用迁移：在单个事务内执行；任何失败都回滚，保留 v7、原数据和原版本记录。
dotnet Storage.Admin/bin/Release/net8.0/Storage.Admin.dll upgrade D:\BotgramBuild\private\schema-upgrade.json

# 4. 重复执行应输出 no migration required；再次 inspect 应显示 1..8。
dotnet Storage.Admin/bin/Release/net8.0/Storage.Admin.dll upgrade D:\BotgramBuild\private\schema-upgrade.json

# 5. 用同一数据库启动当前后端。PostgresBackend.VerifySchemaAsync 只接受精确的 1..8，
#    拒绝旧库或部分迁移；启动后检查 /health、登录、隐私、验证器和通话读取。

# 6. 快照导入/导出回归仍使用 BOTGRAM_MIGRATION_TESTS（临时库，不作为指定库升级证据）：
$env:BOTGRAM_PG_TEST_CONNECTION_STRING = 'Host=127.0.0.1;Port=55432;Database=botgram_test_migration_admin;Username=<limited-user>;Password=<secret>'
$env:BOTGRAM_MIGRATION_TESTS = '1'
dotnet run --project Storage.Tests
```

行为与验证：

- `inspect` 和应用前都核对 schema_versions、目标身份与 `pg_stat_activity` 的其他 client backend。`upgrade` 在同一事务内取得离线维护键、四组件运行时租约和容量写锁；其他读写者、工作进程或并发维护进程存在时明确失败。
- v8 只给 `calls` 增加 `request_protocol`/`accept_protocol` 并从原 `protocol` 回填。账号、有效会话、TL 身份、会话与成员、消息/事件/pts、联系人、回执、隐私规则、验证器/恢复码和通话记录保持逐字节不变。
- 实际定向回归 `BOTGRAM_SCHEMA_UPGRADE_TESTS=1` 覆盖预检查、v1..v6/未知版本拒绝、活动客户端、四组件租约、并发升级、触发器制造真实失败后的整事务回滚、重复执行、启动校验、登录、隐私、验证器和通话读取。
- 实际隔离 PostgreSQL 16.15、loopback 端口及 `botgram_test_*` 上的 CLI `inspect`/`upgrade`/重复 `upgrade`/迁移后应用读取结果记录在客户端 `artifacts/build-info.json` 的 `nativeCallsProtobufAndMigrationFixVerification`；不包含连接凭据或用户数据。
