# 设计与实现约定

本文档面向开发者和需要了解内部机制的人。普通使用、构建与部署请阅读 README。

## 架构

```text
local app -> towc / towc_gui -> SZUT WebVPN -> tows -> target service
```

- `towc` / `towc_gui`：WebVPN 登录、Cookie 续期、本地 TCP 监听、WebSocket 建连，并在单条 WebSocket 上多路复用多条 TCP 流。
- `tows`：接受 WebSocket，解析 OPEN 帧中的目标地址并连接目标 TCP，双向转发。
- 每个本地 TCP 连接对应一条 TCP 流（tunnel_id）；同一 `tows` 的所有隧道共用一条 WebSocket。
- 底层帧协议细节见 [protocol.md](protocol.md)。

## 登录与认证

程序会先刷新本地 WebVPN ticket，并通过实际 WebSocket 升级确认可用；有效则直接复用，否则支持：

- 微信登录：终端显示二维码，GUI 在窗口中显示二维码；
- 短信登录：输入手机号，收到验证码后输入；
- 邮箱验证码：输入邮箱，收到验证码后输入。

微信二维码过期后会自动获取新二维码；也可切换到短信或邮箱登录，无需重启程序。

网络或 TLS 故障无法证明 Cookie 已失效，此时客户端会保留缓存并直接报告连接错误。

认证缓存只保存 WebVPN ticket，不保存用于短信登录的手机号、邮箱、验证码、二维码回调 code 或二维码图片。

### 数据目录

- Windows：`%APPDATA%\tcp_over_websocket\`；缺少 `APPDATA` 时使用 `%LOCALAPPDATA%`；
- Linux：`$XDG_CACHE_HOME/tcp_over_websocket/`；未设置时使用 `~/.cache/tcp_over_websocket/`。

同一目录保存：Cookie 缓存、交互默认值、GUI 配置（`config.json`）、GUI 状态（`gui-state.json`）、程序日志。

### 日志约定

- `towc.log` 与 `tows.log` 采用追加写入，单文件最多 2 MiB；达到上限后保留最新日志。
- 控制台日志正文统一使用 ASCII 英文（避免不支持中文的终端乱码）；每行以彩色组件标签开头：`[towc]` 表示客户端生命周期与认证，隧道日志直接使用 `[隧道名称]`，`[tows]` 表示服务端生命周期。GUI 日志面板使用相同标签但不包含 ANSI 转义序列。

## 会话保活

两种互补机制，缺一不可：

1. **WebSocket 活性保活**：每条 WebSocket 默认每 `60` 秒发送 WebSocket Ping，并要求在下一次 Ping 前收到 Pong；独立连接和数据隧道都执行。用于维持现有连接不被关闭。
2. **WebVPN Cookie 续期**：同一客户端会话每 `10` 分钟全局刷新一次 Cookie，并把最新值更新到内存与本地缓存；新建隧道使用更新后的 Cookie。用于保证空闲一段时间后仍能建立新连接。

故障隔离：

- 单条 WebSocket 断开：只停止属于该 `tows` 的本地监听，其他服务器组继续工作；
- 全局 Cookie 刷新失败：停止全部服务器组。

实测 Cookie 刷新后的有效窗口约为 880–920 秒，因此 GUI 将 Cookie 刷新间隔限制为 60–840 秒（默认 600 秒）。即使 GUI 配置为空，认证任务仍会按全局可配置间隔调用 Cookie 更新接口，但不会建立 WebSocket。

## 网络性能

WebVPN WebSocket、服务端入站连接和目标 TCP 连接均启用 `TCP_NODELAY`，减少 SSH 等交互式小包被 Nagle 算法延迟合并的可能。保活流量只有每 60 秒一个 Ping/Pong 和每 10 分钟一次 HTTP 请求，通常不会造成可感知的吞吐或延迟负担；实际延迟仍主要取决于 WebVPN 路由和网络状况。

## towc_gui 机制

- 按规范化后的 `tows` 地址分组展示隧道；连接可以先于隧道单独创建，没有启用隧道时不建立 WebSocket。
- 某连接的第一条隧道启用时建立一条共享 WebSocket，最后一条关闭时停止该 WebSocket；其他服务器连接不受影响。
- 每个连接可单独设置保活间隔，底层通过 WebSocket PING 实现。
- 程序启动后自动检查 Cookie：有效时进入认证完成状态；无效时自动开始微信登录。二维码过期后可原地重新生成，也可在微信、短信和邮箱登录之间切换。
- 新建隧道默认禁用。“启用”开关会立即绑定或释放该隧道的本地端口，并同步保存 `enabled`；同一 IP 下其他隧道及其 WebSocket 不会重启。禁用隧道时，它已有的 TCP 流会被关闭。
- 隧道名称、目标和监听地址支持运行中热修改，目标与监听分别使用地址和端口输入框；分组标题中的 `tows` 服务器地址仅作只读显示。
- 导入：一个 JSON 文件可以包含多个 IP 的多条隧道，也可把多个 `.json` 文件或包含配置的文件夹拖入窗口。没有重复项时直接合并；发现同名项时弹窗选择跳过、覆盖或整体替换。来源文件永远不会被改写。
- 缺少名称的隧道会得到基于 `tows`、目标和监听地址生成的确定性名称；非法端口和本地监听冲突会被拒绝或高亮。
- 每行最前面的方形选择框与启用开关互不相关；选择状态不持久化，并在触发导出后自动清空。
- 主题和 Cookie 刷新间隔保存在独立的 `gui-state.json`；“导出选中隧道”默认目录为用户桌面。
- GUI 隧道状态只使用颜色圆点：绿色可用，黄色连接中或需注意，红色异常，灰色已禁用；悬停圆点可查看底层状态详情。
- 配置无法读取时，GUI 不会自动覆盖原文件；配置和交互缓存均采用同目录临时文件后原子替换。

## 源码结构

```text
src/lib.rs          库入口：模块组织、版本号、统一日志格式化
src/address.rs      tows / target / listen 地址解析
src/protocol.rs     二进制帧协议编解码（HELLO / OPEN / DATA / EOF / CLOSE）
src/multiplex.rs    单条 WebSocket 上的多路复用：流控与写入调度
src/network.rs      WebVPN 地址加密、WebSocket 握手与连接
src/storage.rs      配置与缓存原子读写、有界日志文件
src/bin/towc.rs     命令行客户端入口
src/bin/tows.rs     服务端入口
src/bin/towc_gui.rs 图形客户端入口
src/client/         登录（微信 / 短信 / 邮箱）、参数解析、本地隧道运行循环
src/server/         接受 WebSocket、解析目标、转发 TCP
src/gui/            egui 界面、GUI 配置读写与导入导出
```

## 限制明细

- 仅支持 TCP；Minecraft 基岩版默认使用 UDP，不能直接转发。
- 单条 WebSocket 最多同时打开或正在打开 64 条 TCP 流。
- 单个 `tows` 进程最多同时接受 128 条 WebSocket 连接、连接 1,024 个目标 TCP 流。
- GUI 中每个 `tows` 地址最多启用 64 条转发规则。
- 单帧数据上限 65,535 字节，较大的 TCP 读取会自动分片。
- 每条 WebSocket 的排队 payload 总量最多 4 MiB。
- 单个服务器组失败后不会影响其他服务器组；认证失效后可直接在 GUI 中重新登录。
- WebVPN ticket 绑定来源 IP，切换网络通常需要重新登录。
