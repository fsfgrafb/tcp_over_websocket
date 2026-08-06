# tcp_over_websocket

`tcp_over_websocket` 通过苏州工学院（SZUT）的 WebVPN，把公网电脑上的本地 TCP 端口转发到校园内网。它适合 SSH、Minecraft Java 版、RDP 和其他 TCP 服务；不支持 UDP。

当前版本为 **v0.5.1**，包含三个程序：

| 程序 | 用途 | 平台 |
|---|---|---|
| `tows` | 部署在校园内网，接受多路复用 WebSocket 并连接目标 TCP | Windows x64、Linux x64、Linux aarch64 |
| `towc` | 单条转发规则的命令行客户端，一条 WS 可复用多条本地 TCP 连接 | Windows x64、Linux x64、Linux aarch64 |
| `towc_gui` | 自包含的多服务器、多隧道图形客户端；同一 `tows` 的隧道共用一条 WS | Windows x64 |

## 下载和部署

从 GitHub Release 下载对应平台压缩包。每个压缩包都包含本 README 和 `docs/protocol.md`。

先在能访问目标服务的内网机器启动服务端：

```text
tows
```

默认监听 `0.0.0.0:4489`。也可指定端口：

```text
tows 54489
```

## towc 最短示例

把本机 `127.0.0.1:14489` 转发到 tows 所在机器可访问的 `127.0.0.1:22`：

```text
towc 10.18.47.77
ssh -p 14489 user@localhost
```

完整参数格式：

```text
towc <tows-host[:port]> --target <host:port|port> --listen <host:port|port> --login <mobile|email>
```

三个 flag 可任意排序。示例：

```text
towc 10.18.47.77:4489 --target 25565 --listen 25565
towc 10.18.47.77 --target 10.18.47.66:3389 --listen 127.0.0.1:13389 --login student@example.com
```

默认值：

- tows 端口：`4489`
- 目标：`127.0.0.1:22`
- 本地监听：`127.0.0.1:14489`
- 登录方式：微信登录

直接运行 `towc` 会进入交互模式。程序先收集并校验 tows、目标和监听地址，再刷新缓存 Cookie 并通过实际 WebSocket 升级验证；缓存有效时直接建立连接，只有无效时才询问登录方式。带默认值的提示可直接按回车复用。

IPv6 必须使用 `[addr]` 或 `[addr]:port`。目标和监听可只写端口，等价于 `127.0.0.1:port`。如果监听地址不是回环地址，CLI 会警告该端口会暴露给局域网。

## 登录

程序会先刷新本地 WebVPN ticket，并通过实际 WebSocket 升级确认它可用；有效时直接复用。否则支持：

- 微信登录：终端显示二维码，GUI 在窗口中显示二维码；
- 手机验证码：输入手机号，收到验证码后在终端或 GUI 输入；
- 邮箱验证码：输入邮箱，收到验证码后在终端或 GUI 输入。

认证缓存只保存 WebVPN ticket，不保存手机号、邮箱、验证码、二维码回调 code 或二维码图片。缓存和配置位于：

- Windows：`%APPDATA%\tcp_over_websocket\`；缺少 `APPDATA` 时使用 `%LOCALAPPDATA%`；
- Linux：`$XDG_CACHE_HOME/tcp_over_websocket/`；未设置时使用 `~/.cache/tcp_over_websocket/`。

每条 WebSocket 每 60 秒独立发送一条协议 PING；同一次客户端会话每 10 分钟只全局刷新一次 WebVPN Cookie。`towc_gui` 中某条 WebSocket 断开时，只停止属于该 `tows` 的本地监听，其他服务器组继续工作；全局 Cookie 刷新失败时才停止全部服务器组。二维码过期或认证失效后，可直接重新获取二维码，或切换到手机号/邮箱登录，无需重启程序。

控制台日志正文统一使用 ASCII 英文，避免不支持中文的终端出现乱码。每行日志以彩色组件标签开头：`[towc]` 表示客户端生命周期与认证，`[tunnel]` 表示连接和转发流，`[tows]` 表示服务端生命周期；GUI 日志面板使用相同标签但不包含 ANSI 转义序列。

## towc_gui

`towc_gui.exe` 内含完整客户端逻辑，不依赖同目录的 `towc.exe`。默认配置文件：

```text
%APPDATA%\tcp_over_websocket\config.json
```

首次启动时配置为空，程序不会预置任何服务器或隧道。可以在界面中创建第一条隧道，也可以导入如下示例配置：

```json
{
  "tunnels": [
    { "name": "77 SSH", "tows": "10.18.47.77:4489", "target": "127.0.0.1:22", "listen": "127.0.0.1:14489", "enabled": true },
    { "name": "77 Minecraft", "tows": "10.18.47.77:4489", "target": "127.0.0.1:25565", "listen": "127.0.0.1:25565", "enabled": true },
    { "name": "66 SSH", "tows": "10.18.47.66:4489", "target": "127.0.0.1:22", "listen": "127.0.0.1:14490", "enabled": true },
    { "name": "66 Minecraft", "tows": "10.18.47.66:4489", "target": "127.0.0.1:25565", "listen": "127.0.0.1:25566", "enabled": true }
  ]
}
```

GUI 按规范化后的 `tows` 地址分组展示隧道，每行自行指定目标和本地监听。上例建立两条 WebSocket，每条承载对应服务器的 SSH 和 Minecraft 隧道。这样不会为每条规则重复建立连接，也不会让一台服务的网络故障中断另一台服务。

程序启动后自动检查 Cookie：有效时直接连接并开启全部 `enabled: true` 的隧道；无效时自动开始微信登录。二维码过期后可原地重新生成，也可在微信、手机号和邮箱登录之间切换。认证成功后该区域显示连接时长、隧道数量、保活连接数和 Cookie 刷新状态。

新建隧道默认禁用。“启用”开关会立即绑定或释放该隧道的本地端口，并同步保存 `enabled`；同一 IP 下其他隧道及其 WebSocket 不会重启。禁用隧道时，它已有的 TCP 流会被关闭。隧道名称、目标和监听地址支持运行中热修改，目标与监听分别使用地址和端口输入框；分组标题中的 `tows` 服务器地址仅作只读显示，不能伪装成可编辑输入框。

一个 JSON 文件可以包含多个 IP 的多条隧道，也可把多个 `.json` 文件或包含配置的文件夹拖入窗口。没有重复项时直接合并；发现同名项时弹窗选择跳过、覆盖或整体替换。来源文件永远不会被改写。缺少名称的隧道会得到基于 `tows`、目标和监听地址生成的确定性名称；非法端口和本地监听冲突会被拒绝或高亮。

每行最前面的复选框与启用开关互不相关，选择状态和主题保存在独立的 `gui-state.json`。“导出选中隧道”打开系统另存为窗口，默认目录为用户桌面，并把全部选中项写入一个 JSON 文件。

配置无法读取时，GUI 不会自动覆盖原文件。配置和交互缓存均采用同目录临时文件后原子替换。

## 限制

- 仅支持 TCP；Minecraft 基岩版默认使用 UDP，不能直接转发。
- 单条 WS 最多同时打开或正在打开 64 条 TCP 流。
- GUI 中每个 `tows` 地址最多启用 64 条转发规则。
- 单帧数据上限 65,535 字节，较大的 TCP 读取会自动分片。
- 单个服务器组失败后不会影响其他服务器组；认证失效后可直接在 GUI 中重新登录。
- WebVPN ticket 绑定来源 IP，切换网络通常需要重新登录。

## 常见故障

**等待 HELLO_ACK 超时或协议版本不一致**

确认 `towc` 与 `tows` 来自同一 Release，并检查服务端是否正在配置端口监听。

**WebVPN 返回 `/wengine-vpn/failed`**

确认 tows 正在目标主机的配置端口监听、防火墙允许访问，且 WebVPN 能路由到该主机。该端口必须运行 WebSocket 服务，普通 SSH/TCP 端口不能直接作为 `ws-{port}` 入口。

**OPEN_FAIL**

tows 已连接，但它无法访问该条规则的目标。检查目标地址、目标服务监听状态和内网防火墙；其他已打开隧道不会受影响。

**本地端口被占用**

更换 `--listen` 或 GUI 中的监听端口。GUI 会在启动前检查多条规则之间的冲突。

**登录突然失效**

来源 IP 变化或 ticket 过期会使 WebVPN 拒绝连接。CLI 重新运行后会先验证缓存；GUI 可直接重新生成微信二维码或改用手机号/邮箱登录。

## 开发

Rust edition 2024。常用命令：

```text
cargo test --all-features
cargo build --release --no-default-features --features server --bin tows
cargo build --release --features client,server --bin towc --bin tows
cargo build --release --all-features --bin towc_gui
```

协议细节见 [docs/protocol.md](docs/protocol.md)。
