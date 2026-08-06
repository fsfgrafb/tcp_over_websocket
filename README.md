# tcp_over_websocket

`tcp_over_websocket` 通过苏州工学院（SZUT）的 WebVPN，把公网电脑上的本地 TCP 端口转发到校园内网。它适合 SSH、Minecraft Java 版、RDP 和其他 TCP 服务；不支持 UDP。

当前版本为 **v0.5.1**，包含三个程序：

| 程序 | 用途 | 平台 |
|---|---|---|
| `tows` | 部署在校园内网，接受多路复用 WebSocket 并连接目标 TCP | Windows x64、Linux x64、Linux aarch64 |
| `towc` | 单条转发规则的命令行客户端，一条 WS 可复用多条本地 TCP 连接 | Windows x64、Linux x64、Linux aarch64 |
| `towc_gui` | 自包含的多隧道图形客户端，所有隧道共用一条 WS | Windows x64 |

## 下载和部署

从 GitHub Release 下载对应平台压缩包。每个压缩包都包含本 README 和 `docs/protocol-v2.md`。

先在能访问目标服务的内网机器启动新版服务端：

```text
tows
```

默认监听 `0.0.0.0:4489`。也可指定端口：

```text
tows 54489
```

内网已有的 **v0.5.0 及更早版本使用旧路径协议，与 v0.5.1 不兼容**。启动新客户端前，必须先升级 `10.18.47.77` 以及实际要使用的其他 tows。协议不匹配时客户端会退出，不会降级。

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
- 登录方式：微信扫码

直接运行 `towc` 会进入交互模式。程序先收集并校验 tows、目标、监听和登录方式，写入独立的交互默认值缓存，然后登录并启动。带默认值的提示可直接按回车复用。

IPv6 必须使用 `[addr]` 或 `[addr]:port`。目标和监听可只写端口，等价于 `127.0.0.1:port`。如果监听地址不是回环地址，CLI 会警告该端口会暴露给局域网。

## 登录

程序会先验证本地 WebVPN ticket 缓存；有效时直接复用。否则支持：

- 微信扫码：终端显示二维码，GUI 在窗口中显示二维码；
- 手机验证码：输入手机号，收到验证码后在终端或 GUI 输入；
- 邮箱验证码：输入邮箱，收到验证码后在终端或 GUI 输入。

认证缓存只保存 WebVPN ticket，不保存手机号、邮箱、验证码、二维码回调 code 或二维码图片。缓存和配置位于：

- Windows：`%APPDATA%\tcp_over_websocket\`；缺少 `APPDATA` 时使用 `%LOCALAPPDATA%`；
- Linux：`$XDG_CACHE_HOME/tcp_over_websocket/`；未设置时使用 `~/.cache/tcp_over_websocket/`。

程序每 60 秒发送一条协议 PING，并每 10 分钟全局刷新一次 WebVPN Cookie。来源 IP 变化、ticket 过期、保活失败或 WS 断开后不会自动重新登录：`towc` 退出；`towc_gui` 停止全部监听并保留窗口、配置和日志，等待手动再次启动。

控制台日志正文统一使用 ASCII 英文，避免不支持中文的终端出现乱码。每行日志以彩色组件标签开头：`[towc]` 表示客户端生命周期与认证，`[tunnel]` 表示连接和转发流，`[tows]` 表示服务端生命周期；GUI 日志面板使用相同标签但不包含 ANSI 转义序列。

## towc_gui

`towc_gui.exe` 内含完整客户端逻辑，不依赖同目录的 `towc.exe`。默认配置文件：

```text
%APPDATA%\tcp_over_websocket\config.json
```

示例配置：

```json
{
  "version": 1,
  "tows": "10.18.47.77:4489",
  "tunnels": [
    { "name": "SSH", "target": "127.0.0.1:22", "listen": "127.0.0.1:14489", "enabled": true },
    { "name": "Minecraft", "target": "127.0.0.1:25565", "listen": "127.0.0.1:25565", "enabled": true }
  ]
}
```

可把一个或多个 `.json` 文件、或包含配置的文件夹拖入窗口。导入前可选择跳过同名、覆盖同名或整体替换。来源文件永远不会被改写。缺少名称的隧道会得到基于目标和监听地址生成的确定性名称；重复名称、非法端口和本地监听冲突会被拒绝或高亮。

配置损坏或版本高于程序支持范围时，GUI 不会自动覆盖原文件。配置和交互缓存均采用同目录临时文件后原子替换。

## 限制

- 仅支持 TCP；Minecraft 基岩版默认使用 UDP，不能直接转发。
- 单条 WS 最多同时打开或正在打开 64 条 TCP 流。
- 单帧数据上限 65,535 字节，较大的 TCP 读取会自动分片。
- 不兼容 v0.5.0 及更早的 `/tcp/...` 路径协议。
- 不自动重连或自动重新登录；网络或认证失败后需用户重新启动。
- WebVPN ticket 绑定来源 IP，切换网络通常需要重新登录。

## 常见故障

**提示旧版 tows 或等待 HELLO_ACK 超时**

先升级内网 `tows` 到 v0.5.1 或更高版本，并确认启动的是新二进制。新版服务端不会发送“连接成功”文本。

**WebVPN 返回 `/wengine-vpn/failed`**

确认 tows 正在目标主机的配置端口监听、防火墙允许访问，且 WebVPN 能路由到该主机。该端口必须运行 WebSocket 服务，普通 SSH/TCP 端口不能直接作为 `ws-{port}` 入口。

**OPEN_FAIL**

tows 已连接，但它无法访问该条规则的目标。检查目标地址、目标服务监听状态和内网防火墙；其他已打开隧道不会受影响。

**本地端口被占用**

更换 `--listen` 或 GUI 中的监听端口。GUI 会在启动前检查多条规则之间的冲突。

**登录突然失效**

来源 IP 变化或 ticket 过期会使 WebVPN 拒绝连接。重新运行 `towc`，或在 GUI 中手动再次登录并启动。

## 开发

Rust edition 2024。常用命令：

```text
cargo test --all-features
cargo build --release --no-default-features --features server --bin tows
cargo build --release --features client,server --bin towc --bin tows
cargo build --release --all-features --bin towc_gui
```

协议细节见 [docs/protocol-v2.md](docs/protocol-v2.md)。
