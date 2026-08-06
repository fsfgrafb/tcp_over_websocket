# tcp_over_websocket

面向苏州工学院（SZUT）WebVPN 的 TCP over WebSocket 转发工具：把公网电脑上的本地 TCP 端口，经 WebVPN 转发到校园内网服务（SSH、Minecraft Java 版、RDP 等 TCP 服务；不支持 UDP）。

```text
local app -> towc / towc_gui -> SZUT WebVPN -> tows -> target service
```

包含三个程序：

| 程序 | 用途 | 平台 |
|---|---|---|
| `tows` | 部署在校园内网，接收 WebSocket 并连接目标 TCP | Windows x64、Linux x64、Linux aarch64 |
| `towc` | 命令行客户端：本地监听 + 一条 WebSocket 复用多条连接 | Windows x64、Linux x64、Linux aarch64 |
| `towc_gui` | 图形客户端：多服务器、多隧道，同一 `tows` 共用一条 WebSocket | Windows x64 |

## 快速使用

**1. 在内网机器启动服务端**（从 GitHub Release 下载对应平台压缩包）：

```bash
tows          # 默认监听 0.0.0.0:4489
tows 54489    # 指定端口
```

**2. 在本机启动客户端**，把 `127.0.0.1:14489` 转发到 tows 所在机器可访问的 `127.0.0.1:22`：

```bash
towc 10.18.47.77
```

**3. 连接本地端口**：

```bash
ssh -p 14489 user@localhost
```

转发其他端口：

```bash
towc 10.18.47.77 --target 3389 --listen 13389
```

不带参数运行 `towc` 进入交互模式，提示可回车复用上次的值。

## 命令

```text
tows [port]
towc <tows-host[:port]> [--target <host:port|port>] [--listen <host:port|port>] [--login <mobile|email>]
```

- 默认值：tows 端口 `4489`，目标 `127.0.0.1:22`，本地监听 `127.0.0.1:14489`，登录方式微信。
- 目标和监听可只写端口（等价于 `127.0.0.1:port`）；IPv6 使用 `[addr]:port`。
- `towc --version` / `tows --version` 查看版本。

## 登录

首次使用需要登录 WebVPN，支持微信扫码（终端显示二维码，GUI 在窗口内显示）、短信验证码、邮箱验证码三种方式。之后自动复用缓存，无需重复登录。

缓存与配置保存在：

- Windows：`%APPDATA%\tcp_over_websocket\`
- Linux：`$XDG_CACHE_HOME/tcp_over_websocket/`（未设置时 `~/.cache/...`）

## 图形客户端 towc_gui

`towc_gui.exe` 自带完整客户端逻辑，配置在 `%APPDATA%\tcp_over_websocket\config.json`。首次启动配置为空，可在界面创建隧道，或导入 JSON 配置、拖入文件。每个 `tows` 地址最多 64 条隧道，支持运行中启停、修改和导出。

## 构建与部署

Rust 1.95+，edition 2024：

```bash
cargo build --release --no-default-features --features server --bin tows
cargo build --release --features client,server --bin towc --bin tows
cargo build --release --all-features --bin towc_gui
```

产物在 `target/release/`（Windows 下为 `.exe`）。Linux 服务端安装：

```bash
sudo install -m 0755 target/release/tows /usr/local/bin/tows
sudo systemctl restart tows
```

协议或逻辑升级时应同时更新两端。

## 开机自启

一般只建议给 `tows` 配置自启（`towc` 依赖 WebVPN 登录态，建议手动启动）。Linux systemd 示例 `/etc/systemd/system/tows.service`：

```ini
[Unit]
Description=tcp_over_websocket server
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/tows 4489
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tows
```

Windows 任务计划程序示例：

```powershell
schtasks /Create /TN "tows" /SC ONSTART /RL HIGHEST /TR "C:\Tools\tcp_over_websocket\tows.exe 4489"
```

## 限制

- 仅支持 TCP；Minecraft 基岩版默认使用 UDP，不能直接转发。
- 单条 WebSocket 最多 64 条并发 TCP 流；单个 `tows` 最多 128 条 WebSocket、1,024 条目标连接。
- WebVPN ticket 绑定来源 IP，切换网络通常需要重新登录。

## 常见问题

- **WebVPN 返回 `/wengine-vpn/failed`**：确认 tows 正在目标主机监听对应端口、防火墙放行，且该端口运行的是 WebSocket 服务（普通 SSH/TCP 端口不能直接作为 `ws-{port}` 入口）。
- **OPEN_FAIL**：tows 已连上，但连不到该规则的目标；检查目标地址、目标服务监听状态和内网防火墙。
- **本地端口被占用**：更换 `--listen` 或 GUI 中的监听端口。
- **登录突然失效**：来源 IP 变化或 ticket 过期；重新运行 `towc` 会先验证缓存，GUI 可直接重新登录。

## 更多文档

- [docs/design.md](docs/design.md)：设计与实现细节（登录、会话保活、GUI 机制、源码结构、限制明细）
- [docs/protocol.md](docs/protocol.md)：towc ↔ tows 多路复用协议

