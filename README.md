# tcp_over_websocket

`tcp_over_websocket` 是一个面向 SZUT WebVPN 场景的 TCP over WebSocket 端口转发工具。它由服务端 `tows` 和客户端 `towc` 组成，可将本地 TCP 连接经 WebVPN 转发到远端内网主机上的 TCP 服务。

典型链路：

```text
local app -> towc -> SZUT WebVPN WebSocket -> tows -> target TCP service
```

## 功能

- 通过 SZUT WebVPN 建立 WebSocket 隧道
- 支持微信扫码登录，以及手机号/邮箱验证码登录
- 自动缓存并校验 WebVPN Cookie
- 支持自定义本地监听地址、远端 `tows` 地址和目标 TCP 地址
- 启动前进行隧道可用性探测，并输出针对 WebVPN、`tows`、目标服务的诊断信息
- `tows` 对普通 HTTP 探测返回 `204 No Content`，便于端口健康检查

## 快速开始

在目标服务所在主机运行服务端：

```bash
tows
```

在本机运行客户端，并连接默认目标 `127.0.0.1:22`：

```bash
towc <tows-ip>
ssh -p 14489 user@127.0.0.1
```

转发 RDP 或其他端口：

```bash
towc <tows-ip> --target 3389 --listen 13389
```

不带参数运行 `towc` 会进入交互模式。

## 命令

### `tows`

```bash
tows [listen]
```

`listen` 可写为端口或完整监听地址：

| 示例 | 含义 |
| --- | --- |
| `tows` | 监听 `0.0.0.0:4489` |
| `tows 4489` | 监听 `0.0.0.0:4489` |
| `tows 127.0.0.1:4489` | 仅监听本机回环地址 |

WebSocket 路径决定 `tows` 要连接的目标 TCP 地址：

| 路径 | 目标 |
| --- | --- |
| `/tcp` | `127.0.0.1:22` |
| `/tcp/3389` | `127.0.0.1:3389` |
| `/tcp/10.0.0.2:5432` | `10.0.0.2:5432` |

### `towc`

```bash
towc <server[:port]> [--target <[host:]port>] [--listen <[addr:]port>] [--login <mobile|email>]
```

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `<server[:port]>` | 必填 | `tows` 的地址；端口默认 `4489` |
| `--target <[host:]port>` | `127.0.0.1:22` | `tows` 侧要连接的目标 TCP 服务 |
| `--listen <[addr:]port>` | `127.0.0.1:14489` | `towc` 本地监听地址 |
| `--login <mobile|email>` | 微信扫码 | 使用手机号或邮箱验证码登录 |

示例：

```bash
towc 192.0.2.10
towc 192.0.2.10:4489 --target 3389 --listen 13389
towc 192.0.2.10 --target 10.0.0.2:5432 --listen 127.0.0.1:15432
towc 192.0.2.10 --login 18888888888
towc 192.0.2.10 --login user@example.com
```

Windows PowerShell 使用方式相同，只需按实际文件名运行：

```powershell
.\tows.exe 4489
.\towc.exe 192.0.2.10 --target 3389 --listen 13389
```

## Cookie 缓存

`towc` 会缓存 WebVPN Cookie。启动时会先用缓存 Cookie 进行连通性探测；如果缓存失效，则重新进入登录流程。

缓存位置：

- Windows: `%APPDATA%\tcp_over_websocket\webvpn.cookie`，若不可用则使用 `%LOCALAPPDATA%`
- Linux/macOS: `$XDG_CACHE_HOME/tcp_over_websocket/webvpn.cookie`，若未设置则使用 `~/.cache/tcp_over_websocket/webvpn.cookie`

运行过程中如果 WebVPN 判定 Cookie 过期，`towc` 会退出并提示重新启动登录。

## 后台运行

Linux systemd 示例：

```ini
# /etc/systemd/system/tows.service
[Unit]
Description=tcp_over_websocket server
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/tcp_over_websocket
ExecStart=/opt/tcp_over_websocket/tows 4489
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tows
sudo systemctl status tows
```

Windows 计划任务示例：

```powershell
$Action = New-ScheduledTaskAction -Execute "C:\Tools\tcp_over_websocket\tows.exe" -Argument "4489"
$Trigger = New-ScheduledTaskTrigger -AtStartup
Register-ScheduledTask -TaskName "tows" -Action $Action -Trigger $Trigger -Description "tcp_over_websocket server" -User "SYSTEM" -RunLevel Highest
Start-ScheduledTask -TaskName "tows"
```

删除计划任务：

```powershell
Unregister-ScheduledTask -TaskName "tows" -Confirm:$false
```

## 构建

```bash
cargo build --release
```

产物位置：

- Linux/macOS: `target/release/tows`、`target/release/towc`
- Windows: `target/release/tows.exe`、`target/release/towc.exe`

发布前建议运行：

```bash
cargo fmt
cargo test
cargo clippy --all-targets -- -D warnings
```

## 排障

- `WebVPN returned /wengine-vpn/failed`：检查 `tows` 是否运行、监听端口是否正确、防火墙是否放行，以及 WebVPN 是否能访问该地址。
- `tows reported target connect failure`：WebVPN 已到达 `tows`，但 `tows` 无法连接目标 TCP 服务。检查目标服务是否监听在 `--target` 指定的地址。
- `cookie expired`：缓存 Cookie 已失效，重新启动 `towc` 并完成登录。
- 本地端口占用：调整 `--listen`，例如 `--listen 10022`。
