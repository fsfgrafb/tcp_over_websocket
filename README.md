# tcp_over_websocket

面向 SZUT WebVPN 的 TCP over WebSocket 转发工具，由 `tows` 和 `towc` 组成。

```text
local app -> towc -> SZUT WebVPN -> tows -> target service
```

## 快速使用

1. 在目标服务所在机器运行 `tows`：

```bash
tows
```

2. 在本机运行 `towc`，然后连接本地监听端口：

```bash
towc <tows-ip>
ssh -p 14489 user@localhost
```

3. 转发其他目标端口时指定 `--target` 和 `--listen`：

```bash
towc <tows-ip> --target 3389 --listen 13389
```

不带参数运行 `towc` 会进入交互模式。默认使用终端微信扫码登录；也可用 `--login <mobile|email>` 走验证码登录。`towc` 会缓存 WebVPN Cookie，后续启动会自动校验。

## 命令

```bash
tows [port]
```

- `port` 默认 `4489`
- 普通 HTTP 探测会返回 `204 No Content`

```bash
towc <tows-ip[:port]> [--target <port>] [--listen <port>] [--login <mobile|email>]
```

- `tows` 端口默认 `4489`
- `--target` 默认 `22`
- `--listen` 默认 `14489`
- `towc` 会通过 WebVPN WebSocket 自动发送心跳，空闲时也会维持会话

## 构建

```bash
cargo build --release
```

产物：

- Linux/macOS: `target/release/tows`, `target/release/towc`
- Windows: `target/release/tows.exe`, `target/release/towc.exe`

## 开机自启

一般只建议给 `tows` 配置开机自启。`towc` 依赖 WebVPN 登录态，建议需要使用时手动启动。下面示例中的路径和端口按实际环境替换。

### Linux systemd

`tows` 任务，例如保存为 `/etc/systemd/system/tows.service`：

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

启用并启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tows
```

### Windows 任务计划程序

以管理员 PowerShell 创建 `tows` 自启任务：

```powershell
schtasks /Create /TN "tows" /SC ONSTART /RL HIGHEST /TR "C:\Tools\tcp_over_websocket\tows.exe 4489"
```

查看或删除任务：

```powershell
schtasks /Query /TN "tows"
schtasks /Delete /TN "tows"
```

## 排障

- `WebVPN returned /wengine-vpn/failed`：检查 `tows` 是否运行、端口是否正确、防火墙是否放行。
- `tows reported target connect failure`：检查目标服务是否监听在 `--target` 指定的端口。
- `cookie expired`：确认 `tows` 与 `towc` 都已更新到支持 WebVPN 心跳的版本；若仍过期，重新启动 `towc` 并登录。
- 本地端口占用：换一个 `--listen` 端口。
