# tcp_over_websocket

Rust TCP over WebSocket 转发工具，用于通过 SZUT WebVPN 访问内网 TCP 服务。

## 构建

```bash
cargo build --release
```

产物：

- Linux: `target/release/tows`、`target/release/towc`
- Windows: `target/release/tows.exe`、`target/release/towc.exe`

## 服务端 `tows`

```bash
tows [listen_addr]
```

- 默认监听 `0.0.0.0:4489`
- 只写端口时自动补全为 `0.0.0.0:<port>`

WebSocket 路径决定服务端要连接的 TCP 目标：

```text
/tcp                  -> 127.0.0.1:9999
/tcp/22               -> 127.0.0.1:22
/tcp/192.168.1.10:22  -> 192.168.1.10:22
```

示例：

```bash
./tows
./tows 4489
```

后台运行：

Linux:

```bash
nohup ./tows 4489 > tows.log 2>&1 &
```

Windows PowerShell:

```powershell
Start-Process -FilePath .\tows.exe -ArgumentList "4489" -WindowStyle Hidden
```

不指定端口时会使用默认的 `0.0.0.0:4489`。

## 客户端 `towc`

```bash
towc <server-ip[:port]> [--target <target-ip[:port]|port>] [--cookie <cookie>] [--login <mobile|email>] [--listen <local-addr>]
```

- `<server-ip[:port]>`：运行 `tows` 的地址，端口默认 `4489`
- `--target`：服务端连接的目标，默认 `127.0.0.1:9999`；可写 `22`、`:22`、`127.0.0.1:22`
- `--listen`：本地监听地址，默认 `127.0.0.1:9999`；只写端口时自动补全为 `127.0.0.1:<port>`
- `--cookie`：WebVPN Cookie；传入后直接使用
- `--login`：手机号或邮箱验证码登录；全数字按手机号发送短信验证码，包含 `@` 按邮箱发送邮件验证码；会自动完成 WebVPN fingerprint 登记
- 未传 `--cookie`、`--login` 时，Windows 会打开登录窗口自动获取 Cookie；Linux 请使用 `--login` 或手动传入 Cookie

## 示例

Windows 自动登录并转发 SSH：

```powershell
.\towc.exe 192.0.2.10:4489 --target 22 --listen 127.0.0.1:9999
ssh -p 9999 root@127.0.0.1
```

短信验证码登录并转发 SSH：

```bash
./towc 192.0.2.10:4489 --target 22 --login <mobile-number> --listen 127.0.0.1:9999
```

邮箱验证码登录：

```bash
./towc 192.0.2.10:4489 --target 22 --login <email-address>
```

Linux 验证码登录：

```bash
./towc 192.0.2.10:4489 --target 22 --login <email-address> --listen 127.0.0.1:9999
```

Linux 手动传 Cookie：

```bash
./towc 192.0.2.10:4489 \
  --target 22 \
  --cookie "wengine_vpn_ticketwebvpn_szut_edu_cn=你的票据" \
  --listen 127.0.0.1:9999

ssh -p 9999 root@127.0.0.1
```

转发远程桌面：

```bash
./towc 192.0.2.10:4489 --target 3389 --listen 127.0.0.1:13389
```

然后连接 `127.0.0.1:13389`。

## 排错

- Cookie 过期时会提示重新登录。
- WebVPN 返回 `/wengine-vpn/failed` 时，检查 `tows` 是否运行、端口是否正确、WebVPN 是否能访问该端口。
- Windows 未传 `--cookie`、`--login` 时的窗口登录依赖 Microsoft Edge WebView2 Runtime。
