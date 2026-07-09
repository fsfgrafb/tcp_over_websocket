# tcp_over_websocket

面向 SZUT WebVPN 的 TCP over WebSocket 转发工具，由服务端 `tows` 和客户端 `towc` 组成。

```text
local app -> towc -> SZUT WebVPN -> tows -> target service
```

## 快速使用

目标服务所在机器运行：

```bash
tows
```

本机运行：

```bash
towc <tows-ip>
ssh -p 14489 user@localhost
```

转发其他目标端口：

```bash
towc <tows-ip> --target 3389 --listen 13389
```

不带参数运行 `towc` 会进入交互模式。默认使用终端微信扫码登录；也可用 `--login <mobile|email>` 走验证码登录。

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
- WebVPN Cookie 会自动缓存并在启动时校验

## 构建

```bash
cargo build --release
```

产物：

- Linux/macOS: `target/release/tows`, `target/release/towc`
- Windows: `target/release/tows.exe`, `target/release/towc.exe`

## 排障

- `WebVPN returned /wengine-vpn/failed`：检查 `tows` 是否运行、端口是否正确、防火墙是否放行。
- `tows reported target connect failure`：检查目标服务是否监听在 `--target` 指定的端口。
- `cookie expired`：重新启动 `towc` 并登录。
- 本地端口占用：换一个 `--listen` 端口。
