# tcp_over_websocket

面向 SZUT WebVPN 的 TCP over WebSocket 转发工具，由客户端 `towc` 和服务端 `tows` 组成。

```text
local app -> towc -> SZUT WebVPN -> tows -> target service
```

`towc` 负责 WebVPN 登录、Cookie 续期、本地 TCP 监听和 WebSocket 建连；`tows` 接收 WebSocket，将数据转发到服务端本机的目标 TCP 服务。默认场景是通过 WebVPN 访问 SSH，也可以转发其他 TCP 端口。

## 快速使用

在目标服务所在机器启动服务端：

```bash
tows
```

在本机启动客户端，并连接本地监听端口：

```bash
towc <tows-ip>
ssh -p 14489 user@localhost
```

转发其他端口时：

```bash
towc <tows-ip> --target 3389 --listen 13389
```

不带参数运行 `towc` 会进入交互模式。客户端读取 `tows` 地址后立即输出对应的 WebVPN location，随即尝试缓存认证；成功时跳过登录方式。缓存缺失、格式无效或明确过期时才询问登录方式：输入手机号/邮箱使用验证码，或直接回车使用终端微信扫码。认证完成后客户端启动 WebSocket 保活，并等到首次保活连接成功、输出连接成功日志后才询问目标地址和本地监听地址。新登录取得的 Cookie 会立即写入本地缓存。网络或 TLS 故障无法证明 Cookie 已失效，此时客户端会保留缓存并直接报告连接错误。

交互模式会把本次实际采用的 `tows`、目标和本地监听地址分别缓存；下次启动时，这三个值会显示为新的默认选项，直接回车即可复用。此配置缓存与 WebVPN Cookie 缓存相互独立。

## 命令

```text
tows [port]
```

- 监听端口默认 `4489`。
- 普通 HTTP 探测返回 `204 No Content`。

```text
towc <tows-ip[:port]> [--target <host:port|port>] [--listen <host:port|port>] [--login <mobile|email>]
```

- `tows` 端口默认 `4489`。
- 目标地址默认 `127.0.0.1:22`。
- 本地监听地址默认 `127.0.0.1:14489`。
- 交互模式首次运行时 `tows` 地址必填，目标和监听端口使用内置默认值；已有交互缓存时，三项提示中的默认值会替换为上次采用的值。
- 带参数启动时也会优先尝试缓存认证。`--login` 仅是缓存缺失、格式无效或明确过期时的验证码登录后备方式；未提供 `--login` 时回退到终端微信扫码，因此认证过程仍可能要求输入验证码或扫码。
- 启动日志会输出程序名和版本，例如 `towc v0.4.0`。

## 会话保活

`towc` 同时维护两种互补的保活机制：

1. WebSocket 活性保活：连接建立后立即发送一次 `连接成功`，之后每 `210` 秒发送一次；`tows` 原样回显。独立保活连接和正在使用的数据隧道都会执行该心跳，避免空闲 WebSocket 被关闭。
2. WebVPN Cookie 续期：每 `180` 秒请求 WebVPN Cookie 接口，并将响应中的最新 Cookie 更新到内存和本地缓存。后续创建的新隧道使用更新后的 Cookie。

缓存或新登录确认后，这两项任务会立即启动；独立 WebSocket 建连后立即发送首个心跳，Cookie 续期任务的首次 HTTP 刷新在 `180` 秒后执行。交互模式会等待独立保活首次连接成功，再读取目标与监听参数；连接失败时每 `5` 秒重试，期间不会提前显示后续输入提示。缓存认证使用独立保活连接，不以目标 TCP 服务是否可用为条件；参数确定后，`towc` 再单独检查所选 `tows`/目标隧道，因此认证成功并不代表目标隧道必然就绪。

第一种机制维持现有连接，第二种机制保证空闲一段时间后仍能创建新连接，二者不能互相替代。周期性成功信息不会重复写入日志；连接建立、断线重连、刷新失败和 Cookie 失效仍会记录。

## 网络性能

WebVPN WebSocket、服务端入站连接和目标 TCP 连接均启用 `TCP_NODELAY`，减少 SSH 等交互式小包被 Nagle 算法延迟合并的可能。保活流量只有每几分钟一个短文本帧和一次 HTTP 请求，通常不会造成可感知的吞吐或延迟负担；实际延迟仍主要取决于 WebVPN 路由和网络状况。

## 构建与升级

```bash
cargo build --release
```

构建产物：

- Linux/macOS：`target/release/tows`、`target/release/towc`
- Windows：`target/release/tows.exe`、`target/release/towc.exe`

协议或保活逻辑升级时应同时更新两端。Linux 服务端示例：

```bash
sudo install -m 0755 target/release/tows /usr/local/bin/tows
sudo systemctl restart tows
sudo systemctl status tows
```

重启后通过启动日志中的版本号确认 systemd 没有继续运行旧二进制。

## 开机自启

一般只建议为 `tows` 配置开机自启。`towc` 依赖 WebVPN 登录态，适合在需要时手动启动。

Linux systemd 单元示例 `/etc/systemd/system/tows.service`：

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

## 源码结构

```text
src/lib.rs          WebVPN 地址生成、加密、WebSocket 握手、心跳和双向转发
src/bin/towc.rs     客户端参数、登录、Cookie 生命周期和本地隧道
src/bin/towc/qr.rs  微信二维码解码与终端渲染
src/bin/tows.rs     服务端监听、探测响应和目标 TCP 连接
```

## 排障

- `WebVPN returned /wengine-vpn/failed`：检查 `tows` 是否运行、端口是否正确、防火墙是否放行。
- `tows reported target connect failure`：检查目标服务是否监听在 `--target` 指定的地址。
- `cookie expired`：确认两端版本一致；若 Cookie 刷新此前持续失败，重新启动 `towc` 并登录。
- `WebVPN keepalive failed`：检查 `towc` 到 WebVPN、WebVPN 到 `tows` 的网络连通性；客户端会每 5 秒尝试重连。
- 本地端口占用：使用其他 `--listen` 端口。
