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

不带参数运行 `towc` 会进入交互模式。程序先依次读取 `tows`、目标和本地监听地址，参数确认并缓存后，通过当前 `tows` 对应的 WebVPN 保活 WebSocket 对已有 Cookie 做一次直接校验。登录重定向会立即判定为过期并进入正常登录流程，不再进行多次 readiness 重试。`tows` 不可达只代表隧道端点故障，不会被误判为 Cookie 过期。仅当缓存缺失、格式无效或明确过期时，才询问登录方式：输入手机号/邮箱使用验证码，或直接回车使用终端微信扫码。新登录取得的 Cookie 会立即写入本地缓存。

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
- 交互模式首次运行时 `tows` 地址必填，目标和监听端口使用内置默认值；已有交互缓存时，三项提示中的默认值会替换为上次采用的值。登录方式始终在这三项参数输入完成且缓存 Cookie 验证失败后才询问。
- 带参数启动时也会优先尝试缓存认证。`--login` 仅是缓存缺失、格式无效或明确过期时的验证码登录后备方式；未提供 `--login` 时回退到终端微信扫码，因此认证过程仍可能要求输入验证码或扫码。
- 启动日志会输出程序名和版本，例如 `towc v0.5.0`。

## 会话与隧道

每个 `towc` 进程只管理一个登录会话、一组本地监听参数和一条隧道。需要多开隧道时，使用不同的本地监听端口启动多个 `towc`；一个 `tows` 进程可以并发服务多个 `towc`。这种进程边界便于上层软件独立启动、停止和重启每条隧道。

`towc` 内部仍将登录会话和转发隧道作为两个独立生命周期：

1. 会话层每 `180` 秒访问 WebVPN 门户 Cookie 接口，将最新 Cookie 更新到内存和缓存。它不访问任何 `tows`。
2. 每条实际建立的数据连接由 `relay_stream` 在建立后立即发送一次 `连接成功`，之后每 `210` 秒发送并由 `tows` 回显，与 v0.4 的有效行为一致。会话同时维持一条独立的 `/webvpn-keepalive` WebSocket，断开后自动重连。

`tows` 只有在目标 TCP 连接成功后才发送 readiness 确认；`towc` 收到确认后才开放本地监听。relay 使用内部控制帧传递 TCP 半关闭，确保请求方向 EOF 后，响应方向仍可继续传输。
readiness 与半关闭属于两端内部协议，部署时应同时更新 `towc` 和 `tows`。

`tows` 不可达只会让当前隧道进入重试，不会退出登录。Cookie 过期时会话回到未登录状态，隧道保留配置并等待重新登录。停止隧道也不会主动退出会话。

每轮隧道 readiness 只探测一次，不在内部连续重试；非认证类失败交给隧道生命周期按固定间隔重新启动探测，避免一次启动产生重复请求和重复诊断。

隧道进入运行状态时，终端会一次性输出规范化后的完整链路，例如 `local 127.0.0.1:14489 -> WebVPN -> tows 10.18.47.77:4489 -> target 127.0.0.1:22`；中间状态不重复刷屏。

Cookie 续期保证空闲一段时间后仍能创建新连接，WebSocket 心跳维持现有连接，二者不能互相替代。周期性成功信息不会重复写入日志；连接建立、断线重连、刷新失败和 Cookie 失效仍会记录。

## 作为库使用

默认 feature 同时启用客户端、服务端和终端 CLI。只导入其中一端时可关闭默认 feature，避免编译另一端及终端二维码的依赖：

```toml
# 仅客户端
tcp_over_websocket = { version = "0.5", default-features = false, features = ["client"] }

# 仅服务端
tcp_over_websocket = { version = "0.5", default-features = false, features = ["server"] }
```

客户端能力通过 `tcp_over_websocket::towc` 导出。上层软件可以为每条配置分别调用单隧道入口 `run_embedded_client`，通过 `EmbeddedClientUi` 接收结构化事件并提供验证码：

```rust,no_run
use std::sync::Arc;
use tcp_over_websocket::towc::{
    EmbeddedClientConfig, EmbeddedClientUi, LoginMethod, run_embedded_client,
};

# fn build_ui() -> Arc<dyn EmbeddedClientUi> { todo!() }
# async fn example() -> anyhow::Result<()> {
let ui = build_ui();
let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
run_embedded_client(
    EmbeddedClientConfig {
        server: "192.0.2.10:4489".into(),
        target: "127.0.0.1:22".into(),
        listen_addr: "127.0.0.1:14489".into(),
        login: LoginMethod::WechatQr,
    },
    ui,
    shutdown_rx,
)
.await?;
# Ok(())
# }
```

`TunnelEvent` 中的 `tunnel_id` 作为结构化接口关联字段继续保留，供未来上层管理软件使用；单隧道终端输出不会显示内部编号。

上层为每条隧道创建独立 `towc` 子进程时，可给各进程设置
`TCP_OVER_WEBSOCKET_CACHE_DIR`，使 Cookie 和交互默认值写入各自目录。未设置时沿用系统缓存目录；跨进程目录分配与生命周期同步由上层负责，库内不引入全局锁。

服务端通过 `tcp_over_websocket::tows` 导出。`TowsServer` 可订阅监听状态，由宿主传入关闭信号；连接建立、HTTP 探测、兼容保活和数据隧道分别产生结构化事件：

```rust,no_run
use std::{net::SocketAddr, sync::Arc};
use tcp_over_websocket::tows::{TowsEventSink, TowsServer, TowsServerConfig};

# fn build_sink() -> Arc<dyn TowsEventSink> { todo!() }
# async fn example() -> anyhow::Result<()> {
let server = TowsServer::new(build_sink());
let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
server
    .run(
        TowsServerConfig {
            listen_addr: SocketAddr::from(([0, 0, 0, 0], 4489)),
        },
        shutdown_rx,
    )
    .await?;
# Ok(())
# }
```

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
src/towc.rs         可导入的登录会话、Cookie 生命周期、隧道管理和客户端实现
src/tows.rs         可导入的服务端监听、连接事件和目标转发实现
src/bin/towc.rs     towc 命令行薄入口
src/towc/qr.rs      微信二维码解码与终端渲染
src/bin/tows.rs     tows 命令行薄入口
```

## 排障

- `WebVPN returned /wengine-vpn/failed`：检查 `tows` 是否运行、端口是否正确、防火墙是否放行。
- `tows reported target connect failure`：检查目标服务是否监听在 `--target` 指定的地址。
- `cookie expired`：确认两端版本一致；若 Cookie 刷新此前持续失败，重新启动 `towc` 并登录。
- 隧道持续处于 `Retrying`：检查 `towc` 到 WebVPN、WebVPN 到 `tows` 及 `tows` 到目标服务的连通性。
- 本地端口占用：使用其他 `--listen` 端口。
