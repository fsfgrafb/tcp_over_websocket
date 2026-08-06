# towc ↔ tows 多路复用协议

本协议在一条 WebSocket 上承载多条独立 TCP 流。WebSocket URL 路径固定为 `/`，目标地址由 OPEN 帧指定。

## 连接握手

客户端建立 WebSocket 后必须先发送 Binary HELLO；服务端最多等待 5 秒并回复 Binary HELLO_ACK。双方记录对端协议版本，版本不一致时写入警告日志但继续连接；当前 `PROTOCOL_VERSION = 2`。

HELLO 或 HELLO_ACK 的 payload 为：

```text
[2B protocol_version, big-endian][non-empty UTF-8 program name]
```

握手超时或消息类型错误会终止整条连接。版本号用于诊断，不参与兼容性分支。

## 帧格式

每条 WebSocket Binary 消息包含一个应用帧：

```text
[1B type][2B tunnel_id, big-endian][2B payload_len, big-endian][payload]
```

消息长度必须严格等于 `5 + payload_len`。

| type | 名称 | 方向 | payload |
|---:|---|---|---|
| `0x00` | HELLO | towc → tows | 协议版本和程序名 |
| `0x01` | OPEN | towc → tows | UTF-8 目标地址 |
| `0x02` | DATA | 双向 | TCP 数据 |
| `0x03` | CLOSE | 双向 | 空 |
| `0x04` | 保留 | — | 不得发送 |
| `0x05` | OPEN_OK | tows → towc | 空 |
| `0x06` | OPEN_FAIL | tows → towc | UTF-8 错误原因 |
| `0x07` | HELLO_ACK | tows → towc | 协议版本和程序名 |
| `0x08` | EOF | 双向 | 空 |

连接级帧使用 `tunnel_id = 0`。TCP 流使用 `1..=65534`，`65535` 保留。HELLO/HELLO_ACK payload 不超过 128 字节，OPEN 不超过 255 字节，OPEN_FAIL 不超过 256 字节，DATA 不超过 65,535 字节。

## TCP 流

客户端为每条本地 TCP 连接分配 tunnel_id 并发送 OPEN。目标地址支持 `host:port`、`[IPv6]:port` 或纯端口；纯端口表示 `127.0.0.1:port`。

服务端在 10 秒内连接目标：

- 成功：发送 OPEN_OK，随后双方可以交换 DATA；
- 失败：发送 OPEN_FAIL 并释放 tunnel_id；
- 客户端等待 OPEN 最多 15 秒。

读到本地 TCP EOF 时发送 EOF。收到 EOF 后执行 TCP 写方向 shutdown，但继续读取反方向数据。双方 EOF 完成且排队数据写完后发送 CLOSE；TCP 错误则立即发送 CLOSE。WebSocket 断开时释放其全部 TCP 流。

GUI 热禁用一条隧道时，会停止该规则的本地监听并向其所有活动 tunnel_id 发送 CLOSE。本地主动关闭的 ID 在收到对端终止帧前不会复用；已在途的 OPEN_OK、DATA 或 EOF 会被丢弃，避免单条隧道的关闭竞态升级为连接级协议错误。

## 心跳与背压

客户端在每条 WebSocket 上按配置间隔独立发送标准 WebSocket Ping，并要求在下一次 Ping 前收到 WebSocket Pong；超时、WebSocket close 或读写错误都会判定连接断开。同一客户端可以同时连接多个 tows，每条连接的握手、心跳、流编号空间、背压和故障范围互相独立。

WebVPN Cookie 刷新不属于本协议。客户端会话共享一个 Cookie，并每 10 分钟执行一次刷新；单条 WebSocket 断开只终止它承载的隧道，Cookie 刷新失败则终止使用该会话的全部连接。

- 每条 WebSocket 最多 64 条正在打开或已打开的 TCP 流；
- 单个服务端进程最多接受 128 条 WebSocket 连接，并在所有连接间合计打开或正在打开 1,024 条目标 TCP 流；
- 每流发送队列最多 16 个 DATA 帧；
- 每条 WebSocket 排队 payload 总量最多 4 MiB；
- 队列满时暂停对应 TCP 读取；
- 单一 WebSocket 写任务轮转各流，避免大流量独占；
- 所有 TCP 流启用 `TCP_NODELAY`。

## 协议错误

下列情况使用 WebSocket close code `1002` 关闭整条连接：

- 非 Binary 应用消息；
- 帧长度、类型、方向、payload 或 UTF-8 非法；
- 握手顺序错误或重复 HELLO；
- tunnel_id 非法、重复或指向不存在的流；
- DATA/EOF 不符合当前流状态。

OPEN_FAIL 和普通 TCP 失败只影响对应 TCP 流。
