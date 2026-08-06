# towc ↔ tows 多路复用协议 v2

本文档描述 `tcp_over_websocket` v0.5.1 使用的唯一隧道协议。旧版 `/tcp/{target}` 路径解析和文本心跳均已废弃，不提供兼容或降级。

## 连接和握手

客户端通过 SZUT WebVPN 建立：

```text
wss://webvpn.szut.edu.cn/ws-{tows端口}/{WebVPN编码的tows主机}/
```

URL 路径固定为 `/`，目标地址只出现在 OPEN 帧内。

建连后的第一条 WebSocket 消息必须是 Binary HELLO。服务端最多等待 5 秒，然后回 Binary HELLO_ACK。服务端即使发现协议版本不同也会回自己的 HELLO_ACK，由客户端比较后退出。

- 收到旧服务端的“连接成功”文本、HELLO_ACK 超时或协议版本不同：客户端提示升级 tows 并退出；
- 服务端首帧不是 HELLO 或等待超过 5 秒：拒绝并关闭连接；
- 程序版本字符串只用于诊断，不参与兼容判断；兼容性只由协议版本决定。

当前 `PROTOCOL_VERSION = 2`。

## 帧格式

每个 WebSocket Binary 消息恰好包含一个应用帧：

```text
[1B type][2B tunnel_id，大端][2B payload_len，大端][payload]
```

接收端必须验证消息总长度严格等于 `5 + payload_len`。

| type | 名称 | 方向 | payload |
|---:|---|---|---|
| `0x00` | HELLO | towc → tows | 2B 大端协议版本 + 非空 UTF-8 程序版本 |
| `0x01` | OPEN | towc → tows | UTF-8 目标地址 |
| `0x02` | DATA | 双向 | 原始 TCP 数据 |
| `0x03` | CLOSE | 双向 | 空 |
| `0x04` | PING | towc → tows | 空；tows 忽略且不回复 |
| `0x05` | OPEN_OK | tows → towc | 空 |
| `0x06` | OPEN_FAIL | tows → towc | UTF-8 错误原因 |
| `0x07` | HELLO_ACK | tows → towc | 2B 大端协议版本 + 非空 UTF-8 程序版本 |
| `0x08` | EOF | 双向 | 空 |

HELLO、HELLO_ACK 和 PING 的 `tunnel_id` 必须为 `0x0000`。流级帧只能使用 `0x0001..=0xFFFE`；`0xFFFF` 保留。

其他约束：

- HELLO/HELLO_ACK 总 payload 长度为 3..=128 字节；
- OPEN 为 1..=255 字节、合法 UTF-8，并按统一地址语法解析；
- OPEN_FAIL 最多 256 字节且必须是合法 UTF-8，不得包含调用栈或凭据；
- DATA 最多 65,535 字节；更大的 TCP 数据必须拆成多帧；
- OPEN_OK、CLOSE、PING、EOF 的 payload 必须为空。

## tunnel_id 和 OPEN

tunnel_id 由客户端分配。同一连接中，它不能与正在打开、已打开或正在关闭的流重复；完全关闭后才可复用。

客户端发送 OPEN 后，在收到对应 OPEN_OK 前不得读取本地 TCP 并发送 DATA。OPEN 等待最多 15 秒；超时或 OPEN_FAIL 后关闭本地 TCP 并释放 id。

服务端收到 OPEN 后：

1. 校验 id 未占用、并发数未超限和目标地址合法；
2. 最多用 10 秒连接目标 TCP；
3. 成功后建立 `tunnel_id → Tunnel` 映射并发 OPEN_OK；
4. 失败时发 OPEN_FAIL，不保留映射，也不影响其他流。

目标语法：`host:port`、`[IPv6]:port`，或纯 `port`（等价于 `127.0.0.1:port`）。端口范围为 `1..=65535`，裸 IPv6 被拒绝。

## 数据、EOF 和关闭

OPEN_OK 后双方可发送 DATA。每条流维护：

- `local_eof_sent`：本地 TCP reader 已读到 EOF，并已发送一次 EOF；
- `remote_eof_seen`：已收到对端 EOF；
- TCP 写任务是否已把 EOF 前排队的数据写完并执行 `shutdown(Write)`。

本地 reader 得到 EOF 时只发送 EOF，不发送 CLOSE。收到 EOF 时，对本地 TCP 执行 `shutdown(Write)`，但继续保留读方向。这样请求端半关闭后，仍能收到完整响应，SSH 等协议不会因过早 CLOSE 丢数据。

双方 EOF 均完成、且 EOF 前的数据已写完后，发送一次 CLOSE 并释放流。收到 CLOSE 时立即中止该流、关闭 TCP 并释放资源。对已关闭 id 的重复 CLOSE 作为幂等终止忽略。

非 EOF 的 TCP 读写错误会发送一次 CLOSE，只终止对应流。WS 断开或整条连接发生协议错误时，释放该 WS 下全部流。

## 协议错误

以下情况用 WebSocket close code `1002` 关闭整条 WS：

- WebSocket Text 应用消息；
- 帧总长度与 payload_len 不一致；
- 未知 type、方向错误的 type 或控制帧带有非法 payload；
- 重复 HELLO，或握手阶段出现其他应用帧；
- 非法、重复占用的 tunnel_id；
- DATA/EOF 指向未知或不允许的流状态；
- 非法 UTF-8、超出字段长度限制。

普通 TCP/WS 断开不是协议错误，按连接失败处理。

## 心跳、限额和背压

客户端每 60 秒在整条 WS 上发送一个 PING 应用帧。所有流共享该心跳；tows 不回复。WebSocket 层的 TCP 断开、close 帧或读写错误负责断线判定，PING 不承担探活应答。

v0.5.1 的基线限额：

- 每条 WS 最多 64 条正在打开或已打开的隧道；
- 每流 WS 发送队列最多 16 个 65,535 字节数据帧，约 1 MiB；
- 每条 WS 所有流的排队 payload 总量最多 16 MiB；
- TCP → WS 使用有界队列，队列满时暂停该流的 TCP 读取；
- 唯一 WS 写任务轮转各非空流，每轮至多取一个帧，避免大流量长期饿死 SSH 等小包流；
- 所有 TCP 流启用 `TCP_NODELAY`。

Cookie 保活不属于隧道帧协议。客户端全局每 10 分钟请求一次 WebVPN Cookie 刷新接口；失败时关闭 WS 和全部本地监听，且不自动重新登录。

## 兼容性

协议 v1 指旧的路径解析实现，没有 HELLO/HELLO_ACK。v2 客户端和服务端只运行本文协议：

- 不解析 `/tcp/...` 路径；
- 不把“连接成功”文本当心跳或就绪消息；
- 不降级、不重试旧协议；
- v0.5.0 及更早的 tows 必须先升级。
