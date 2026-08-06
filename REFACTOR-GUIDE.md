# tcp_over_websocket 重构指引（给接手的 AI）

> **本文档是重构的唯一输入依据**。仓库当前已清空（历史在 git 中）。**请先阅读本文档**，再决定如何从最简形态重构。

## 0. 仓库状态说明

- 本仓库所有旧代码**已删除并 commit**，全部历史版本保存在 git 中：
  - `git log --oneline` 可查看：`v0.1.0` → `v0.3.x` → `v0.4.0` → `acca315`（新版快照）→ `da69f60`（清空）
  - 查看任意版本：`git show <commit>:<path>`
  - 旧版结构参考：`v0.3.0`（src/bin/towc.rs + src/bin/tows.rs + src/lib.rs）、`v0.4.0`（+保活）、`acca315`（新版拆分为 src/client/ + src/server/ + src/network.rs）
- **当前仓库仅含本文档**（REFACTOR-GUIDE.md，无任何代码）；本文档是重构的唯一输入依据，请先完整阅读。

---

## 1. 项目目标（最重要）

**让 SZUT（苏州工学院）学生在公网环境下，通过学校 WebVPN 建立一条通向校园内网任意 TCP 服务的隧道。**

典型应用：
- 外网 **SSH** 连接内网设备（如 `ssh -p 14489 user@localhost`）
- 外网 **Minecraft（Java 版）联机**（内网 MC 服务器，端口 25565）
- 外网 RDP / 其他任意 TCP 服务

核心架构（已验证可行）：
```
外网应用 → towc(本机) → WebVPN WebSocket隧道 → tows(内网某机器,监听端口) → 目标TCP服务
```
多隧道（towc_gui 方案 2，单 WS 多路复用）：
```
外网应用1 ─┐
外网应用2 ─┤→ towc_gui(本机,单进程) → 1条WS(多路复用帧) → WebVPN → tows(内网) → 目标TCP服务1/2/3
外网应用3 ─┘
```

三个可执行文件（本仓库产出）：
- **towc**（控制台，跨平台）：跑在用户自己电脑（外网）。单进程单隧道（1 条 WS 连接）。负责 WebVPN 登录、本地监听端口、把本地 TCP 连接封装成 WebSocket 转发。
  - **带参模式**：`towc <tows-ip[:port]> [--target <host:port|port>] [--listen <host:port|port>] [--login <mobile|email>]`
    - `<tows-ip[:port]>`：唯一位置参数，放第一位；tows 端口默认 `4489`
    - `--target`：目标内网服务，默认 `127.0.0.1:22`
    - `--listen`：本地监听地址，默认 `127.0.0.1:14489`
    - `--login mobile|email`：验证码登录偏好，省略则回退微信扫码
    - 三个 flag **任意顺序**；README/帮助示例顺序固定为 `--target → --listen → --login`（逻辑流：远端目标 → 本地入口 → 认证方式）
  - **无参交互式登录模式**：`towc`（不带参数）进入交互，行为与 v0.4 一致（见 §1.1）
- **tows**（控制台，跨平台）：跑在能访问目标服务的内网机器上。监听固定端口（**可选启动参数** `tows [port]`，默认 4489）、接受 WebSocket、路由到目标 TCP、双向转发。
  - **必须具备包级路由能力（2026-08-05 用户要求）**：收到数据包后可根据**单个数据包**（帧头 tunnel_id）确定它属于哪个内网服务（ip+端口），从而实现客户端**并发建立维持多个隧道**
  - **只支持多路复用协议**（§3.1/3.2）：建连后等客户端 HELLO → 回 HELLO_ACK；收到非 HELLO 或超时 → **拒绝连接**（不兼容旧客户端，无降级/路径解析）
- **towc_gui**（Windows 专属 GUI）：**单进程管理多条隧道**（方案 2，2026-08-05 用户选定）。
  - **编译产物自包含，不依赖同目录 towc.exe 等**（完整 towc 逻辑内嵌）
  - 与 tows 之间仅 **1 条 WS 连接**，多路复用所有隧道 → 只需 **1 个 WS 心跳**；cookie 保活也由 GUI 统一（全局一个）
  - GUI 上配置的若干条隧道 → 进程内并发建立/维持，无子进程、无 IPC

### 1.1 towc 无参交互模式（与 v0.4 行为一致）
1. 读取 tows 地址 → 立即输出对应的 WebVPN location → 尝试缓存认证
2. 缓存有效 → 跳过登录方式；缓存缺失 / 格式无效 / 明确过期 → 询问登录方式（输入手机号/邮箱用验证码，或直接回车用终端微信扫码）
3. 认证完成（ticket 已激活）→ 询问目标地址和本地监听地址（新架构**无独立保活 WS**；参数确定后建立隧道并输出连接成功日志，失败则提示）
4. 新登录取得的 Cookie 立即写入本地缓存；网络或 TLS 故障无法证明 Cookie 已失效时，保留缓存并直接报告连接错误
5. 交互模式缓存本次实际采用的 tows / 目标 / 本地监听地址，下次启动作为新默认选项（回车即复用）
6. 配置缓存与 WebVPN Cookie 缓存**相互独立**

### 1.2 发布矩阵（win/linux，主力 win；2026-08-06 用户确认 3 个可执行文件）
**发布 3 个可执行文件**（逻辑程序 3 个，平台二进制共 7 个）：

| 程序 | 角色 | Windows x64 | Linux x64 | Linux aarch64 |
|------|------|:-----------:|:---------:|:-------------:|
| tows | 内网服务端（路由转发） | ✅ | ✅ | ✅ **必须**（内网 Orange Pi = aarch64）|
| towc | 控制台客户端（单隧道） | ✅ | ✅ | ✅ 顺手编译 |
| towc_gui | **主力** GUI 客户端（多隧道，自包含） | ✅ | — | — |

- **主力**：Windows 用户用 `towc_gui`（多隧道图形管理）；`towc.exe` 提供命令行/脚本场景
- **Linux**：用 `towc`（控制台）；`tows` 部署内网（10.18.47.77 = Orange Pi 5 Plus aarch64）
- 三程序共享同一版本号，一起发布（GitHub Release 打包 zip）
- 交叉编译目标：
  - `tows` / `towc`：`x86_64-pc-windows-msvc`、`x86_64-unknown-linux-gnu`、`aarch64-unknown-linux-gnu`（aarch64 为 tows 必需、towc 可选）
  - `towc_gui`：仅 `x86_64-pc-windows-msvc`
- **不采用 busybox 单二进制**（`tcpow server|client|gui`）：GUI 依赖会打进 Linux 版、体积增大、与 aarch64 交叉编译互相拖累

---

## 2. WebVPN 传输机制（全部为实测结论，2026-08-05 验证）

### 2.1 网关
- SZUT WebVPN = **网瑞达（wrdtech）WebVPN**（免客户端产品），入口 `https://webvpn.szut.edu.cn`
- 官方支持协议：HTTP/HTTPS/Telnet/SSH/RDP/VNC（浏览器内置 HTML5 客户端）；**底层是 HTTP 代理 + WebSocket 隧道**

### 2.2 两种传输通道（实测）
| 通道 | 格式 | 能力 |
|------|------|------|
| HTTP 代理 | `/{http\|https}/{编码主机}/路径` | 仅 HTTP/HTTPS 协议（实测：非 HTTP 字节返回 400） |
| **WS 隧道** | `wss://webvpn.szut.edu.cn/ws-{端口}/{编码主机}/{路径}` | **任意 TCP**（实测：SSH banner、MC 握手、数据流全通） |

- **任意 TCP 必须走 WS 隧道**；`/http/`、`/https/` 只是 HTTP 代理
- **UDP 不支持**（网关无 UDP 通道；学校 SSLVPN 服务端未开通）
- 复用协议下 WS URL 路径**可固定为 `/`**（目标在 OPEN 帧内指定，见 §3.1）

### 2.3 地址编码（已破解，Key/IV 固定）
```
编码 = "77726476706e69737468656265737421" + hex(AES-128-CFB-128bit(主机名))
Key = IV = "wrdvpnisthebest!"（16字节 ASCII）
```
- 例：`cas.szut.edu.cn` → `77726476706e69737468656265737421f3f652d2342a7d44300d8db9d6562d`
- 例：`10.18.47.77` → `77726476706e69737468656265737421a1a70fcd7f7e3c07305fde`
- `/{http|https}/` 前缀决定端口（http=80、https=443）；`/ws-{端口}/` 中端口为**目标端口**

### 2.4 WS 隧道端口机制（实测）
- `/ws-{端口}/{编码}/...` **任意端口可用**，但**目标端口必须运行 tows 类 WS 服务**：
  - 网关会先探测该端口能否完成 WebSocket 握手，能→转发，不能→302 `/wengine-vpn/failed`（CONNECTION_FAILED）
- 例：10.18.47.77 上 tows 监听 4489，客户端建 WS 到 `/ws-4489/{编码77}/`（路径可固定为 `/`），目标在 OPEN 帧内指定（见 §3.0/3.1）

### 2.5 会话/ticket（重要）
- 隧道会话 Cookie：`wengine_vpn_ticketwebvpn_szut_edu_cn`（值形如 `wrdvpn1-{32hex}`）
- 访问目标**只需这一个 cookie**（其余 show_vpn/heartbeat 等是 UI 开关，非必需）
- **ticket 绑定来源 IP**：IP 变化 → 网关强制登出（`302 /login?logoutByIpChange=true`），需重新登录
- **活动续期制**：定期访问受保护资源可长期存活；完全静止约 15 分钟（880~920s）过期
- 心跳文本 `"连接成功"` 会被网关回显（维持连接）——⚠️ 这是网关机制；**新协议心跳改用 PING 帧**（§3.1），`"连接成功"` 文本已退出新协议（仅用于旧版判别）
- 保活接口：`GET /wengine-vpn/cookie?method=get&host=cas.szut.edu.cn&scheme=https&path=/personal-center&vpn_timestamp={ms}` → 200

### 2.5.1 保活架构设计（2026-08-05 用户确认；参数可配置，⚠️ 默认值为临时设定）
**核心：多路复用单连接架构（方案 2，2026-08-05 选定）**
- **towc_gui = 1 个进程 = 1 条 WS 连接**，该连接**多路复用所有隧道**（帧头 tunnel_id 路由，见 §3.1），同时承载数据与 **60s 心跳** → 网关视角到 tows 的连接始终活跃；无独立保活 WS、无静默连接概念
- **所有隧道只需 1 个 WS 心跳**（1 条连接），心跳定时器由 GUI 单任务统一驱动
- **Cookie 刷新 = 全局一个，间隔默认 10 分钟（600s）**：GUI 内一个任务统一执行，所有隧道共享同一 ticket
- **towc（单隧道控制台）**：仍为 1 进程 = 1 条 WS = 1 隧道，心跳 60s；**与 towc_gui 统一走多路复用协议**（单隧道 = 1 个 tunnel_id，§3.1/3.2），与 towc_gui **共享同一套协议层**；**不兼容旧版 tows**（版本不符即报错退出，见 §3.0）
- **错误处理：不做 IP 变化检测、不自动重登**——IP 变化 / cookie 过期等一切失效都通过保活/连接反馈直接体现（302 `/login?logoutByIpChange=true`、WS 握手被拒、保活失败），检测到任一失败即**直接退出**并提示用户重新启动程序重新登录；不引入"检测-重登"状态机，保持简单可靠
- ✅ **带宽实测结论（2026-08-05 纯外网，多连接分段下载 200MB）**：WebVPN 是**账号级总带宽限制 ~5.3MB/s**，非按连接限速——1 连接 4.32 / 3 连接 5.21 / 10 连接 5.27 MB/s，多连接**不能线性叠加**（最多 +20% 后饱和）。→ **方案 2 单 WS 多路复用不会损失吞吐**（单连接本身即可跑满 ~5.3MB/s），此前"共享单连接带宽受限"的担忧**排除**

### 2.6 登录（最简流程，已实测）
```
1. 直达 CAS 登录页（0 跳，HTTP 200，无需先访问目标）
   https://webvpn.szut.edu.cn/https/{编码(cas.szut.edu.cn)}/cas/login?service={urlencode(https://webvpn.szut.edu.cn/login?cas_login=true)}
2. 激活指纹（不可跳过！ST 校验时网关强制）
   GET /set-fingerprint?fingerprint=5a0b00fe6ae8277a4bfadd4e103f6e1c   （硬编码 MD5 即可）
   → 302 /login → 302 → 回 CAS 登录页
3. 微信扫码（推荐直连，不用走代理）：
   - 二维码页: https://open.weixin.qq.com/connect/qrconnect?appid=wx16c67d169e7a9290&redirect_uri={urlencode(https://cas.szut.edu.cn/cas/login?client_name=WeiXinClient)}&response_type=code&scope=snsapi_login&state=...
   - 注意 redirect_uri 必须直连 CAS（微信注册域名），不能用 webvpn 代理 URL
   - 解析 uuid → 下载二维码 https://open.weixin.qq.com/connect/qrcode/{uuid}
   - 轮询 https://lp.open.weixin.qq.com/connect/l/qrconnect?uuid={uuid}&_={ms}
     状态码: 408未扫 / 404已扫待确认 / 403取消 / 402过期 / 405成功(code)
4. CAS 回调: https://cas.szut.edu.cn/cas/login?service=...&client_name=WeiXinClient&code={code}&state=...
   跳转: →302(发ST-票据)→ webvpn/login?cas_login=true&ticket=ST-... →302→ / →302→ 回CAS →200
   （#0 发 ST = 认证成功标志；#1 校验 ST 激活 ticket = 隧道会话建立）
5. 访问目标验证: /{http|https}/{编码}/ → 200
```
- 缓存复用：保存 ticket cookie，下次启动先用它访问 WebVPN 会话入口验证（未认证则重新登录）
- ⚠️ 微信二维码/轮询推荐**直连**（简单、不依赖 ticket）；代理方式（`/https/{编码(open.weixin.qq.com)}/...`）是官方浏览器方式，仅作兜底，且轮询域（lp.open.weixin.qq.com）与 open 域编码不同，注意区分

### 2.7 登录支持三种方式
统一前置（三种方式相同）：直达 CAS 登录页 → 激活指纹；统一后置：CAS 回调 → 激活 ticket（见 2.6 步骤 4~5）。

**方式一：微信扫码**（推荐）——完整流程见 2.6，流程最简、无需收验证码。

**方式二：手机验证码 / 方式三：邮箱验证码**（2026-08-05 浏览器实测【手机+邮箱均端到端登录成功】+ JS 逆向确认，逻辑相同）：
```
前置：GET {CAS}/cas/login?service=... → 提取表单隐藏字段 execution token
      （登录页还有 getPubKey 公钥，见下）

发送验证码：
  GET {CAS}/v2/services/sedsms?mobile={手机号}     （手机）
  GET {CAS}/v2/services/sendEmailYzm?email={邮箱}  （邮箱）
  返回: "success"=已发送 / "valid"=已有未过期验证码(不重发,直接复用) / "unbind"=未绑定(不发码) / 其他=错误

RSA 加密（提交前）：
  GET {CAS}/v2/getPubKey → {modulus, exponent}（公钥）
  验证码字符串 倒序 → RSA 加密（JS RSAUtils.encryptedString；Rust 版: 62字符/块 modpow，hex 空格拼接）
  → 作为表单 password 字段（登录页 #ppassword 输入框的值经 JS 加密后填入隐藏 #password）
  ⚠️ 必须先倒序再 RSA（漏倒序 → 服务端返回"验证码错误"；2026-08-05 脚本实测踩坑）

提交（POST 表单到登录 URL 本身）：
  POST {CAS}/cas/login?service=...   字段:
    username   = 手机号/邮箱
    password   = RSA(倒序(验证码))
    rememberMe = true   ← 可选，可省略（对照测试验证：不带也能成功，与历史版本一致）
    execution  = token
    _eventId   = submit
  需带 Origin: https://webvpn.szut.edu.cn 与 Referer 头
  若响应仍是 CAS 登录页（execution 变化）→ 换新 execution 重试一次（最多 2 次）
  成功 → 走 2.6 步骤 4~5（ST 票据 + 指纹激活 + 提取 ticket）
```
- 登录页三种登录方式 Tab：扫码 / 手机(#fm2) / 邮箱(#fm3)，另有密码登录(#fm4，多 authcode 图形验证码字段，本轮可不支持)
- 所有接口经 webvpn 代理：`{CAS} = https://webvpn.szut.edu.cn/https/{编码(cas.szut.edu.cn)}/cas`（注意路径含 `/cas/` 段）
- 验证码发送接口存在 60s 倒计时（服务端限频），脚本需处理"已发送/未绑定/限频"返回值
- 登录入口统一走 `https://webvpn.szut.edu.cn/https/{编码(cas.szut.edu.cn)}/cas/login`（webvpn 代理）

---

## 3. towc↔tows 隧道协议（自定义）

### 3.0 协议模式与版本协商（单模式，不兼容旧版）
- **只支持多路复用协议**（towc/towc_gui 统一使用）：1 条 WS 连接承载多个隧道（towc 单隧道 = 1 个 tunnel_id），帧头带 tunnel_id，tows 按包路由（见 §3.1/3.2）
- **版本协商（2026-08-06 用户确认）**：客户端建 WS 后**第一帧发 HELLO**（0x00，带协议版本+程序版本）→ tows 校验后回 HELLO_ACK（0x07）
- **客户端**：收到 HELLO_ACK 且协议版本一致 → 正常复用；收到 `"连接成功"` 文本（仅旧 tows 会发）/ **5s 超时** / 版本不一致 → **打印警告并退出**（提示升级 tows），**不降级、不重试**
- **tows**：收到 HELLO → 回 HELLO_ACK；收到**非 HELLO**（旧客户端）或 **5s 超时** → **关闭连接拒绝**（不兼容旧客户端，需升级）
- ⚠️ 判别可靠性：新版 tows **绝不发** `"连接成功"` 文本（就绪确认 = HELLO_ACK），因此客户端收到该文本即判定为旧版

### 3.1 多路复用帧格式（建议设计，可调整）
WS **二进制帧**：
```
[1B type][2B tunnel_id 大端][2B payload_len 大端][payload...]
```
| type | 含义 | payload |
|------|------|---------|
| 0x00 HELLO | towc→tows 版本协商（建连后第一帧必发） | `[2B 协议版本大端] + 程序版本字符串`（如 `2` + `towc 0.6.0`） |
| 0x01 OPEN | towc→tows 建隧道 | 目标地址 UTF-8（`host:port` 或 `port`=127.0.0.1） |
| 0x02 DATA | 双向隧道数据 | 原始 TCP 数据 |
| 0x03 CLOSE | 双向关隧道 | 空 |
| 0x04 PING | towc→tows 心跳 | 空（tows 可忽略或回 PING；任何帧都算网关活跃流量） |
| 0x05 OPEN_OK | tows→towc 建隧成功 | 空 |
| 0x06 OPEN_FAIL | tows→towc 建隧失败 | 错误原因 UTF-8 |
| 0x07 HELLO_ACK | tows→towc 版本协商应答 | `[2B 协议版本大端] + 程序版本字符串`（如 `2` + `tows 0.6.0`） |

- **协议版本常量：PROTOCOL_VERSION = 2**（v1 = 旧路径解析无握手）；HELLO 中协议版本不匹配 → 客户端提示并**退出**（不降级）
- **HELLO / HELLO_ACK 帧的 tunnel_id 固定填 0x0000**（两端忽略）；仅 OPEN/DATA/CLOSE 使用 0x0001~0xFFFE（动态分配；0xFFFF 保留）
- ⚠️ **payload_len 为 2B（上限 65535）**：DATA 帧单帧 ≤64KB，TCP 数据超过需发送端分片（按 ≤64KB 切块发多个 DATA 帧）
- **心跳**：每 60s 一条 PING（1 条连接一个心跳即可，所有隧道共享）
- **EOF 处理**：单隧道内 TCP EOF → 发 CLOSE（tunnel_id）并关闭该流；所有隧道关闭且无新连接 → 可关 WS

### 3.2 tows 路由（多路复用）
- 维护 `HashMap<tunnel_id, TcpStream>`
- OPEN：解析目标 → 连接 TCP → 成功回 OPEN_OK / 失败回 OPEN_FAIL（含原因）
- DATA：按 tunnel_id 查表转发
- CLOSE：关闭对应 TcpStream 并从表移除
- ⚠️ 背压：单连接多流需注意一个慢目标拖累整条连接；tows 每个流用独立 async 任务

### 3.3 路径解析模式（已废弃）
- 旧 v0.4/acca315 的 `/tcp/{host}:{port}` 路径解析**不再支持**（见 §4 历史），tows 只接受多路复用协议

### 3.4 性能要点
- **TCP_NODELAY** 必须开启（避免 Nagle 延迟，SSH 交互关键）
- 多路复用模式注意帧头开销（5B/帧，可忽略）与**队头阻塞（HOL）**：单连接上一个隧道大流量可能延迟其他隧道小包（交互式 SSH 在低流量场景无感）
- 实测基线（目标内网 10.18.47.77，网关为中转）：
  - 隧道 RTT ≈ 2.05ms（直连 0.65ms，**网关开销约 1.4ms**）
  - 内网单连接吞吐 ≈ 6.10 MB/s；**外网单连接 ≈ 4.3 MB/s**
  - **账号级总带宽 ≈ 5.3 MB/s**（外网实测：1 连接 4.32 / 3 连接 5.21 / 10 连接 5.27，多连接不线性叠加，见 `docs/2026-08-05-多连接并发下载测试记录.md`）
- 重构目标：**最低延迟、最高转发效率**——注意减少每帧开销、缓冲大小、心跳频率权衡（心跳每 60s 开销可忽略）

---

## 4. 历史实现要点（可从 git 参考）

### v0.4.0（保活完整版，最接近生产）
- 结构：`src/bin/towc.rs`（客户端+登录）、`src/bin/tows.rs`、`src/lib.rs`（网络层）
- 双后台保活：
  - keepalive WS：`/ws-{端口}/{编码}/webvpn-keepalive` 路径，每 210s 心跳，断线 5s 重连
  - cookie 刷新：每 180s 调 `/wengine-vpn/cookie?method=get&...`
- 登录：微信扫码 + 手机/邮箱验证码（RSA，`v2/getPubKey`）
- ⚠️ v0.4.0 心跳消息是乱码 `"杩炴帴鎴愬姛"`（应为 "连接成功"，UTF-8 被误读 GBK）——**bug，勿沿用**
- v0.4.0 中 `"连接成功"` 仅作心跳：**towc 主动发、tows 回显**（`WebVpnHeartbeatRole::Server.echoes_heartbeat()`），tows 不主动发送

### 新版（commit acca315，未发布）
- 拆分 `src/network.rs`（URL/握手/转发）+ `src/client/`（登录/会话）+ `src/server/`（tows）
- 心跳已修正为 `"连接成功"`；**删除了保活**（README 声明不再后台续期）——设计决策，可重新评估
- 新增 `cookie_keepalive_test` 实验程序（HTTP-only 保活实验）
- 增加了 EOF 控制帧 `tows-tcp-eof`
- **`"连接成功"` 兼作就绪确认**：`TOWS_READY_MESSAGE = WEBVPN_HEARTBEAT_MESSAGE`（network.rs:62），tows 连目标成功后**主动发送**（server/runtime.rs:59），客户端收到后开始转发

### 可沿用/可放弃
- 保留：WS URL 构建、AES 编码、心跳/EOF 概念（新协议用 PING/CLOSE 帧实现）、微信扫码登录（直连方式）
- 保活已决策（见 §2.5）：数据+心跳合体（60s）+ 全局 cookie 刷新（10min）+ 失败即退出；旧版"独立 keepalive WS"方案不采用

### 验证码登录 vs 历史版本（2026-08-05 实测比对）
- **v0.4.0** 有验证码登录（`login_with_verification_code`）：接口/execution/RSA倒序/重试均与实测一致 ✅
- **新版 acca315** 已**删除**验证码登录（仅保留微信扫码）→ 重构需重新实现（全流程见 §2.7）
- 指纹激活时机：v0.4 在提交后按需激活；实测脚本在提交前激活——均可，建议提交前（与最简流程一致）
- ⚠️ 历史结论修正：曾疑 v0.4 缺 `rememberMe` 字段是手机登录 bug，后对照测试证明 **rememberMe 可省略**（当时失败为会话污染误判）→ v0.4 无此 bug

---

## 5. 重构要求（用户指定）

1. **从最简形态开始**：先做出能用的最小闭环（登录 + 单隧道 + 转发），再逐步加功能；**初版优先**
2. **优先最低延迟 + 最高转发效率**：核心指标，任何功能不能损害转发性能
3. **三个可执行文件**：tows（可选启动参数，**只支持多路复用协议**）+ towc（**带参模式 + 无参交互模式**，交互行为 v0.4 兼容）+ towc_gui（**Windows 专属 GUI，单进程多隧道，自包含不依赖 towc.exe**）
4. **登录三种方式**：微信扫码 / 手机验证码 / 邮箱验证码（见 §2.6/2.7）
5. **多路复用协议 + 版本协商**：towc/towc_gui 与 tows 之间单 WS 多路复用（见 §3.1/3.2），**建连第一帧发 HELLO 握手**（0x00/0x07，§3.0），**不兼容旧版**（版本不符即报错退出），帧格式以简洁为准
6. 保持"学生在外网连内网 TCP"的目标场景（SSH / MC / RDP）
7. 代码语言：Rust（原项目为 Rust，edition 2024，依赖 tokio/tokio-tungstenite/reqwest/rustls）
8. 完成后需与 10.18.47.77 上的环境配合实测（见 §6）；带宽基线：账号总带宽 ~5.3MB/s（已实测，见 §2.5/§3.4）

---

## 6. 测试环境（可用）

- **内网目标机**：`10.18.47.77`（Orange Pi 5 Plus，aarch64 Debian，SSH root / 密码 fj.10.23）
  - 已运行：tows（systemd tows.service，当前监听 4489，v0.5.0——**内网部署版，与仓库 git 历史独立**）、MC 服务器（Docker MCSManager，TCP 25565，Leaves 1.21.8）、sshd（22）
  - ⚠️ **不要改动/干扰 MC 服务器**（25565），只可做只读探测
- 用户本机（Windows）当前能直达 10.18.47.77（同内网）；**外网场景**需通过 WebVPN 隧道
- 验证方法：本机运行新版 towc / towc_gui，经 `/ws-4489/{编码77}/` 建 WS → HELLO 握手 → OPEN 帧指定目标（22 / 25565），确认端到端可用（见 §3）
- ⚠️ **实测前必须先升级内网 tows 到新版**：内网现有 v0.5.0 仅支持旧路径解析，新版客户端连它会因版本协商失败而退出
- WebVPN 登录需要**用户微信扫码**配合（二维码图片给用户扫）

---

## 7. 参考资料

- **研究记录**（`C:\Development\test\docs\`，非常详细，含全部实测日志）：
  - `2026-08-05-WebVPN正确应用方式研究.md`（协议清单、端口机制、SSLVPN 结论）
  - `2026-08-05-WebVPN隧道端到端验证记录.md`（WS 隧道验证）
  - `2026-08-05-WebVPN传输能力研究报告.md`（吞吐/RTT 实测）
  - `2026-08-05-最简登录流程详解.md`（登录全流程）
  - `2026-08-05-tcp_over_websocket版本研究记录.md`（历史版本分析）
  - `2026-08-05-记忆恢复与项目状态记录.md`（全项目背景）
  - `2026-08-05-多连接并发下载测试记录.md`（带宽实测：账号级限速 ~5.3MB/s）
- **测试脚本**（`C:\Development\test\`）：`verify_webvpn_encode.py`（编码/解码）、`minimal_login_test.py`（最简登录）、`tunnel_client.py`（Python 版隧道客户端参考）、`ws_probe.py`（端点探测）、`throughput_test.py`/`latency_test.py`（性能）、`multi_conn_download_test.py`（多连接并发分段下载测试）
- WebVPN 前端源码：`C:\Development\test\wengine_main.js`（网瑞达网关 JS，可分析但混淆严重）

---

## 8. 用户后续可能补充

- **已补充（2026-08-05）**：
  - 三个可执行文件：tows / towc / towc_gui（GUI 为 Windows 专属）
  - 登录支持：微信扫码 / 手机验证码 / 邮箱验证码
  - **架构：方案 2 单进程多隧道**——towc_gui 自包含、单 WS 多路复用、1 心跳、cookie 全局（§1/§2.5/§3）
  - **tows 包级路由能力**（只支持多路复用协议，§1/§3）
  - **towc 参数规格**（flag 任意顺序、示例顺序、无参交互 v0.4 一致，§1/§1.1）
  - 保活/错误处理（1 心跳 + cookie 10min + 失败即退出，§2.5）
  - ✅ 带宽实测（账号级限速 ~5.3MB/s，§2.5/§3.4）
- **已补充（2026-08-06）**：
  - 发布矩阵（§1.2）、命名保持 towc/tows/towc_gui、towc 保持单隧道
  - 版本协商 + 不兼容旧版（HELLO/HELLO_ACK，§3.0/§3.1）
- 用户表示还会提供更多信息，收到后请更新本文档
- 若有疑问，优先查阅 `C:\Development\test\docs\` 的详细记录
