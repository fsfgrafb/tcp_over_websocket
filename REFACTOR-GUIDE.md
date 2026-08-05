# tcp_over_websocket 重构指引（给接手的 AI）

> **本文档是重构的唯一输入依据**。仓库当前已清空（历史在 git 中）。请先阅读本文档，再决定如何从最简形态重构。

## 0. 仓库状态说明

- 本仓库所有旧代码**已删除并 commit**，全部历史版本保存在 git 中：
  - `git log --oneline` 可查看：`v0.1.0` → `v0.3.x` → `v0.4.0` → `acca315`（新版快照）→ `da69f60`（清空）
  - 查看任意版本：`git show <commit>:<path>`
  - 旧版结构参考：`v0.3.0`（src/bin/towc.rs + src/bin/tows.rs + src/lib.rs）、`v0.4.0`（+保活）、`acca315`（新版拆分为 src/client/ + src/server/ + src/network.rs）
- 本文件（REFACTOR-GUIDE.md）是重构的起点，请先完整阅读。

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

三个可执行文件（本仓库产出）：
- **towc**（控制台，跨平台）：跑在用户自己电脑（外网）。负责 WebVPN 登录、本地监听端口、把每个本地 TCP 连接封装成一条 WebSocket 转发。
  - **带参模式**：`towc <tows-ip[:port]> [--target <host:port|port>] [--listen <host:port|port>]`
  - **无参交互式登录模式**：`towc`（不带参数）进入交互，依次询问 tows 地址/端口、目标端口、本地监听端口；可缓存上次输入作默认值。**交互模式后续再扩充，初版先保证可用**
- **tows**（控制台，跨平台）：跑在能访问目标服务的内网机器上。监听固定端口（**可选启动参数** `tows [port]`，默认 4489）、接受 WebSocket、从路径解析目标、连接目标 TCP、双向转发。**行为与旧版 tows 一致**。
- **towc_gui**（Windows 专属 GUI）：towc 的图形界面版（Windows 专属；本轮重构可先不做，或预留接口/目录，后续补充）

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
- 例：10.18.47.77 上 tows 监听 4489，则 `/ws-4489/{编码77}/tcp/22` 可连（tows 再把 /tcp/22 解析为 127.0.0.1:22）

### 2.5 会话/ticket（重要）
- 隧道会话 Cookie：`wengine_vpn_ticketwebvpn_szut_edu_cn`（值形如 `wrdvpn1-{32hex}`）
- 访问目标**只需这一个 cookie**（其余 show_vpn/heartbeat 等是 UI 开关，非必需）
- **ticket 绑定来源 IP**：IP 变化 → 网关强制登出（`302 /login?logoutByIpChange=true`），需重新登录
- **活动续期制**：定期访问受保护资源可长期存活；完全静止约 15 分钟（880~920s）过期
- 心跳文本 `"连接成功"` 会被网关回显（维持连接）
- 保活接口：`GET /wengine-vpn/cookie?method=get&host=cas.szut.edu.cn&scheme=https&path=/personal-center&vpn_timestamp={ms}` → 200

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
  返回: "success"=已发送 / "valid"=需图形验证码(边界情况) / "unbind"=手机号/邮箱未绑定(不发码) / 其他=错误

RSA 加密（提交前）：
  GET {CAS}/v2/getPubKey → {modulus, exponent}（公钥）
  验证码字符串 倒序 → RSA 加密（JS RSAUtils.encryptedString；Rust 版: 62字符/块 modpow，hex 空格拼接）
  → 作为表单 password 字段（登录页 #ppassword 输入框的值经 JS 加密后填入隐藏 #password）

提交（POST 表单到登录 URL 本身）：
  POST {CAS}/cas/login?service=...   字段:
    username   = 手机号/邮箱
    password   = RSA(倒序(验证码))
    rememberMe = true
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

## 3. towc↔tows 隧道协议（自定义，历史版本实现，可沿用或优化）

### 3.1 连接建立
1. towc 用 `wss://webvpn.szut.edu.cn/ws-{tows端口}/{编码(tows所在主机)}/{目标路径}` 建 WebSocket（请求头带 `Cookie: wengine_vpn_ticket...=...`）
2. tows 接受 WS，从**请求路径**解析目标：
   - `/tcp` → 默认 `127.0.0.1:22`
   - `/tcp/{port}` → `127.0.0.1:{port}`
   - `/tcp/{host}:{port}` → 指定 host
3. tows 连目标 TCP 成功后，发送文本 `"连接成功"`（TOWS_READY_MESSAGE）作为就绪确认
4. 双向转发开始

### 3.2 数据与控制帧
- **二进制帧** = TCP 数据（双向）
- **文本 `"连接成功"`** = 心跳（客户端每 60s 发；服务端回显；客户端收到就忽略）
- **文本 `"tows-tcp-eof"`** = 某一方向 TCP 到达 EOF（对端应 shutdown 另一方向）
- TCP 双向都 EOF → 发 Close

### 3.3 性能要点
- **TCP_NODELAY** 必须开启（避免 Nagle 延迟，SSH 交互关键）
- 单连接单转发（每本地连接 = 一条 WS + 一条目标 TCP，无共享池）
- 实测基线（目标内网 10.18.47.77，网关为中转）：
  - 隧道 RTT ≈ 2.05ms（直连 0.65ms，**网关开销约 1.4ms**）
  - 隧道吞吐 ≈ 6.10 MB/s（约 48Mbps，**网关上限**，非本机瓶颈）
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

### 新版（commit acca315，未发布）
- 拆分 `src/network.rs`（URL/握手/转发）+ `src/client/`（登录/会话）+ `src/server/`（tows）
- 心跳已修正为 `"连接成功"`；**删除了保活**（README 声明不再后台续期）——设计决策，可重新评估
- 新增 `cookie_keepalive_test` 实验程序（HTTP-only 保活实验）
- 增加了 EOF 控制帧 `tows-tcp-eof`

### 可沿用/可放弃
- 保留：WS URL 构建、AES 编码、心跳协议、EOF 处理、微信扫码登录（直连方式）
- 评估：保活机制（keepalive WS + cookie 刷新）是否恢复；ticket 绑定 IP 意味着保活重点是"IP 稳定 + 活动"，而非盲目刷新

---

## 5. 重构要求（用户指定）

1. **从最简形态开始**：先做出能用的最小闭环（登录 + 单隧道 + 转发），再逐步加功能；**初版优先**，交互模式等可后续扩充
2. **优先最低延迟 + 最高转发效率**：核心指标，任何功能不能损害转发性能
3. **三个可执行文件**：tows（可选启动参数，行为同旧版）+ towc（**带参模式 + 无参交互式登录模式**）+ towc_gui（Windows 专属 GUI，可后续）
4. **登录三种方式**：微信扫码 / 手机验证码 / 邮箱验证码（见 §2.6/2.7）
5. 保持"学生在外网连内网 TCP"的目标场景（SSH / MC / RDP）
6. 代码语言：Rust（原项目为 Rust，edition 2024，依赖 tokio/tokio-tungstenite/reqwest/rustls）
7. 完成后需与 10.18.47.77 上的环境配合实测（见 §6）

---

## 6. 测试环境（可用）

- **内网目标机**：`10.18.47.77`（Orange Pi 5 Plus，aarch64 Debian，SSH root / 密码 fj.10.23）
  - 已运行：tows（systemd tows.service，当前监听 4489，v0.5.0）、MC 服务器（Docker MCSManager，TCP 25565，Leaves 1.21.8）、sshd（22）
  - ⚠️ **不要改动/干扰 MC 服务器**（25565），只可做只读探测
- 用户本机（Windows）当前能直达 10.18.47.77（同内网）；**外网场景**需通过 WebVPN 隧道
- 验证方法：本机运行 towc（或等价的 WS 隧道客户端），经 `/ws-{port}/{编码77}/tcp/22` 连 SSH / `/tcp/25565` 连 MC，确认端到端可用
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
- **测试脚本**（`C:\Development\test\`）：`verify_webvpn_encode.py`（编码/解码）、`minimal_login_test.py`（最简登录）、`tunnel_client.py`（Python 版隧道客户端参考）、`ws_probe.py`（端点探测）、`throughput_test.py`/`latency_test.py`（性能）
- WebVPN 前端源码：`C:\Development\test\wengine_main.js`（网瑞达网关 JS，可分析但混淆严重）

---

## 8. 用户后续可能补充

- **已补充（2026-08-05）**：
  - 三个可执行文件：tows / towc / towc_gui（GUI 为 Windows 专属，本轮可后续）
  - tows 与旧版行为一致，可选启动参数；towc 保留带参版 + 无参交互版（交互后续扩充，初版先写好）
  - 登录支持：微信扫码 / 手机验证码 / 邮箱验证码
- 用户表示还会提供更多信息，收到后请更新本文档
- 若有疑问，优先查阅 `C:\Development\test\docs\` 的详细记录
