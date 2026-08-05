# 工作原理

这个项目只解决一件事：把本机 TCP 连接通过 SZUT WebVPN 转发到远端机器可访问的 TCP 服务。

~~~text
本地应用 → towc → WebVPN WebSocket → tows → 目标 TCP 服务
~~~

## 建立连接

1. 在目标机器运行 tows。它监听 WebSocket 请求，并从请求路径取得目标地址。
2. 在本机运行 towc。它先验证缓存的 WebVPN Cookie；缓存不存在或已过期时显示微信二维码供用户登录。
3. towc 在本机开放监听端口。每有一个本地 TCP 连接，就建立一条新的、带 Cookie 的 WebSocket。
4. tows 连接目标 TCP 服务。连接成功后向 towc 发送确认，再开始双向转发数据。

每个本地 TCP 连接对应一条 WebSocket 和一条目标 TCP 连接。这样没有共享数据通道和连接池，出错范围小，也不需要额外的调度逻辑。

## 登录

项目只保留微信二维码登录：

1. 客户端打开 WebVPN 登录入口，必要时完成指纹激活。
2. 客户端请求并在终端渲染微信二维码。
3. 微信确认后，客户端轮询得到授权 code，并完成 CAS 回调。
4. 从 Cookie Jar 取出 WebVPN ticket Cookie，写入本地缓存。

Cookie 缓存可减少重复扫码。复用前，客户端只带着缓存 Cookie 连续两次访问 WebVPN 自身的受保护入口。HTTP 客户端会自动跟随 WebVPN 的跳转；只有两次请求都未落到 WebVPN 登录/指纹页、且 Cookie Jar 中仍有 ticket，才使用跳转后的完整 Cookie 连接 WebSocket。内层 CAS 信息门户不作为此项验证依据，因为 CAS 会话和 WebVPN ticket 是不同状态，刚扫码后 CAS 页面仍可能显示登录表单。第一次请求中的 Cookie 更新也不能单独说明登录有效：已观察到过期 Cookie 也可能被 WebVPN 接口或跳转改写。因此，只有第二次 WebVPN 会话检查通过后才接受并保存更新值；任一次未认证都会改为扫码登录。扫码登录完成后也会先检查 WebVPN 会话，失败不会写入缓存或报告成功。不会额外访问个人中心页面。

交互模式还会保存上次的 tows、目标和本地监听地址到 interactive.defaults；下次启动时这些值会显示为默认值。Cookie 和交互配置均是本地数据：Windows 位于 %APPDATA%\tcp_over_websocket，Linux/macOS 位于 $XDG_CACHE_HOME/tcp_over_websocket 或 ~/.cache/tcp_over_websocket。

程序不再进行后台 Cookie 续期、独立会话保活或自动重新登录。Cookie 失效时，重启 towc 并重新扫码即可。这降低了后台请求和状态管理复杂度。

## 保活和性能

数据连接启用 TCP_NODELAY，避免 SSH 等交互式小数据包被 Nagle 算法延迟合并。

每条实际数据 WebSocket 在建立后立即发送一次短心跳，之后每 60 秒发送一次；tows 只负责回显。该心跳仅在有实际本地连接时存在，不会维持独立的空闲 WebSocket。

客户端启动后立即监听本地端口，不再预先探测目标服务。这少了一次 WebSocket 往返；首次应用连接会直接完成 WebSocket 握手、目标 TCP 连接和数据转发。

## 简化后的源码结构

~~~text
src/lib.rs                 模块入口
src/network.rs             WebVPN 地址、握手、心跳和双向转发
src/client/runtime.rs      参数、Cookie、微信扫码和本地监听
src/client/qr.rs           终端二维码渲染
src/server/runtime.rs      WebSocket 监听和目标 TCP 连接
src/bin/towc.rs            客户端命令入口
src/bin/tows.rs            服务端命令入口
~~~
