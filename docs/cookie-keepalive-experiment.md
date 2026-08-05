# Cookie 保活实验

本文记录已知现象、尚未证实的推测，以及用于验证的独立测试程序。

## 已知现象

登录信息门户后，浏览器开发者工具可以看到类似下面的 WebSocket：

~~~text
wss://webvpn.szut.edu.cn/wss/<加密站点标识>/websocket/<学号>
~~~

路径中的学号与当前登录用户对应。浏览器会周期性发送文本消息“连接成功”，服务器会回显同一消息。这说明该连接具有应用层心跳。

目前只能确认它可以帮助维持这条 WebSocket 及其 WebVPN 转发映射。它是否能延长 WebVPN ticket Cookie 的服务端有效期尚未证实：

- 心跳响应不会直接返回新的 Cookie；
- ticket 可能是固定过期时间，也可能是活动续期；
- 只有对照实验才能区分这两种情况。

因此，项目不把这个 WSS 当作 Cookie 有效性的依据。

## HTTP-only 对照实验

程序 cookie_keepalive_test 不打开任何 WebSocket，也不读取或连接 tows。它的行为是：

1. 复用已缓存的 Cookie；缓存不存在或验证失败时显示微信二维码登录。
2. 每 3 分钟请求 WebVPN 的 Cookie 刷新接口。
3. 立即访问一次 WebVPN 自身的受保护入口作为基线；后续探测按指数退避进行：1 分钟、2 分钟、4 分钟、8 分钟……最长 24 小时一次。
4. 为每次刷新和 WebVPN 会话探测记录请求状态码、重定向链、耗时、最终 URL、响应大小及 Cookie 名称和 ticket 是否存在。Cookie 值和 URL 中的一次性 ticket、code、UUID、token 都会脱敏；会话探测认证通过时，会额外打印响应网页正文。
5. 基线或后续 WebVPN 会话探测落到 WebVPN 登录/指纹页、ticket 缺失或其他未认证状态时，输出结果并结束。刷新接口的结果不会单独决定登录是否有效。

运行方式：

~~~bash
cargo run --release --bin cookie_keepalive_test
~~~

也可以直接运行构建后的 cookie_keepalive_test 可执行文件。按 Ctrl+C 可安全结束实验。

WebVPN 会话检查并非只看 Cookie 文件是否存在：程序会访问 WebVPN 根入口，检查请求没有落到 WebVPN 登录/指纹页，并确认 Cookie Jar 中仍有 WebVPN ticket。它不再用内层 CAS 信息门户作为依据：CAS 会话与 WebVPN ticket 是不同的状态，刚扫码后 CAS 页面仍可能显示登录表单。网络请求出错会单独输出错误，不会被误判为登录过期。反过来，Cookie 刷新接口返回 HTTP 200、ticket 仍存在，甚至接口或 WebVPN 跳转改写了 Cookie，都只说明 HTTP 请求和 Cookie Jar 发生了变化，**不能证明 WebVPN 登录仍可用**。已经观察到过期的缓存 Cookie 也会被刷新接口更新，但该情况的 WebVPN 会话探测仍会回到登录流程。

因此 Cookie 刷新产生的新值只在实验进程的 Cookie Jar 中暂存；只有同一个 Cookie Jar 的 WebVPN 会话探测认证通过后，才会写回本地缓存。若基线会话探测未认证，实验会立刻结束，不会继续刷新或覆盖缓存。常规客户端复用缓存时也会连续完成两次 WebVPN 会话检查；第一次请求中的 Cookie 改写必须在第二次请求中仍通过认证，才会接受并保存。扫码流程也会执行同一检查，未通过就不会报告登录成功或写入缓存。

## 如何记录结果

建议记录开始时间、每次 Cookie 刷新结果、每次门户检查结果和最终失效时间。完成至少一次不开 WSS 的实验后，再使用相同登录方式和相近时间段运行带 WSS 心跳的对照组，才可以判断该 WSS 是否对登录态寿命有影响。

请只使用自己的账户，不要记录、共享或提交 Cookie 文件。
