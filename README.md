# tcp_over_websocket

供苏州工学院学生使用的 TCP 转发工具。它可以借助 SZUT WebVPN，把本机的一个端口转发到另一台机器上的服务；最常见的用途是通过 SSH 访问该机器。

本项目包含两个程序：

- tows：运行在目标服务所在的机器上。
- towc：运行在自己的电脑上，负责登录 WebVPN 和建立本地端口。

详细的通信原理见 [工作原理](docs/working-principles.md)。

需要测量 WebVPN Cookie 是否会在纯 HTTP 刷新下保持有效时，见 [Cookie 保活实验](docs/cookie-keepalive-experiment.md)。

## 使用前准备

1. 准备两台机器：自己的电脑运行 towc，能够访问目标服务的机器运行 tows。
2. 确保 tows 的监听端口可被 WebVPN 访问；默认端口为 4489，必要时在服务器防火墙中放行。
3. 下载与系统对应的 tows、towc 可执行文件，或按下文从源码构建。两端请使用相同版本。

## 快速启动

### 1. 在目标机器启动服务端

在目标服务所在的机器上运行：

~~~bash
tows
~~~

程序默认监听 4489 端口。保持该终端运行即可。

### 2. 在本机启动客户端

将 <tows-ip> 替换成目标机器的 IP 地址：

~~~bash
towc <tows-ip>
~~~

第一次启动时，终端会显示微信二维码；使用微信扫码确认即可登录 WebVPN。

当终端显示隧道已就绪后，默认可通过本机 14489 端口访问目标机器的 SSH 服务：

~~~bash
ssh -p 14489 user@localhost
~~~

将 user 换成目标机器上的用户名。

## 配置转发

### 服务端

~~~text
tows [port]
~~~

port 为服务端监听端口，默认是 4489。例如：

~~~bash
tows 54489
~~~

### 客户端

~~~text
towc <tows-ip[:port]> [--target <host:port|port>] [--listen <host:port|port>]
~~~

| 参数 | 作用 | 默认值 |
| --- | --- | --- |
| <tows-ip[:port]> | tows 所在机器的地址和端口 | 端口省略时使用 4489 |
| --target | 要访问的目标服务；该地址相对于 tows 所在机器 | 127.0.0.1:22 |
| --listen | 在自己电脑上开放的本地地址 | 127.0.0.1:14489 |

只填写端口时，--target 使用 tows 本机的该端口；--listen 使用本机回环地址。例如，把远程 Windows 远程桌面端口转发到本机 13389：

~~~bash
towc <tows-ip> --target 3389 --listen 13389
~~~

随后在远程桌面客户端中连接：

~~~text
127.0.0.1:13389
~~~

也可以指定完整地址：

~~~bash
towc <tows-ip>:54489 --target 192.168.1.20:3306 --listen 127.0.0.1:13306
~~~

不带参数运行 towc 会进入交互模式。程序会询问 tows 地址、目标端口和本地端口。

## 登录与多条隧道

towc 会保存 WebVPN 登录状态并在下次启动时复用。登录状态失效后，重新启动 towc 并扫码即可。

如果需要多个转发，请为每个 towc 进程选择不同的 --listen 端口。例如：

~~~bash
towc <tows-ip> --target 22 --listen 14489
towc <tows-ip> --target 3389 --listen 13389
~~~

一个 tows 可以同时服务多个 towc。

## 服务端开机自启（可选）

通常只需让 tows 开机自启；towc 需要 WebVPN 登录，建议按需在本机手动启动。

Linux 使用 systemd 时，可创建 /etc/systemd/system/tows.service：

~~~ini
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
~~~

然后执行：

~~~bash
sudo systemctl daemon-reload
sudo systemctl enable --now tows
~~~

Windows 可在“任务计划程序”中新建“启动时”任务，程序或脚本填写 tows.exe 的完整路径，参数填写 4489。

## 从源码构建

需要安装 Rust 1.85 或更新版本。在项目目录执行：

~~~bash
cargo build --release
~~~

生成的文件位于：

- Linux/macOS：target/release/tows、target/release/towc
- Windows：target/release/tows.exe、target/release/towc.exe

升级后请同时替换 tows 和 towc，再重启相应程序。

## 常见问题

| 现象 | 处理方式 |
| --- | --- |
| 连接失败或被关闭 | 确认 tows 正在运行、地址和端口正确，目标服务可用，并检查服务器防火墙。 |
| WebVPN login expired | 重新启动 towc 并扫码登录。 |
| 本地端口已被占用 | 换一个 --listen 端口，例如 --listen 14490。 |
