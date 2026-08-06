use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::Message;

use crate::address::Endpoint;
use crate::multiplex::{FlowWriter, WsWriter, spawn_writer};
use crate::network::{
    HEARTBEAT_INTERVAL, build_webvpn_ws_url, client_handshake, connect_websocket,
};
use crate::protocol::{Frame, FrameType, MAX_DATA_LEN, MAX_TUNNELS};
use crate::{APP_VERSION, init_tracing};

use super::auth::{
    AuthPrompt, SessionCookie, login_or_restore, login_with_preference, refresh_ticket,
    restore_valid_cached_ticket,
};
use super::config::{ParsedArgs, parse_args, prompt_interactive, prompt_login};

const OPEN_TIMEOUT: Duration = Duration::from_secs(15);
const COOKIE_REFRESH_INTERVAL: Duration = Duration::from_secs(600);
const TCP_QUEUE_FRAMES: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardRule {
    pub name: String,
    pub target: Endpoint,
    pub listen: Endpoint,
}

#[derive(Debug, Clone)]
pub struct ServerGroup {
    pub server: Endpoint,
    pub rules: Vec<ForwardRule>,
    pub heartbeat_interval: Duration,
}

pub trait ClientObserver: Send + Sync {
    fn status(&self, message: &str);
    fn tunnel_status(&self, name: &str, message: &str);
}

struct TerminalUi;

impl AuthPrompt for TerminalUi {
    fn status(&self, message: &str) {
        tracing::info!(target: "towc", "{message}");
    }

    fn show_qr(&self, image: Vec<u8>) -> Result<()> {
        super::qr::print(&image)
    }

    fn request_code(&self, label: &str) -> Result<String> {
        use std::io::Write;
        print!("Enter the {label} verification code: ");
        std::io::stdout().flush()?;
        let mut code = String::new();
        std::io::stdin().read_line(&mut code)?;
        Ok(code.trim().to_string())
    }
}

impl ClientObserver for TerminalUi {
    fn status(&self, message: &str) {
        tracing::info!(target: "towc", "{message}");
    }

    fn tunnel_status(&self, name: &str, message: &str) {
        tracing::info!(target: "tunnel", "<{name}> {message}");
    }
}

pub async fn run_cli() -> Result<()> {
    init_tracing("towc");
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (mut config, interactive) = match parse_args(&args)? {
        ParsedArgs::Help => {
            print_help();
            return Ok(());
        }
        ParsedArgs::Interactive => (prompt_interactive()?, true),
        ParsedArgs::Run(config) => (config, false),
    };

    if !config.listen.is_loopback() {
        tracing::warn!(target: "towc", "listen address {} is not loopback; the local port will be exposed to the LAN", config.listen);
    }
    let ui = Arc::new(TerminalUi);
    let auth: Arc<dyn AuthPrompt> = ui.clone();
    let observer: Arc<dyn ClientObserver> = ui;
    let cookie = if interactive {
        if let Some(cookie) = restore_valid_cached_ticket().await {
            auth.status("reusing a valid WebVPN login cache");
            cookie
        } else {
            config.login = prompt_login()?;
            login_with_preference(auth, config.login).await?
        }
    } else {
        login_or_restore(auth, config.login).await?
    };
    let rule = ForwardRule {
        name: "towc".to_string(),
        target: config.target,
        listen: config.listen,
    };
    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = stop_tx.send(true);
    });
    run_tunnels(config.server, vec![rule], cookie, stop_rx, observer).await
}

pub async fn run_tunnels(
    server: Endpoint,
    rules: Vec<ForwardRule>,
    cookie: SessionCookie,
    stop: watch::Receiver<bool>,
    observer: Arc<dyn ClientObserver>,
) -> Result<()> {
    run_server_groups(
        vec![ServerGroup {
            server,
            rules,
            heartbeat_interval: HEARTBEAT_INTERVAL,
        }],
        cookie,
        stop,
        observer,
    )
    .await
}

pub async fn run_server_groups(
    groups: Vec<ServerGroup>,
    cookie: SessionCookie,
    stop: watch::Receiver<bool>,
    observer: Arc<dyn ClientObserver>,
) -> Result<()> {
    let groups = groups
        .into_iter()
        .map(|group| {
            Ok(ConnectionGroup {
                url: build_webvpn_ws_url(&group.server)?,
                server: group.server,
                rules: group.rules,
                heartbeat_interval: group.heartbeat_interval,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    run_connection_groups(groups, cookie, stop, observer).await
}

pub async fn run_dynamic_server_groups(
    groups: Vec<ServerGroup>,
    cookie: SessionCookie,
    stop: watch::Receiver<bool>,
    updates: mpsc::UnboundedReceiver<Vec<ServerGroup>>,
    observer: Arc<dyn ClientObserver>,
    cookie_refresh_interval: watch::Receiver<Duration>,
) -> Result<()> {
    let groups = groups
        .into_iter()
        .map(|group| {
            Ok(ConnectionGroup {
                url: build_webvpn_ws_url(&group.server)?,
                server: group.server,
                rules: group.rules,
                heartbeat_interval: group.heartbeat_interval,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    run_dynamic_connection_groups(
        groups,
        cookie,
        stop,
        updates,
        observer,
        cookie_refresh_interval,
    )
    .await
}

struct ConnectionGroup {
    url: String,
    server: Endpoint,
    rules: Vec<ForwardRule>,
    heartbeat_interval: Duration,
}

async fn run_connection_groups(
    groups: Vec<ConnectionGroup>,
    cookie: SessionCookie,
    mut stop: watch::Receiver<bool>,
    observer: Arc<dyn ClientObserver>,
) -> Result<()> {
    if groups.is_empty() {
        bail!("no tows server groups are enabled");
    }

    let mut servers = HashSet::new();
    for group in &groups {
        if group.rules.is_empty() {
            bail!("tows {} has no enabled tunnels", group.server);
        }
        if group.rules.len() > MAX_TUNNELS {
            bail!("tows {} has more than {MAX_TUNNELS} tunnels", group.server);
        }
        if !servers.insert(group.server.clone()) {
            bail!("duplicate tows server group: {}", group.server);
        }
    }

    let total = groups.len();
    let (session_stop_tx, session_stop_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
    for group in groups {
        let server = group.server;
        let names: Vec<String> = group.rules.iter().map(|rule| rule.name.clone()).collect();
        let cookie = cookie.clone();
        let group_stop = session_stop_rx.clone();
        let group_observer = Arc::clone(&observer);
        tasks.spawn(async move {
            let result = run_tunnels_to_url(
                group.url,
                server.clone(),
                group.rules,
                group.heartbeat_interval,
                cookie,
                group_stop,
                group_observer,
            )
            .await;
            (server, names, result)
        });
    }

    let mut refresh = tokio::time::interval_at(
        tokio::time::Instant::now() + COOKIE_REFRESH_INTERVAL,
        COOKIE_REFRESH_INTERVAL,
    );
    let mut failures = Vec::new();

    while !tasks.is_empty() {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    let _ = session_stop_tx.send(true);
                    while tasks.join_next().await.is_some() {}
                    return Ok(());
                }
            }
            _ = refresh.tick() => {
                if let Err(error) = refresh_ticket(&cookie).await {
                    let _ = session_stop_tx.send(true);
                    while tasks.join_next().await.is_some() {}
                    return Err(error.context("WebVPN cookie refresh failed"));
                }
                observer.status("WebVPN cookie refreshed");
            }
            joined = tasks.join_next() => {
                let Some(joined) = joined else { break };
                match joined {
                    Ok((server, names, Ok(()))) => {
                        for name in names {
                            observer.tunnel_status(&name, &format!("tows {server} stopped"));
                        }
                    }
                    Ok((server, names, Err(error))) => {
                        let reason = format!("tows {server} failed: {error:#}");
                        for name in names {
                            observer.tunnel_status(&name, &reason);
                        }
                        failures.push(reason);
                    }
                    Err(error) => failures.push(format!("tows task failed: {error}")),
                }
                observer.status(&format!("{}/{} tows connections active", tasks.len(), total));
            }
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        bail!("all tows connections stopped: {}", failures.join("; "))
    }
}

async fn run_dynamic_connection_groups(
    groups: Vec<ConnectionGroup>,
    cookie: SessionCookie,
    mut stop: watch::Receiver<bool>,
    mut updates: mpsc::UnboundedReceiver<Vec<ServerGroup>>,
    observer: Arc<dyn ClientObserver>,
    mut cookie_refresh_interval: watch::Receiver<Duration>,
) -> Result<()> {
    validate_connection_groups(&groups, true)?;
    let initial_cookie_interval = *cookie_refresh_interval.borrow();
    if initial_cookie_interval.is_zero() {
        bail!("cookie refresh interval cannot be zero");
    }

    let mut known_urls = groups
        .iter()
        .map(|group| (group.server.clone(), group.url.clone()))
        .collect::<HashMap<_, _>>();
    let mut controls = HashMap::<Endpoint, DynamicControl>::new();
    let mut stopping = HashSet::<Endpoint>::new();
    let mut pending = HashMap::<Endpoint, ConnectionGroup>::new();
    let mut tasks = JoinSet::<(Endpoint, u64, Result<()>)>::new();
    let mut next_generation = 1_u64;
    for group in groups.into_iter().filter(|group| !group.rules.is_empty()) {
        spawn_dynamic_group(
            group,
            &cookie,
            &observer,
            &mut controls,
            &mut tasks,
            &mut next_generation,
        );
    }

    let mut refresh = tokio::time::interval_at(
        tokio::time::Instant::now() + initial_cookie_interval,
        initial_cookie_interval,
    );
    let mut updates_open = true;
    let mut cookie_updates_open = true;

    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    for control in controls.values() {
                        let _ = control.stop.send(true);
                    }
                    while tasks.join_next().await.is_some() {}
                    return Ok(());
                }
            }
            update = async {
                if updates_open {
                    updates.recv().await
                } else {
                    std::future::pending().await
                }
            } => {
                let Some(groups) = update else {
                    updates_open = false;
                    continue;
                };
                let mut requested = HashMap::new();
                let mut request_valid = true;
                for group in groups {
                    let url = if let Some(url) = known_urls.get(&group.server) {
                        url.clone()
                    } else {
                        let Ok(url) = build_webvpn_ws_url(&group.server) else {
                            observer.status("tunnel update rejected: invalid tows address");
                            request_valid = false;
                            break;
                        };
                        known_urls.insert(group.server.clone(), url.clone());
                        url
                    };
                    requested.insert(group.server.clone(), ConnectionGroup {
                        url,
                        server: group.server,
                        rules: group.rules,
                        heartbeat_interval: group.heartbeat_interval,
                    });
                }
                if !request_valid {
                    continue;
                }
                if requested.values().any(|group| group.rules.len() > MAX_TUNNELS) {
                    observer.status("tunnel update rejected: a tows connection exceeds the rule limit");
                    continue;
                }

                let removed = controls.keys().filter(|server| {
                    requested.get(*server).is_none_or(|group| group.rules.is_empty())
                }).cloned().collect::<Vec<_>>();
                for server in removed {
                    pending.remove(&server);
                    if let Some(control) = controls.remove(&server) {
                        let _ = control.stop.send(true);
                        stopping.insert(server.clone());
                        observer.status(&format!("tows {server} keepalive stopped"));
                    }
                }

                for (server, group) in requested.drain() {
                    if group.rules.is_empty() {
                        continue;
                    }
                    let restart = controls.get(&server).is_some_and(|control| {
                        control.heartbeat_interval != group.heartbeat_interval
                    });
                    if restart && let Some(control) = controls.remove(&server) {
                        let _ = control.stop.send(true);
                        stopping.insert(server.clone());
                        pending.insert(server, group);
                        continue;
                    }
                    if let Some(control) = controls.get(&server) {
                        let _ = control.rules.send(group.rules);
                    } else if stopping.contains(&server) {
                        pending.insert(server, group);
                    } else {
                        spawn_dynamic_group(
                            group,
                            &cookie,
                            &observer,
                            &mut controls,
                            &mut tasks,
                            &mut next_generation,
                        );
                    }
                }
            }
            _ = refresh.tick() => {
                if let Err(error) = refresh_ticket(&cookie).await {
                    for control in controls.values() {
                        let _ = control.stop.send(true);
                    }
                    while tasks.join_next().await.is_some() {}
                    return Err(error.context("WebVPN cookie refresh failed"));
                }
                observer.status("WebVPN cookie refreshed");
            }
            changed = cookie_refresh_interval.changed(), if cookie_updates_open => {
                if changed.is_ok() {
                    let interval = *cookie_refresh_interval.borrow_and_update();
                    if !interval.is_zero() {
                        refresh = tokio::time::interval_at(
                            tokio::time::Instant::now() + interval,
                            interval,
                        );
                        observer.status(&format!("Cookie keepalive interval updated to {} seconds", interval.as_secs()));
                    }
                } else {
                    cookie_updates_open = false;
                }
            }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                let Some(joined) = joined else { continue };
                match joined {
                    Ok((server, generation, result)) => {
                        stopping.remove(&server);
                        if controls.get(&server).is_some_and(|control| control.generation == generation) {
                            controls.remove(&server);
                        }
                        if let Err(error) = result {
                            observer.status(&format!("tows {server} failed: {error:#}"));
                        }
                        if let Some(group) = pending.remove(&server) {
                            spawn_dynamic_group(
                                group,
                                &cookie,
                                &observer,
                                &mut controls,
                                &mut tasks,
                                &mut next_generation,
                            );
                        }
                    }
                    Err(error) => observer.status(&format!("tows task failed: {error}")),
                }
                observer.status(&format!("{} tows connections active", controls.len()));
            }
        }
    }
}

struct DynamicControl {
    generation: u64,
    rules: watch::Sender<Vec<ForwardRule>>,
    stop: watch::Sender<bool>,
    heartbeat_interval: Duration,
}

fn spawn_dynamic_group(
    group: ConnectionGroup,
    cookie: &SessionCookie,
    observer: &Arc<dyn ClientObserver>,
    controls: &mut HashMap<Endpoint, DynamicControl>,
    tasks: &mut JoinSet<(Endpoint, u64, Result<()>)>,
    next_generation: &mut u64,
) {
    let generation = *next_generation;
    *next_generation = next_generation.wrapping_add(1);
    let server = group.server.clone();
    let heartbeat_interval = group.heartbeat_interval;
    let (rules_tx, rules_rx) = watch::channel(group.rules);
    let (stop_tx, stop_rx) = watch::channel(false);
    controls.insert(
        server.clone(),
        DynamicControl {
            generation,
            rules: rules_tx,
            stop: stop_tx,
            heartbeat_interval,
        },
    );
    let cookie = cookie.clone();
    let group_observer = Arc::clone(observer);
    tasks.spawn(async move {
        let status_rules = rules_rx.clone();
        let result = run_dynamic_tunnels_to_url(
            group.url,
            server.clone(),
            rules_rx,
            heartbeat_interval,
            cookie,
            stop_rx,
            Arc::clone(&group_observer),
        )
        .await;
        if let Err(error) = &result {
            let names = status_rules
                .borrow()
                .iter()
                .map(|rule| rule.name.clone())
                .collect::<Vec<_>>();
            let reason = format!("tows {server} failed: {error:#}");
            for name in names {
                group_observer.tunnel_status(&name, &reason);
            }
        }
        (server, generation, result)
    });
}
fn validate_connection_groups(groups: &[ConnectionGroup], allow_empty: bool) -> Result<()> {
    if groups.is_empty() && !allow_empty {
        bail!("no tows server groups are configured");
    }
    let mut servers = HashSet::new();
    for group in groups {
        if !allow_empty && group.rules.is_empty() {
            bail!("tows {} has no enabled tunnels", group.server);
        }
        if group.rules.len() > MAX_TUNNELS {
            bail!("tows {} has more than {MAX_TUNNELS} tunnels", group.server);
        }
        if !servers.insert(group.server.clone()) {
            bail!("duplicate tows server group: {}", group.server);
        }
    }
    Ok(())
}

async fn run_tunnels_to_url(
    url: String,
    server: Endpoint,
    rules: Vec<ForwardRule>,
    heartbeat_interval: Duration,
    cookie: SessionCookie,
    stop: watch::Receiver<bool>,
    observer: Arc<dyn ClientObserver>,
) -> Result<()> {
    let (rules_tx, rules_rx) = watch::channel(rules);
    let result = run_controlled_tunnels_to_url(
        url,
        server,
        rules_rx,
        false,
        heartbeat_interval,
        cookie,
        stop,
        observer,
    )
    .await;
    drop(rules_tx);
    result
}

async fn run_dynamic_tunnels_to_url(
    url: String,
    server: Endpoint,
    rules: watch::Receiver<Vec<ForwardRule>>,
    heartbeat_interval: Duration,
    cookie: SessionCookie,
    stop: watch::Receiver<bool>,
    observer: Arc<dyn ClientObserver>,
) -> Result<()> {
    run_controlled_tunnels_to_url(
        url,
        server,
        rules,
        true,
        heartbeat_interval,
        cookie,
        stop,
        observer,
    )
    .await
}

async fn run_controlled_tunnels_to_url(
    url: String,
    server: Endpoint,
    mut rules: watch::Receiver<Vec<ForwardRule>>,
    dynamic: bool,
    heartbeat_interval: Duration,
    cookie: SessionCookie,
    mut stop: watch::Receiver<bool>,
    observer: Arc<dyn ClientObserver>,
) -> Result<()> {
    // 登录完成后先绑定全部端口；任何冲突都在建立 WS 前清晰报出。
    let mut listeners = Vec::new();
    let initial_rules = rules.borrow().clone();
    for rule in initial_rules {
        let address = rule.listen.resolve().await?;
        let listener = TcpListener::bind(address).await.with_context(|| {
            format!(
                "failed to listen on {} (the port may be in use)",
                rule.listen
            )
        })?;
        listeners.push((rule, listener));
    }

    observer.status(&format!("connecting to tows {server} through WebVPN"));
    let mut websocket = connect_websocket(&url, &cookie.snapshot())
        .await
        .map_err(|error| anyhow!(error))?;
    client_handshake(&mut websocket, &format!("towc {APP_VERSION}")).await?;
    observer.status(&format!("connected to tows {server}"));

    let (sink, mut source) = websocket.split();
    let (writer, mut writer_task) = spawn_writer(sink);
    let (open_tx, mut open_rx) = mpsc::channel::<LocalOpen>(MAX_TUNNELS * 2);
    let (event_tx, mut event_rx) = mpsc::channel::<TunnelEvent>(256);
    let mut accept_tasks = HashMap::new();
    for (rule, listener) in listeners {
        let name = rule.name.clone();
        let sender = open_tx.clone();
        let observer = Arc::clone(&observer);
        let task_stop = stop.clone();
        observer.tunnel_status(
            &rule.name,
            &format!("ready: {} -> {} -> {}", rule.listen, server, rule.target),
        );
        let task = tokio::spawn(accept_loop(
            rule.clone(),
            listener,
            sender,
            task_stop,
            observer,
        ));
        accept_tasks.insert(name, (rule, task));
    }

    let mut tunnels = HashMap::<u16, Tunnel>::new();
    let mut retired_ids = HashSet::new();
    let mut next_id = 1_u16;
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + heartbeat_interval,
        heartbeat_interval,
    );
    let mut heartbeat_waiting_for_pong = false;

    let result = loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break Ok(());
                }
            }
            changed = rules.changed(), if dynamic => {
                if changed.is_err() {
                    break Ok(());
                }
                let desired = rules
                    .borrow_and_update()
                    .clone()
                    .into_iter()
                    .map(|rule| (rule.name.clone(), rule))
                    .collect::<HashMap<_, _>>();
                let remove = accept_tasks
                    .iter()
                    .filter(|(name, (active, _))| desired.get(*name) != Some(active))
                    .map(|(name, _)| name.clone())
                    .collect::<Vec<_>>();
                for name in remove {
                    if let Some((_, task)) = accept_tasks.remove(&name) {
                        task.abort();
                        let _ = task.await;
                    }
                    close_named(
                        &name,
                        &writer,
                        &observer,
                        &mut tunnels,
                        &mut retired_ids,
                    )
                    .await?;
                    observer.tunnel_status(&name, "disabled");
                }
                for (name, rule) in desired {
                    if accept_tasks.contains_key(&name) {
                        continue;
                    }
                    let address = match rule.listen.resolve().await {
                        Ok(address) => address,
                        Err(error) => {
                            observer.tunnel_status(&name, &format!("enable failed: {error:#}"));
                            continue;
                        }
                    };
                    let listener = match TcpListener::bind(address).await {
                        Ok(listener) => listener,
                        Err(error) => {
                            observer.tunnel_status(&name, &format!("enable failed: could not listen on {}: {error}", rule.listen));
                            continue;
                        }
                    };
                    observer.tunnel_status(
                        &name,
                        &format!("ready: {} -> {} -> {}", rule.listen, server, rule.target),
                    );
                    let task = tokio::spawn(accept_loop(
                        rule.clone(),
                        listener,
                        open_tx.clone(),
                        stop.clone(),
                        Arc::clone(&observer),
                    ));
                    accept_tasks.insert(name, (rule, task));
                }
            }
            _ = heartbeat.tick() => {
                if heartbeat_waiting_for_pong {
                    break Err(anyhow!("WebSocket keepalive timed out waiting for Pong"));
                }
                writer.raw(Message::Ping(Vec::new().into())).await;
                heartbeat_waiting_for_pong = true;
            }
            local = open_rx.recv() => {
                let Some(local) = local else { break Ok(()) };
                if tunnels.len() >= MAX_TUNNELS {
                    observer.tunnel_status(&local.name, "connection rejected: 64 concurrent streams are already open");
                    continue;
                }
                let id = allocate_id(&tunnels, &retired_ids, &mut next_id)?;
                writer.send(Frame::new(FrameType::Open, id, local.target.to_string().into_bytes())?).await?;
                observer.tunnel_status(&local.name, &format!("opening stream {id}"));
                tunnels.insert(id, Tunnel::Opening(OpeningTunnel {
                    stream: Some(local.stream),
                    name: local.name,
                }));
                let timeout_events = event_tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(OPEN_TIMEOUT).await;
                    let _ = timeout_events.send(TunnelEvent::OpenTimeout(id)).await;
                });
            }
            message = source.next() => {
                match message {
                    Some(Ok(message)) => {
                        if matches!(message, Message::Pong(_)) {
                            heartbeat_waiting_for_pong = false;
                        }
                        if let Err(error) = handle_ws_message(
                            message,
                            &writer,
                            &event_tx,
                            &observer,
                            &mut tunnels,
                            &mut retired_ids,
                        ).await {
                            writer.protocol_close(error.to_string()).await;
                            break Err(error);
                        }
                    }
                    Some(Err(error)) => break Err(anyhow!(error).context("WebSocket read failed; restart and sign in again")),
                    None => break Err(anyhow!("WebSocket disconnected; restart and sign in again")),
                }
            }
            event = event_rx.recv() => {
                let Some(event) = event else { break Ok(()) };
                if let Err(error) = handle_tunnel_event(
                    event,
                    &writer,
                    &event_tx,
                    &observer,
                    &mut tunnels,
                    &mut retired_ids,
                ).await {
                    break Err(error);
                }
            }
            writer_result = &mut writer_task => {
                break match writer_result {
                    Ok(Ok(())) => Err(anyhow!("WebSocket writer task stopped")),
                    Ok(Err(error)) => Err(error.context("WebSocket writer task failed")),
                    Err(error) => Err(anyhow!(error).context("WebSocket writer task terminated unexpectedly")),
                };
            }
        }
    };

    let disabled_names = accept_tasks.keys().cloned().collect::<Vec<_>>();
    for (_, task) in accept_tasks.values() {
        task.abort();
    }
    for (_, task) in accept_tasks.into_values() {
        let _ = task.await;
    }
    if result.is_ok() {
        for name in disabled_names {
            observer.tunnel_status(&name, "disabled");
        }
    }
    close_all(&writer, &mut tunnels).await;
    writer.normal_close().await;
    if !writer_task.is_finished() {
        let _ = writer_task.await;
    }
    result
}

struct LocalOpen {
    stream: TcpStream,
    target: Endpoint,
    name: String,
}

enum Tunnel {
    Opening(OpeningTunnel),
    Open(OpenTunnel),
}

struct OpeningTunnel {
    stream: Option<TcpStream>,
    name: String,
}

struct OpenTunnel {
    name: String,
    tcp_sender: mpsc::Sender<TcpCommand>,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    local_eof_sent: bool,
    remote_eof_seen: bool,
    tcp_writer_done: bool,
}

enum TcpCommand {
    Data(Vec<u8>),
    Eof,
}

enum TunnelEvent {
    OpenTimeout(u16),
    LocalEof(u16),
    TcpWriterDone(u16),
    TcpError(u16, String),
}

async fn accept_loop(
    rule: ForwardRule,
    listener: TcpListener,
    sender: mpsc::Sender<LocalOpen>,
    mut stop: watch::Receiver<bool>,
    observer: Arc<dyn ClientObserver>,
) {
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() { return; }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        observer.tunnel_status(&rule.name, &format!("local connection from {peer}"));
                        if sender.send(LocalOpen {
                            stream,
                            target: rule.target.clone(),
                            name: rule.name.clone(),
                        }).await.is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        observer.tunnel_status(&rule.name, &format!("listener failed: {error}"));
                        return;
                    }
                }
            }
        }
    }
}

async fn handle_ws_message(
    message: Message,
    writer: &WsWriter,
    events: &mpsc::Sender<TunnelEvent>,
    observer: &Arc<dyn ClientObserver>,
    tunnels: &mut HashMap<u16, Tunnel>,
    retired_ids: &mut HashSet<u16>,
) -> Result<()> {
    match message {
        Message::Binary(bytes) => {
            let frame = Frame::decode(&bytes)?;
            frame.validate_server_to_client(true)?;
            handle_frame(frame, writer, events, observer, tunnels, retired_ids).await
        }
        Message::Ping(payload) => {
            writer.raw(Message::Pong(payload)).await;
            Ok(())
        }
        Message::Pong(_) => Ok(()),
        Message::Close(frame) => bail!("server closed the WebSocket: {frame:?}"),
        Message::Text(_) => bail!("the protocol only accepts WebSocket Binary messages"),
        Message::Frame(_) => Ok(()),
    }
}

async fn handle_frame(
    frame: Frame,
    writer: &WsWriter,
    events: &mpsc::Sender<TunnelEvent>,
    observer: &Arc<dyn ClientObserver>,
    tunnels: &mut HashMap<u16, Tunnel>,
    retired_ids: &mut HashSet<u16>,
) -> Result<()> {
    if retired_ids.contains(&frame.tunnel_id) {
        match frame.kind {
            FrameType::Close | FrameType::OpenFail => {
                retired_ids.remove(&frame.tunnel_id);
            }
            FrameType::OpenOk => {
                writer
                    .send(Frame::new(FrameType::Close, frame.tunnel_id, Vec::new())?)
                    .await?;
            }
            FrameType::Data | FrameType::Eof => {}
            _ => bail!("server sent a disallowed {:?} frame", frame.kind),
        }
        return Ok(());
    }
    match frame.kind {
        FrameType::OpenOk => {
            let Some(Tunnel::Opening(opening)) = tunnels.get_mut(&frame.tunnel_id) else {
                bail!(
                    "OPEN_OK refers to unknown or non-opening stream {}",
                    frame.tunnel_id
                );
            };
            let stream = opening
                .stream
                .take()
                .context("local TCP stream was already taken")?;
            let name = opening.name.clone();
            stream
                .set_nodelay(true)
                .context("failed to enable TCP_NODELAY on local TCP stream")?;
            let flow = writer.register(frame.tunnel_id).await?;
            let tunnel =
                spawn_tcp_tasks(frame.tunnel_id, name.clone(), stream, flow, events.clone());
            tunnels.insert(frame.tunnel_id, Tunnel::Open(tunnel));
            observer.tunnel_status(&name, &format!("stream {} established", frame.tunnel_id));
            Ok(())
        }
        FrameType::OpenFail => {
            let Some(Tunnel::Opening(opening)) = tunnels.remove(&frame.tunnel_id) else {
                bail!(
                    "OPEN_FAIL refers to unknown or non-opening stream {}",
                    frame.tunnel_id
                );
            };
            let reason = std::str::from_utf8(&frame.payload)?;
            observer.tunnel_status(&opening.name, &format!("open failed: {reason}"));
            Ok(())
        }
        FrameType::Data => {
            let tunnel = open_tunnel_mut(tunnels, frame.tunnel_id)?;
            if tunnel.remote_eof_seen {
                bail!("stream {} received DATA after EOF", frame.tunnel_id);
            }
            tunnel
                .tcp_sender
                .send(TcpCommand::Data(frame.payload))
                .await
                .map_err(|_| {
                    anyhow!(
                        "local TCP writer for stream {} has stopped",
                        frame.tunnel_id
                    )
                })
        }
        FrameType::Eof => {
            let tunnel = open_tunnel_mut(tunnels, frame.tunnel_id)?;
            if tunnel.remote_eof_seen {
                bail!("stream {} received duplicate EOF", frame.tunnel_id);
            }
            tunnel.remote_eof_seen = true;
            tunnel.tcp_sender.send(TcpCommand::Eof).await.map_err(|_| {
                anyhow!(
                    "local TCP writer for stream {} has stopped",
                    frame.tunnel_id
                )
            })?;
            maybe_finish(frame.tunnel_id, writer, observer, tunnels, retired_ids).await
        }
        FrameType::Close => {
            if tunnels.contains_key(&frame.tunnel_id) {
                remove_tunnel(frame.tunnel_id, writer, observer, tunnels, "peer closed").await;
            }
            Ok(())
        }
        _ => bail!("server sent a disallowed {:?} frame", frame.kind),
    }
}

async fn handle_tunnel_event(
    event: TunnelEvent,
    writer: &WsWriter,
    _events: &mpsc::Sender<TunnelEvent>,
    observer: &Arc<dyn ClientObserver>,
    tunnels: &mut HashMap<u16, Tunnel>,
    retired_ids: &mut HashSet<u16>,
) -> Result<()> {
    match event {
        TunnelEvent::OpenTimeout(id) => {
            if let Some(Tunnel::Opening(opening)) = tunnels.remove(&id) {
                observer.tunnel_status(&opening.name, "OPEN timed out after 15 seconds");
                writer
                    .send(Frame::new(FrameType::Close, id, Vec::new())?)
                    .await?;
                retired_ids.insert(id);
            }
        }
        TunnelEvent::LocalEof(id) => {
            if let Some(Tunnel::Open(tunnel)) = tunnels.get_mut(&id) {
                tunnel.local_eof_sent = true;
                maybe_finish(id, writer, observer, tunnels, retired_ids).await?;
            }
        }
        TunnelEvent::TcpWriterDone(id) => {
            if let Some(Tunnel::Open(tunnel)) = tunnels.get_mut(&id) {
                tunnel.tcp_writer_done = true;
                maybe_finish(id, writer, observer, tunnels, retired_ids).await?;
            }
        }
        TunnelEvent::TcpError(id, reason) => {
            if tunnels.contains_key(&id) {
                writer
                    .send(Frame::new(FrameType::Close, id, Vec::new())?)
                    .await?;
                retired_ids.insert(id);
                remove_tunnel(
                    id,
                    writer,
                    observer,
                    tunnels,
                    &format!("TCP error: {reason}"),
                )
                .await;
            }
        }
    }
    Ok(())
}

fn spawn_tcp_tasks(
    id: u16,
    name: String,
    stream: TcpStream,
    flow: FlowWriter,
    events: mpsc::Sender<TunnelEvent>,
) -> OpenTunnel {
    let (reader, writer) = stream.into_split();
    let (tcp_sender, receiver) = mpsc::channel(TCP_QUEUE_FRAMES);
    let read_events = events.clone();
    let reader_task = tokio::spawn(read_tcp(id, reader, flow, read_events));
    let writer_task = tokio::spawn(write_tcp(id, writer, receiver, events));
    OpenTunnel {
        name,
        tcp_sender,
        reader_task,
        writer_task,
        local_eof_sent: false,
        remote_eof_seen: false,
        tcp_writer_done: false,
    }
}

async fn read_tcp(
    id: u16,
    mut reader: OwnedReadHalf,
    flow: FlowWriter,
    events: mpsc::Sender<TunnelEvent>,
) {
    let mut buffer = vec![0_u8; MAX_DATA_LEN];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => {
                let eof =
                    Frame::new(FrameType::Eof, id, Vec::new()).expect("static EOF frame is valid");
                if flow.send_flushed(eof).await.is_ok() {
                    let _ = events.send(TunnelEvent::LocalEof(id)).await;
                }
                return;
            }
            Ok(size) => {
                let frame = Frame::new(FrameType::Data, id, buffer[..size].to_vec())
                    .expect("TCP chunk does not exceed the protocol limit");
                if flow.send(frame).await.is_err() {
                    return;
                }
            }
            Err(error) if is_normal_close(&error) => {
                let eof =
                    Frame::new(FrameType::Eof, id, Vec::new()).expect("static EOF frame is valid");
                if flow.send_flushed(eof).await.is_ok() {
                    let _ = events.send(TunnelEvent::LocalEof(id)).await;
                }
                return;
            }
            Err(error) => {
                let _ = events
                    .send(TunnelEvent::TcpError(id, error.to_string()))
                    .await;
                return;
            }
        }
    }
}

async fn write_tcp(
    id: u16,
    mut writer: OwnedWriteHalf,
    mut receiver: mpsc::Receiver<TcpCommand>,
    events: mpsc::Sender<TunnelEvent>,
) {
    while let Some(command) = receiver.recv().await {
        match command {
            TcpCommand::Data(data) => {
                if let Err(error) = writer.write_all(&data).await {
                    let _ = events
                        .send(TunnelEvent::TcpError(id, error.to_string()))
                        .await;
                    return;
                }
            }
            TcpCommand::Eof => {
                if let Err(error) = writer.shutdown().await {
                    let _ = events
                        .send(TunnelEvent::TcpError(id, error.to_string()))
                        .await;
                } else {
                    let _ = events.send(TunnelEvent::TcpWriterDone(id)).await;
                }
                return;
            }
        }
    }
}

fn open_tunnel_mut(tunnels: &mut HashMap<u16, Tunnel>, id: u16) -> Result<&mut OpenTunnel> {
    match tunnels.get_mut(&id) {
        Some(Tunnel::Open(tunnel)) => Ok(tunnel),
        Some(Tunnel::Opening(_)) => bail!("stream {id} received data before OPEN_OK"),
        None => bail!("frame refers to unknown tunnel_id {id}"),
    }
}

async fn maybe_finish(
    id: u16,
    writer: &WsWriter,
    observer: &Arc<dyn ClientObserver>,
    tunnels: &mut HashMap<u16, Tunnel>,
    retired_ids: &mut HashSet<u16>,
) -> Result<()> {
    let finished = matches!(
        tunnels.get(&id),
        Some(Tunnel::Open(tunnel))
            if tunnel.local_eof_sent && tunnel.remote_eof_seen && tunnel.tcp_writer_done
    );
    if finished {
        writer
            .send(Frame::new(FrameType::Close, id, Vec::new())?)
            .await?;
        retired_ids.insert(id);
        remove_tunnel(id, writer, observer, tunnels, "bidirectional EOF").await;
    }
    Ok(())
}

async fn remove_tunnel(
    id: u16,
    writer: &WsWriter,
    observer: &Arc<dyn ClientObserver>,
    tunnels: &mut HashMap<u16, Tunnel>,
    reason: &str,
) {
    if let Some(tunnel) = tunnels.remove(&id) {
        match tunnel {
            Tunnel::Opening(opening) => {
                if !reason.is_empty() {
                    observer.tunnel_status(&opening.name, reason);
                }
            }
            Tunnel::Open(open) => {
                open.reader_task.abort();
                open.writer_task.abort();
                if !reason.is_empty() {
                    observer.tunnel_status(&open.name, reason);
                }
            }
        }
    }
    writer.remove(id).await;
}

async fn close_named(
    name: &str,
    writer: &WsWriter,
    observer: &Arc<dyn ClientObserver>,
    tunnels: &mut HashMap<u16, Tunnel>,
    retired_ids: &mut HashSet<u16>,
) -> Result<()> {
    let ids = tunnels
        .iter()
        .filter_map(|(id, tunnel)| {
            let tunnel_name = match tunnel {
                Tunnel::Opening(opening) => &opening.name,
                Tunnel::Open(open) => &open.name,
            };
            (tunnel_name == name).then_some(*id)
        })
        .collect::<Vec<_>>();
    for id in ids {
        writer
            .send(Frame::new(FrameType::Close, id, Vec::new())?)
            .await?;
        retired_ids.insert(id);
        remove_tunnel(id, writer, observer, tunnels, "").await;
    }
    Ok(())
}

async fn close_all(writer: &WsWriter, tunnels: &mut HashMap<u16, Tunnel>) {
    let ids: Vec<u16> = tunnels.keys().copied().collect();
    for (_, tunnel) in tunnels.drain() {
        if let Tunnel::Open(open) = tunnel {
            open.reader_task.abort();
            open.writer_task.abort();
        }
    }
    for id in ids {
        writer.remove(id).await;
    }
}

fn allocate_id(
    tunnels: &HashMap<u16, Tunnel>,
    retired_ids: &HashSet<u16>,
    next: &mut u16,
) -> Result<u16> {
    for _ in 0..u16::MAX - 1 {
        if *next == 0 || *next == u16::MAX {
            *next = 1;
        }
        let candidate = *next;
        *next = next.saturating_add(1);
        if !tunnels.contains_key(&candidate) && !retired_ids.contains(&candidate) {
            return Ok(candidate);
        }
    }
    bail!("no tunnel_id is available")
}

fn is_normal_close(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    )
}

fn print_help() {
    println!(
        "Usage:\n  towc\n  towc <tows-host[:port]> [--target <host:port|port>] [--listen <host:port|port>] [--login <mobile|email>]\n\nDefaults:\n  tows port: 4489\n  --target 127.0.0.1:22\n  --listen 127.0.0.1:14489\n  Without --login, WeChat QR is used; a valid cache is always reused first."
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use std::sync::Mutex;
    use tokio::sync::oneshot;
    use tokio_tungstenite::tungstenite::Message;

    struct TestObserver;

    impl ClientObserver for TestObserver {
        fn status(&self, _message: &str) {}
        fn tunnel_status(&self, _name: &str, _message: &str) {}
    }

    struct ChannelObserver {
        events: mpsc::UnboundedSender<(String, String)>,
    }

    impl ClientObserver for ChannelObserver {
        fn status(&self, _message: &str) {}

        fn tunnel_status(&self, name: &str, message: &str) {
            let _ = self.events.send((name.to_string(), message.to_string()));
        }
    }

    #[test]
    fn tunnel_ids_skip_reserved_values_and_active_ids() {
        let mut tunnels = HashMap::new();
        tunnels.insert(
            1,
            Tunnel::Opening(OpeningTunnel {
                stream: None,
                name: "test".to_string(),
            }),
        );
        let retired_ids = HashSet::new();
        let mut next = 1;
        assert_eq!(allocate_id(&tunnels, &retired_ids, &mut next).unwrap(), 2);
        next = u16::MAX;
        assert_eq!(allocate_id(&tunnels, &retired_ids, &mut next).unwrap(), 2);
    }

    #[test]
    fn server_address_parser_remains_available_for_gui() {
        assert_eq!(
            crate::address::parse_tows("example.test").unwrap().port(),
            4489
        );
    }

    #[tokio::test]
    async fn websocket_failure_stops_listeners_and_allows_manual_restart() {
        let local_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_port = local_probe.local_addr().unwrap().port();
        drop(local_probe);

        for _ in 0..2 {
            let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ws_address = ws_listener.local_addr().unwrap();
            tokio::spawn(async move {
                let (stream, _) = ws_listener.accept().await.unwrap();
                let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
                crate::network::server_handshake(&mut websocket, "test-tows")
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                websocket.send(Message::Close(None)).await.unwrap();
            });

            let rule = ForwardRule {
                name: "restart-test".to_string(),
                target: crate::address::parse_target("22").unwrap(),
                listen: crate::address::parse_listen(&local_port.to_string()).unwrap(),
            };
            let cookie = SessionCookie(Arc::new(Mutex::new(format!(
                "wengine_vpn_ticketwebvpn_szut_edu_cn=wrdvpn1-{}",
                "0".repeat(32)
            ))));
            let (_stop_tx, stop_rx) = watch::channel(false);
            let result = run_tunnels_to_url(
                format!("ws://{ws_address}/"),
                crate::address::parse_tows("127.0.0.1").unwrap(),
                vec![rule],
                HEARTBEAT_INTERVAL,
                cookie,
                stop_rx,
                Arc::new(TestObserver),
            )
            .await;
            assert!(result.is_err());
            assert!(TcpStream::connect(("127.0.0.1", local_port)).await.is_err());
        }
    }

    #[tokio::test]
    async fn one_server_failure_does_not_stop_another_server_group() {
        let good_ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let good_ws_address = good_ws.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = good_ws.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            crate::network::server_handshake(&mut websocket, "good-tows")
                .await
                .unwrap();
            while let Some(message) = websocket.next().await {
                if matches!(message, Ok(Message::Close(_))) {
                    return;
                }
            }
        });

        let bad_ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bad_ws_address = bad_ws.local_addr().unwrap();
        let (close_bad_tx, close_bad_rx) = oneshot::channel();
        tokio::spawn(async move {
            let (stream, _) = bad_ws.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            crate::network::server_handshake(&mut websocket, "bad-tows")
                .await
                .unwrap();
            close_bad_rx.await.unwrap();
            websocket.send(Message::Close(None)).await.unwrap();
        });

        let good_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let good_port = good_probe.local_addr().unwrap().port();
        let bad_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bad_port = bad_probe.local_addr().unwrap().port();
        drop(good_probe);
        drop(bad_probe);

        let cookie = SessionCookie(Arc::new(Mutex::new(format!(
            "wengine_vpn_ticketwebvpn_szut_edu_cn=wrdvpn1-{}",
            "0".repeat(32)
        ))));
        let groups = vec![
            ConnectionGroup {
                url: format!("ws://{good_ws_address}/"),
                server: crate::address::parse_tows("127.0.0.1").unwrap(),
                rules: vec![ForwardRule {
                    name: "good".to_string(),
                    target: crate::address::parse_target("22").unwrap(),
                    listen: crate::address::parse_listen(&good_port.to_string()).unwrap(),
                }],
                heartbeat_interval: HEARTBEAT_INTERVAL,
            },
            ConnectionGroup {
                url: format!("ws://{bad_ws_address}/"),
                server: crate::address::parse_tows("127.0.0.2").unwrap(),
                rules: vec![ForwardRule {
                    name: "bad".to_string(),
                    target: crate::address::parse_target("22").unwrap(),
                    listen: crate::address::parse_listen(&bad_port.to_string()).unwrap(),
                }],
                heartbeat_interval: HEARTBEAT_INTERVAL,
            },
        ];
        let (stop_tx, stop_rx) = watch::channel(false);
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let coordinator = tokio::spawn(run_connection_groups(
            groups,
            cookie,
            stop_rx,
            Arc::new(ChannelObserver { events: event_tx }),
        ));

        tokio::time::timeout(Duration::from_secs(2), async {
            let mut ready = HashSet::new();
            while ready.len() < 2 {
                let (name, message) = event_rx.recv().await.unwrap();
                if message.starts_with("ready:") {
                    ready.insert(name);
                }
            }
        })
        .await
        .unwrap();
        assert!(TcpStream::connect(("127.0.0.1", good_port)).await.is_ok());
        assert!(TcpStream::connect(("127.0.0.1", bad_port)).await.is_ok());

        close_bad_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let (name, message) = event_rx.recv().await.unwrap();
                if name == "bad" && message.contains("failed:") {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(TcpStream::connect(("127.0.0.1", bad_port)).await.is_err());
        assert!(TcpStream::connect(("127.0.0.1", good_port)).await.is_ok());

        stop_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), coordinator)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn empty_configuration_keeps_cookie_session_running_without_ws() {
        let cookie = SessionCookie(Arc::new(Mutex::new(format!(
            "wengine_vpn_ticketwebvpn_szut_edu_cn=wrdvpn1-{}",
            "0".repeat(32)
        ))));
        let (stop_tx, stop_rx) = watch::channel(false);
        let (_updates_tx, updates_rx) = mpsc::unbounded_channel();
        let (_interval_tx, interval_rx) = watch::channel(COOKIE_REFRESH_INTERVAL);
        let (event_tx, _) = mpsc::unbounded_channel();
        let coordinator = tokio::spawn(run_dynamic_connection_groups(
            Vec::new(),
            cookie,
            stop_rx,
            updates_rx,
            Arc::new(ChannelObserver { events: event_tx }),
            interval_rx,
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!coordinator.is_finished());
        stop_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), coordinator)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn last_tunnel_stops_ws_and_reenable_starts_a_new_ws() {
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_address = ws_listener.local_addr().unwrap();
        tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = ws_listener.accept().await.unwrap();
                let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
                crate::network::server_handshake(&mut websocket, "dynamic-tows")
                    .await
                    .unwrap();
                while let Some(message) = websocket.next().await {
                    match message.unwrap() {
                        Message::Binary(bytes) => {
                            let frame = Frame::decode(&bytes).unwrap();
                            if frame.kind == FrameType::Open && frame.tunnel_id != 0 {
                                websocket
                                    .send(Message::Binary(
                                        Frame::new(FrameType::OpenOk, frame.tunnel_id, Vec::new())
                                            .unwrap()
                                            .encode()
                                            .into(),
                                    ))
                                    .await
                                    .unwrap();
                            }
                        }
                        Message::Close(_) => break,
                        _ => {}
                    }
                }
            }
        });

        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_port = probe.local_addr().unwrap().port();
        drop(probe);
        let server = crate::address::parse_tows("127.0.0.1").unwrap();
        let rule = ForwardRule {
            name: "dynamic".to_string(),
            target: crate::address::parse_target("22").unwrap(),
            listen: crate::address::parse_listen(&local_port.to_string()).unwrap(),
        };
        let cookie = SessionCookie(Arc::new(Mutex::new(format!(
            "wengine_vpn_ticketwebvpn_szut_edu_cn=wrdvpn1-{}",
            "0".repeat(32)
        ))));
        let groups = vec![ConnectionGroup {
            url: format!("ws://{ws_address}/"),
            server: server.clone(),
            rules: vec![rule.clone()],
            heartbeat_interval: HEARTBEAT_INTERVAL,
        }];
        let (stop_tx, stop_rx) = watch::channel(false);
        let (updates_tx, updates_rx) = mpsc::unbounded_channel();
        let (_interval_tx, interval_rx) = watch::channel(COOKIE_REFRESH_INTERVAL);
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let coordinator = tokio::spawn(run_dynamic_connection_groups(
            groups,
            cookie,
            stop_rx,
            updates_rx,
            Arc::new(ChannelObserver { events: event_tx }),
            interval_rx,
        ));

        wait_for_tunnel_status(&mut event_rx, "dynamic", "ready:").await;
        assert!(TcpStream::connect(("127.0.0.1", local_port)).await.is_ok());

        updates_tx
            .send(vec![ServerGroup {
                server: server.clone(),
                rules: Vec::new(),
                heartbeat_interval: HEARTBEAT_INTERVAL,
            }])
            .unwrap();
        wait_for_tunnel_status(&mut event_rx, "dynamic", "disabled").await;
        assert!(TcpStream::connect(("127.0.0.1", local_port)).await.is_err());

        updates_tx
            .send(vec![ServerGroup {
                server,
                rules: vec![rule],
                heartbeat_interval: HEARTBEAT_INTERVAL,
            }])
            .unwrap();
        wait_for_tunnel_status(&mut event_rx, "dynamic", "ready:").await;
        assert!(TcpStream::connect(("127.0.0.1", local_port)).await.is_ok());

        stop_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), coordinator)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    async fn wait_for_tunnel_status(
        events: &mut mpsc::UnboundedReceiver<(String, String)>,
        name: &str,
        prefix: &str,
    ) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let (event_name, message) = events.recv().await.unwrap();
                if event_name == name && message.starts_with(prefix) {
                    return;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {name} status {prefix}"));
    }
}
