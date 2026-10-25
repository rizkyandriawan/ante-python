use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ante_core::{Ante, DeliveryToken, Error, NewMessage};
use ante_security::{BoxedStream, Permission, Principal, Security, TlsAcceptor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex};

use crate::frame::{
    channel_error_args, connection_error_args, parse_content_header, read_frame, write_content,
    write_frame, write_method, BasicProperties, Encoder, Frame, Parser, TableValue, BASIC_ACK,
    BASIC_CANCEL, BASIC_CANCEL_OK, BASIC_CONSUME, BASIC_CONSUME_OK, BASIC_DELIVER, BASIC_GET,
    BASIC_GET_EMPTY, BASIC_GET_OK, BASIC_NACK, BASIC_PUBLISH, BASIC_QOS, BASIC_QOS_OK,
    BASIC_RECOVER, BASIC_RECOVER_OK, BASIC_REJECT, BASIC_RETURN, CHANNEL_CLOSE, CHANNEL_CLOSE_OK,
    CHANNEL_FLOW, CHANNEL_FLOW_OK, CHANNEL_OPEN, CHANNEL_OPEN_OK, CLASS_BASIC, CLASS_CHANNEL,
    CLASS_CONFIRM, CLASS_CONNECTION, CLASS_EXCHANGE, CLASS_QUEUE, CONFIRM_SELECT,
    CONFIRM_SELECT_OK, CONNECTION_CLOSE, CONNECTION_CLOSE_OK, CONNECTION_OPEN, CONNECTION_OPEN_OK,
    CONNECTION_START, CONNECTION_START_OK, CONNECTION_TUNE, CONNECTION_TUNE_OK, EXCHANGE_BIND,
    EXCHANGE_BIND_OK, EXCHANGE_DECLARE, EXCHANGE_DELETE, EXCHANGE_DELETE_OK, FRAME_BODY,
    FRAME_HEADER, FRAME_HEARTBEAT, FRAME_METHOD, QUEUE_BIND, QUEUE_BIND_OK, QUEUE_DECLARE,
    QUEUE_DECLARE_OK, QUEUE_DELETE, QUEUE_DELETE_OK, QUEUE_PURGE, QUEUE_PURGE_OK, QUEUE_UNBIND,
    QUEUE_UNBIND_OK, REPLY_ACCESS_REFUSED, REPLY_CHANNEL_ERROR, REPLY_COMMAND_INVALID,
    REPLY_FRAME_ERROR, REPLY_NOT_ALLOWED, REPLY_NOT_FOUND, REPLY_NOT_IMPLEMENTED, REPLY_NO_ROUTE,
    REPLY_PRECONDITION_FAILED, REPLY_UNEXPECTED_FRAME,
};

const SERVER_FRAME_MAX: u32 = 131_072;
const SERVER_CHANNEL_MAX: u16 = 16;
const MAX_TAKE: usize = 1024;
const CONSUMER_POLL: Duration = Duration::from_millis(1);
const RECONCILE_INTERVAL: Duration = Duration::from_millis(500);
const IDENTITY_INTERVAL: Duration = Duration::from_secs(5);
const CONSUMER_SHUTDOWN_GRACE: Duration = Duration::from_millis(500);
const DEFAULT_ACK_TIMEOUT_MS: u64 = 30 * 60 * 1000;
const DEFAULT_HEARTBEAT_MS: u64 = 30_000;
const CONNECTION_FORCED: u16 = 320;

#[derive(Debug, Clone, Copy)]
pub struct ServerOptions {
    /// Lease applied to deliveries sent to consumers.
    pub lease_ms: u64,
    /// Heartbeat interval advertised during connection tuning, in milliseconds.
    /// Zero disables heartbeats; non-zero values are rounded up to one second.
    pub heartbeat_ms: u64,
}

impl Default for ServerOptions {
    fn default() -> Self {
        ServerOptions {
            lease_ms: DEFAULT_ACK_TIMEOUT_MS,
            heartbeat_ms: DEFAULT_HEARTBEAT_MS,
        }
    }
}

pub async fn serve(listener: TcpListener, ante: Arc<Ante>) -> io::Result<()> {
    let (_tx, rx) = oneshot::channel::<()>();
    serve_until(listener, ante, rx).await
}

pub async fn serve_until(
    listener: TcpListener,
    ante: Arc<Ante>,
    shutdown: oneshot::Receiver<()>,
) -> io::Result<()> {
    serve_until_with_options(listener, ante, ServerOptions::default(), shutdown).await
}

pub async fn serve_until_with_options(
    listener: TcpListener,
    ante: Arc<Ante>,
    options: ServerOptions,
    shutdown: oneshot::Receiver<()>,
) -> io::Result<()> {
    serve_loop(listener, ante, options, None, shutdown).await
}

/// Serve AMQP over TLS with credential authentication and queue permissions.
pub async fn serve_secure_until_with_options(
    listener: TcpListener,
    ante: Arc<Ante>,
    options: ServerOptions,
    security: Arc<Security>,
    shutdown: oneshot::Receiver<()>,
) -> io::Result<()> {
    serve_loop(listener, ante, options, Some(security), shutdown).await
}

async fn serve_loop(
    listener: TcpListener,
    ante: Arc<Ante>,
    options: ServerOptions,
    security: Option<Arc<Security>>,
    mut shutdown: oneshot::Receiver<()>,
) -> io::Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ticks = 0u64;
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let registry = Arc::new(ConsumerRegistry::default());
    let channel_registry = Arc::new(ChannelRegistry::default());
    let queue_locks = Arc::new(QueueLocks::default());
    let drain = Drain::default();
    let reaper = ConsumerReaper::default();
    let mut connections = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                ticks = ticks.wrapping_add(1);
                let prune = ticks.is_multiple_of(60);
                let ante = ante.clone();
                let guard = drain.begin();
                tokio::task::spawn_blocking(move || {
                    let _guard = guard;
                    if let Err(err) = ante.recover_expired() {
                        eprintln!("ante-amqp: lease recovery failed: {err}");
                    }
                    if prune {
                        if let Err(err) = ante.prune_history() {
                            eprintln!("ante-amqp: history prune failed: {err}");
                        }
                    }
                });
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(pair) => pair,
                    Err(err) => {
                        eprintln!("ante-amqp: accept failed: {err}");
                        continue;
                    }
                };
                let _ = stream.set_nodelay(true);
                let setup = ConnectionSetup {
                    ante: ante.clone(),
                    lease_ms: options.lease_ms,
                    heartbeat_ms: options.heartbeat_ms,
                    registry: registry.clone(),
                    channel_registry: channel_registry.clone(),
                    queue_locks: queue_locks.clone(),
                    drain: drain.clone(),
                    reaper: reaper.clone(),
                    security: security.clone(),
                };
                let stop = stop_rx.clone();
                let acceptor = security
                    .as_ref()
                    .map(|security| TlsAcceptor::from(security.tls_config()));
                connections.spawn(async move {
                    let result = match acceptor {
                        Some(acceptor) => {
                            let stream = match tokio::time::timeout(
                                Duration::from_secs(10),
                                acceptor.accept(stream),
                            )
                            .await
                            {
                                Ok(Ok(stream)) => stream,
                                Ok(Err(err)) => {
                                    eprintln!("ante-amqp: TLS handshake failed from {peer}: {err}");
                                    return;
                                }
                                Err(_) => {
                                    eprintln!("ante-amqp: TLS handshake timed out from {peer}");
                                    return;
                                }
                            };
                            handle_connection(Box::new(stream), setup, stop).await
                        }
                        None => handle_connection(Box::new(stream), setup, stop).await,
                    };
                    if let Err(err) = result {
                        if err.kind() != io::ErrorKind::UnexpectedEof {
                            eprintln!("ante-amqp: connection {peer} closed: {err}");
                        }
                    }
                });
            }
            _ = &mut shutdown => break,
            _ = connections.join_next(), if !connections.is_empty() => {},
        }
    }

    let _ = stop_tx.send(true);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !connections.is_empty() {
        match tokio::time::timeout_at(deadline, connections.join_next()).await {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {
                connections.abort_all();
                while connections.join_next().await.is_some() {}
                break;
            }
        }
    }
    for mut handle in reaper.take() {
        if tokio::time::timeout_at(deadline, &mut handle)
            .await
            .is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
    }
    if !drain.wait_until(deadline).await {
        eprintln!(
            "ante-amqp: shutdown deadline reached with {} storage operations still running",
            drain.active()
        );
    }
    Ok(())
}

type StreamRead = ReadHalf<BoxedStream>;
type StreamWrite = WriteHalf<BoxedStream>;

struct FrameWriter {
    inner: BufWriter<StreamWrite>,
    frame_max: usize,
}

impl FrameWriter {
    async fn method(
        &mut self,
        channel: u16,
        class_id: u16,
        method_id: u16,
        args: &[u8],
    ) -> io::Result<()> {
        write_method(&mut self.inner, channel, class_id, method_id, args).await?;
        self.inner.flush().await
    }

    async fn content(
        &mut self,
        channel: u16,
        properties: &BasicProperties,
        body: &[u8],
    ) -> io::Result<()> {
        write_content(&mut self.inner, channel, properties, body, self.frame_max).await?;
        self.inner.flush().await
    }

    async fn heartbeat(&mut self) -> io::Result<()> {
        write_frame(&mut self.inner, FRAME_HEARTBEAT, 0, &[]).await?;
        self.inner.flush().await
    }
}

struct Shared {
    ante: Arc<Ante>,
    writer: Arc<Mutex<FrameWriter>>,
    channels: Arc<Mutex<HashMap<u16, ChannelState>>>,
    frame_max: std::sync::atomic::AtomicU32,
    channel_max: std::sync::atomic::AtomicU16,
    heartbeat_ms: AtomicU64,
    lease_ms: u64,
    shutdown: Arc<AtomicBool>,
    next_channel_generation: AtomicU64,
    tasks: Tasks,
    registry: Arc<ConsumerRegistry>,
    channel_registry: Arc<ChannelRegistry>,
    queue_locks: Arc<QueueLocks>,
    registrations: Arc<std::sync::Mutex<std::collections::HashSet<u64>>>,
    client_cancel_notify: AtomicBool,
    drain: Drain,
    security: Option<Arc<Security>>,
    principal: std::sync::RwLock<Option<Principal>>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        let ids: Vec<u64> = self
            .registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .drain()
            .collect();
        for id in ids {
            self.registry.unregister(id);
        }
    }
}

/// Handles from aborted connections. The server joins these after the
/// connection tasks are gone, so a forced shutdown still waits for consumers.
#[derive(Clone, Default)]
struct ConsumerReaper(Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>);

impl ConsumerReaper {
    fn adopt(&self, handles: Vec<tokio::task::JoinHandle<()>>) {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(handles);
    }

    fn take(&self) -> Vec<tokio::task::JoinHandle<()>> {
        std::mem::take(
            &mut *self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

/// Owns every consumer task of a connection. Finished handles are reaped on
/// push; on drop the handles are aborted and adopted by the reaper instead of
/// being detached.
struct Tasks {
    handles: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    reaper: ConsumerReaper,
}

impl Tasks {
    fn new(reaper: ConsumerReaper) -> Self {
        Tasks {
            handles: std::sync::Mutex::new(Vec::new()),
            reaper,
        }
    }

    fn push(&self, handle: tokio::task::JoinHandle<()>) {
        let mut handles = self
            .handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        handles.retain(|handle| !handle.is_finished());
        handles.push(handle);
    }

    fn take(&self) -> Vec<tokio::task::JoinHandle<()>> {
        std::mem::take(
            &mut *self
                .handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    fn reaper(&self) -> ConsumerReaper {
        self.reaper.clone()
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        let handles = self.take();
        for handle in &handles {
            handle.abort();
        }
        self.reaper.adopt(handles);
    }
}

/// Counts storage work that must finish before shutdown returns. The guard is
/// moved into the blocking closure, so cancelling the async caller does not
/// stop the commit from being tracked.
#[derive(Clone, Default)]
struct Drain(Arc<DrainInner>);

#[derive(Default)]
struct DrainInner {
    active: std::sync::atomic::AtomicUsize,
    idle: tokio::sync::Notify,
}

struct DrainGuard(Arc<DrainInner>);

impl Drop for DrainGuard {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_one();
        }
    }
}

impl Drain {
    fn begin(&self) -> DrainGuard {
        self.0.active.fetch_add(1, Ordering::AcqRel);
        DrainGuard(self.0.clone())
    }

    fn active(&self) -> usize {
        self.0.active.load(Ordering::Acquire)
    }

    async fn wait(&self) {
        loop {
            if self.active() == 0 {
                return;
            }
            self.0.idle.notified().await;
        }
    }

    async fn wait_until(&self, deadline: tokio::time::Instant) -> bool {
        tokio::time::timeout_at(deadline, self.wait()).await.is_ok()
    }
}

struct Subscription {
    id: std::sync::atomic::AtomicU64,
    cancel: AtomicBool,
    active: AtomicBool,
    notified: AtomicBool,
}

impl Subscription {
    fn new() -> Self {
        Subscription {
            id: std::sync::atomic::AtomicU64::new(0),
            cancel: AtomicBool::new(false),
            active: AtomicBool::new(false),
            notified: AtomicBool::new(false),
        }
    }

    fn id(&self) -> u64 {
        self.id.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
struct ConsumerEntry {
    subscription: Arc<Subscription>,
}

type ChannelStore = Mutex<HashMap<u16, ChannelState>>;
type ChannelMap = Arc<ChannelStore>;
type WeakChannelMap = std::sync::Weak<ChannelStore>;

/// Every live connection's channel map. Outstanding deliveries can exist on a
/// channel with no active consumer (for example `basic.get`), so queue
/// deletion needs a view wider than the consumer registry.
#[derive(Default)]
struct ChannelRegistry(std::sync::Mutex<Vec<WeakChannelMap>>);

impl ChannelRegistry {
    fn add(&self, channels: &ChannelMap) {
        let mut maps = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        maps.retain(|map| map.strong_count() > 0);
        maps.push(Arc::downgrade(channels));
    }

    async fn drop_outstanding_for_queue(&self, queue_id: u64) {
        let maps: Vec<ChannelMap> = {
            let mut maps = self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            maps.retain(|map| map.strong_count() > 0);
            maps.iter().filter_map(std::sync::Weak::upgrade).collect()
        };
        for map in maps {
            // Snapshot without holding the map while waiting for a delivery
            // lock: writers take delivery_lock -> map, and must never see the
            // inverse order here.
            let channels: Vec<(u16, u64, Arc<Mutex<()>>)> = {
                let guard = map.lock().await;
                guard
                    .iter()
                    .filter(|(_, state)| {
                        state
                            .outstanding
                            .values()
                            .any(|entry| entry.queue_id == queue_id)
                    })
                    .map(|(channel, state)| {
                        (*channel, state.generation, state.delivery_lock.clone())
                    })
                    .collect()
            };
            for (channel, generation, lock) in channels {
                let _delivery_guard = lock.lock().await;
                let mut guard = map.lock().await;
                let Some(state) = guard.get_mut(&channel) else {
                    continue;
                };
                if state.generation != generation {
                    continue;
                }
                state
                    .outstanding
                    .retain(|_, entry| entry.queue_id != queue_id);
            }
        }
    }
}

/// Consumers from every connection. `if_unused` and queue deletion need this
/// global view; `Shared::channels` is per connection.
#[derive(Default)]
struct ConsumerRegistry {
    next_id: std::sync::atomic::AtomicU64,
    inner: std::sync::Mutex<HashMap<u64, Registration>>,
}

#[derive(Clone)]
struct Registration {
    queue: String,
    queue_id: u64,
    subscription: Arc<Subscription>,
    delivery_lock: Arc<Mutex<()>>,
    writer: Arc<Mutex<FrameWriter>>,
    channels: ChannelMap,
    channel_registry: Arc<ChannelRegistry>,
    channel: u16,
    consumer_tag: String,
    notify: bool,
}

impl ConsumerRegistry {
    fn register(&self, registration: Registration) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        registration.subscription.id.store(id, Ordering::Release);
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id, registration);
        id
    }

    fn unregister(&self, id: u64) {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&id);
    }

    /// Count only consumers bound to this queue identity. A stale consumer of
    /// a deleted queue must not block operations on a recreated queue.
    fn in_use(&self, queue: &str, queue_id: u64) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .any(|registration| registration.queue == queue && registration.queue_id == queue_id)
    }

    /// Consumers currently bound to this queue identity.
    fn count_for_queue(&self, queue: &str, queue_id: u64) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .filter(|registration| registration.queue == queue && registration.queue_id == queue_id)
            .count()
    }

    fn take_by_queue(&self, queue: &str) -> Vec<Registration> {
        let mut removed = Vec::new();
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|_, registration| {
                if registration.queue == queue {
                    removed.push(registration.clone());
                    false
                } else {
                    true
                }
            });
        removed
    }
}

/// Set cancellation, wait for any in-flight batch, then tell the client if
/// the tag is still owned by this registration. Holding the delivery lock
/// keeps a new subscription from reusing the tag mid-notification.
async fn notify_if_owned(registration: &Registration) {
    {
        let _guard = registration.delivery_lock.lock().await;
        debug_assert!(
            registration.subscription.active.load(Ordering::Acquire),
            "cancellation before consume-ok"
        );

        let owns = {
            let channels = registration.channels.lock().await;
            channels
                .get(&registration.channel)
                .and_then(|state| state.consumers.get(&registration.consumer_tag))
                .is_some_and(|entry| entry.subscription.id() == registration.subscription.id())
        };
        if !owns {
            return;
        }

        if registration.notify
            && !registration
                .subscription
                .notified
                .swap(true, Ordering::AcqRel)
        {
            let mut writer = registration.writer.lock().await;
            let mut args = Encoder::new();
            args.shortstr(&registration.consumer_tag).u8(1);
            let _ = writer
                .method(
                    registration.channel,
                    CLASS_BASIC,
                    BASIC_CANCEL,
                    &args.finish(),
                )
                .await;
        } else {
            registration
                .subscription
                .notified
                .store(true, Ordering::Release);
        }

        {
            let mut channels = registration.channels.lock().await;
            if let Some(state) = channels.get_mut(&registration.channel) {
                let still_owned = state
                    .consumers
                    .get(&registration.consumer_tag)
                    .is_some_and(|entry| entry.subscription.id() == registration.subscription.id());
                if still_owned {
                    state.consumers.remove(&registration.consumer_tag);
                }
            }
        }
    }
    // The queue's in-flight records were removed by the delete; keeping their
    // tags would consume prefetch slots until the old lease expired. This
    // waits on each channel's delivery lock, so it runs without our own lock.
    registration
        .channel_registry
        .drop_outstanding_for_queue(registration.queue_id)
        .await;
}

fn should_notify(cancelled: bool, identity_lost: bool, shutdown: bool) -> bool {
    !shutdown && (cancelled || identity_lost)
}

async fn remove_consumer_tag(registration: &Registration) {
    let _guard = registration.delivery_lock.lock().await;
    let mut channels = registration.channels.lock().await;
    if let Some(state) = channels.get_mut(&registration.channel) {
        let owned = state
            .consumers
            .get(&registration.consumer_tag)
            .is_some_and(|entry| entry.subscription.id() == registration.subscription.id());
        if owned {
            state.consumers.remove(&registration.consumer_tag);
        }
    }
}

async fn cancel_registrations(registrations: Vec<Registration>) {
    for registration in &registrations {
        registration
            .subscription
            .cancel
            .store(true, Ordering::Release);
    }
    for registration in registrations {
        notify_if_owned(&registration).await;
    }
}

/// Serializes registration against delete for the same queue name, so
/// `if_unused` cannot miss a consumer that is registering concurrently.
#[derive(Default)]
struct QueueLocks(std::sync::Mutex<HashMap<String, std::sync::Weak<Mutex<()>>>>);

impl QueueLocks {
    fn get(&self, queue: &str) -> Arc<Mutex<()>> {
        let mut locks = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(queue).and_then(std::sync::Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(queue.to_string(), Arc::downgrade(&lock));
        lock
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}

struct Outstanding {
    token: DeliveryToken,
    deadline_ms: u64,
    queue: String,
    queue_id: u64,
    /// True only after the delivery frame has been written to the client.
    /// Acknowledgements must never touch unsent deliveries.
    sent: bool,
    /// Subscription identity, not its reusable string tag. None for basic.get.
    consumer_id: Option<u64>,
}

struct ChannelState {
    generation: u64,
    prefetch: u16,
    global_prefetch: u16,
    next_tag: u64,
    next_consumer: u64,
    outstanding: BTreeMap<u64, Outstanding>,
    reserved: usize,
    /// Serializes (assign delivery tag -> write frames -> mark sent) on this
    /// channel. Shared by all consumers on the channel and by ack/cancel/close.
    delivery_lock: Arc<Mutex<()>>,
    consumers: HashMap<String, ConsumerEntry>,
    confirm: bool,
    next_publish: u64,
    pending: Option<PendingPublish>,
}

impl ChannelState {
    fn new(generation: u64) -> Self {
        ChannelState {
            generation,
            prefetch: 0,
            global_prefetch: 0,
            next_tag: 1,
            next_consumer: 1,
            outstanding: BTreeMap::new(),
            reserved: 0,
            delivery_lock: Arc::new(Mutex::new(())),
            consumers: HashMap::new(),
            confirm: false,
            next_publish: 1,
            pending: None,
        }
    }

    fn prune_expired(&mut self, now: u64) {
        self.outstanding.retain(|_, entry| entry.deadline_ms > now);
    }

    fn available(&self, consumer_id: u64, prefetch: u16, no_ack: bool) -> usize {
        if no_ack {
            return MAX_TAKE;
        }
        let remaining = |limit: u16, used: usize| {
            if limit == 0 {
                MAX_TAKE
            } else {
                (limit as usize).saturating_sub(used)
            }
        };
        let own = self
            .outstanding
            .values()
            .filter(|entry| entry.consumer_id == Some(consumer_id))
            .count();
        let total = self
            .outstanding
            .values()
            .filter(|entry| entry.consumer_id.is_some())
            .count();
        remaining(prefetch, own).min(remaining(self.global_prefetch, total + self.reserved))
    }
}

struct PendingPublish {
    exchange: String,
    routing_key: String,
    mandatory: bool,
    header_received: bool,
    body_size: u64,
    properties: BasicProperties,
    body: Vec<u8>,
}

struct ConnectionSetup {
    ante: Arc<Ante>,
    lease_ms: u64,
    heartbeat_ms: u64,
    registry: Arc<ConsumerRegistry>,
    channel_registry: Arc<ChannelRegistry>,
    queue_locks: Arc<QueueLocks>,
    drain: Drain,
    reaper: ConsumerReaper,
    security: Option<Arc<Security>>,
}

async fn handle_connection(
    stream: BoxedStream,
    setup: ConnectionSetup,
    stop: tokio::sync::watch::Receiver<bool>,
) -> io::Result<()> {
    let (read_half, write_half) = tokio::io::split(stream);
    let reader = BufReader::new(read_half);
    let channels = Arc::new(Mutex::new(HashMap::new()));
    setup.channel_registry.add(&channels);
    let shared = Arc::new(Shared {
        ante: setup.ante,
        writer: Arc::new(Mutex::new(FrameWriter {
            inner: BufWriter::with_capacity(64 * 1024, write_half),
            frame_max: SERVER_FRAME_MAX as usize,
        })),
        channels,
        frame_max: std::sync::atomic::AtomicU32::new(SERVER_FRAME_MAX),
        channel_max: std::sync::atomic::AtomicU16::new(SERVER_CHANNEL_MAX),
        heartbeat_ms: AtomicU64::new(if setup.heartbeat_ms == 0 {
            0
        } else {
            setup.heartbeat_ms.max(1000)
        }),
        lease_ms: setup.lease_ms,
        shutdown: Arc::new(AtomicBool::new(false)),
        next_channel_generation: AtomicU64::new(0),
        tasks: Tasks::new(setup.reaper),
        registry: setup.registry,
        channel_registry: setup.channel_registry,
        queue_locks: setup.queue_locks,
        registrations: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        client_cancel_notify: AtomicBool::new(false),
        drain: setup.drain,
        security: setup.security,
        principal: std::sync::RwLock::new(None),
    });

    let result = run_connection(reader, &shared, stop).await;
    shutdown_consumers(&shared).await;
    cleanup_all(&shared).await;
    result
}

async fn run_connection(
    mut reader: BufReader<StreamRead>,
    shared: &Arc<Shared>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> io::Result<()> {
    let ok = if shared.security.is_some() {
        match tokio::time::timeout(Duration::from_secs(10), handshake(&mut reader, shared)).await {
            Ok(result) => result?,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "authentication handshake timed out",
                ))
            }
        }
    } else {
        handshake(&mut reader, shared).await?
    };
    if !ok {
        return Ok(());
    }

    let frame_max = shared.frame_max.load(Ordering::Relaxed);
    let (frame_tx, mut frames) = tokio::sync::mpsc::channel(16);
    let reader_task = tokio::spawn(async move {
        loop {
            let frame = match read_frame(&mut reader, frame_max).await {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(err) => {
                    let _ = frame_tx.send(Err(err)).await;
                    break;
                }
            };
            if frame_tx.send(Ok(frame)).await.is_err() {
                break;
            }
        }
    });

    let heartbeat_ms = shared.heartbeat_ms.load(Ordering::Relaxed);
    let tick_ms = if heartbeat_ms == 0 {
        86_400_000
    } else {
        (heartbeat_ms / 2).max(100)
    };
    let mut heartbeat_tick = tokio::time::interval(Duration::from_millis(tick_ms));
    heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat_tick.tick().await;
    let mut last_received = tokio::time::Instant::now();

    let result = async {
        loop {
            tokio::select! {
                _ = stop.changed() => break Ok(()),
                frame = frames.recv() => {
                    last_received = tokio::time::Instant::now();
                    let frame = match frame {
                        Some(Ok(frame)) => frame,
                        Some(Err(err)) => break Err(err),
                        None => break Ok(()),
                    };
                    match frame.kind {
                        FRAME_HEARTBEAT => continue,
                        FRAME_METHOD => {
                            if handle_method(shared, frame).await? {
                                break Ok(());
                            }
                        }
                        FRAME_HEADER => {
                            if handle_header(shared, frame).await? {
                                break Ok(());
                            }
                        }
                        FRAME_BODY => {
                            if handle_body(shared, frame).await? {
                                break Ok(());
                            }
                        }
                        other => {
                            let text = format!("unsupported frame type {other}");
                            send_connection_close(shared, REPLY_FRAME_ERROR, &text, 0, 0).await?;
                            break Ok(());
                        }
                    }
                }
                _ = heartbeat_tick.tick(), if heartbeat_ms > 0 => {
                    if last_received.elapsed()
                        >= Duration::from_millis(heartbeat_ms.saturating_mul(2))
                    {
                        send_connection_close(
                            shared,
                            CONNECTION_FORCED,
                            "heartbeat timeout",
                            0,
                            0,
                        )
                        .await?;
                        break Ok(());
                    }
                    shared.writer.lock().await.heartbeat().await?;
                }
            }
        }
    }
    .await;
    reader_task.abort();
    let _ = reader_task.await;
    result
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

async fn handshake(reader: &mut BufReader<StreamRead>, shared: &Arc<Shared>) -> io::Result<bool> {
    let mut protocol_header = [0u8; 8];
    reader.read_exact(&mut protocol_header).await?;
    if protocol_header != *b"AMQP\x00\x00\x09\x01" {
        let mut writer = shared.writer.lock().await;
        writer.inner.write_all(b"AMQP\x00\x00\x09\x01").await?;
        writer.inner.flush().await?;
        return Ok(false);
    }

    {
        let mut args = Encoder::new();
        args.u8(0).u8(9);
        args.table(&[
            ("product", TableValue::String("Ante")),
            ("version", TableValue::String(env!("CARGO_PKG_VERSION"))),
            ("platform", TableValue::String("Rust")),
            (
                "capabilities",
                TableValue::Table(&[
                    ("publisher_confirms", TableValue::Bool(true)),
                    ("basic.nack", TableValue::Bool(true)),
                    ("consumer_cancel_notify", TableValue::Bool(true)),
                ]),
            ),
        ]);
        if shared.security.is_some() {
            args.longstr(b"PLAIN");
        } else {
            args.longstr(b"PLAIN AMQPLAIN");
        }
        args.longstr(b"en_US");
        let mut writer = shared.writer.lock().await;
        writer
            .method(0, CLASS_CONNECTION, CONNECTION_START, &args.finish())
            .await?;
    }

    // StartOk
    let start_ok = read_expected(reader, shared, CLASS_CONNECTION, CONNECTION_START_OK).await?;
    {
        let mut parser = Parser::new(&start_ok);
        let client_properties = parser.longstr_bytes()?;
        shared.client_cancel_notify.store(
            table_flag(&client_properties, "capabilities", "consumer_cancel_notify"),
            Ordering::Relaxed,
        );
        let mechanism = parser.shortstr()?;
        let response = parser.longstr_bytes()?;
        let _locale = parser.shortstr()?;
        parser.finish()?;
        if let Some(security) = &shared.security {
            let principal = match parse_plain(&mechanism, &response) {
                Some((username, password)) => {
                    let security = Arc::clone(security);
                    let username = username.to_string();
                    let password = password.to_string();
                    tokio::task::spawn_blocking(move || security.authenticate(&username, &password))
                        .await
                        .ok()
                        .flatten()
                }
                None => None,
            };
            let Some(principal) = principal else {
                send_connection_close(
                    shared,
                    REPLY_ACCESS_REFUSED,
                    "authentication failed",
                    CLASS_CONNECTION,
                    CONNECTION_START_OK,
                )
                .await?;
                return Ok(false);
            };
            *shared
                .principal
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(principal);
        }
    }

    {
        let mut args = Encoder::new();
        args.u16(SERVER_CHANNEL_MAX)
            .u32(SERVER_FRAME_MAX)
            .u16((shared.heartbeat_ms.load(Ordering::Relaxed) / 1000).min(u16::MAX as u64) as u16);
        let mut writer = shared.writer.lock().await;
        writer
            .method(0, CLASS_CONNECTION, CONNECTION_TUNE, &args.finish())
            .await?;
    }

    let tune_ok = read_expected(reader, shared, CLASS_CONNECTION, CONNECTION_TUNE_OK).await?;
    {
        let mut parser = Parser::new(&tune_ok);
        let client_channel_max = parser.u16()?;
        let client_frame_max = parser.u32()?;
        let client_heartbeat = parser.u16()? as u64 * 1000;
        parser.finish()?;
        let server_heartbeat = shared.heartbeat_ms.load(Ordering::Relaxed);
        let heartbeat = match (client_heartbeat, server_heartbeat) {
            (0, server) => server,
            (client, 0) => client,
            (client, server) => client.min(server),
        };
        shared.heartbeat_ms.store(heartbeat, Ordering::Relaxed);
        let frame_max = if client_frame_max == 0 {
            SERVER_FRAME_MAX
        } else {
            client_frame_max.clamp(4096, SERVER_FRAME_MAX)
        };
        let channel_max = if client_channel_max == 0 {
            SERVER_CHANNEL_MAX
        } else {
            client_channel_max.min(SERVER_CHANNEL_MAX)
        };
        shared.frame_max.store(frame_max, Ordering::Relaxed);
        shared.channel_max.store(channel_max, Ordering::Relaxed);
        shared.writer.lock().await.frame_max = frame_max as usize;
    }

    let open = read_expected(reader, shared, CLASS_CONNECTION, CONNECTION_OPEN).await?;
    {
        let mut parser = Parser::new(&open);
        let vhost = parser.shortstr()?;
        let _capabilities = parser.shortstr()?;
        parser.u8()?;
        parser.finish()?;
        if shared.security.is_some() && vhost != "/" {
            send_connection_close(
                shared,
                REPLY_NOT_ALLOWED,
                "only the default virtual host is supported",
                CLASS_CONNECTION,
                CONNECTION_OPEN,
            )
            .await?;
            return Ok(false);
        }
    }

    let mut args = Encoder::new();
    args.shortstr("");
    let mut writer = shared.writer.lock().await;
    writer
        .method(0, CLASS_CONNECTION, CONNECTION_OPEN_OK, &args.finish())
        .await?;
    Ok(true)
}

/// Decode the SASL PLAIN payload: `[authzid] NUL authcid NUL passwd`.
fn parse_plain<'a>(mechanism: &str, response: &'a [u8]) -> Option<(&'a str, &'a str)> {
    if !mechanism.eq_ignore_ascii_case("PLAIN") {
        return None;
    }
    let mut parts = response.split(|byte| *byte == 0);
    let _authzid = parts.next()?;
    let username = std::str::from_utf8(parts.next()?).ok()?;
    let password = std::str::from_utf8(parts.next()?).ok()?;
    if parts.next().is_some() || username.is_empty() {
        return None;
    }
    Some((username, password))
}

fn authorized(shared: &Shared, permission: Permission, queue: &str) -> bool {
    let Some(security) = &shared.security else {
        return true;
    };
    let principal = shared
        .principal
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    security.allows(principal.as_ref(), permission, queue)
}

async fn require_access(
    shared: &Arc<Shared>,
    channel: u16,
    permission: Permission,
    queue: &str,
    class_id: u16,
    method_id: u16,
) -> io::Result<bool> {
    if authorized(shared, permission, queue) {
        return Ok(true);
    }
    channel_error(
        shared,
        channel,
        REPLY_ACCESS_REFUSED,
        &format!(
            "permission denied: {} on queue {queue}",
            permission.as_str()
        ),
        class_id,
        method_id,
    )
    .await?;
    Ok(false)
}

async fn read_expected(
    reader: &mut BufReader<StreamRead>,
    shared: &Arc<Shared>,
    expected_class: u16,
    expected_method: u16,
) -> io::Result<Vec<u8>> {
    loop {
        let frame = match read_frame(reader, shared.frame_max.load(Ordering::Relaxed)).await? {
            Some(frame) => frame,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "handshake aborted",
                ))
            }
        };
        if frame.kind == FRAME_HEARTBEAT {
            continue;
        }
        if frame.kind != FRAME_METHOD || frame.channel != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected frame during handshake",
            ));
        }
        let (class_id, method_id, args) = split_method(&frame.payload)?;
        if class_id == CLASS_CONNECTION && method_id == CONNECTION_CLOSE {
            send_connection_close(shared, 200, "OK", 0, 0).await?;
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client closed during handshake",
            ));
        }
        if class_id != expected_class || method_id != expected_method {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected method during handshake",
            ));
        }
        return Ok(args);
    }
}

async fn handle_method(shared: &Arc<Shared>, frame: Frame) -> io::Result<bool> {
    let (class_id, method_id, args) = split_method(&frame.payload)?;
    let channel = frame.channel;

    if class_id == CLASS_CONNECTION {
        return match method_id {
            CONNECTION_CLOSE => {
                shared
                    .writer
                    .lock()
                    .await
                    .method(0, CLASS_CONNECTION, CONNECTION_CLOSE_OK, &[])
                    .await?;
                Ok(true)
            }
            CONNECTION_CLOSE_OK => Ok(true),
            _ => {
                send_connection_close(
                    shared,
                    REPLY_COMMAND_INVALID,
                    "unexpected connection method",
                    class_id,
                    method_id,
                )
                .await?;
                Ok(true)
            }
        };
    }

    if channel == 0 {
        send_connection_close(
            shared,
            REPLY_COMMAND_INVALID,
            "method requires a channel",
            class_id,
            method_id,
        )
        .await?;
        return Ok(true);
    }

    match (class_id, method_id) {
        (CLASS_CHANNEL, CHANNEL_OPEN) => {
            if channel > shared.channel_max.load(Ordering::Relaxed) {
                send_connection_close(
                    shared,
                    REPLY_CHANNEL_ERROR,
                    "channel exceeds negotiated maximum",
                    CLASS_CHANNEL,
                    CHANNEL_OPEN,
                )
                .await?;
                return Ok(true);
            }
            let mut parser = Parser::new(&args);
            parser.shortstr()?;
            {
                let mut channels = shared.channels.lock().await;
                if channels.contains_key(&channel) {
                    drop(channels);
                    send_connection_close(
                        shared,
                        REPLY_CHANNEL_ERROR,
                        "channel is already open",
                        CLASS_CHANNEL,
                        CHANNEL_OPEN,
                    )
                    .await?;
                    return Ok(true);
                }
                let generation = shared
                    .next_channel_generation
                    .fetch_add(1, Ordering::Relaxed)
                    + 1;
                channels.insert(channel, ChannelState::new(generation));
            }
            let mut out = Encoder::new();
            out.longstr(b"");
            shared
                .writer
                .lock()
                .await
                .method(channel, CLASS_CHANNEL, CHANNEL_OPEN_OK, &out.finish())
                .await?;
            Ok(false)
        }
        (CLASS_CHANNEL, CHANNEL_CLOSE) => {
            let mut parser = Parser::new(&args);
            let code = parser.u16()?;
            let text = parser.shortstr()?;
            let _ = (code, text);
            close_channel(shared, channel, None).await?;
            shared
                .writer
                .lock()
                .await
                .method(channel, CLASS_CHANNEL, CHANNEL_CLOSE_OK, &[])
                .await?;
            Ok(false)
        }
        (CLASS_CHANNEL, CHANNEL_CLOSE_OK) => {
            remove_channel(shared, channel).await;
            Ok(false)
        }
        (CLASS_CHANNEL, CHANNEL_FLOW) => {
            let mut parser = Parser::new(&args);
            let active = parser.u8()? & 1;
            let mut out = Encoder::new();
            out.u8(active);
            shared
                .writer
                .lock()
                .await
                .method(channel, CLASS_CHANNEL, CHANNEL_FLOW_OK, &out.finish())
                .await?;
            Ok(false)
        }
        (CLASS_QUEUE, QUEUE_DECLARE) => {
            handle_queue_declare(shared, channel, &args).await?;
            Ok(false)
        }
        (CLASS_QUEUE, QUEUE_BIND) => {
            let mut parser = Parser::new(&args);
            let _reserved = parser.short()?;
            let queue = parser.shortstr()?;
            let exchange = parser.shortstr()?;
            let _routing_key = parser.shortstr()?;
            let no_wait = parser.u8().map(|bits| bits & 1 != 0).unwrap_or(false);
            if !require_access(
                shared,
                channel,
                Permission::Inspect,
                &queue,
                class_id,
                method_id,
            )
            .await?
            {
                return Ok(false);
            }
            if !exchange.is_empty() {
                channel_error(
                    shared,
                    channel,
                    REPLY_NOT_IMPLEMENTED,
                    "named exchanges are not supported",
                    class_id,
                    method_id,
                )
                .await?;
                return Ok(false);
            }
            if !no_wait {
                let mut out = Encoder::new();
                out.shortstr(&queue);
                shared
                    .writer
                    .lock()
                    .await
                    .method(channel, CLASS_QUEUE, QUEUE_BIND_OK, &out.finish())
                    .await?;
            }
            Ok(false)
        }
        (CLASS_QUEUE, QUEUE_UNBIND) => {
            let mut parser = Parser::new(&args);
            let _reserved = parser.short()?;
            let queue = parser.shortstr()?;
            if !require_access(
                shared,
                channel,
                Permission::Inspect,
                &queue,
                class_id,
                method_id,
            )
            .await?
            {
                return Ok(false);
            }
            let mut out = Encoder::new();
            out.shortstr(&queue);
            shared
                .writer
                .lock()
                .await
                .method(channel, CLASS_QUEUE, QUEUE_UNBIND_OK, &out.finish())
                .await?;
            Ok(false)
        }
        (CLASS_QUEUE, QUEUE_PURGE) => {
            let mut parser = Parser::new(&args);
            let _reserved = parser.short()?;
            let queue = parser.shortstr()?;
            let no_wait = parser.u8().map(|bits| bits & 1 != 0).unwrap_or(false);
            if !require_access(
                shared,
                channel,
                Permission::Purge,
                &queue,
                class_id,
                method_id,
            )
            .await?
            {
                return Ok(false);
            }
            let ante = shared.ante.clone();
            let count = match blocking(&shared.drain, move || ante.purge(&queue)).await {
                Ok(count) => count,
                Err(err) => {
                    channel_error(
                        shared,
                        channel,
                        map_error(&err),
                        &err.to_string(),
                        class_id,
                        method_id,
                    )
                    .await?;
                    return Ok(false);
                }
            };
            if !no_wait {
                let mut out = Encoder::new();
                out.u32(count as u32);
                shared
                    .writer
                    .lock()
                    .await
                    .method(channel, CLASS_QUEUE, QUEUE_PURGE_OK, &out.finish())
                    .await?;
            }
            Ok(false)
        }
        (CLASS_QUEUE, QUEUE_DELETE) => {
            let mut parser = Parser::new(&args);
            let _reserved = parser.short()?;
            let queue = parser.shortstr()?;
            let bits = parser.u8()?;
            parser.finish()?;
            let if_unused = bits & 1 != 0;
            let if_empty = bits & 2 != 0;
            let no_wait = bits & 4 != 0;
            if !require_access(
                shared,
                channel,
                Permission::Delete,
                &queue,
                class_id,
                method_id,
            )
            .await?
            {
                return Ok(false);
            }

            let lifecycle = shared.queue_locks.get(&queue);
            let lifecycle_guard = lifecycle.lock().await;

            let ante = shared.ante.clone();
            let queue_for_call = queue.clone();
            let current_id =
                match blocking(&shared.drain, move || ante.queue_id(&queue_for_call)).await {
                    Ok(Some(id)) => id,
                    Ok(None) => {
                        drop(lifecycle_guard);
                        channel_error(
                            shared,
                            channel,
                            REPLY_NOT_FOUND,
                            "queue not found",
                            class_id,
                            method_id,
                        )
                        .await?;
                        return Ok(false);
                    }
                    Err(err) => {
                        drop(lifecycle_guard);
                        channel_error(
                            shared,
                            channel,
                            map_error(&err),
                            &err.to_string(),
                            class_id,
                            method_id,
                        )
                        .await?;
                        return Ok(false);
                    }
                };

            if if_unused && shared.registry.in_use(&queue, current_id) {
                drop(lifecycle_guard);
                channel_error(
                    shared,
                    channel,
                    REPLY_PRECONDITION_FAILED,
                    "queue is in use",
                    class_id,
                    method_id,
                )
                .await?;
                return Ok(false);
            }

            let ante = shared.ante.clone();
            let queue_for_call = queue.clone();
            let count = if if_empty {
                match blocking(&shared.drain, move || {
                    ante.delete_queue_if_empty(&queue_for_call)
                })
                .await
                {
                    Ok(Some(count)) => count,
                    Ok(None) => {
                        channel_error(
                            shared,
                            channel,
                            REPLY_PRECONDITION_FAILED,
                            "queue is not empty",
                            class_id,
                            method_id,
                        )
                        .await?;
                        return Ok(false);
                    }
                    Err(err) => {
                        channel_error(
                            shared,
                            channel,
                            map_error(&err),
                            &err.to_string(),
                            class_id,
                            method_id,
                        )
                        .await?;
                        return Ok(false);
                    }
                }
            } else {
                match blocking(&shared.drain, move || ante.delete_queue(&queue_for_call)).await {
                    Ok(count) => count,
                    Err(err) => {
                        channel_error(
                            shared,
                            channel,
                            map_error(&err),
                            &err.to_string(),
                            class_id,
                            method_id,
                        )
                        .await?;
                        return Ok(false);
                    }
                }
            };

            // End every consumer bound to the deleted queue across all
            // connections, then wait for its in-flight batch and notify the
            // client. Holding the lifecycle lock prevents a re-subscribe from
            // slipping in before cancellation completes.
            let registrations = shared.registry.take_by_queue(&queue);
            cancel_registrations(registrations).await;
            shared
                .channel_registry
                .drop_outstanding_for_queue(current_id)
                .await;
            drop(lifecycle_guard);

            if !no_wait {
                let mut out = Encoder::new();
                out.u32(count as u32);
                shared
                    .writer
                    .lock()
                    .await
                    .method(channel, CLASS_QUEUE, QUEUE_DELETE_OK, &out.finish())
                    .await?;
            }
            Ok(false)
        }
        (CLASS_EXCHANGE, EXCHANGE_DECLARE) => {
            channel_error(
                shared,
                channel,
                REPLY_NOT_IMPLEMENTED,
                "named exchanges are not supported; use the default exchange",
                class_id,
                method_id,
            )
            .await?;
            Ok(false)
        }
        (CLASS_EXCHANGE, EXCHANGE_DELETE) => {
            shared
                .writer
                .lock()
                .await
                .method(channel, CLASS_EXCHANGE, EXCHANGE_DELETE_OK, &[])
                .await?;
            Ok(false)
        }
        (CLASS_EXCHANGE, EXCHANGE_BIND) => {
            shared
                .writer
                .lock()
                .await
                .method(channel, CLASS_EXCHANGE, EXCHANGE_BIND_OK, &[])
                .await?;
            Ok(false)
        }
        (CLASS_BASIC, BASIC_QOS) => {
            let mut parser = Parser::new(&args);
            let size = parser.u32()?;
            let prefetch = parser.u16()?;
            let global = parser.u8()? & 1 != 0;
            if size != 0 {
                channel_error(
                    shared,
                    channel,
                    REPLY_NOT_IMPLEMENTED,
                    "byte QoS is unsupported",
                    CLASS_BASIC,
                    BASIC_QOS,
                )
                .await?;
                return Ok(false);
            }
            if let Some(state) = shared.channels.lock().await.get_mut(&channel) {
                if global {
                    state.global_prefetch = prefetch;
                } else {
                    state.prefetch = prefetch;
                }
            }
            shared
                .writer
                .lock()
                .await
                .method(channel, CLASS_BASIC, BASIC_QOS_OK, &[])
                .await?;
            Ok(false)
        }
        (CLASS_BASIC, BASIC_CONSUME) => {
            handle_consume(shared, channel, &args).await?;
            Ok(false)
        }
        (CLASS_BASIC, BASIC_CANCEL) => {
            handle_cancel(shared, channel, &args).await?;
            Ok(false)
        }
        (CLASS_BASIC, BASIC_PUBLISH) => {
            let mut parser = Parser::new(&args);
            let _reserved = parser.short()?;
            let exchange = parser.shortstr()?;
            let routing_key = parser.shortstr()?;
            let bits = parser.u8()?;
            parser.finish()?;
            let mandatory = bits & 1 != 0;
            let mut channels = shared.channels.lock().await;
            let Some(state) = channels.get_mut(&channel) else {
                drop(channels);
                channel_error(
                    shared,
                    channel,
                    REPLY_COMMAND_INVALID,
                    "unknown channel",
                    class_id,
                    method_id,
                )
                .await?;
                return Ok(false);
            };
            if state.pending.is_some() {
                drop(channels);
                send_connection_close(
                    shared,
                    REPLY_UNEXPECTED_FRAME,
                    "publish already in progress",
                    class_id,
                    method_id,
                )
                .await?;
                return Ok(true);
            }
            state.pending = Some(PendingPublish {
                exchange,
                routing_key,
                mandatory,
                header_received: false,
                body_size: 0,
                properties: BasicProperties::default(),
                body: Vec::new(),
            });
            Ok(false)
        }
        (CLASS_BASIC, BASIC_GET) => {
            handle_get(shared, channel, &args).await?;
            Ok(false)
        }
        (CLASS_BASIC, BASIC_ACK) => {
            handle_ack(shared, channel, &args).await?;
            Ok(false)
        }
        (CLASS_BASIC, BASIC_REJECT) => {
            handle_reject(shared, channel, &args, false).await?;
            Ok(false)
        }
        (CLASS_BASIC, BASIC_NACK) => {
            handle_reject(shared, channel, &args, true).await?;
            Ok(false)
        }
        (CLASS_BASIC, BASIC_RECOVER) => {
            if !recover_channel(shared, channel).await? {
                return Ok(false);
            }
            shared
                .writer
                .lock()
                .await
                .method(channel, CLASS_BASIC, BASIC_RECOVER_OK, &[])
                .await?;
            Ok(false)
        }
        (CLASS_CONFIRM, CONFIRM_SELECT) => {
            let mut parser = Parser::new(&args);
            let nowait = parser.u8().map(|bits| bits & 1 != 0).unwrap_or(false);
            if let Some(state) = shared.channels.lock().await.get_mut(&channel) {
                state.confirm = true;
                state.next_publish = 1;
            }
            if !nowait {
                shared
                    .writer
                    .lock()
                    .await
                    .method(channel, CLASS_CONFIRM, CONFIRM_SELECT_OK, &[])
                    .await?;
            }
            Ok(false)
        }
        _ => {
            channel_error(
                shared,
                channel,
                REPLY_NOT_IMPLEMENTED,
                "method not implemented",
                class_id,
                method_id,
            )
            .await?;
            Ok(false)
        }
    }
}

async fn handle_queue_declare(shared: &Arc<Shared>, channel: u16, args: &[u8]) -> io::Result<()> {
    let mut parser = Parser::new(args);
    let _reserved = parser.short()?;
    let queue = parser.shortstr()?;
    let bits = parser.u8()?;
    let arguments = parser.longstr_bytes()?;
    let passive = bits & 1 != 0;
    let no_wait = bits & 0x10 != 0;

    if bits & 0x0c != 0 || !arguments.is_empty() {
        channel_error(
            shared,
            channel,
            REPLY_NOT_IMPLEMENTED,
            "exclusive/auto-delete queues and queue arguments are unsupported",
            CLASS_QUEUE,
            QUEUE_DECLARE,
        )
        .await?;
        return Ok(());
    }

    if queue.is_empty() {
        channel_error(
            shared,
            channel,
            REPLY_NOT_IMPLEMENTED,
            "server-named queues are not supported",
            CLASS_QUEUE,
            QUEUE_DECLARE,
        )
        .await?;
        return Ok(());
    }
    let permission = if passive {
        Permission::Inspect
    } else {
        Permission::Declare
    };
    if !require_access(
        shared,
        channel,
        permission,
        &queue,
        CLASS_QUEUE,
        QUEUE_DECLARE,
    )
    .await?
    {
        return Ok(());
    }

    let ante = shared.ante.clone();
    let queue_for_call = queue.clone();
    let result = blocking(&shared.drain, move || {
        if !ante.queue_exists(&queue_for_call)? {
            if passive {
                return Err(Error::QueueNotFound(queue_for_call));
            }
            ante.declare_queue(&queue_for_call)?;
        }
        let info = ante.info(&queue_for_call)?;
        let queue_id = ante.queue_id(&queue_for_call)?;
        Ok((info, queue_id))
    })
    .await;

    let (info, queue_id) = match result {
        Ok(value) => value,
        Err(err) => {
            channel_error(
                shared,
                channel,
                map_error(&err),
                &err.to_string(),
                CLASS_QUEUE,
                QUEUE_DECLARE,
            )
            .await?;
            return Ok(());
        }
    };

    if !no_wait {
        let consumers = queue_id
            .map(|id| shared.registry.count_for_queue(&queue, id))
            .unwrap_or(0);
        let mut out = Encoder::new();
        out.shortstr(&queue)
            .u32(info.ready as u32)
            .u32(consumers as u32);
        shared
            .writer
            .lock()
            .await
            .method(channel, CLASS_QUEUE, QUEUE_DECLARE_OK, &out.finish())
            .await?;
    }
    Ok(())
}

async fn handle_consume(shared: &Arc<Shared>, channel: u16, args: &[u8]) -> io::Result<()> {
    let mut parser = Parser::new(args);
    let _reserved = parser.short()?;
    let queue = parser.shortstr()?;
    let requested_tag = parser.shortstr()?;
    let bits = parser.u8()?;
    let arguments = parser.longstr_bytes()?;
    if bits & 0x04 != 0 || !arguments.is_empty() {
        channel_error(
            shared,
            channel,
            REPLY_NOT_IMPLEMENTED,
            "exclusive consumers and consumer arguments are unsupported",
            CLASS_BASIC,
            BASIC_CONSUME,
        )
        .await?;
        return Ok(());
    }
    let no_ack = bits & 0x02 != 0;
    let no_wait = bits & 0x08 != 0;
    if !require_access(
        shared,
        channel,
        Permission::Consume,
        &queue,
        CLASS_BASIC,
        BASIC_CONSUME,
    )
    .await?
    {
        return Ok(());
    }

    let lifecycle = shared.queue_locks.get(&queue);
    let lifecycle_guard = lifecycle.lock().await;

    let ante = shared.ante.clone();
    let queue_for_call = queue.clone();
    let queue_id = match blocking(&shared.drain, move || ante.queue_id(&queue_for_call)).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            channel_error(
                shared,
                channel,
                REPLY_NOT_FOUND,
                "queue not found",
                CLASS_BASIC,
                BASIC_CONSUME,
            )
            .await?;
            return Ok(());
        }
        Err(err) => {
            channel_error(
                shared,
                channel,
                map_error(&err),
                &err.to_string(),
                CLASS_BASIC,
                BASIC_CONSUME,
            )
            .await?;
            return Ok(());
        }
    };
    let Some(delivery_lock) = channel_delivery_lock(&shared.channels, channel).await else {
        drop(lifecycle_guard);
        channel_error(
            shared,
            channel,
            REPLY_COMMAND_INVALID,
            "unknown channel",
            CLASS_BASIC,
            BASIC_CONSUME,
        )
        .await?;
        return Ok(());
    };
    let delivery_guard = delivery_lock.lock().await;
    let subscription = Arc::new(Subscription::new());
    let registration_info = {
        let mut channels = shared.channels.lock().await;
        let Some(state) = channels.get_mut(&channel) else {
            drop(channels);
            drop(delivery_guard);
            drop(lifecycle_guard);
            channel_error(
                shared,
                channel,
                REPLY_COMMAND_INVALID,
                "unknown channel",
                CLASS_BASIC,
                BASIC_CONSUME,
            )
            .await?;
            return Ok(());
        };
        if !requested_tag.is_empty() && state.consumers.contains_key(&requested_tag) {
            None
        } else {
            let tag = if requested_tag.is_empty() {
                loop {
                    let tag = format!("ctag-{channel}-{}", state.next_consumer);
                    state.next_consumer += 1;
                    if !state.consumers.contains_key(&tag) {
                        break tag;
                    }
                }
            } else {
                requested_tag
            };
            let registration = Registration {
                queue: queue.clone(),
                queue_id,
                subscription: subscription.clone(),
                delivery_lock: delivery_lock.clone(),
                writer: shared.writer.clone(),
                channels: shared.channels.clone(),
                channel_registry: shared.channel_registry.clone(),
                channel,
                consumer_tag: tag.clone(),
                notify: shared.client_cancel_notify.load(Ordering::Relaxed),
            };
            let registration_id = shared.registry.register(registration);
            state.consumers.insert(
                tag.clone(),
                ConsumerEntry {
                    subscription: subscription.clone(),
                },
            );
            shared
                .registrations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(registration_id);
            Some((tag, state.generation, state.prefetch))
        }
    };
    let Some((consumer_tag, generation, prefetch)) = registration_info else {
        drop(delivery_guard);
        drop(lifecycle_guard);
        channel_error(
            shared,
            channel,
            REPLY_NOT_ALLOWED,
            "consumer tag already in use",
            CLASS_BASIC,
            BASIC_CONSUME,
        )
        .await?;
        return Ok(());
    };

    if !no_wait {
        let mut out = Encoder::new();
        out.shortstr(&consumer_tag);
        let result = shared
            .writer
            .lock()
            .await
            .method(channel, CLASS_BASIC, BASIC_CONSUME_OK, &out.finish())
            .await;
        if let Err(err) = result {
            shared.registry.unregister(subscription.id());
            shared
                .registrations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&subscription.id());
            let mut channels = shared.channels.lock().await;
            if let Some(state) = channels.get_mut(&channel) {
                state.consumers.remove(&consumer_tag);
            }
            drop(channels);
            drop(delivery_guard);
            drop(lifecycle_guard);
            return Err(err);
        }
    }
    subscription.active.store(true, Ordering::Release);
    drop(delivery_guard);
    drop(lifecycle_guard);

    let registration = Registration {
        queue: queue.clone(),
        queue_id,
        subscription: subscription.clone(),
        delivery_lock: delivery_lock.clone(),
        writer: shared.writer.clone(),
        channels: shared.channels.clone(),
        channel_registry: shared.channel_registry.clone(),
        channel,
        consumer_tag: consumer_tag.clone(),
        notify: shared.client_cancel_notify.load(Ordering::Relaxed),
    };
    let cleanup = registration.clone();
    let registration_ante = shared.ante.clone();
    let registration_id = subscription.id();
    let shutdown = shared.shutdown.clone();
    let lease_ms = shared.lease_ms;
    let registry = shared.registry.clone();
    let registrations = shared.registrations.clone();
    let consumer_drain = shared.drain.clone();
    let consumer_shared = shared.clone();
    let handle = tokio::spawn(async move {
        let context = ConsumerContext {
            shared: consumer_shared,
            ante: registration_ante.clone(),
            registration,
            generation,
            prefetch,
            no_ack,
            shutdown: shutdown.clone(),
            lease_ms,
            drain: consumer_drain,
        };
        let identity_lost = consumer_loop(context).await.unwrap_or(false);
        registry.unregister(registration_id);
        registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&registration_id);
        let shutdown_now = shutdown.load(Ordering::Relaxed);
        let cancelled = cleanup.subscription.cancel.load(Ordering::Relaxed);
        if shutdown_now {
            remove_consumer_tag(&cleanup).await;
        } else if should_notify(cancelled, identity_lost, false) {
            notify_if_owned(&cleanup).await;
        } else {
            // A write failure must not release valid in-flight records; the
            // connection teardown requeues them. Only the tag is retired.
            remove_consumer_tag(&cleanup).await;
        }
    });
    shared.tasks.push(handle);
    Ok(())
}

struct ConsumerContext {
    shared: Arc<Shared>,
    ante: Arc<Ante>,
    registration: Registration,
    generation: u64,
    prefetch: u16,
    no_ack: bool,
    shutdown: Arc<AtomicBool>,
    lease_ms: u64,
    drain: Drain,
}

/// Reserve prefetch capacity on the channel instance this consumer was
/// registered on. A closed and reopened channel number must not inherit work
/// from the old task.
async fn reserve(
    channels: &ChannelMap,
    channel: u16,
    generation: u64,
    consumer_id: u64,
    prefetch: u16,
    no_ack: bool,
) -> Option<usize> {
    let mut guard = channels.lock().await;
    let state = guard.get_mut(&channel)?;
    if state.generation != generation {
        return None;
    }
    state.prune_expired(now_ms());
    let available = state.available(consumer_id, prefetch, no_ack).min(MAX_TAKE);
    if available > 0 {
        state.reserved += available;
    }
    Some(available)
}

/// Drop outstanding entries whose queue identity no longer exists. Native/core
/// deletes are invisible to the AMQP registry, so a channel with no active
/// subscription (or a cancelled one) reconciles lazily before granting
/// prefetch capacity. Tokens of a deleted identity are not requeued: the
/// engine removed those in-flight records with the queue.
async fn reconcile_outstanding(
    channels: &ChannelMap,
    ante: &Arc<Ante>,
    drain: &Drain,
    channel: u16,
    generation: u64,
) -> bool {
    let pairs: Vec<(String, u64)> = {
        let mut guard = channels.lock().await;
        let Some(state) = guard.get_mut(&channel) else {
            return false;
        };
        state.prune_expired(now_ms());
        let mut pairs: Vec<(String, u64)> = state
            .outstanding
            .values()
            .map(|entry| (entry.queue.clone(), entry.queue_id))
            .collect();
        pairs.sort();
        pairs.dedup();
        pairs
    };
    if pairs.is_empty() {
        return false;
    }

    let mut gone: Vec<(String, u64)> = Vec::new();
    for (queue, expected) in pairs {
        let engine = ante.clone();
        let queue_for_call = queue.clone();
        match blocking(drain, move || engine.queue_id(&queue_for_call)).await {
            Ok(Some(current)) if current == expected => {}
            Ok(_) => gone.push((queue, expected)),
            Err(err) => eprintln!("ante-amqp: identity check failed: {err}"),
        }
    }
    if gone.is_empty() {
        return false;
    }

    remove_stale_outstanding(channels, channel, generation, &gone).await
}

/// Remove stale tags only while holding the channel's delivery lock, so an
/// in-flight batch cannot keep writing after its tags disappear.
async fn remove_stale_outstanding(
    channels: &ChannelMap,
    channel: u16,
    generation: u64,
    gone: &[(String, u64)],
) -> bool {
    let Some(lock) = channel_delivery_lock(channels, channel).await else {
        return false;
    };
    let _delivery_guard = lock.lock().await;
    let mut guard = channels.lock().await;
    let Some(state) = guard.get_mut(&channel) else {
        return false;
    };
    if state.generation != generation {
        return false;
    }
    let before = state.outstanding.len();
    state.outstanding.retain(|_, entry| {
        !gone
            .iter()
            .any(|(queue, id)| *id == entry.queue_id && *queue == entry.queue)
    });
    state.outstanding.len() != before
}

async fn consumer_loop(context: ConsumerContext) -> io::Result<bool> {
    let ConsumerContext {
        shared,
        ante,
        registration,
        generation,
        prefetch,
        no_ack,
        shutdown,
        lease_ms,
        drain,
    } = context;
    let Registration {
        queue,
        queue_id,
        subscription,
        writer,
        channels,
        channel,
        consumer_tag,
        ..
    } = registration;
    let ante = ante.clone();
    let mut last_reconcile: Option<tokio::time::Instant> = None;
    let mut last_identity: Option<tokio::time::Instant> = None;
    loop {
        if subscription.cancel.load(Ordering::Relaxed) || shutdown.load(Ordering::Relaxed) {
            return Ok(false);
        }

        let Some(to_take) = reserve(
            &channels,
            channel,
            generation,
            subscription.id(),
            prefetch,
            no_ack,
        )
        .await
        else {
            return Ok(false);
        };

        if to_take == 0 {
            // Native deletes leave no registry event. Reconcile lazily (and at
            // a bounded rate) before deciding the prefetch window is full.
            let now_instant = tokio::time::Instant::now();
            if last_reconcile
                .is_none_or(|last| now_instant.duration_since(last) >= RECONCILE_INTERVAL)
            {
                last_reconcile = Some(now_instant);
                if reconcile_outstanding(&channels, &ante, &drain, channel, generation).await {
                    continue;
                }
            }
            if last_identity
                .is_none_or(|last| now_instant.duration_since(last) >= IDENTITY_INTERVAL)
            {
                last_identity = Some(now_instant);
                let engine = ante.clone();
                let queue_for_call = queue.clone();
                match blocking(&drain, move || engine.queue_id(&queue_for_call)).await {
                    Ok(Some(current)) if current == queue_id => {}
                    Ok(_) => {
                        release_reservation(&channels, channel, generation, to_take).await;
                        return Ok(true);
                    }
                    Err(_) => {}
                }
            }
            tokio::time::sleep(CONSUMER_POLL).await;
            continue;
        }

        let engine = ante.clone();
        let queue_for_call = queue.clone();
        let result = blocking(&drain, move || {
            engine.take_if_queue_id(&queue_for_call, queue_id, to_take, lease_ms)
        })
        .await;
        let deliveries = match result {
            Ok(Some(deliveries)) => deliveries,
            Ok(None) => {
                release_reservation(&channels, channel, generation, to_take).await;
                return Ok(true);
            }
            Err(err) => {
                release_reservation(&channels, channel, generation, to_take).await;
                eprintln!("ante-amqp: consumer take failed: {err}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        if deliveries.is_empty() {
            release_reservation(&channels, channel, generation, to_take).await;
            tokio::time::sleep(CONSUMER_POLL).await;
            continue;
        }

        if subscription.cancel.load(Ordering::Relaxed) || shutdown.load(Ordering::Relaxed) {
            release_reservation(&channels, channel, generation, to_take).await;
            requeue_tokens(&ante, &drain, deliveries.into_iter().map(|d| d.token)).await;
            return Ok(false);
        }

        // Serialize tag assignment + frame writes on this channel. Everything
        // inserted into `outstanding` is ackable only after `sent` is set.
        let Some(delivery_lock) = channel_delivery_lock(&channels, channel).await else {
            release_reservation(&channels, channel, generation, to_take).await;
            requeue_tokens(&ante, &drain, deliveries.into_iter().map(|d| d.token)).await;
            return Ok(false);
        };
        let delivery_guard = delivery_lock.lock().await;

        if subscription.cancel.load(Ordering::Relaxed) || shutdown.load(Ordering::Relaxed) {
            release_reservation(&channels, channel, generation, to_take).await;
            requeue_tokens(&ante, &drain, deliveries.into_iter().map(|d| d.token)).await;
            return Ok(false);
        }

        let mut entries = Vec::with_capacity(deliveries.len());
        {
            let mut guard = channels.lock().await;
            match guard.get_mut(&channel) {
                Some(state) if state.generation == generation => {
                    state.reserved = state.reserved.saturating_sub(to_take);
                    for delivery in deliveries {
                        let tag = state.next_tag;
                        state.next_tag += 1;
                        state.outstanding.insert(
                            tag,
                            Outstanding {
                                token: delivery.token.clone(),
                                deadline_ms: delivery.deadline_ms,
                                queue: queue.clone(),
                                queue_id: delivery.queue_id,
                                sent: false,
                                consumer_id: if no_ack {
                                    None
                                } else {
                                    Some(subscription.id())
                                },
                            },
                        );
                        entries.push((tag, delivery));
                    }
                }
                _ => {
                    drop(guard);
                    requeue_tokens(&ante, &drain, deliveries.into_iter().map(|d| d.token)).await;
                    return Ok(false);
                }
            }
        }

        let mut entries = entries.into_iter();
        while let Some((tag, delivery)) = entries.next() {
            if shutdown.load(Ordering::Relaxed) {
                remove_and_requeue(&channels, &ante, &drain, channel, tag).await;
                for (tag, _) in entries {
                    remove_and_requeue(&channels, &ante, &drain, channel, tag).await;
                }
                return Ok(false);
            }
            let properties = BasicProperties {
                content_type: delivery.content_type.clone(),
                delivery_mode: Some(2),
                correlation_id: delivery.correlation_id.clone(),
                reply_to: delivery.reply_to.clone(),
                message_id: delivery.application_message_id.clone(),
            };
            let mut args = Encoder::new();
            args.shortstr(&consumer_tag)
                .u64(tag)
                .u8(if delivery.attempts > 1 { 1 } else { 0 })
                .shortstr("")
                .shortstr(&queue);
            let result = {
                let mut writer = writer.lock().await;
                match writer
                    .method(channel, CLASS_BASIC, BASIC_DELIVER, &args.finish())
                    .await
                {
                    Ok(()) => {
                        writer
                            .content(channel, &properties, &delivery.payload)
                            .await
                    }
                    Err(err) => Err(err),
                }
            };
            if let Err(err) = result {
                remove_and_requeue(&channels, &ante, &drain, channel, tag).await;
                for (tag, _) in entries {
                    remove_and_requeue(&channels, &ante, &drain, channel, tag).await;
                }
                return Err(err);
            }

            {
                let mut guard = channels.lock().await;
                if let Some(entry) = guard
                    .get_mut(&channel)
                    .and_then(|state| state.outstanding.get_mut(&tag))
                {
                    entry.sent = true;
                }
            }

            if no_ack {
                let engine = ante.clone();
                let token = delivery.token.clone();
                if let Err(err) = blocking(&drain, move || engine.ack(&token)).await {
                    drop(delivery_guard);
                    channel_error(
                        &shared,
                        channel,
                        map_error(&err),
                        &err.to_string(),
                        CLASS_BASIC,
                        BASIC_CONSUME,
                    )
                    .await?;
                    return Ok(false);
                }
                let mut guard = channels.lock().await;
                if let Some(state) = guard.get_mut(&channel) {
                    state.outstanding.remove(&tag);
                }
            }
        }
    }
}

async fn remove_and_requeue(
    channels: &Arc<Mutex<HashMap<u16, ChannelState>>>,
    ante: &Arc<Ante>,
    drain: &Drain,
    channel: u16,
    tag: u64,
) {
    let token = {
        let mut guard = channels.lock().await;
        guard
            .get_mut(&channel)
            .and_then(|state| state.outstanding.remove(&tag))
            .map(|entry| entry.token)
    };
    if let Some(token) = token {
        requeue_tokens(ante, drain, std::iter::once(token)).await;
    }
}

/// Release a reservation only on the channel instance that made it. A closed
/// and reopened channel number must not inherit the old consumer's count.
async fn release_reservation(channels: &ChannelMap, channel: u16, generation: u64, count: usize) {
    let mut guard = channels.lock().await;
    if let Some(state) = guard.get_mut(&channel) {
        if state.generation == generation {
            state.reserved = state.reserved.saturating_sub(count);
        }
    }
}

async fn requeue_tokens(
    ante: &Arc<Ante>,
    drain: &Drain,
    tokens: impl IntoIterator<Item = DeliveryToken>,
) {
    let tokens: Vec<DeliveryToken> = tokens.into_iter().collect();
    if tokens.is_empty() {
        return;
    }
    let engine = ante.clone();
    let guard = drain.begin();
    let _ = tokio::task::spawn_blocking(move || {
        let _guard = guard;
        for token in tokens {
            engine.requeue(&token)?;
        }
        Ok::<(), Error>(())
    })
    .await;
}

async fn shutdown_consumers(shared: &Arc<Shared>) {
    shared.shutdown.store(true, Ordering::Relaxed);
    let mut handles = AbortOnDrop {
        handles: shared.tasks.take(),
        reaper: shared.tasks.reaper(),
    };
    for handle in &mut handles.handles {
        if tokio::time::timeout(CONSUMER_SHUTDOWN_GRACE, &mut *handle)
            .await
            .is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
    }
}

/// If this connection is aborted mid-shutdown, the remaining consumer tasks
/// must still be cancelled. A plain `Vec<JoinHandle>` would detach them.
struct AbortOnDrop {
    handles: Vec<tokio::task::JoinHandle<()>>,
    reaper: ConsumerReaper,
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        let handles = std::mem::take(&mut self.handles);
        for handle in &handles {
            handle.abort();
        }
        // Handles already awaited by the graceful loop must not be awaited
        // again; only those still pending need the reaper.
        let pending: Vec<_> = handles
            .into_iter()
            .filter(|handle| !handle.is_finished())
            .collect();
        if !pending.is_empty() {
            self.reaper.adopt(pending);
        }
    }
}

async fn handle_cancel(shared: &Arc<Shared>, channel: u16, args: &[u8]) -> io::Result<()> {
    let mut parser = Parser::new(args);
    let consumer_tag = parser.shortstr()?;
    let no_wait = parser.u8().map(|bits| bits & 1 != 0).unwrap_or(false);
    let Some(delivery_lock) = channel_delivery_lock(&shared.channels, channel).await else {
        return Ok(());
    };
    let delivery_guard = delivery_lock.lock().await;
    let subscription = {
        let mut channels = shared.channels.lock().await;
        channels
            .get_mut(&channel)
            .and_then(|state| state.consumers.remove(&consumer_tag))
            .map(|consumer| consumer.subscription)
    };
    if let Some(subscription) = subscription {
        subscription.cancel.store(true, Ordering::Release);
        // The client already knows; no server-side notification is needed.
        subscription.notified.store(true, Ordering::Release);
        let id = subscription.id();
        shared.registry.unregister(id);
        shared
            .registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&id);
    }
    if !no_wait {
        let mut out = Encoder::new();
        out.shortstr(&consumer_tag);
        shared
            .writer
            .lock()
            .await
            .method(channel, CLASS_BASIC, BASIC_CANCEL_OK, &out.finish())
            .await?;
    }
    drop(delivery_guard);
    Ok(())
}

async fn handle_header(shared: &Arc<Shared>, frame: Frame) -> io::Result<bool> {
    let parsed = parse_content_header(&frame.payload)?;
    let max_payload = shared.ante.options().max_payload_bytes;
    if parsed.body_size > max_payload as u64 {
        channel_error(
            shared,
            frame.channel,
            REPLY_PRECONDITION_FAILED,
            "message is too large",
            CLASS_BASIC,
            BASIC_PUBLISH,
        )
        .await?;
        return Ok(false);
    }

    let mut channels = shared.channels.lock().await;
    let Some(state) = channels.get_mut(&frame.channel) else {
        drop(channels);
        send_connection_close(
            shared,
            REPLY_UNEXPECTED_FRAME,
            "content header on unknown channel",
            0,
            0,
        )
        .await?;
        return Ok(true);
    };
    let Some(pending) = state.pending.as_mut() else {
        drop(channels);
        send_connection_close(
            shared,
            REPLY_UNEXPECTED_FRAME,
            "content header without publish",
            0,
            0,
        )
        .await?;
        return Ok(true);
    };
    if pending.header_received {
        drop(channels);
        send_connection_close(
            shared,
            REPLY_UNEXPECTED_FRAME,
            "content header already received",
            0,
            0,
        )
        .await?;
        return Ok(true);
    }
    pending.header_received = true;
    pending.body_size = parsed.body_size;
    pending.properties = parsed.properties;
    if pending.body_size == 0 {
        let pending = state.pending.take().unwrap();
        drop(channels);
        finish_publish(shared, frame.channel, pending).await?;
    }
    Ok(false)
}

async fn handle_body(shared: &Arc<Shared>, frame: Frame) -> io::Result<bool> {
    let mut channels = shared.channels.lock().await;
    let Some(state) = channels.get_mut(&frame.channel) else {
        drop(channels);
        send_connection_close(
            shared,
            REPLY_UNEXPECTED_FRAME,
            "body frame on unknown channel",
            0,
            0,
        )
        .await?;
        return Ok(true);
    };
    let Some(pending) = state.pending.as_mut() else {
        drop(channels);
        send_connection_close(
            shared,
            REPLY_UNEXPECTED_FRAME,
            "body frame without publish",
            0,
            0,
        )
        .await?;
        return Ok(true);
    };
    if !pending.header_received {
        drop(channels);
        send_connection_close(
            shared,
            REPLY_UNEXPECTED_FRAME,
            "body frame before content header",
            0,
            0,
        )
        .await?;
        return Ok(true);
    }
    let exceeds =
        (frame.payload.len() as u64) > pending.body_size.saturating_sub(pending.body.len() as u64);
    if exceeds {
        state.pending = None;
        drop(channels);
        send_connection_close(
            shared,
            REPLY_FRAME_ERROR,
            "body exceeds declared size",
            0,
            0,
        )
        .await?;
        return Ok(true);
    }
    pending.body.extend_from_slice(&frame.payload);
    if pending.body.len() as u64 == pending.body_size {
        let pending = state.pending.take().unwrap();
        drop(channels);
        finish_publish(shared, frame.channel, pending).await?;
    }
    Ok(false)
}

async fn finish_publish(
    shared: &Arc<Shared>,
    channel: u16,
    pending: PendingPublish,
) -> io::Result<()> {
    if !pending.exchange.is_empty() {
        channel_error(
            shared,
            channel,
            REPLY_NOT_IMPLEMENTED,
            "named exchanges are not supported",
            CLASS_BASIC,
            BASIC_PUBLISH,
        )
        .await?;
        return Ok(());
    }
    let queue = pending.routing_key.clone();
    if !require_access(
        shared,
        channel,
        Permission::Publish,
        &queue,
        CLASS_BASIC,
        BASIC_PUBLISH,
    )
    .await?
    {
        return Ok(());
    }
    let ante = shared.ante.clone();
    let queue_for_check = queue.clone();
    let exists = match blocking(&shared.drain, move || ante.queue_exists(&queue_for_check)).await {
        Ok(exists) => exists,
        Err(err) => {
            channel_error(
                shared,
                channel,
                map_error(&err),
                &err.to_string(),
                CLASS_BASIC,
                BASIC_PUBLISH,
            )
            .await?;
            return Ok(());
        }
    };

    if !exists {
        if pending.mandatory {
            let mut args = Encoder::new();
            args.u16(REPLY_NO_ROUTE)
                .shortstr("NO_ROUTE")
                .shortstr(&pending.exchange)
                .shortstr(&queue);
            let mut writer = shared.writer.lock().await;
            writer
                .method(channel, CLASS_BASIC, BASIC_RETURN, &args.finish())
                .await?;
            writer
                .content(channel, &pending.properties, &pending.body)
                .await?;
        }
        return send_confirm(shared, channel, true).await;
    }

    let message = NewMessage {
        payload: pending.body,
        content_type: pending.properties.content_type,
        message_id: pending.properties.message_id,
        correlation_id: pending.properties.correlation_id,
        reply_to: pending.properties.reply_to,
    };
    let ante = shared.ante.clone();
    let result = blocking(&shared.drain, move || ante.publish(&queue, message)).await;
    match result {
        Ok(_) => send_confirm(shared, channel, true).await,
        Err(err) => {
            let code = map_error(&err);
            channel_error(
                shared,
                channel,
                code,
                &err.to_string(),
                CLASS_BASIC,
                BASIC_PUBLISH,
            )
            .await?;
            send_confirm(shared, channel, false).await
        }
    }
}

async fn send_confirm(shared: &Arc<Shared>, channel: u16, ok: bool) -> io::Result<()> {
    let tag = {
        let mut channels = shared.channels.lock().await;
        match channels.get_mut(&channel) {
            Some(state) if state.confirm => {
                let tag = state.next_publish;
                state.next_publish += 1;
                Some(tag)
            }
            _ => None,
        }
    };
    let Some(tag) = tag else { return Ok(()) };
    let mut args = Encoder::new();
    args.u64(tag).u8(0);
    let method = if ok { BASIC_ACK } else { BASIC_NACK };
    shared
        .writer
        .lock()
        .await
        .method(channel, CLASS_BASIC, method, &args.finish())
        .await
}

async fn handle_get(shared: &Arc<Shared>, channel: u16, args: &[u8]) -> io::Result<()> {
    let mut parser = Parser::new(args);
    let _reserved = parser.short()?;
    let queue = parser.shortstr()?;
    let no_ack = parser.u8().map(|bits| bits & 1 != 0).unwrap_or(false);
    if !require_access(
        shared,
        channel,
        Permission::Consume,
        &queue,
        CLASS_BASIC,
        BASIC_GET,
    )
    .await?
    {
        return Ok(());
    }

    // Serialize with queue deletion: the take and the outstanding insertion
    // must be one lifecycle step, otherwise cleanup can run in between.
    let lifecycle = shared.queue_locks.get(&queue);
    let lifecycle_guard = lifecycle.lock().await;

    let ante = shared.ante.clone();
    let queue_for_call = queue.clone();
    let lease_ms = shared.lease_ms;
    let result = blocking(&shared.drain, move || {
        let deliveries = ante.take(&queue_for_call, 1, lease_ms)?;
        let info = ante.info(&queue_for_call)?;
        Ok((deliveries, info))
    })
    .await;

    let (deliveries, info) = match result {
        Ok(value) => value,
        Err(err) => {
            channel_error(
                shared,
                channel,
                map_error(&err),
                &err.to_string(),
                CLASS_BASIC,
                BASIC_GET,
            )
            .await?;
            return Ok(());
        }
    };

    let Some(delivery) = deliveries.into_iter().next() else {
        let mut out = Encoder::new();
        out.shortstr("");
        shared
            .writer
            .lock()
            .await
            .method(channel, CLASS_BASIC, BASIC_GET_EMPTY, &out.finish())
            .await?;
        return Ok(());
    };

    let Some(delivery_lock) = channel_delivery_lock(&shared.channels, channel).await else {
        requeue_tokens(
            &shared.ante,
            &shared.drain,
            std::iter::once(delivery.token.clone()),
        )
        .await;
        return Ok(());
    };
    let delivery_guard = delivery_lock.lock().await;
    let tag = {
        let mut channels = shared.channels.lock().await;
        let Some(state) = channels.get_mut(&channel) else {
            drop(channels);
            drop(delivery_guard);
            requeue_tokens(
                &shared.ante,
                &shared.drain,
                std::iter::once(delivery.token.clone()),
            )
            .await;
            return Ok(());
        };
        let tag = state.next_tag;
        state.next_tag += 1;
        state.outstanding.insert(
            tag,
            Outstanding {
                token: delivery.token.clone(),
                deadline_ms: delivery.deadline_ms,
                queue: queue.clone(),
                queue_id: delivery.queue_id,
                sent: false,
                consumer_id: None,
            },
        );
        tag
    };
    // The delivery lock is held through the response write, so cleanup cannot
    // drop this tag early. Releasing the lifecycle lock here keeps a stalled
    // reader from blocking queue.delete indefinitely.
    drop(lifecycle_guard);

    let properties = BasicProperties {
        content_type: delivery.content_type.clone(),
        delivery_mode: Some(2),
        correlation_id: delivery.correlation_id.clone(),
        reply_to: delivery.reply_to.clone(),
        message_id: delivery.application_message_id.clone(),
    };
    let mut out = Encoder::new();
    out.u64(tag)
        .u8(if delivery.attempts > 1 { 1 } else { 0 })
        .shortstr("")
        .shortstr(&queue)
        .u32(info.ready as u32);
    {
        let mut writer = shared.writer.lock().await;
        writer
            .method(channel, CLASS_BASIC, BASIC_GET_OK, &out.finish())
            .await?;
        writer
            .content(channel, &properties, &delivery.payload)
            .await?;
    }
    {
        let mut channels = shared.channels.lock().await;
        if let Some(entry) = channels
            .get_mut(&channel)
            .and_then(|state| state.outstanding.get_mut(&tag))
        {
            entry.sent = true;
        }
    }
    drop(delivery_guard);

    if no_ack {
        let ante = shared.ante.clone();
        let token = delivery.token.clone();
        if let Err(err) = blocking(&shared.drain, move || ante.ack(&token)).await {
            return channel_error(
                shared,
                channel,
                map_error(&err),
                &err.to_string(),
                CLASS_BASIC,
                BASIC_GET,
            )
            .await;
        }
        let mut channels = shared.channels.lock().await;
        if let Some(state) = channels.get_mut(&channel) {
            state.outstanding.remove(&tag);
        }
    }
    Ok(())
}

async fn handle_ack(shared: &Arc<Shared>, channel: u16, args: &[u8]) -> io::Result<()> {
    let mut parser = Parser::new(args);
    let tag = parser.u64()?;
    let multiple = parser.u8().map(|b| b & 1 != 0).unwrap_or(false);

    let Some(delivery_lock) = channel_delivery_lock(&shared.channels, channel).await else {
        return Ok(());
    };
    let delivery_guard = delivery_lock.lock().await;
    let keys = {
        let mut channels = shared.channels.lock().await;
        let Some(state) = channels.get_mut(&channel) else {
            return Ok(());
        };
        state.prune_expired(now_ms());
        settle_keys(state, tag, multiple)
    };
    let Some(keys) = keys else {
        drop(delivery_guard);
        channel_error(
            shared,
            channel,
            REPLY_PRECONDITION_FAILED,
            "unknown delivery tag",
            CLASS_BASIC,
            BASIC_ACK,
        )
        .await?;
        return Ok(());
    };
    if keys.is_empty() {
        return Ok(());
    }
    let denied = {
        let channels = shared.channels.lock().await;
        channels.get(&channel).is_some_and(|state| {
            keys.iter().any(|key| {
                state
                    .outstanding
                    .get(key)
                    .is_some_and(|entry| !authorized(shared, Permission::Consume, &entry.queue))
            })
        })
    };
    if denied {
        drop(delivery_guard);
        channel_error(
            shared,
            channel,
            REPLY_ACCESS_REFUSED,
            "permission denied: consume on outstanding queue",
            CLASS_BASIC,
            BASIC_ACK,
        )
        .await?;
        return Ok(());
    }
    let tokens = {
        let channels = shared.channels.lock().await;
        let Some(state) = channels.get(&channel) else {
            return Ok(());
        };
        keys.iter()
            .filter_map(|key| state.outstanding.get(key))
            .map(|entry| entry.token.clone())
            .collect::<Vec<_>>()
    };
    let ante = shared.ante.clone();
    if let Err(err) = blocking(&shared.drain, move || ante.ack_batch(&tokens)).await {
        drop(delivery_guard);
        // Keep ownership until the commit succeeds. Channel teardown can then
        // requeue the claims if settlement failed instead of losing their tags.
        return channel_error(
            shared,
            channel,
            map_error(&err),
            &err.to_string(),
            CLASS_BASIC,
            BASIC_ACK,
        )
        .await;
    }
    if let Some(state) = shared.channels.lock().await.get_mut(&channel) {
        remove_keys(state, &keys);
    }
    Ok(())
}

fn settle_keys(state: &ChannelState, tag: u64, multiple: bool) -> Option<Vec<u64>> {
    if multiple {
        if tag == 0 {
            Some(
                state
                    .outstanding
                    .iter()
                    .filter(|(_, entry)| entry.sent)
                    .map(|(key, _)| *key)
                    .collect(),
            )
        } else {
            match state.outstanding.get(&tag) {
                Some(entry) if entry.sent => {}
                _ => return None,
            }
            Some(
                state
                    .outstanding
                    .range(..=tag)
                    .filter(|(_, entry)| entry.sent)
                    .map(|(key, _)| *key)
                    .collect(),
            )
        }
    } else {
        match state.outstanding.get(&tag) {
            Some(entry) if entry.sent => Some(vec![tag]),
            _ => None,
        }
    }
}

fn remove_keys(state: &mut ChannelState, keys: &[u64]) -> Vec<(DeliveryToken, String)> {
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys {
        if let Some(entry) = state.outstanding.remove(key) {
            entries.push((entry.token, entry.queue));
        }
    }
    entries
}

#[cfg(test)]
fn take_outstanding(
    state: &mut ChannelState,
    tag: u64,
    multiple: bool,
) -> Option<Vec<(DeliveryToken, String)>> {
    let keys = settle_keys(state, tag, multiple)?;
    Some(remove_keys(state, &keys))
}

async fn channel_delivery_lock(
    channels: &Arc<Mutex<HashMap<u16, ChannelState>>>,
    channel: u16,
) -> Option<Arc<Mutex<()>>> {
    channels
        .lock()
        .await
        .get(&channel)
        .map(|state| state.delivery_lock.clone())
}

async fn handle_reject(
    shared: &Arc<Shared>,
    channel: u16,
    args: &[u8],
    nack: bool,
) -> io::Result<()> {
    let mut parser = Parser::new(args);
    let tag = parser.u64()?;
    let (multiple, requeue) = if nack {
        let bits = parser.u8().unwrap_or(0);
        (bits & 1 != 0, bits & 2 != 0)
    } else {
        (false, parser.u8().map(|b| b & 1 != 0).unwrap_or(false))
    };

    let Some(delivery_lock) = channel_delivery_lock(&shared.channels, channel).await else {
        return Ok(());
    };
    let delivery_guard = delivery_lock.lock().await;
    let keys = {
        let mut channels = shared.channels.lock().await;
        let Some(state) = channels.get_mut(&channel) else {
            return Ok(());
        };
        state.prune_expired(now_ms());
        settle_keys(state, tag, multiple)
    };
    let Some(keys) = keys else {
        drop(delivery_guard);
        channel_error(
            shared,
            channel,
            REPLY_PRECONDITION_FAILED,
            "unknown delivery tag",
            CLASS_BASIC,
            BASIC_REJECT,
        )
        .await?;
        return Ok(());
    };
    let denied = {
        let channels = shared.channels.lock().await;
        channels.get(&channel).is_some_and(|state| {
            keys.iter().any(|key| {
                state
                    .outstanding
                    .get(key)
                    .is_some_and(|entry| !authorized(shared, Permission::Consume, &entry.queue))
            })
        })
    };
    if denied {
        drop(delivery_guard);
        channel_error(
            shared,
            channel,
            REPLY_ACCESS_REFUSED,
            "permission denied: consume on outstanding queue",
            CLASS_BASIC,
            BASIC_REJECT,
        )
        .await?;
        return Ok(());
    }
    let entries = {
        let channels = shared.channels.lock().await;
        let Some(state) = channels.get(&channel) else {
            return Ok(());
        };
        keys.iter()
            .filter_map(|key| state.outstanding.get(key))
            .map(|entry| entry.token.clone())
            .collect::<Vec<_>>()
    };

    let ante = shared.ante.clone();
    let result = blocking(&shared.drain, move || {
        for token in entries {
            if requeue {
                ante.requeue(&token)?;
            } else {
                ante.discard(&token)?;
            }
        }
        Ok(())
    })
    .await;
    if let Err(err) = result {
        drop(delivery_guard);
        return channel_error(
            shared,
            channel,
            map_error(&err),
            &err.to_string(),
            CLASS_BASIC,
            if nack { BASIC_NACK } else { BASIC_REJECT },
        )
        .await;
    }
    if let Some(state) = shared.channels.lock().await.get_mut(&channel) {
        remove_keys(state, &keys);
    }
    Ok(())
}

async fn recover_channel(shared: &Arc<Shared>, channel: u16) -> io::Result<bool> {
    let Some(delivery_lock) = channel_delivery_lock(&shared.channels, channel).await else {
        return Ok(true);
    };
    let delivery_guard = delivery_lock.lock().await;
    let entries = {
        let mut channels = shared.channels.lock().await;
        match channels.get_mut(&channel) {
            Some(state) => {
                let entries: Vec<(DeliveryToken, String)> = state
                    .outstanding
                    .values()
                    .map(|entry| (entry.token.clone(), entry.queue.clone()))
                    .collect();
                if entries
                    .iter()
                    .any(|(_, queue)| !authorized(shared, Permission::Consume, queue))
                {
                    None
                } else {
                    Some(entries)
                }
            }
            None => Some(Vec::new()),
        }
    };
    let Some(entries) = entries else {
        drop(delivery_guard);
        channel_error(
            shared,
            channel,
            REPLY_ACCESS_REFUSED,
            "permission denied: consume on outstanding queue",
            CLASS_BASIC,
            BASIC_RECOVER,
        )
        .await?;
        return Ok(false);
    };
    if entries.is_empty() {
        return Ok(true);
    }
    let ante = shared.ante.clone();
    let result = blocking(&shared.drain, move || {
        for (token, _) in entries {
            ante.requeue(&token)?;
        }
        Ok(())
    })
    .await;
    if let Err(err) = result {
        drop(delivery_guard);
        channel_error(
            shared,
            channel,
            map_error(&err),
            &err.to_string(),
            CLASS_BASIC,
            BASIC_RECOVER,
        )
        .await?;
        return Ok(false);
    }
    if let Some(state) = shared.channels.lock().await.get_mut(&channel) {
        state.outstanding.clear();
    }
    Ok(true)
}

async fn channel_error(
    shared: &Arc<Shared>,
    channel: u16,
    code: u16,
    text: &str,
    class_id: u16,
    method_id: u16,
) -> io::Result<()> {
    close_channel(shared, channel, Some((code, text.to_string()))).await?;
    let args = channel_error_args(code, text, class_id, method_id);
    shared
        .writer
        .lock()
        .await
        .method(channel, CLASS_CHANNEL, CHANNEL_CLOSE, &args)
        .await
}

async fn close_channel(
    shared: &Arc<Shared>,
    channel: u16,
    error: Option<(u16, String)>,
) -> io::Result<()> {
    let delivery_lock = channel_delivery_lock(&shared.channels, channel).await;
    let delivery_guard = match delivery_lock.as_ref() {
        Some(lock) => Some(lock.lock().await),
        None => None,
    };
    let (tokens, registrations) = {
        let mut channels = shared.channels.lock().await;
        match channels.remove(&channel) {
            Some(state) => {
                let mut ids = Vec::new();
                for consumer in state.consumers.values() {
                    consumer.subscription.cancel.store(true, Ordering::Release);
                    consumer
                        .subscription
                        .notified
                        .store(true, Ordering::Release);
                    ids.push(consumer.subscription.id());
                }
                (
                    state
                        .outstanding
                        .values()
                        .map(|entry| entry.token.clone())
                        .collect::<Vec<_>>(),
                    ids,
                )
            }
            None => (Vec::new(), Vec::new()),
        }
    };
    {
        let mut tracked = shared
            .registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for id in registrations {
            shared.registry.unregister(id);
            tracked.remove(&id);
        }
    }
    drop(delivery_guard);
    if tokens.is_empty() {
        return Ok(());
    }
    let ante = shared.ante.clone();
    let _ = blocking(&shared.drain, move || {
        for token in tokens {
            ante.requeue(&token)?;
        }
        Ok(())
    })
    .await;

    if let Some((code, text)) = error {
        eprintln!("ante-amqp: channel {channel} closed: {code} {text}");
    }
    Ok(())
}

async fn remove_channel(shared: &Arc<Shared>, channel: u16) {
    let removed = shared.channels.lock().await.remove(&channel);
    if let Some(state) = removed {
        let mut ids = Vec::new();
        for consumer in state.consumers.values() {
            consumer.subscription.cancel.store(true, Ordering::Release);
            consumer
                .subscription
                .notified
                .store(true, Ordering::Release);
            ids.push(consumer.subscription.id());
        }
        let mut registrations = shared
            .registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for id in ids {
            shared.registry.unregister(id);
            registrations.remove(&id);
        }
    }
}

async fn cleanup_all(shared: &Arc<Shared>) {
    let channels: Vec<u16> = {
        let channels = shared.channels.lock().await;
        channels.keys().copied().collect()
    };
    for channel in channels {
        let _ = close_channel(shared, channel, None).await;
    }
}

async fn send_connection_close(
    shared: &Arc<Shared>,
    code: u16,
    text: &str,
    class_id: u16,
    method_id: u16,
) -> io::Result<()> {
    let args = connection_error_args(code, text, class_id, method_id);
    shared
        .writer
        .lock()
        .await
        .method(0, CLASS_CONNECTION, CONNECTION_CLOSE, &args)
        .await
}

fn split_method(payload: &[u8]) -> io::Result<(u16, u16, Vec<u8>)> {
    let mut parser = Parser::new(payload);
    let class_id = parser.u16()?;
    let method_id = parser.u16()?;
    let rest = payload[4..].to_vec();
    Ok((class_id, method_id, rest))
}

fn skip_field_value(parser: &mut Parser<'_>, kind: u8) -> io::Result<()> {
    match kind {
        b't' | b'b' | b'B' => {
            parser.u8()?;
        }
        b'U' | b'u' => {
            parser.u16()?;
        }
        b'I' | b'i' | b'f' => {
            parser.u32()?;
        }
        b'L' | b'l' | b'd' | b'T' => {
            parser.u64()?;
        }
        b'D' => {
            parser.u8()?;
            parser.u32()?;
        }
        b'S' | b'x' => {
            parser.longstr_bytes()?;
        }
        b'A' | b'F' => {
            parser.skip_table()?;
        }
        b'V' => {}
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown field-table type",
            ))
        }
    }
    Ok(())
}

/// Read `table[section][flag]` from an AMQP field table (without its length
/// prefix). Unknown or malformed entries are treated as absent.
fn table_flag(table: &[u8], section: &str, flag: &str) -> bool {
    fn inner(table: &[u8], section: &str, flag: &str) -> io::Result<bool> {
        let mut parser = Parser::new(table);
        while !parser.is_empty() {
            let key = parser.shortstr()?;
            let kind = parser.u8()?;
            if key == section && kind == b'F' {
                let nested = parser.longstr_bytes()?;
                let mut nested = Parser::new(&nested);
                while !nested.is_empty() {
                    let name = nested.shortstr()?;
                    let kind = nested.u8()?;
                    if name == flag && kind == b't' {
                        return Ok(nested.u8()? != 0);
                    }
                    skip_field_value(&mut nested, kind)?;
                }
                return Ok(false);
            }
            skip_field_value(&mut parser, kind)?;
        }
        Ok(false)
    }
    inner(table, section, flag).unwrap_or(false)
}

fn map_error(err: &Error) -> u16 {
    match err {
        Error::QueueNotFound(_) => REPLY_NOT_FOUND,
        Error::PayloadTooLarge { .. }
        | Error::BatchTooLarge { .. }
        | Error::BatchTooManyBytes { .. } => REPLY_PRECONDITION_FAILED,
        _ => REPLY_COMMAND_INVALID,
    }
}

async fn blocking<T, F>(drain: &Drain, f: F) -> ante_core::Result<T>
where
    F: FnOnce() -> ante_core::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let guard = drain.begin();
    match tokio::task::spawn_blocking(move || {
        let _guard = guard;
        f()
    })
    .await
    {
        Ok(result) => result,
        Err(err) => Err(Error::Invalid(format!("engine task failed: {err}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(delivery_id: u64, sent: bool) -> Outstanding {
        Outstanding {
            token: DeliveryToken {
                message_id: delivery_id,
                delivery_id,
            },
            deadline_ms: u64::MAX,
            queue: String::new(),
            queue_id: 0,
            sent,
            consumer_id: Some(1),
        }
    }

    #[test]
    fn acks_never_touch_unsent_deliveries() {
        let mut state = ChannelState::new(1);
        state.outstanding.insert(1, entry(1, true));
        state.outstanding.insert(2, entry(2, false));
        state.outstanding.insert(3, entry(3, true));

        let tokens = take_outstanding(&mut state, 0, true).unwrap();
        assert_eq!(
            tokens
                .iter()
                .map(|(token, _)| token.message_id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert!(
            state.outstanding.contains_key(&2),
            "unsent delivery was acked"
        );

        assert!(take_outstanding(&mut state, 2, true).is_none());
        assert!(take_outstanding(&mut state, 2, false).is_none());
    }

    #[tokio::test]
    async fn drain_tracks_storage_work_until_the_guard_drops() {
        let drain = Drain::default();
        let guard = drain.begin();
        assert_eq!(drain.active(), 1);
        let short = tokio::time::Instant::now() + Duration::from_millis(50);
        assert!(!drain.wait_until(short).await, "drain finished too early");
        drop(guard);
        assert_eq!(drain.active(), 0);
        let long = tokio::time::Instant::now() + Duration::from_secs(1);
        assert!(drain.wait_until(long).await);
    }

    #[tokio::test]
    async fn dropping_the_task_set_aborts_consumers() {
        let reaper = ConsumerReaper::default();
        let tasks = Tasks::new(reaper.clone());
        let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = ticks.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(1)).await;
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }));
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(tasks);
        let adopted = reaper.take();
        assert_eq!(adopted.len(), 1, "aborted consumer was detached");
        for handle in adopted {
            let _ = handle.await;
        }
        let settled = ticks.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            ticks.load(Ordering::Relaxed),
            settled,
            "consumer still running"
        );
    }

    #[test]
    fn client_capability_is_parsed_from_start_ok() {
        let mut encoder = Encoder::new();
        encoder.table(&[(
            "capabilities",
            TableValue::Table(&[("consumer_cancel_notify", TableValue::Bool(true))]),
        )]);
        let encoded = encoder.finish();
        assert!(table_flag(
            &encoded[4..],
            "capabilities",
            "consumer_cancel_notify"
        ));

        let mut encoder = Encoder::new();
        encoder.table(&[(
            "capabilities",
            TableValue::Table(&[("consumer_cancel_notify", TableValue::Bool(false))]),
        )]);
        let encoded = encoder.finish();
        assert!(!table_flag(
            &encoded[4..],
            "capabilities",
            "consumer_cancel_notify"
        ));
        assert!(!table_flag(&encoded[4..], "capabilities", "missing"));
    }

    #[tokio::test]
    async fn aborting_the_drain_hands_consumers_to_the_reaper() {
        let reaper = ConsumerReaper::default();
        let guard = AbortOnDrop {
            handles: vec![tokio::spawn(async {
                std::future::pending::<()>().await;
            })],
            reaper: reaper.clone(),
        };
        drop(guard);
        let adopted = reaper.take();
        assert_eq!(adopted.len(), 1, "aborted drain detached its consumers");
        for handle in adopted {
            let _ = handle.await;
        }
    }

    #[tokio::test]
    async fn finished_consumer_handles_are_reaped() {
        let tasks = Tasks::new(ConsumerReaper::default());
        tasks.push(tokio::spawn(async {}));
        tokio::time::sleep(Duration::from_millis(20)).await;
        tasks.push(tokio::spawn(async {
            std::future::pending::<()>().await;
        }));
        let handles = tasks.take();
        assert_eq!(handles.len(), 1, "finished handle was retained");
        for handle in handles {
            handle.abort();
        }
    }

    #[test]
    fn queue_locks_do_not_grow_with_historical_names() {
        let locks = QueueLocks::default();
        for index in 0..10_000 {
            let lock = locks.get(&format!("queue-{index}"));
            drop(lock);
        }
        assert!(
            locks.len() <= 1,
            "queue lock map retained {} entries",
            locks.len()
        );
    }

    #[tokio::test]
    async fn reserve_refuses_a_reopened_channel() {
        let channels: ChannelMap = Arc::new(Mutex::new(HashMap::new()));
        let mut reopened = ChannelState::new(2);
        reopened.prefetch = 1;
        channels.lock().await.insert(0, reopened);

        assert_eq!(
            reserve(&channels, 0, 1, 1, 1, false).await,
            None,
            "a stale task reserved on the reopened instance"
        );
        assert_eq!(channels.lock().await.get(&0).unwrap().reserved, 0);
        assert_eq!(reserve(&channels, 0, 2, 1, 1, false).await, Some(1));
        assert_eq!(channels.lock().await.get(&0).unwrap().reserved, 1);
    }

    #[tokio::test]
    async fn stale_reconcile_waits_for_the_delivery_lock() {
        let channels: ChannelMap = Arc::new(Mutex::new(HashMap::new()));
        let mut state = ChannelState::new(1);
        state.outstanding.insert(1, entry(1, true));
        let delivery_lock = state.delivery_lock.clone();
        channels.lock().await.insert(0, state);

        let guard = delivery_lock.lock().await;
        let gone = vec![(String::new(), 0)];
        let removal = remove_stale_outstanding(&channels, 0, 1, &gone);
        tokio::pin!(removal);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut removal)
                .await
                .is_err(),
            "reconcile removed tags during an in-flight batch"
        );
        assert_eq!(channels.lock().await.get(&0).unwrap().outstanding.len(), 1);

        drop(guard);
        assert!(removal.await);
        assert_eq!(channels.lock().await.get(&0).unwrap().outstanding.len(), 0);
    }

    #[tokio::test]
    async fn cleanup_does_not_wait_on_unrelated_channels() {
        let registry = ChannelRegistry::default();
        let map: ChannelMap = Arc::new(Mutex::new(HashMap::new()));

        let mut target = ChannelState::new(1);
        let mut targeted = entry(1, true);
        targeted.queue_id = 7;
        target.outstanding.insert(1, targeted);

        let mut unrelated = ChannelState::new(2);
        let mut other = entry(2, true);
        other.queue_id = 8;
        unrelated.outstanding.insert(1, other);
        let unrelated_lock = unrelated.delivery_lock.clone();

        {
            let mut guard = map.lock().await;
            guard.insert(0, target);
            guard.insert(1, unrelated);
        }
        registry.add(&map);

        let guard = unrelated_lock.lock().await;
        tokio::time::timeout(
            Duration::from_secs(1),
            registry.drop_outstanding_for_queue(7),
        )
        .await
        .expect("delete waited on an unrelated channel");
        assert_eq!(map.lock().await.get(&0).unwrap().outstanding.len(), 0);
        assert_eq!(map.lock().await.get(&1).unwrap().outstanding.len(), 1);
        drop(guard);
    }

    #[tokio::test]
    async fn cleanup_waits_for_an_in_flight_delivery_batch() {
        let registry = ChannelRegistry::default();
        let map: ChannelMap = Arc::new(Mutex::new(HashMap::new()));
        let mut state = ChannelState::new(1);
        let mut queued = entry(1, true);
        queued.queue_id = 7;
        state.outstanding.insert(1, queued);
        let delivery_lock = state.delivery_lock.clone();
        map.lock().await.insert(1, state);
        registry.add(&map);

        let guard = delivery_lock.lock().await;
        let cleanup = registry.drop_outstanding_for_queue(7);
        tokio::pin!(cleanup);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut cleanup)
                .await
                .is_err(),
            "cleanup ran while a delivery batch held the channel lock"
        );
        assert_eq!(map.lock().await.get(&1).unwrap().outstanding.len(), 1);

        drop(guard);
        cleanup.await;
        assert_eq!(map.lock().await.get(&1).unwrap().outstanding.len(), 0);
    }

    #[tokio::test]
    async fn stale_generation_cannot_release_a_new_reservation() {
        let channels: ChannelMap = Arc::new(Mutex::new(HashMap::new()));
        let mut old = ChannelState::new(1);
        old.reserved = 2;
        channels.lock().await.insert(0, old);

        let mut reopened = ChannelState::new(2);
        reopened.reserved = 1;
        channels.lock().await.insert(0, reopened);

        release_reservation(&channels, 0, 1, 1).await;
        assert_eq!(
            channels.lock().await.get(&0).unwrap().reserved,
            1,
            "a stale consumer released the reopened channel's reservation"
        );

        release_reservation(&channels, 0, 2, 1).await;
        assert_eq!(channels.lock().await.get(&0).unwrap().reserved, 0);
    }

    #[test]
    fn only_real_cancellation_releases_queue_deliveries() {
        assert!(
            !should_notify(false, false, false),
            "a write failure must preserve valid in-flight records"
        );
        assert!(
            !should_notify(true, false, true),
            "shutdown must not take the cancellation path"
        );
        assert!(
            should_notify(true, false, false),
            "delete/cancel must notify"
        );
        assert!(
            should_notify(false, true, false),
            "identity loss must clean the queue's prefetch slots"
        );
    }

    #[test]
    fn multiple_ack_with_unknown_tag_is_rejected() {
        let mut state = ChannelState::new(1);
        state.outstanding.insert(1, entry(1, true));
        state.outstanding.insert(2, entry(2, true));
        assert!(take_outstanding(&mut state, 99, true).is_none());
        assert_eq!(state.outstanding.len(), 2, "ack must not mutate the map");
    }

    #[test]
    fn ack_all_with_only_unsent_is_a_noop() {
        let mut state = ChannelState::new(1);
        state.outstanding.insert(1, entry(1, false));
        assert!(take_outstanding(&mut state, 0, true).unwrap().is_empty());
        assert!(state.outstanding.contains_key(&1));
    }
}
