//! Minimal AMQP 1.0 adapter for Ante.

use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use ante_core::{Ante, Delivery, Error as CoreError, NewMessage};
use ante_security::{Permission, Principal, Security, TlsAcceptor};
use fe2o3_amqp::acceptor::sasl_acceptor::SaslServerFrame;
use fe2o3_amqp::acceptor::{
    ConnectionAcceptor, LinkAcceptor, LinkEndpoint, ListenerConnectionHandle,
    ListenerSessionHandle, SaslAcceptor, SessionAcceptor,
};
use fe2o3_amqp::link::{LinkStateError, RecvError, SendError};
use fe2o3_amqp::types::definitions::{AmqpError, Error as AmqpErrorBody, Role, SenderSettleMode};
use fe2o3_amqp::types::messaging::{
    AmqpValue, Body, Data, Message, MessageId, Outcome, Properties, Target, TargetArchetype,
};
use fe2o3_amqp::types::primitives::{Array, Symbol, Value};
use fe2o3_amqp::types::sasl::{SaslCode, SaslInit, SaslOutcome, SaslResponse};
use fe2o3_amqp::{Receiver as RemoteSender, Sender as RemoteReceiver, SenderAbortHandle};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;

const DEFAULT_LEASE_MS: u64 = 30 * 60 * 1000;
const TAKE_POLL: Duration = Duration::from_millis(50);
const DEFAULT_CREDIT_WAIT: Duration = Duration::from_secs(1);
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub struct ServerOptions {
    pub lease_ms: u64,
    pub credit_wait: Duration,
    pub handshake_timeout: Duration,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            lease_ms: DEFAULT_LEASE_MS,
            credit_wait: DEFAULT_CREDIT_WAIT,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
        }
    }
}

#[derive(Clone)]
struct ConnectionSetup {
    ante: Arc<Ante>,
    options: ServerOptions,
    security: Option<Arc<Security>>,
    principal: Option<Principal>,
    abort_socket: Arc<OwnedFd>,
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
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut connections = JoinSet::new();
    let mut maintenance = tokio::time::interval(Duration::from_secs(1));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ticks = 0u64;

    loop {
        tokio::select! {
            _ = maintenance.tick() => {
                ticks = ticks.wrapping_add(1);
                let prune = ticks.is_multiple_of(60);
                let ante = ante.clone();
                tokio::task::spawn_blocking(move || {
                    if let Err(error) = ante.recover_expired() {
                        eprintln!("ante-amqp1: lease recovery failed: {error}");
                    }
                    if prune {
                        if let Err(error) = ante.prune_history() {
                            eprintln!("ante-amqp1: history prune failed: {error}");
                        }
                    }
                });
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(pair) => pair,
                    Err(error) => {
                        eprintln!("ante-amqp1: accept failed: {error}");
                        continue;
                    }
                };
                let _ = stream.set_nodelay(true);
                let abort_socket = match stream.as_fd().try_clone_to_owned() {
                    Ok(socket) => Arc::new(socket),
                    Err(error) => {
                        eprintln!("ante-amqp1: cannot create abort handle for {peer}: {error}");
                        continue;
                    }
                };
                let setup = ConnectionSetup {
                    ante: ante.clone(),
                    options,
                    security: security.clone(),
                    principal: None,
                    abort_socket,
                };
                let stop = stop_rx.clone();
                connections.spawn(async move {
                    if let Err(error) = handle_socket(stream, setup, stop).await {
                        if error.kind() != io::ErrorKind::UnexpectedEof {
                            eprintln!("ante-amqp1: connection {peer} closed: {error}");
                        }
                    }
                });
            }
            _ = &mut shutdown => break,
            _ = connections.join_next(), if !connections.is_empty() => {}
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
    Ok(())
}

async fn handle_socket(
    stream: TcpStream,
    mut setup: ConnectionSetup,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    let handshake_timeout = setup.options.handshake_timeout;
    match setup.security.clone() {
        Some(security) => {
            let mut stream = stream;
            let mut first = [0u8; 1];
            let read = tokio::time::timeout(handshake_timeout, stream.peek(&mut first))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "TLS protocol header timed out")
                })??;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed before TLS",
                ));
            }
            if first[0] != 0x16 {
                let mut header = [0u8; 8];
                tokio::time::timeout(handshake_timeout, stream.read_exact(&mut header))
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "TLS protocol header timed out")
                    })??;
                if header != *b"AMQP\x02\x01\x00\x00" {
                    stream.write_all(b"AMQP\x00\x01\x00\x00").await?;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "expected TLS or AMQP protocol header",
                    ));
                }
                stream.write_all(&header).await?;
            }
            let acceptor = TlsAcceptor::from(security.tls_config());
            let stream = tokio::time::timeout(handshake_timeout, acceptor.accept(stream))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out"))?
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let slot = Arc::new(StdMutex::new(None));
            let sasl = SecuritySasl {
                security: security.clone(),
                principal: slot.clone(),
            };
            let connection_acceptor = ConnectionAcceptor::builder()
                .container_id("ante")
                .sasl_acceptor(sasl)
                .build();
            let connection = accept_with_deadline(
                connection_acceptor.accept(stream),
                handshake_timeout,
                &mut stop,
            )
            .await?;
            setup.principal = slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            run_connection(connection, setup, stop).await
        }
        None => {
            let connection_acceptor = ConnectionAcceptor::new("ante");
            let connection = accept_with_deadline(
                connection_acceptor.accept(stream),
                handshake_timeout,
                &mut stop,
            )
            .await?;
            run_connection(connection, setup, stop).await
        }
    }
}

async fn accept_with_deadline<F, E>(
    accept: F,
    handshake_timeout: Duration,
    stop: &mut watch::Receiver<bool>,
) -> io::Result<ListenerConnectionHandle>
where
    F: std::future::Future<Output = Result<ListenerConnectionHandle, E>>,
    E: std::fmt::Display,
{
    tokio::select! {
        _ = stop.changed() => Err(io::Error::new(io::ErrorKind::Interrupted, "server shutting down")),
        result = tokio::time::timeout(handshake_timeout, accept) => {
            match result {
                Ok(Ok(connection)) => Ok(connection),
                Ok(Err(error)) => Err(amqp_io(error)),
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "AMQP handshake timed out",
                )),
            }
        }
    }
}

async fn run_connection(
    mut connection: ListenerConnectionHandle,
    setup: ConnectionSetup,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    let session_acceptor = SessionAcceptor::new();
    let (conn_stop_tx, conn_stop_rx) = watch::channel(false);
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            _ = stop.changed() => {
                let _ = conn_stop_tx.send(true);
                break;
            }
            incoming = connection.next_incoming_session() => {
                let Some(incoming) = incoming else { break };
                let session = session_acceptor
                    .accept_incoming_session(incoming, &mut connection)
                    .await
                    .map_err(amqp_io)?;
                sessions.spawn(handle_session(
                    session,
                    setup.clone(),
                    conn_stop_rx.clone(),
                ));
            }
            _ = sessions.join_next(), if !sessions.is_empty() => {}
        }
    }
    let _ = conn_stop_tx.send(true);
    drain_tasks(&mut sessions, Duration::from_secs(5)).await;
    let _ = connection.close().await;
    Ok(())
}

async fn handle_session(
    mut session: ListenerSessionHandle,
    setup: ConnectionSetup,
    mut stop: watch::Receiver<bool>,
) {
    let link_acceptor = LinkAcceptor::new();
    let mut links = JoinSet::new();
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            incoming = session.next_incoming_attach() => {
                let Some(mut attach) = incoming else { break };
                // fe2o3's local sender requires a target on the peer's attach.
                // A conforming receiver attaches with source only, so mirror the
                // source address into target before handing it to the acceptor.
                if attach.role == Role::Receiver && attach.target.is_none() {
                    if let Some(source) = &attach.source {
                        attach.target = Some(Box::new(TargetArchetype::Target(Target {
                            address: source.address.clone(),
                            ..Target::default()
                        })));
                    }
                }
                let endpoint = match link_acceptor
                    .accept_incoming_attach(attach, &mut session)
                    .await
                {
                    Ok(endpoint) => endpoint,
                    Err(error) => {
                        eprintln!("ante-amqp1: link attach failed: {error:?}");
                        break;
                    }
                };
                match endpoint {
                    LinkEndpoint::Sender(sender) => {
                        let queue = sender
                            .source()
                            .as_ref()
                            .and_then(|source| source.address.clone());
                        let Some(queue) = queue else {
                            let _ = sender
                                .detach_with_error(amqp_error(AmqpError::NotAllowed, "missing source address"))
                                .await;
                            continue;
                        };
                        if !authorized(&setup, Permission::Consume, &queue) {
                            let _ = sender
                                .detach_with_error(amqp_error(AmqpError::UnauthorizedAccess, "consume not allowed"))
                                .await;
                            continue;
                        }
                        let Some(queue_id) = queue_id(&setup.ante, &queue) else {
                            let _ = sender
                                .detach_with_error(amqp_error(AmqpError::NotFound, "queue not found"))
                                .await;
                            continue;
                        };
                        links.spawn(run_sender(
                            sender,
                            setup.ante.clone(),
                            queue,
                            queue_id,
                            setup.options.lease_ms,
                            setup.options.credit_wait,
                            stop.clone(),
                            setup.abort_socket.clone(),
                        ));
                    }
                    LinkEndpoint::Receiver(receiver) => {
                        let queue = receiver
                            .target()
                            .as_ref()
                            .and_then(|target| target.address.clone());
                        let Some(queue) = queue else {
                            let _ = receiver
                                .detach_with_error(amqp_error(AmqpError::NotAllowed, "missing target address"))
                                .await;
                            continue;
                        };
                        if !authorized(&setup, Permission::Publish, &queue) {
                            let _ = receiver
                                .detach_with_error(amqp_error(AmqpError::UnauthorizedAccess, "publish not allowed"))
                                .await;
                            continue;
                        }
                        if queue_id(&setup.ante, &queue).is_none() {
                            let _ = receiver
                                .detach_with_error(amqp_error(AmqpError::NotFound, "queue not found"))
                                .await;
                            continue;
                        }
                        links.spawn(run_receiver(
                            receiver,
                            setup.ante.clone(),
                            queue,
                            stop.clone(),
                        ));
                    }
                }
            }
            _ = links.join_next(), if !links.is_empty() => {}
        }
    }
    drain_tasks(&mut links, Duration::from_secs(5)).await;
}

async fn drain_tasks(tasks: &mut JoinSet<()>, grace: Duration) {
    let deadline = tokio::time::Instant::now() + grace;
    while !tasks.is_empty() {
        match tokio::time::timeout_at(deadline, tasks.join_next()).await {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                break;
            }
        }
    }
}

async fn abort_link(socket: &Arc<OwnedFd>, handle: &SenderAbortHandle) -> bool {
    let socket = socket2::SockRef::from(socket.as_ref());
    // shutdown affects every descriptor for this socket, including the writer
    // owned by fe2o3, and interrupts pending I/O before a claim can be released.
    if let Err(error) = socket.set_linger(Some(Duration::ZERO)) {
        eprintln!("ante-amqp1: cannot configure abort linger: {error}");
    }
    let stopped = match socket.shutdown(std::net::Shutdown::Both) {
        Ok(()) => true,
        Err(error) if error.kind() == io::ErrorKind::NotConnected => true,
        Err(error) => {
            eprintln!("ante-amqp1: socket shutdown failed; retaining lease: {error}");
            false
        }
    };
    // The socket is already disabled. This is protocol-task cleanup only.
    let _ = tokio::time::timeout(Duration::from_secs(1), handle.abort_connection()).await;
    stopped
}

async fn abort_and_requeue(
    socket: &Arc<OwnedFd>,
    handle: &SenderAbortHandle,
    ante: &Ante,
    token: &ante_core::DeliveryToken,
) {
    if abort_link(socket, handle).await {
        settle(ante, token, Settlement::Requeue);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_sender(
    mut sender: RemoteReceiver,
    ante: Arc<Ante>,
    queue: String,
    queue_id: u64,
    lease_ms: u64,
    credit_wait: Duration,
    mut stop: watch::Receiver<bool>,
    abort_socket: Arc<OwnedFd>,
) {
    'link: loop {
        if *stop.borrow() {
            break;
        }
        if sender.credit() == 0 {
            tokio::select! {
                _ = stop.changed() => break,
                _ = sender.on_detach() => break,
                _ = tokio::time::sleep(TAKE_POLL) => {}
            }
            continue;
        }
        let take_ante = ante.clone();
        let take_queue = queue.clone();
        let deliveries = match tokio::task::spawn_blocking(move || {
            take_ante.take_if_queue_id(&take_queue, queue_id, 1, lease_ms)
        })
        .await
        {
            Ok(Ok(Some(deliveries))) => deliveries,
            Ok(Ok(None)) => break,
            Ok(Err(error)) => {
                eprintln!("ante-amqp1: take {queue} failed: {error}");
                break;
            }
            Err(error) => {
                eprintln!("ante-amqp1: take {queue} task failed: {error}");
                break;
            }
        };
        if *stop.borrow() {
            for delivery in deliveries {
                settle(&ante, &delivery.token, Settlement::Requeue);
            }
            break;
        }
        if deliveries.is_empty() {
            tokio::select! {
                _ = stop.changed() => break,
                _ = sender.on_detach() => break,
                _ = tokio::time::sleep(TAKE_POLL) => {}
            }
            continue;
        }
        for delivery in deliveries {
            let token = delivery.token.clone();
            let abort: SenderAbortHandle = sender.abort_handle();
            let remaining = delivery.deadline_ms.saturating_sub(now_ms());
            if remaining == 0 {
                settle(&ante, &token, Settlement::Requeue);
                continue;
            }
            let lease_deadline = tokio::time::Instant::now() + Duration::from_millis(remaining);
            let transfer_deadline = lease_deadline.min(tokio::time::Instant::now() + credit_wait);
            let message = outgoing_message(&delivery);
            let pending = {
                let send = sender.send_batchable(message);
                tokio::pin!(send);
                tokio::select! {
                    biased;
                    _ = stop.changed() => {
                        abort_and_requeue(&abort_socket, &abort, &ante, &token).await;
                        break 'link;
                    }
                    _ = tokio::time::sleep_until(transfer_deadline) => {
                        abort_and_requeue(&abort_socket, &abort, &ante, &token).await;
                        break 'link;
                    }
                    result = &mut send => result,
                }
            };
            let outcome = match pending {
                Ok(settlement) => {
                    // The marker follows every fragment on the same FIFO and
                    // completes only at the transport writer. Window capacity
                    // after the final frame is irrelevant to completion.
                    let flushed = tokio::select! {
                        biased;
                        _ = stop.changed() => false,
                        _ = tokio::time::sleep_until(transfer_deadline) => false,
                        flushed = sender.wait_for_transport() => flushed,
                    };
                    tokio::pin!(settlement);
                    if !flushed {
                        // A real terminal outcome that arrived during the write
                        // wait takes precedence over ambiguous cancellation.
                        // Pre-settled links cannot supply such evidence.
                        let accepted = if sender.sender_settle_mode() != SenderSettleMode::Settled {
                            tokio::time::timeout(Duration::ZERO, &mut settlement)
                                .await
                                .ok()
                        } else {
                            None
                        };
                        if let Some(result) = accepted {
                            result
                        } else {
                            abort_and_requeue(&abort_socket, &abort, &ante, &token).await;
                            break 'link;
                        }
                    } else {
                        tokio::select! {
                            biased;
                            result = &mut settlement => result,
                            _ = stop.changed() => {
                                let grace = lease_deadline.min(tokio::time::Instant::now() + Duration::from_millis(250));
                                match tokio::time::timeout_at(grace, &mut settlement).await {
                                    Ok(result) => result,
                                    Err(_) => {
                                        abort_and_requeue(&abort_socket, &abort, &ante, &token).await;
                                        break 'link;
                                    }
                                }
                            }
                            _ = tokio::time::sleep_until(lease_deadline) => {
                                abort_and_requeue(&abort_socket, &abort, &ante, &token).await;
                                break 'link;
                            }
                        }
                    }
                }
                Err(error) => Err(error),
            };
            match outcome {
                Ok(Outcome::Accepted(_)) => settle(&ante, &token, Settlement::Ack),
                Ok(Outcome::Released(_) | Outcome::Modified(_)) => {
                    settle(&ante, &token, Settlement::Requeue)
                }
                Ok(Outcome::Rejected(_)) => settle(&ante, &token, Settlement::Discard),
                Err(error) => {
                    if !is_remote_close_send(&error) {
                        eprintln!("ante-amqp1: send failed: {error}");
                    }
                    abort_and_requeue(&abort_socket, &abort, &ante, &token).await;
                    break 'link;
                }
            }
        }
    }
}

async fn run_receiver(
    mut receiver: RemoteSender,
    ante: Arc<Ante>,
    queue: String,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        let delivery = tokio::select! {
            _ = stop.changed() => break,
            result = receiver.recv::<Body<Value>>() => match result {
                Ok(delivery) => delivery,
                Err(error) => {
                    if !is_remote_close_recv(&error) {
                        eprintln!("ante-amqp1: receive failed: {error}");
                    }
                    break;
                }
            }
        };
        let message = delivery.message();
        let incoming = match incoming_message(message) {
            Ok(message) => message,
            Err(error) => {
                eprintln!("ante-amqp1: decode failed: {error}");
                let detail = AmqpErrorBody::new(AmqpError::DecodeError, error.to_string(), None);
                let _ = receiver.reject(&delivery, Some(detail)).await;
                continue;
            }
        };
        let publish = {
            let ante = ante.clone();
            let queue = queue.clone();
            tokio::task::spawn_blocking(move || ante.publish(&queue, incoming)).await
        };
        match publish {
            Ok(Ok(_)) => {
                if let Err(error) = receiver.accept(&delivery).await {
                    eprintln!("ante-amqp1: disposition failed: {error}");
                    break;
                }
            }
            Ok(Err(error)) => {
                eprintln!("ante-amqp1: publish {queue} failed: {error}");
                let detail = AmqpErrorBody::new(AmqpError::InternalError, error.to_string(), None);
                let _ = receiver.reject(&delivery, Some(detail)).await;
            }
            Err(error) => {
                eprintln!("ante-amqp1: publish task failed: {error}");
                let _ = receiver
                    .reject(
                        &delivery,
                        Some(amqp_error(AmqpError::InternalError, "publish task failed")),
                    )
                    .await;
            }
        }
    }
}

fn outgoing_message(delivery: &Delivery) -> Message<Data> {
    let message_id = delivery
        .application_message_id
        .clone()
        .map(MessageId::String)
        .unwrap_or(MessageId::Ulong(delivery.message_id));
    let properties = Properties {
        message_id: Some(message_id),
        correlation_id: delivery.correlation_id.clone().map(MessageId::String),
        reply_to: delivery.reply_to.clone(),
        content_type: delivery.content_type.clone().map(Symbol::new),
        ..Properties::default()
    };
    Message::builder()
        .properties(properties)
        .data(delivery.payload.clone())
        .build()
}

fn incoming_message(message: &Message<Body<Value>>) -> Result<NewMessage, CoreError> {
    let payload = body_bytes(&message.body)?;
    let properties = message.properties.as_ref();
    Ok(NewMessage {
        payload,
        content_type: properties
            .and_then(|properties| properties.content_type.as_ref())
            .map(|symbol| symbol.as_str().to_string()),
        message_id: properties
            .and_then(|properties| properties.message_id.as_ref())
            .map(message_id_string)
            .transpose()?,
        correlation_id: properties
            .and_then(|properties| properties.correlation_id.as_ref())
            .map(message_id_string)
            .transpose()?,
        reply_to: properties.and_then(|properties| properties.reply_to.clone()),
    })
}

fn body_bytes(body: &Body<Value>) -> Result<Vec<u8>, CoreError> {
    match body {
        Body::Data(batch) => {
            let mut payload = Vec::new();
            for data in batch {
                payload.extend_from_slice(data.0.as_ref());
            }
            Ok(payload)
        }
        Body::Value(AmqpValue(Value::Binary(binary))) => Ok(binary.to_vec()),
        Body::Value(AmqpValue(Value::String(text))) => Ok(text.as_bytes().to_vec()),
        Body::Value(AmqpValue(Value::Null)) | Body::Empty => Ok(Vec::new()),
        other => Err(CoreError::Invalid(format!(
            "unsupported AMQP body: {other:?}"
        ))),
    }
}

fn message_id_string(id: &MessageId) -> Result<String, CoreError> {
    match id {
        MessageId::Ulong(value) => Ok(value.to_string()),
        MessageId::Uuid(value) => Ok(format!("{value:x}")),
        MessageId::String(value) => Ok(value.clone()),
        MessageId::Binary(_) => Err(CoreError::Invalid(
            "binary AMQP message identifiers are not supported".into(),
        )),
    }
}

fn queue_id(ante: &Ante, queue: &str) -> Option<u64> {
    ante.queue_id(queue).ok().flatten()
}

fn authorized(setup: &ConnectionSetup, permission: Permission, queue: &str) -> bool {
    match &setup.principal {
        Some(principal) => principal.allows(permission, queue),
        None => setup.security.is_none(),
    }
}

#[derive(Clone, Copy)]
enum Settlement {
    Ack,
    Requeue,
    Discard,
}

fn settle(ante: &Ante, token: &ante_core::DeliveryToken, settlement: Settlement) {
    let result = match settlement {
        Settlement::Ack => ante.ack(token).map(|_| ()),
        Settlement::Requeue => ante.requeue(token).map(|_| ()),
        Settlement::Discard => ante.discard(token).map(|_| ()),
    };
    if let Err(error) = result {
        eprintln!("ante-amqp1: settlement failed: {error}");
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn is_remote_close(state: &LinkStateError) -> bool {
    matches!(
        state,
        LinkStateError::RemoteClosed
            | LinkStateError::RemoteDetached
            | LinkStateError::SessionStopped(_)
    )
}

fn is_remote_close_send(error: &SendError) -> bool {
    matches!(error, SendError::LinkStateError(state) if is_remote_close(state))
}

fn is_remote_close_recv(error: &RecvError) -> bool {
    matches!(error, RecvError::LinkStateError(state) if is_remote_close(state))
}

fn amqp_error(condition: AmqpError, text: &str) -> AmqpErrorBody {
    AmqpErrorBody::new(condition, text.to_string(), None)
}

fn amqp_io(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[derive(Clone)]
struct SecuritySasl {
    security: Arc<Security>,
    principal: Arc<StdMutex<Option<Principal>>>,
}

impl SaslAcceptor for SecuritySasl {
    fn mechanisms(&self) -> Array<Symbol> {
        Array::from(vec![Symbol::new("PLAIN")])
    }

    fn on_init(&mut self, init: SaslInit) -> SaslServerFrame {
        let code = match init.mechanism.as_str() {
            "PLAIN" => match plain_credentials(&init) {
                Some((username, password)) => {
                    match authenticate(&self.security, &username, &password) {
                        Some(principal) => {
                            *self
                                .principal
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(principal);
                            SaslCode::Ok
                        }
                        None => SaslCode::Auth,
                    }
                }
                None => SaslCode::Auth,
            },
            _ => SaslCode::Auth,
        };
        SaslServerFrame::Outcome(SaslOutcome {
            code,
            additional_data: None,
        })
    }

    fn on_response(&mut self, _response: SaslResponse) -> SaslServerFrame {
        SaslServerFrame::Outcome(SaslOutcome {
            code: SaslCode::Auth,
            additional_data: None,
        })
    }
}

fn authenticate(security: &Security, username: &str, password: &str) -> Option<Principal> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| security.authenticate(username, password))
        }
        _ => security.authenticate(username, password),
    }
}

fn plain_credentials(init: &SaslInit) -> Option<(String, String)> {
    let response = init.initial_response.as_ref()?;
    let mut parts = response.as_ref().split(|byte| *byte == 0);
    let _authzid = parts.next()?;
    let username = parts.next()?;
    let password = parts.next()?;
    Some((
        String::from_utf8(username.to_vec()).ok()?,
        String::from_utf8(password.to_vec()).ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_socket_shutdown_does_not_release_claim() {
        let directory = tempfile::tempdir().unwrap();
        let ante = Arc::new(Ante::open(directory.path().join("ante.sqlite3")).unwrap());
        ante.declare_queue("jobs").unwrap();
        ante.publish(
            "jobs",
            NewMessage {
                payload: b"retain-on-abort-error".to_vec(),
                ..Default::default()
            },
        )
        .unwrap();
        let deliveries = ante.take("jobs", 1, 30_000).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, shutdown) = oneshot::channel();
        let server_ante = ante.clone();
        let server = tokio::spawn(async move {
            serve_until(listener, server_ante, shutdown).await.unwrap();
        });
        let mut connection =
            fe2o3_amqp::Connection::open("failed-abort", format!("amqp://{address}").as_str())
                .await
                .unwrap();
        let mut session = fe2o3_amqp::Session::begin(&mut connection).await.unwrap();
        let sender = RemoteReceiver::attach(&mut session, "failed-abort", "jobs")
            .await
            .unwrap();
        // An owned non-socket FD deterministically injects ENOTSOCK without
        // modifying process-wide FD limits or closing another task's socket.
        let invalid_socket = Arc::new(OwnedFd::from(std::fs::File::open("/dev/null").unwrap()));
        abort_and_requeue(
            &invalid_socket,
            &sender.abort_handle(),
            &ante,
            &deliveries[0].token,
        )
        .await;
        let info = ante.info("jobs").unwrap();
        assert_eq!(info.inflight, 1);
        assert_eq!(info.ready, 0);
        drop(sender);
        drop(session);
        drop(connection);
        let _ = stop.send(());
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }
}
