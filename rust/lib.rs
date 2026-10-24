use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;

use ante_core::{AckOutcome, Ante, DeliveryToken, NewMessage, RequeueOutcome, Settlement};
use ante_security::Security;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

pyo3::create_exception!(_native, NativeError, PyRuntimeError);

fn error(err: ante_core::Error) -> PyErr {
    NativeError::new_err((err.code(), err.to_string()))
}

#[pyclass(frozen)]
struct Engine {
    inner: RwLock<Option<Arc<Ante>>>,
    server: Mutex<Option<AmqpServer>>,
}

struct AmqpServer {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<std::io::Result<()>>>,
}

impl AmqpServer {
    fn stop(&mut self) -> PyResult<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| NativeError::new_err(("AMQP", "listener thread panicked")))?
                .map_err(|e| NativeError::new_err(("AMQP", e.to_string())))?;
        }
        Ok(())
    }
}

impl Drop for AmqpServer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

impl Engine {
    // Never wait for storage/locks while holding the Python GIL.
    fn run<T: Send>(
        &self,
        py: Python<'_>,
        operation: impl FnOnce(&Ante) -> ante_core::Result<T> + Send,
    ) -> PyResult<T> {
        py.allow_threads(|| {
            let guard = self
                .inner
                .read()
                .map_err(|_| NativeError::new_err(("INTERNAL", "engine lock poisoned")))?;
            let engine = guard
                .as_ref()
                .ok_or_else(|| NativeError::new_err(("CLOSED", "engine is closed")))?;
            operation(engine).map_err(error)
        })
    }
}

#[pyclass]
struct Record {
    #[pyo3(get)]
    token: String,
    #[pyo3(get)]
    message_id: u64,
    #[pyo3(get)]
    attempts: u64,
    payload: Vec<u8>,
    #[pyo3(get)]
    content_type: Option<String>,
    #[pyo3(get)]
    correlation_id: Option<String>,
    #[pyo3(get)]
    reply_to: Option<String>,
    #[pyo3(get)]
    application_message_id: Option<String>,
    #[pyo3(get)]
    deadline_ms: u64,
}

#[pymethods]
impl Record {
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.payload)
    }
}

#[derive(Clone)]
#[pyclass(get_all)]
struct Attempt {
    attempt: u64,
    delivery_id: u64,
    started_at_ms: u64,
    finished_at_ms: Option<u64>,
    outcome: String,
}

impl From<ante_core::AttemptInfo> for Attempt {
    fn from(attempt: ante_core::AttemptInfo) -> Self {
        Self {
            attempt: attempt.attempt,
            delivery_id: attempt.delivery_id,
            started_at_ms: attempt.started_at_ms,
            finished_at_ms: attempt.finished_at_ms,
            outcome: attempt.outcome.as_str().to_owned(),
        }
    }
}

#[derive(Clone)]
#[pyclass(get_all)]
struct Status {
    queue: String,
    message_id: u64,
    state: String,
    attempts: u64,
    created_at_ms: u64,
    updated_at_ms: u64,
    history: Vec<Attempt>,
}

impl From<ante_core::MessageStatus> for Status {
    fn from(status: ante_core::MessageStatus) -> Self {
        Self {
            queue: status.queue,
            message_id: status.message_id,
            state: status.state.as_str().to_owned(),
            attempts: status.attempts,
            created_at_ms: status.created_at_ms,
            updated_at_ms: status.updated_at_ms,
            history: status.history.into_iter().map(Attempt::from).collect(),
        }
    }
}

#[pyclass(get_all)]
struct Info {
    name: String,
    id: u64,
    ready: u64,
    inflight: u64,
}

impl From<ante_core::QueueInfo> for Info {
    fn from(info: ante_core::QueueInfo) -> Self {
        Self {
            name: info.name,
            id: info.id,
            ready: info.ready,
            inflight: info.inflight,
        }
    }
}

#[pymethods]
impl Engine {
    #[new]
    fn new(py: Python<'_>, path: String) -> PyResult<Self> {
        let engine = py.allow_threads(|| Ante::open(path)).map_err(error)?;
        Ok(Self {
            inner: RwLock::new(Some(Arc::new(engine))),
            server: Mutex::new(None),
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            let mut guard = self
                .inner
                .write()
                .map_err(|_| NativeError::new_err(("INTERNAL", "engine lock poisoned")))?;
            if let Some(mut server) = self
                .server
                .lock()
                .map_err(|_| NativeError::new_err(("INTERNAL", "server lock poisoned")))?
                .take()
            {
                server.stop()?;
            }
            guard.take();
            Ok(())
        })
    }

    #[pyo3(signature = (host, port, security_path=None))]
    fn serve_amqp(
        &self,
        py: Python<'_>,
        host: String,
        port: u16,
        security_path: Option<String>,
    ) -> PyResult<(String, u16)> {
        py.allow_threads(|| {
            let security = match security_path {
                Some(path) => {
                    Some(Arc::new(Security::load(&path).map_err(|error| {
                        NativeError::new_err(("SECURITY", error.to_string()))
                    })?))
                }
                None => None,
            };
            let guard = self
                .inner
                .read()
                .map_err(|_| NativeError::new_err(("INTERNAL", "engine lock poisoned")))?;
            let engine = guard
                .as_ref()
                .ok_or_else(|| NativeError::new_err(("CLOSED", "engine is closed")))?
                .clone();
            let mut server = self
                .server
                .lock()
                .map_err(|_| NativeError::new_err(("INTERNAL", "server lock poisoned")))?;
            if server.is_some() {
                return Err(NativeError::new_err(("AMQP", "listener already running")));
            }
            let listener = std::net::TcpListener::bind((host.as_str(), port))
                .map_err(|e| NativeError::new_err(("AMQP", e.to_string())))?;
            listener
                .set_nonblocking(true)
                .map_err(|e| NativeError::new_err(("AMQP", e.to_string())))?;
            let address = listener
                .local_addr()
                .map_err(|e| NativeError::new_err(("AMQP", e.to_string())))?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| NativeError::new_err(("AMQP", e.to_string())))?;
            let (stop, shutdown) = tokio::sync::oneshot::channel();
            let thread = std::thread::Builder::new()
                .name("ante-amqp".into())
                .spawn(move || {
                    runtime.block_on(async move {
                        let listener = tokio::net::TcpListener::from_std(listener)?;
                        match security {
                            Some(security) => {
                                ante_amqp::serve_secure_until_with_options(
                                    listener,
                                    engine,
                                    ante_amqp::ServerOptions::default(),
                                    security,
                                    shutdown,
                                )
                                .await
                            }
                            None => ante_amqp::serve_until(listener, engine, shutdown).await,
                        }
                    })
                })
                .map_err(|e| NativeError::new_err(("AMQP", e.to_string())))?;
            *server = Some(AmqpServer {
                stop: Some(stop),
                thread: Some(thread),
            });
            Ok((address.ip().to_string(), address.port()))
        })
    }

    fn stop_amqp(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            if let Some(mut server) = self
                .server
                .lock()
                .map_err(|_| NativeError::new_err(("INTERNAL", "server lock poisoned")))?
                .take()
            {
                server.stop()?;
            }
            Ok(())
        })
    }

    fn declare(&self, py: Python<'_>, queue: String) -> PyResult<u64> {
        self.run(py, |engine| engine.declare_queue(&queue))
    }

    fn publish(
        &self,
        py: Python<'_>,
        queue: String,
        payload: Vec<u8>,
        metadata: (String, String, String, String),
    ) -> PyResult<u64> {
        let (content_type, message_id, correlation_id, reply_to) = metadata;
        self.run(py, |engine| {
            engine.publish(
                &queue,
                NewMessage {
                    payload,
                    content_type: Some(content_type),
                    message_id: (!message_id.is_empty()).then_some(message_id),
                    correlation_id: (!correlation_id.is_empty()).then_some(correlation_id),
                    reply_to: (!reply_to.is_empty()).then_some(reply_to),
                },
            )
        })
    }

    #[pyo3(signature = (token, queue, payload, metadata, discard=false))]
    fn complete(
        &self,
        py: Python<'_>,
        token: String,
        queue: String,
        payload: Vec<u8>,
        metadata: (String, String, String, String),
        discard: bool,
    ) -> PyResult<u64> {
        let (content_type, message_id, correlation_id, reply_to) = metadata;
        let completion = self.run(py, |engine| {
            engine.complete(
                &DeliveryToken::parse(&token)?,
                &queue,
                NewMessage {
                    payload,
                    content_type: Some(content_type),
                    message_id: (!message_id.is_empty()).then_some(message_id),
                    correlation_id: (!correlation_id.is_empty()).then_some(correlation_id),
                    reply_to: (!reply_to.is_empty()).then_some(reply_to),
                },
                if discard {
                    Settlement::Discard
                } else {
                    Settlement::Ack
                },
            )
        })?;
        match completion.outcome {
            AckOutcome::Acked => completion
                .message_id
                .ok_or_else(|| NativeError::new_err(("INTERNAL", "completion has no message id"))),
            AckOutcome::Stale => Err(NativeError::new_err(("STALE_DELIVERY", "stale delivery"))),
            AckOutcome::Unknown => Err(NativeError::new_err((
                "UNKNOWN_DELIVERY",
                "unknown delivery",
            ))),
        }
    }

    fn publish_batch(
        &self,
        py: Python<'_>,
        queue: String,
        messages: Vec<(Vec<u8>, String)>,
    ) -> PyResult<Vec<u64>> {
        self.run(py, |engine| {
            engine.publish_batch(
                &queue,
                messages
                    .into_iter()
                    .map(|(payload, kind)| NewMessage {
                        payload,
                        content_type: Some(kind),
                        ..Default::default()
                    })
                    .collect(),
            )
        })
    }

    #[pyo3(signature = (queue, max, lease_ms, queue_id=None))]
    fn take(
        &self,
        py: Python<'_>,
        queue: String,
        max: usize,
        lease_ms: u64,
        queue_id: Option<u64>,
    ) -> PyResult<Vec<Record>> {
        self.run(py, |engine| {
            engine.recover_expired()?;
            let deliveries = if let Some(id) = queue_id {
                engine
                    .take_if_queue_id(&queue, id, max, lease_ms)?
                    .ok_or_else(|| ante_core::Error::QueueNotFound(queue.clone()))?
            } else {
                engine.take(&queue, max, lease_ms)?
            };
            Ok(deliveries
                .into_iter()
                .map(|d| Record {
                    token: d.token.to_wire(),
                    message_id: d.message_id,
                    attempts: d.attempts,
                    payload: d.payload,
                    content_type: d.content_type,
                    correlation_id: d.correlation_id,
                    reply_to: d.reply_to,
                    application_message_id: d.application_message_id,
                    deadline_ms: d.deadline_ms,
                })
                .collect())
        })
    }

    fn ack(&self, py: Python<'_>, tokens: Vec<String>) -> PyResult<Vec<String>> {
        self.run(py, |engine| {
            let tokens = tokens
                .iter()
                .map(|t| DeliveryToken::parse(t))
                .collect::<ante_core::Result<Vec<_>>>()?;
            Ok(engine
                .ack_batch(&tokens)?
                .into_iter()
                .map(|outcome| {
                    match outcome {
                        AckOutcome::Acked => "OK",
                        AckOutcome::Stale => "STALE_DELIVERY",
                        AckOutcome::Unknown => "UNKNOWN_DELIVERY",
                    }
                    .to_owned()
                })
                .collect())
        })
    }

    fn discard(&self, py: Python<'_>, token: String) -> PyResult<String> {
        self.run(py, |engine| {
            Ok(match engine.discard(&DeliveryToken::parse(&token)?)? {
                AckOutcome::Acked => "OK",
                AckOutcome::Stale => "STALE_DELIVERY",
                AckOutcome::Unknown => "UNKNOWN_DELIVERY",
            }
            .to_owned())
        })
    }

    fn requeue(&self, py: Python<'_>, token: String) -> PyResult<String> {
        self.run(py, |engine| {
            Ok(match engine.requeue(&DeliveryToken::parse(&token)?)? {
                RequeueOutcome::Requeued => "OK",
                RequeueOutcome::Stale => "STALE_DELIVERY",
                RequeueOutcome::Unknown => "UNKNOWN_DELIVERY",
            }
            .to_owned())
        })
    }

    fn info(&self, py: Python<'_>, queue: String) -> PyResult<Info> {
        self.run(py, |engine| engine.info(&queue).map(Info::from))
    }

    fn message_status(
        &self,
        py: Python<'_>,
        queue: String,
        message_id: u64,
    ) -> PyResult<Option<Status>> {
        self.run(py, |engine| {
            Ok(engine.message_status(&queue, message_id)?.map(Status::from))
        })
    }

    fn list_queues(&self, py: Python<'_>) -> PyResult<Vec<Info>> {
        self.run(py, |engine| {
            Ok(engine.list_queues()?.into_iter().map(Info::from).collect())
        })
    }

    fn queue_exists(&self, py: Python<'_>, queue: String) -> PyResult<bool> {
        self.run(py, |engine| engine.queue_exists(&queue))
    }

    fn recover_expired(&self, py: Python<'_>) -> PyResult<u64> {
        self.run(py, |engine| engine.recover_expired())
    }

    fn purge(&self, py: Python<'_>, queue: String) -> PyResult<u64> {
        self.run(py, |engine| engine.purge(&queue))
    }

    fn delete_queue(&self, py: Python<'_>, queue: String) -> PyResult<u64> {
        self.run(py, |engine| engine.delete_queue(&queue))
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Engine>()?;
    m.add("NativeError", m.py().get_type::<NativeError>())?;
    Ok(())
}
