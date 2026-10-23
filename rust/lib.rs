use std::sync::RwLock;

use ante_core::{AckOutcome, Ante, DeliveryToken, NewMessage, RequeueOutcome};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

pyo3::create_exception!(_native, NativeError, PyRuntimeError);

fn error(err: ante_core::Error) -> PyErr {
    NativeError::new_err((err.code(), err.to_string()))
}

#[pyclass(frozen)]
struct Engine {
    inner: RwLock<Option<Ante>>,
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
            inner: RwLock::new(Some(engine)),
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            let mut guard = self
                .inner
                .write()
                .map_err(|_| NativeError::new_err(("INTERNAL", "engine lock poisoned")))?;
            guard.take();
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
