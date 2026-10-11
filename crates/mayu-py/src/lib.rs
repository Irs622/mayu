use std::collections::HashMap;
use std::time::Duration;

use mayu_core::{Broker, Header, Link, Outcome, ReadyTask, Runtime, RuntimeConfig};
use mayu_redis::RedisBroker;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

fn rt_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// Producer-side handle: enqueue tasks and read results.
#[pyclass(frozen, module = "mayu._native")]
struct Client {
    rt: tokio::runtime::Runtime,
    broker: RedisBroker,
}

#[pymethods]
impl Client {
    #[new]
    fn new(py: Python<'_>, url: String) -> PyResult<Self> {
        py.detach(|| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(rt_err)?;
            let broker = rt.block_on(RedisBroker::connect(&url, "producer")).map_err(rt_err)?;
            Ok(Client { rt, broker })
        })
    }

    #[pyo3(signature = (task, queue, payload, content_type, max_retries, eta_ms=None))]
    #[allow(clippy::too_many_arguments)]
    fn enqueue(
        &self,
        py: Python<'_>,
        task: String,
        queue: String,
        payload: Vec<u8>,
        content_type: String,
        max_retries: u32,
        eta_ms: Option<u64>,
    ) -> PyResult<String> {
        let mut header = Header::new(task, queue, max_retries, content_type);
        header.eta = eta_ms;
        py.detach(|| self.rt.block_on(self.broker.enqueue(&header, &payload))).map_err(rt_err)?;
        Ok(header.id)
    }

    fn get_result<'py>(&self, py: Python<'py>, id: &str, timeout_ms: u64) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let found = py.detach(|| {
            self.rt.block_on(mayu_core::wait_result(&self.broker, id, Duration::from_millis(timeout_ms)))
        });
        Ok(found.map_err(rt_err)?.map(|v| PyBytes::new(py, &v)))
    }

    fn worker_info<'py>(&self, py: Python<'py>, worker_id: &str) -> PyResult<Option<Bound<'py, PyDict>>> {
        let info = py.detach(|| self.rt.block_on(self.broker.worker_info(worker_id))).map_err(rt_err)?;
        let Some(w) = info else { return Ok(None) };
        let d = PyDict::new(py);
        d.set_item("worker_id", w.worker_id)?;
        d.set_item("hostname", w.hostname)?;
        d.set_item("pid", w.pid)?;
        d.set_item("queues", w.queues)?;
        d.set_item("concurrency", w.concurrency)?;
        d.set_item("inflight", w.inflight)?;
        d.set_item("started_at", w.started_at)?;
        d.set_item("last_seen", w.last_seen)?;
        Ok(Some(d))
    }

    fn queue_stats<'py>(&self, py: Python<'py>, queue: &str) -> PyResult<Bound<'py, PyDict>> {
        let s = py.detach(|| self.rt.block_on(self.broker.queue_stats(queue))).map_err(rt_err)?;
        let d = PyDict::new(py);
        d.set_item("stream", s.stream)?;
        d.set_item("pending", s.pending)?;
        d.set_item("delayed", s.delayed)?;
        d.set_item("dead", s.dead)?;
        Ok(d)
    }
}

/// Worker-side runtime: Python threads pull tasks and report outcomes.
#[pyclass(frozen, module = "mayu._native")]
struct Worker {
    inner: Runtime<RedisBroker>,
}

#[pymethods]
impl Worker {
    #[new]
    #[pyo3(signature = (url, queues, concurrency, prefetch_multiplier=2, result_ttl_secs=None, consumer=None, visibility_timeout_ms=None, reclaim_interval_ms=None, heartbeat_interval_ms=None, max_deliveries=None, strict_priority=false, rate_limits=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        url: String,
        queues: Vec<String>,
        concurrency: usize,
        prefetch_multiplier: usize,
        result_ttl_secs: Option<u64>,
        consumer: Option<String>,
        visibility_timeout_ms: Option<u64>,
        reclaim_interval_ms: Option<u64>,
        heartbeat_interval_ms: Option<u64>,
        max_deliveries: Option<u64>,
        strict_priority: bool,
        rate_limits: Option<HashMap<String, f64>>,
    ) -> PyResult<Self> {
        let consumer = consumer.unwrap_or_else(|| format!("w-{}-{}", std::process::id(), ulid::Ulid::generate()));
        let mut cfg = RuntimeConfig {
            queues, concurrency, prefetch_multiplier, result_ttl_secs,
            worker_id: consumer.clone(),
            strict_priority,
            rate_limits: rate_limits.unwrap_or_default(),
            ..RuntimeConfig::default()
        };
        if let Some(v) = visibility_timeout_ms { cfg.visibility_timeout_ms = v; }
        if let Some(v) = reclaim_interval_ms { cfg.reclaim_interval_ms = v; }
        if let Some(v) = heartbeat_interval_ms { cfg.heartbeat_interval_ms = v; }
        if let Some(v) = max_deliveries { cfg.max_deliveries = v; }
        let inner = py
            .detach(move || Runtime::start(move || async move { RedisBroker::connect(&url, consumer).await }, cfg))
            .map_err(rt_err)?;
        Ok(Worker { inner })
    }

    #[getter]
    fn worker_id(&self) -> &str {
        self.inner.worker_id()
    }

    fn recv(&self, py: Python<'_>, timeout_ms: u64) -> Option<PyReadyTask> {
        py.detach(|| self.inner.recv_timeout(Duration::from_millis(timeout_ms))).map(PyReadyTask)
    }

    #[pyo3(signature = (token, result=None, links=None))]
    fn complete_success(&self, token: u64, result: Option<Vec<u8>>, links: Option<Vec<(String, String, Vec<u8>, u32)>>) {
        let links = links
            .unwrap_or_default()
            .into_iter()
            .map(|(task, queue, payload, max_retries)| Link { task, queue, max_retries, payload })
            .collect();
        self.inner.complete(token, Outcome::Success { result, links });
    }

    fn complete_failure(&self, token: u64, error: String, traceback: String) {
        self.inner.complete(token, Outcome::Failure { error, traceback });
    }

    #[pyo3(signature = (token, countdown_ms, error))]
    fn complete_retry(&self, token: u64, countdown_ms: Option<u64>, error: String) {
        self.inner.complete(token, Outcome::Retry { countdown_ms, error });
    }

    fn stop_fetching(&self) {
        self.inner.stop_fetching();
    }

    fn inflight(&self) -> usize {
        self.inner.inflight()
    }

    fn shutdown(&self, py: Python<'_>, grace_ms: u64) {
        py.detach(|| self.inner.shutdown(Duration::from_millis(grace_ms)));
    }
}

#[pyclass(frozen, name = "ReadyTask", module = "mayu._native")]
struct PyReadyTask(ReadyTask);

#[pymethods]
impl PyReadyTask {
    #[getter]
    fn token(&self) -> u64 {
        self.0.token
    }
    #[getter]
    fn id(&self) -> &str {
        &self.0.id
    }
    #[getter]
    fn task(&self) -> &str {
        &self.0.task
    }
    #[getter]
    fn queue(&self) -> &str {
        &self.0.queue
    }
    #[getter]
    fn attempt(&self) -> u32 {
        self.0.attempt
    }
    #[getter]
    fn max_retries(&self) -> u32 {
        self.0.max_retries
    }
    #[getter]
    fn content_type(&self) -> &str {
        &self.0.content_type
    }
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.0.payload)
    }
    fn __repr__(&self) -> String {
        format!("ReadyTask(id={:?}, task={:?}, attempt={})", self.0.id, self.0.task, self.0.attempt)
    }
}

/// Schedules `exc` to be raised in the thread `thread_id` (a `threading.get_ident()` value) at its
/// next bytecode boundary; `None` clears a pending one. Returns the number of threads affected.
#[pyfunction]
#[pyo3(signature = (thread_id, exc=None))]
fn async_raise(thread_id: u64, exc: Option<Bound<'_, PyAny>>) -> i32 {
    let ptr = exc.as_ref().map_or(std::ptr::null_mut(), |e| e.as_ptr());
    // SAFETY: the interpreter is attached (we hold `Bound`s); the call only reads `ptr` and takes
    // its own reference. pyo3-ffi declares the id as c_long; the cast preserves the bits.
    unsafe { pyo3::ffi::PyThreadState_SetAsyncExc(thread_id as std::os::raw::c_long, ptr) }
}

#[pymodule]
#[pyo3(name = "_native")]
fn mayu_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<Worker>()?;
    m.add_class::<PyReadyTask>()?;
    m.add_function(wrap_pyfunction!(async_raise, m)?)?;
    Ok(())
}
