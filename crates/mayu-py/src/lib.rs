use std::collections::HashMap;
use std::time::Duration;

use mayu_core::{
    Broker, Header, Link, Outcome, ReadyTask, Resources, Runtime, RuntimeConfig, WorkflowNode, WorkflowSpec,
    WorkflowState,
};
use mayu_redis::RedisBroker;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

fn rt_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// Bad input (a spec the core rejects) is a `ValueError`; anything else is a `RuntimeError`.
fn core_err(e: mayu_core::Error) -> PyErr {
    match e {
        mayu_core::Error::Config(m) => PyValueError::new_err(m),
        e => rt_err(e),
    }
}

/// (task, queue, payload, content_type, max_retries, parents, trace_ctx)
type NodeArgs = (String, String, Vec<u8>, String, u32, Vec<usize>, Option<String>);

/// Producer-side handle: enqueue tasks and read results.
#[pyclass(frozen, module = "mayu._native")]
struct Client {
    rt: tokio::runtime::Runtime,
    broker: RedisBroker,
}

fn worker_dict<'py>(py: Python<'py>, w: mayu_core::WorkerInfo) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("worker_id", w.worker_id)?;
    d.set_item("hostname", w.hostname)?;
    d.set_item("pid", w.pid)?;
    d.set_item("queues", w.queues)?;
    d.set_item("concurrency", w.concurrency)?;
    d.set_item("inflight", w.inflight)?;
    d.set_item("started_at", w.started_at)?;
    d.set_item("last_seen", w.last_seen)?;
    Ok(d)
}

fn workflow_dict<'py>(py: Python<'py>, w: WorkflowState) -> PyResult<Bound<'py, PyDict>> {
    let sum = w.summary();
    let d = PyDict::new(py);
    d.set_item("id", &w.id)?;
    d.set_item("name", &w.name)?;
    d.set_item("created_at", w.created_at)?;
    d.set_item("cancelled", w.cancelled)?;
    d.set_item("state", sum.state)?;
    d.set_item("waiting", sum.waiting)?;
    d.set_item("queued", sum.queued)?;
    d.set_item("done", sum.done)?;
    d.set_item("dead", sum.dead)?;
    d.set_item("cancelled_nodes", sum.cancelled)?;
    let nodes = w.nodes.iter().map(|n| {
        let nd = PyDict::new(py);
        nd.set_item("node", &n.node)?;
        nd.set_item("id", &n.task_id)?;
        nd.set_item("task", &n.task)?;
        nd.set_item("queue", &n.queue)?;
        nd.set_item("state", n.state.as_str())?;
        Ok(nd)
    }).collect::<PyResult<Vec<_>>>()?;
    d.set_item("nodes", nodes)?;
    Ok(d)
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

    #[pyo3(signature = (task, queue, payload, content_type, max_retries, eta_ms=None, trace_ctx=None))]
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
        trace_ctx: Option<String>,
    ) -> PyResult<String> {
        let mut header = Header::new(task, queue, max_retries, content_type);
        header.eta = eta_ms;
        header.trace_ctx = trace_ctx;
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
        info.map(|w| worker_dict(py, w)).transpose()
    }

    fn workers<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let ws = py.detach(|| self.rt.block_on(self.broker.workers())).map_err(rt_err)?;
        ws.into_iter().map(|w| worker_dict(py, w)).collect()
    }

    fn known_queues(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| self.rt.block_on(self.broker.known_queues())).map_err(rt_err)
    }

    #[pyo3(signature = (queue, after=None, until=None, count=100))]
    fn dead_letters<'py>(&self, py: Python<'py>, queue: &str, after: Option<String>, until: Option<String>,
                         count: usize) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let items = py.detach(|| self.rt.block_on(
            self.broker.dead_letters(queue, after.as_deref(), until.as_deref(), count))).map_err(rt_err)?;
        items.into_iter().map(|e| {
            let d = PyDict::new(py);
            d.set_item("entry_id", &e.entry_id)?;
            d.set_item("dead_at_ms", e.dead_at_ms())?;
            d.set_item("error", &e.error)?;
            d.set_item("payload", PyBytes::new(py, &e.payload))?;
            let h = e.header.as_ref();
            d.set_item("id", h.map(|h| h.id.clone()))?;
            d.set_item("task", h.map(|h| h.task.clone()))?;
            d.set_item("attempt", h.map(|h| h.attempt))?;
            d.set_item("max_retries", h.map(|h| h.max_retries))?;
            d.set_item("content_type", h.map(|h| h.content_type.clone()))?;
            d.set_item("created_at", h.map(|h| h.created_at))?;
            Ok(d)
        }).collect()
    }

    fn last_dead_id(&self, py: Python<'_>, queue: &str) -> PyResult<Option<String>> {
        py.detach(|| self.rt.block_on(self.broker.last_dead_id(queue))).map_err(rt_err)
    }

    fn replay_dead(&self, py: Python<'_>, queue: &str, entry_ids: Vec<String>) -> PyResult<usize> {
        py.detach(|| self.rt.block_on(self.broker.replay_dead(queue, &entry_ids))).map_err(rt_err)
    }

    /// Writes the DAG and enqueues its roots in one step; returns the workflow id and each node's task id.
    fn submit_workflow(&self, py: Python<'_>, name: String, ttl_secs: u64, nodes: Vec<NodeArgs>)
        -> PyResult<(String, Vec<String>)> {
        let nodes = nodes.into_iter().map(|(task, queue, payload, content_type, max_retries, parents, trace_ctx)| {
            let mut header = Header::new(task, queue, max_retries, content_type);
            header.trace_ctx = trace_ctx;
            WorkflowNode { header, payload, parents }
        }).collect();
        let wf = WorkflowSpec::new(name, ttl_secs, nodes);
        wf.validate().map_err(core_err)?;
        py.detach(|| self.rt.block_on(self.broker.submit_workflow(&wf))).map_err(core_err)?;
        Ok((wf.id.clone(), wf.nodes.iter().map(|n| n.header.id.clone()).collect()))
    }

    fn workflow<'py>(&self, py: Python<'py>, id: &str) -> PyResult<Option<Bound<'py, PyDict>>> {
        let w = py.detach(|| self.rt.block_on(self.broker.workflow(id))).map_err(core_err)?;
        w.map(|w| workflow_dict(py, w)).transpose()
    }

    fn cancel_workflow(&self, py: Python<'_>, id: &str) -> PyResult<bool> {
        py.detach(|| self.rt.block_on(self.broker.cancel_workflow(id))).map_err(core_err)
    }

    fn workflows<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let ws = py.detach(|| self.rt.block_on(self.broker.workflows())).map_err(core_err)?;
        ws.into_iter().map(|w| workflow_dict(py, w)).collect()
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
    #[pyo3(signature = (url, queues, concurrency, prefetch_multiplier=2, result_ttl_secs=None, consumer=None, visibility_timeout_ms=None, reclaim_interval_ms=None, heartbeat_interval_ms=None, max_deliveries=None, strict_priority=false, rate_limits=None, metrics_addr=None, resources=None, task_resources=None))]
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
        metrics_addr: Option<String>,
        resources: Option<HashMap<String, f64>>,
        task_resources: Option<HashMap<String, HashMap<String, f64>>>,
    ) -> PyResult<Self> {
        let consumer = consumer.unwrap_or_else(|| format!("w-{}-{}", std::process::id(), ulid::Ulid::generate()));
        let mut cfg = RuntimeConfig {
            queues, concurrency, prefetch_multiplier, result_ttl_secs,
            worker_id: consumer.clone(),
            strict_priority,
            rate_limits: rate_limits.unwrap_or_default(),
            resources: Resources {
                capacity: resources.unwrap_or_default(),
                requests: task_resources.unwrap_or_default(),
            },
            ..RuntimeConfig::default()
        };
        if let Some(v) = visibility_timeout_ms { cfg.visibility_timeout_ms = v; }
        if let Some(v) = reclaim_interval_ms { cfg.reclaim_interval_ms = v; }
        if let Some(v) = heartbeat_interval_ms { cfg.heartbeat_interval_ms = v; }
        if let Some(v) = max_deliveries { cfg.max_deliveries = v; }
        if let Some(a) = metrics_addr {
            use std::net::ToSocketAddrs;
            let bad = |e: String| PyValueError::new_err(format!("bad metrics address {a:?}: {e}"));
            cfg.metrics_addr = Some(
                a.to_socket_addrs().map_err(|e| bad(e.to_string()))?
                    .next().ok_or_else(|| bad("resolves to no address".into()))?,
            );
        }
        let inner = py
            .detach(move || Runtime::start(move || async move { RedisBroker::connect(&url, consumer).await }, cfg))
            .map_err(rt_err)?;
        Ok(Worker { inner })
    }

    #[getter]
    fn worker_id(&self) -> &str {
        self.inner.worker_id()
    }

    #[getter]
    fn metrics_port(&self) -> Option<u16> {
        self.inner.metrics_addr().map(|a| a.port())
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

    /// Pools resize their `pool:<name>` resource as instances are discarded and rebuilt.
    fn adjust_capacity(&self, resource: &str, delta: f64) -> PyResult<()> {
        self.inner.adjust_capacity(resource, delta).map_err(core_err)
    }

    fn pool_rebuilt(&self, name: &str) {
        self.inner.pool_rebuilt(name);
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
    #[getter]
    fn trace_ctx(&self) -> Option<&str> {
        self.0.trace_ctx.as_deref()
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

/// Routes the Rust core's log events to stderr as text or JSON lines. Only the first call installs
/// the subscriber; later calls return `false`.
#[pyfunction]
fn init_logging(format: &str, level: &str) -> PyResult<bool> {
    let level: tracing::Level = match level {
        "debug" => tracing::Level::DEBUG,
        "info" => tracing::Level::INFO,
        "warn" => tracing::Level::WARN,
        "error" => tracing::Level::ERROR,
        _ => return Err(PyValueError::new_err(format!("bad log level {level:?}"))),
    };
    let base = tracing_subscriber::fmt().with_writer(std::io::stderr).with_max_level(level).with_ansi(false);
    let installed = match format {
        "json" => base.json().flatten_event(true).with_current_span(false).with_span_list(false).try_init(),
        "text" => base.with_target(false).try_init(),
        _ => return Err(PyValueError::new_err(format!("bad log format {format:?}"))),
    };
    Ok(installed.is_ok())
}

#[pymodule]
#[pyo3(name = "_native")]
fn mayu_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<Worker>()?;
    m.add_class::<PyReadyTask>()?;
    m.add_function(wrap_pyfunction!(async_raise, m)?)?;
    m.add_function(wrap_pyfunction!(init_logging, m)?)?;
    Ok(())
}
