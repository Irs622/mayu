use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::broker::{Broker, Delivery, StoredResult, WorkerInfo};
use crate::envelope::{Header, now_ms};
use crate::error::{Error, Result};
use crate::policy::{Action, Outcome, RetryPolicy, decide, full_jitter};
use crate::result::ResultRecord;

/// Heartbeats between consumer-pruning passes.
const PRUNE_EVERY_TICKS: u64 = 12;
/// Exit code when a core loop panics (EX_SOFTWARE).
const PANIC_EXIT_CODE: i32 = 70;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub queues: Vec<String>,
    pub concurrency: usize,
    pub prefetch_multiplier: usize,
    pub result_ttl_secs: Option<u64>,
    pub fetch_block_ms: u64,
    pub promote_interval_ms: u64,
    pub retry: RetryPolicy,
    pub worker_id: String,
    pub visibility_timeout_ms: u64,
    pub reclaim_interval_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub max_deliveries: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            queues: vec!["default".into()],
            concurrency: 8,
            prefetch_multiplier: 2,
            result_ttl_secs: None,
            fetch_block_ms: 1_000,
            promote_interval_ms: 100,
            retry: RetryPolicy::default(),
            worker_id: format!("w-{}-{}", std::process::id(), ulid::Ulid::generate()),
            visibility_timeout_ms: 30_000,
            reclaim_interval_ms: 5_000,
            heartbeat_interval_ms: 5_000,
            max_deliveries: 5,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadyTask {
    pub token: u64,
    pub id: String,
    pub task: String,
    pub queue: String,
    pub attempt: u32,
    pub max_retries: u32,
    pub content_type: String,
    pub payload: Vec<u8>,
}

impl ReadyTask {
    fn from_delivery(token: u64, d: &Delivery) -> Self {
        ReadyTask {
            token,
            id: d.header.id.clone(),
            task: d.header.task.clone(),
            queue: d.queue.clone(),
            attempt: d.header.attempt,
            max_retries: d.header.max_retries,
            content_type: d.header.content_type.clone(),
            payload: d.payload.clone(),
        }
    }
}

/// A message this worker has claimed and not yet settled.
struct InFlight {
    delivery: Delivery,
    last_claim: Instant,
    /// Another worker reclaimed it; stop extending.
    lost: bool,
}

type Inflight = Arc<Mutex<HashMap<u64, InFlight>>>;

struct Handles {
    fetch: JoinHandle<()>,
    complete: JoinHandle<()>,
    promote: JoinHandle<()>,
    keepalive: JoinHandle<()>,
    heartbeat: JoinHandle<()>,
}

pub struct Runtime<B: Broker> {
    rt: tokio::runtime::Runtime,
    broker: Arc<B>,
    worker_id: String,
    ready_rx: Receiver<ReadyTask>,
    complete_tx: RwLock<Option<mpsc::UnboundedSender<(u64, Outcome)>>>,
    stop_tx: watch::Sender<bool>,
    inflight: Inflight,
    handles: Mutex<Option<Handles>>,
}

impl<B: Broker> Runtime<B> {
    pub fn start<F, Fut>(connect: F, cfg: RuntimeConfig) -> Result<Self>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<B>>,
    {
        if cfg.concurrency == 0 || cfg.prefetch_multiplier == 0 {
            return Err(Error::Config("concurrency and prefetch_multiplier must be >= 1".into()));
        }
        if cfg.queues.is_empty() {
            return Err(Error::Config("at least one queue is required".into()));
        }
        if cfg.visibility_timeout_ms == 0 || cfg.reclaim_interval_ms == 0 || cfg.heartbeat_interval_ms == 0 {
            return Err(Error::Config("visibility, reclaim and heartbeat intervals must be > 0".into()));
        }
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("mayu-rt")
            .enable_all()
            .build()
            .map_err(|e| Error::Config(e.to_string()))?;
        let broker = Arc::new(rt.block_on(async {
            let b = connect().await?;
            b.ensure_queues(&cfg.queues).await?;
            Ok::<_, Error>(b)
        })?);

        let cap = cfg.concurrency * cfg.prefetch_multiplier;
        let (ready_tx, ready_rx) = bounded(cap);
        let (complete_tx, complete_rx) = mpsc::unbounded_channel();
        let (stop_tx, stop_rx) = watch::channel(false);
        let sem = Arc::new(Semaphore::new(cap));
        let inflight: Inflight = Arc::default();

        let fetch = rt.spawn(critical("fetch", fetch_loop(FetchCtx {
            broker: broker.clone(),
            queues: cfg.queues.clone(),
            sem: sem.clone(),
            batch: cap,
            block_ms: cfg.fetch_block_ms,
            inflight: inflight.clone(),
            ready_tx,
            stop: stop_rx.clone(),
            visibility_ms: cfg.visibility_timeout_ms,
            reclaim_interval: Duration::from_millis(cfg.reclaim_interval_ms),
            max_deliveries: cfg.max_deliveries,
            result_ttl: cfg.result_ttl_secs,
        })));
        let complete = rt.spawn(critical("completion", completion_loop(
            broker.clone(),
            complete_rx,
            inflight.clone(),
            sem,
            cfg.retry.clone(),
            cfg.result_ttl_secs,
            Duration::from_millis(cfg.visibility_timeout_ms),
        )));
        let promote = rt.spawn(critical("promote",
            promote_loop(broker.clone(), cfg.queues.clone(), cfg.promote_interval_ms, stop_rx)));
        let keepalive = rt.spawn(critical("keepalive",
            keepalive_loop(broker.clone(), inflight.clone(), cfg.visibility_timeout_ms)));
        let info = WorkerInfo {
            worker_id: cfg.worker_id.clone(),
            hostname: hostname(),
            pid: std::process::id(),
            queues: cfg.queues.clone(),
            concurrency: cfg.concurrency,
            inflight: 0,
            started_at: now_ms(),
            last_seen: 0,
        };
        let prune_idle_ms = cfg.visibility_timeout_ms.max(cfg.heartbeat_interval_ms.saturating_mul(3)).saturating_mul(2);
        let heartbeat = rt.spawn(critical("heartbeat",
            heartbeat_loop(broker.clone(), info, inflight.clone(), cfg.heartbeat_interval_ms, prune_idle_ms)));

        Ok(Runtime {
            rt,
            broker,
            worker_id: cfg.worker_id,
            ready_rx,
            complete_tx: RwLock::new(Some(complete_tx)),
            stop_tx,
            inflight,
            handles: Mutex::new(Some(Handles { fetch, complete, promote, keepalive, heartbeat })),
        })
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Option<ReadyTask> {
        self.ready_rx.recv_timeout(timeout).ok()
    }

    pub fn complete(&self, token: u64, outcome: Outcome) {
        if let Some(tx) = self.complete_tx.read().unwrap().as_ref() {
            let _ = tx.send((token, outcome));
        }
    }

    pub fn stop_fetching(&self) {
        self.stop_tx.send_replace(true);
    }

    pub fn inflight(&self) -> usize {
        self.inflight.lock().unwrap().len()
    }

    pub fn shutdown(&self, grace: Duration) {
        self.stop_fetching();
        let Some(h) = self.handles.lock().unwrap().take() else { return };
        self.rt.block_on(async {
            let _ = h.fetch.await;
            let _ = h.promote.await;
            let leftovers: Vec<ReadyTask> = self.ready_rx.try_iter().collect();
            for t in leftovers {
                let f = self.inflight.lock().unwrap().remove(&t.token);
                if let Some(f) = f {
                    if let Err(e) = self.broker.requeue(&f.delivery).await {
                        tracing::warn!(error = %e, task_id = %f.delivery.header.id, "requeue on shutdown failed");
                    }
                }
            }
            self.complete_tx.write().unwrap().take();
            if tokio::time::timeout(grace, h.complete).await.is_err() {
                tracing::warn!("completion loop did not drain within shutdown grace");
            }
            // Tasks still running past the grace period are reclaimed by other workers.
            h.keepalive.abort();
            h.heartbeat.abort();
            let _ = h.keepalive.await;
            let _ = h.heartbeat.await;
            if let Err(e) = self.broker.unregister(&self.worker_id).await {
                tracing::warn!(error = %e, "unregistering worker failed");
            }
        });
    }
}

/// Runs a core loop; a panic inside it terminates the process (fail fast, spec §5.2).
/// Cancellation (abort, runtime drop) is not a panic and exits quietly.
async fn critical<F: Future<Output = ()>>(name: &'static str, fut: F) {
    struct ExitOnPanic(&'static str);
    impl Drop for ExitOnPanic {
        fn drop(&mut self) {
            if std::thread::panicking() {
                eprintln!("mayu: fatal: panic in the {} loop; exiting", self.0);
                std::process::exit(PANIC_EXIT_CODE);
            }
        }
    }
    let _guard = ExitOnPanic(name);
    fut.await;
}

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "unknown".into())
}

struct FetchCtx<B> {
    broker: Arc<B>,
    queues: Vec<String>,
    sem: Arc<Semaphore>,
    batch: usize,
    block_ms: u64,
    inflight: Inflight,
    ready_tx: Sender<ReadyTask>,
    stop: watch::Receiver<bool>,
    visibility_ms: u64,
    reclaim_interval: Duration,
    max_deliveries: u64,
    result_ttl: Option<u64>,
}

async fn fetch_loop<B: Broker>(mut c: FetchCtx<B>) {
    let mut next_token: u64 = 1;
    let mut next_reclaim = Instant::now();
    let mut cursors: HashMap<String, String> = HashMap::new();
    loop {
        let permit = tokio::select! {
            p = c.sem.clone().acquire_owned() => match p { Ok(p) => p, Err(_) => return },
            _ = c.stop.wait_for(|s| *s) => return,
        };
        permit.forget();
        let mut n = 1;
        while n < c.batch {
            match c.sem.try_acquire() {
                Ok(p) => { p.forget(); n += 1; }
                Err(_) => break,
            }
        }
        let mut got: Vec<Delivery> = Vec::new();
        if Instant::now() >= next_reclaim {
            next_reclaim = Instant::now() + c.reclaim_interval;
            got = reclaim_due(&c, &mut cursors, n).await;
        }
        if got.is_empty() {
            // Not cancelled on stop: a cancelled XREADGROUP could claim messages we never see.
            match c.broker.fetch(&c.queues, n, c.block_ms).await {
                Ok(d) => got = d,
                Err(e) => {
                    tracing::warn!(error = %e, "fetch failed; backing off");
                    c.sem.add_permits(n);
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                        _ = c.stop.wait_for(|s| *s) => return,
                    }
                    continue;
                }
            }
        }
        c.sem.add_permits(n.saturating_sub(got.len()));
        for d in got {
            let token = next_token;
            next_token += 1;
            let task = ReadyTask::from_delivery(token, &d);
            c.inflight.lock().unwrap().insert(token, InFlight { delivery: d, last_claim: Instant::now(), lost: false });
            // Never blocks: channel capacity equals the permit count.
            if c.ready_tx.send(task).is_err() { return; }
        }
        if *c.stop.borrow() { return; }
    }
}

/// Claims messages idle longer than the visibility timeout (their worker died).
/// Messages delivered more than `max_deliveries` times are dead-lettered instead.
async fn reclaim_due<B: Broker>(c: &FetchCtx<B>, cursors: &mut HashMap<String, String>, limit: usize) -> Vec<Delivery> {
    let mut got = Vec::new();
    for q in &c.queues {
        if got.len() >= limit { break; }
        let cursor = cursors.get(q).cloned().unwrap_or_else(|| "0-0".to_string());
        match c.broker.reclaim(q, c.visibility_ms, &cursor, limit - got.len()).await {
            Ok(r) => {
                cursors.insert(q.clone(), r.next_cursor);
                for d in r.deliveries {
                    if d.delivery_count > c.max_deliveries {
                        bury_poison(c, &d).await;
                    } else {
                        tracing::info!(task_id = %d.header.id, deliveries = d.delivery_count, "reclaimed abandoned task");
                        got.push(d);
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, queue = %q, "reclaim failed"),
        }
    }
    got
}

async fn bury_poison<B: Broker>(c: &FetchCtx<B>, d: &Delivery) {
    let error = format!("poison message: delivered {} times without completing", d.delivery_count);
    let stored = c.result_ttl.map(|ttl_secs| StoredResult {
        id: d.header.id.clone(),
        record: ResultRecord::failure(&error, "", &d.header.content_type).encode(),
        ttl_secs,
    });
    if let Err(e) = c.broker.dead_letter(d, &error, stored).await {
        tracing::error!(error = %e, task_id = %d.header.id, "failed to dead-letter poison message");
    }
}

/// How an outcome is written to the broker.
enum Record {
    Ack(Option<StoredResult>),
    Retry(Header),
    Dead(String, Option<StoredResult>),
}

async fn record<B: Broker>(broker: &B, d: &Delivery, r: &Record) -> Result<()> {
    match r {
        Record::Ack(stored) => broker.ack(d, stored.clone()).await,
        Record::Retry(header) => broker.retry(d, header).await,
        Record::Dead(error, stored) => broker.dead_letter(d, error, stored.clone()).await,
    }
}

async fn completion_loop<B: Broker>(
    broker: Arc<B>,
    mut rx: mpsc::UnboundedReceiver<(u64, Outcome)>,
    inflight: Inflight,
    sem: Arc<Semaphore>,
    retry: RetryPolicy,
    ttl: Option<u64>,
    record_deadline: Duration,
) {
    while let Some((token, outcome)) = rx.recv().await {
        // Unknown token: a zombie finished after its message was settled. Drop it.
        let Some(f) = inflight.lock().unwrap().remove(&token) else { continue };
        let d = f.delivery;
        let ct = d.header.content_type.clone();
        let rec = match decide(&d.header, outcome, &retry, now_ms(), full_jitter) {
            Action::Ack { result } => Record::Ack(match (result, ttl) {
                (Some(v), Some(ttl_secs)) => Some(StoredResult {
                    id: d.header.id.clone(),
                    record: ResultRecord::success(v, &ct).encode(),
                    ttl_secs,
                }),
                _ => None,
            }),
            Action::Retry { header } => Record::Retry(header),
            Action::Dead { error, traceback } => {
                let stored = ttl.map(|ttl_secs| StoredResult {
                    id: d.header.id.clone(),
                    record: ResultRecord::failure(&error, &traceback, &ct).encode(),
                    ttl_secs,
                });
                Record::Dead(error, stored)
            }
        };
        // Buffer through broker outages (bounded by the prefetch permits this message holds).
        let started = Instant::now();
        let mut delay = Duration::from_millis(100);
        loop {
            match record(&*broker, &d, &rec).await {
                Ok(()) => break,
                Err(e) if started.elapsed() + delay < record_deadline => {
                    tracing::warn!(error = %e, task_id = %d.header.id, "recording outcome failed; retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(5));
                }
                Err(e) => {
                    tracing::error!(error = %e, task_id = %d.header.id,
                        "giving up recording outcome; the message will be reclaimed and run again");
                    break;
                }
            }
        }
        sem.add_permits(1);
    }
}

async fn promote_loop<B: Broker>(
    broker: Arc<B>,
    queues: Vec<String>,
    interval_ms: u64,
    mut stop: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(interval_ms.max(1)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = stop.wait_for(|s| *s) => return,
        }
        for q in &queues {
            if let Err(e) = broker.promote(q, now_ms(), 1_000).await {
                tracing::warn!(error = %e, queue = %q, "promote failed");
            }
        }
    }
}

/// Re-claims messages this worker holds before they look abandoned (spec §4.4 step 4).
async fn keepalive_loop<B: Broker>(broker: Arc<B>, inflight: Inflight, visibility_ms: u64) {
    let threshold = Duration::from_millis(visibility_ms / 2);
    let mut tick = tokio::time::interval(Duration::from_millis((visibility_ms / 4).max(10)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let due: HashMap<String, Vec<(u64, String)>> = {
            let map = inflight.lock().unwrap();
            let mut due: HashMap<String, Vec<(u64, String)>> = HashMap::new();
            for (token, f) in map.iter() {
                if !f.lost && f.last_claim.elapsed() >= threshold {
                    due.entry(f.delivery.queue.clone()).or_default().push((*token, f.delivery.stream_id.clone()));
                }
            }
            due
        };
        for (queue, items) in due {
            let ids: Vec<String> = items.iter().map(|(_, id)| id.clone()).collect();
            match broker.extend(&queue, &ids).await {
                Ok(lost) => {
                    let now = Instant::now();
                    let mut map = inflight.lock().unwrap();
                    for (token, id) in &items {
                        let Some(f) = map.get_mut(token) else { continue };
                        if lost.contains(id) {
                            f.lost = true;
                            tracing::warn!(task_id = %f.delivery.header.id,
                                "lost ownership of a running task; another worker may run it too");
                        } else {
                            f.last_claim = now;
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, queue = %queue, "extending ownership failed"),
            }
        }
    }
}

async fn heartbeat_loop<B: Broker>(
    broker: Arc<B>,
    base: WorkerInfo,
    inflight: Inflight,
    interval_ms: u64,
    prune_idle_ms: u64,
) {
    let ttl_ms = interval_ms.saturating_mul(3);
    let mut tick = tokio::time::interval(Duration::from_millis(interval_ms.max(10)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut beats: u64 = 0;
    loop {
        tick.tick().await;
        let mut info = base.clone();
        info.inflight = inflight.lock().unwrap().len();
        info.last_seen = now_ms();
        if let Err(e) = broker.heartbeat(&info, ttl_ms).await {
            tracing::warn!(error = %e, "heartbeat failed");
        }
        if beats % PRUNE_EVERY_TICKS == 0 {
            for q in &info.queues {
                if let Err(e) = broker.prune_consumers(q, prune_idle_ms).await {
                    tracing::warn!(error = %e, queue = %q, "pruning consumers failed");
                }
            }
        }
        beats += 1;
    }
}
