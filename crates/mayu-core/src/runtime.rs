use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, bounded};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::broker::{Broker, Delivery, StoredResult};
use crate::envelope::now_ms;
use crate::error::{Error, Result};
use crate::policy::{Action, Outcome, RetryPolicy, decide, full_jitter};
use crate::result::ResultRecord;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub queues: Vec<String>,
    pub concurrency: usize,
    pub prefetch_multiplier: usize,
    pub result_ttl_secs: Option<u64>,
    pub fetch_block_ms: u64,
    pub promote_interval_ms: u64,
    pub retry: RetryPolicy,
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

type Inflight = Arc<Mutex<HashMap<u64, Delivery>>>;

struct Handles {
    fetch: JoinHandle<()>,
    complete: JoinHandle<()>,
    promote: JoinHandle<()>,
}

pub struct Runtime<B: Broker> {
    rt: tokio::runtime::Runtime,
    broker: Arc<B>,
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

        let fetch = rt.spawn(fetch_loop(FetchCtx {
            broker: broker.clone(),
            queues: cfg.queues.clone(),
            sem: sem.clone(),
            batch: cap,
            block_ms: cfg.fetch_block_ms,
            inflight: inflight.clone(),
            ready_tx,
            stop: stop_rx.clone(),
        }));
        let complete = rt.spawn(completion_loop(
            broker.clone(), complete_rx, inflight.clone(), sem, cfg.retry.clone(), cfg.result_ttl_secs,
        ));
        let promote = rt.spawn(promote_loop(broker.clone(), cfg.queues.clone(), cfg.promote_interval_ms, stop_rx));

        Ok(Runtime {
            rt,
            broker,
            ready_rx,
            complete_tx: RwLock::new(Some(complete_tx)),
            stop_tx,
            inflight,
            handles: Mutex::new(Some(Handles { fetch, complete, promote })),
        })
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
                let d = self.inflight.lock().unwrap().remove(&t.token);
                if let Some(d) = d {
                    if let Err(e) = self.broker.requeue(&d).await {
                        tracing::warn!(error = %e, task_id = %d.header.id, "requeue on shutdown failed");
                    }
                }
            }
            self.complete_tx.write().unwrap().take();
            if tokio::time::timeout(grace, h.complete).await.is_err() {
                tracing::warn!("completion loop did not drain within shutdown grace");
            }
        });
    }
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
}

async fn fetch_loop<B: Broker>(mut c: FetchCtx<B>) {
    let mut next_token: u64 = 1;
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
        // Not cancelled on stop: a cancelled XREADGROUP could claim messages we never see.
        let deliveries = match c.broker.fetch(&c.queues, n, c.block_ms).await {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(error = %e, "fetch failed; backing off");
                c.sem.add_permits(n);
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        c.sem.add_permits(n.saturating_sub(deliveries.len()));
        for d in deliveries {
            let token = next_token;
            next_token += 1;
            let task = ReadyTask::from_delivery(token, &d);
            c.inflight.lock().unwrap().insert(token, d);
            // Never blocks: channel capacity equals the permit count.
            if c.ready_tx.send(task).is_err() { return; }
        }
        if *c.stop.borrow() { return; }
    }
}

async fn completion_loop<B: Broker>(
    broker: Arc<B>,
    mut rx: mpsc::UnboundedReceiver<(u64, Outcome)>,
    inflight: Inflight,
    sem: Arc<Semaphore>,
    retry: RetryPolicy,
    ttl: Option<u64>,
) {
    while let Some((token, outcome)) = rx.recv().await {
        // Unknown token: a zombie finished after its message was settled. Drop it.
        let Some(d) = inflight.lock().unwrap().remove(&token) else { continue };
        let ct = d.header.content_type.clone();
        let res = match decide(&d.header, outcome, &retry, now_ms(), full_jitter) {
            Action::Ack { result } => {
                let stored = match (result, ttl) {
                    (Some(v), Some(ttl_secs)) => Some(StoredResult {
                        id: d.header.id.clone(),
                        record: ResultRecord::success(v, &ct).encode(),
                        ttl_secs,
                    }),
                    _ => None,
                };
                broker.ack(&d, stored).await
            }
            Action::Retry { header } => broker.retry(&d, &header).await,
            Action::Dead { error, traceback } => {
                let stored = ttl.map(|ttl_secs| StoredResult {
                    id: d.header.id.clone(),
                    record: ResultRecord::failure(&error, &traceback, &ct).encode(),
                    ttl_secs,
                });
                broker.dead_letter(&d, &error, stored).await
            }
        };
        if let Err(e) = res {
            tracing::error!(error = %e, task_id = %d.header.id,
                "failed to record outcome; message stays pending for reclaim");
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
