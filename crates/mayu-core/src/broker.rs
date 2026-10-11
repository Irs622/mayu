use std::future::Future;

use crate::envelope::Header;
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct Delivery {
    pub queue: String,
    pub stream_id: String,
    pub header: Header,
    pub raw_header: Vec<u8>,
    pub payload: Vec<u8>,
    pub delivery_count: u64,
}

#[derive(Debug, Clone)]
pub struct StoredResult {
    pub id: String,
    pub record: Vec<u8>,
    pub ttl_secs: u64,
}

/// A queue's state in the broker. `stream` counts unclaimed and claimed entries (acked entries
/// are deleted), so `ready()` is what no worker has claimed yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueStats {
    pub stream: u64,
    pub pending: u64,
    pub delayed: u64,
    pub dead: u64,
}

impl QueueStats {
    pub fn ready(&self) -> u64 {
        self.stream.saturating_sub(self.pending)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerInfo {
    pub worker_id: String,
    pub hostname: String,
    pub pid: u32,
    pub queues: Vec<String>,
    pub concurrency: usize,
    pub inflight: usize,
    pub started_at: u64,
    pub last_seen: u64,
}

#[derive(Debug, Clone, Default)]
pub struct Reclaimed {
    pub next_cursor: String,
    pub deliveries: Vec<Delivery>,
}

/// The only component aware of the broker. Every state transition must be atomic.
pub trait Broker: Send + Sync + 'static {
    fn ensure_queues(&self, queues: &[String]) -> impl Future<Output = Result<()>> + Send;
    fn enqueue(&self, header: &Header, payload: &[u8]) -> impl Future<Output = Result<()>> + Send;
    /// Claims new entries: at most `count` in total when entries are ready, earlier `queues` first.
    /// When none are ready it blocks up to `block_ms` and may return up to one entry per queue.
    fn fetch(&self, queues: &[String], count: usize, block_ms: u64)
        -> impl Future<Output = Result<Vec<Delivery>>> + Send;
    /// Acks `d` and enqueues `follow_ups` (link callbacks) in one atomic step. The follow-ups are
    /// enqueued only if this call is the one that acks `d`, so a repeated ack adds no duplicates.
    fn ack(&self, d: &Delivery, result: Option<StoredResult>, follow_ups: &[(Header, Vec<u8>)])
        -> impl Future<Output = Result<()>> + Send;
    fn retry(&self, d: &Delivery, header: &Header) -> impl Future<Output = Result<()>> + Send;
    fn dead_letter(&self, d: &Delivery, error: &str, result: Option<StoredResult>)
        -> impl Future<Output = Result<()>> + Send;
    fn requeue(&self, d: &Delivery) -> impl Future<Output = Result<()>> + Send;
    fn promote(&self, queue: &str, now_ms: u64, limit: usize) -> impl Future<Output = Result<usize>> + Send;
    fn get_result(&self, id: &str) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send;
    /// Resets idle time on entries this consumer still owns; returns the ids it no longer owns.
    fn extend(&self, queue: &str, stream_ids: &[String]) -> impl Future<Output = Result<Vec<String>>> + Send;
    /// Claims entries idle for at least `min_idle_ms` (their worker died).
    fn reclaim(&self, queue: &str, min_idle_ms: u64, cursor: &str, count: usize)
        -> impl Future<Output = Result<Reclaimed>> + Send;
    fn heartbeat(&self, info: &WorkerInfo, ttl_ms: u64) -> impl Future<Output = Result<()>> + Send;
    fn unregister(&self, worker_id: &str) -> impl Future<Output = Result<()>> + Send;
    /// Removes consumers with no pending entries, idle at least `min_idle_ms`, and no heartbeat.
    fn prune_consumers(&self, queue: &str, min_idle_ms: u64) -> impl Future<Output = Result<usize>> + Send;
}
