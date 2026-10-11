//! In-process `Broker` used to test the runtime without Redis.
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::broker::{Broker, Delivery, Reclaimed, StoredResult, WorkerInfo};
use crate::envelope::{Header, decode_header, encode_header, now_ms};
use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct DeadEntry {
    pub queue: String,
    pub header: Option<Header>,
    pub error: String,
}

#[derive(Clone)]
struct Entry {
    id: String,
    h: Vec<u8>,
    p: Vec<u8>,
}

struct Pending {
    queue: String,
    entry: Entry,
    owner: String,
    last_delivery: Instant,
    count: u64,
}

/// (eta ms, header, payload)
type DelayedEntry = (u64, Vec<u8>, Vec<u8>);

#[derive(Default)]
struct State {
    seq: u64,
    streams: HashMap<String, VecDeque<Entry>>,
    pending: HashMap<String, Pending>,
    delayed: HashMap<String, Vec<DelayedEntry>>,
    dead: Vec<DeadEntry>,
    results: HashMap<String, Vec<u8>>,
    workers: HashMap<String, (WorkerInfo, Instant)>,
}

impl State {
    fn push_stream(&mut self, queue: &str, h: Vec<u8>, p: Vec<u8>) {
        self.seq += 1;
        let id = format!("{}-0", self.seq);
        self.streams.entry(queue.to_string()).or_default().push_back(Entry { id, h, p });
    }

    fn bury(&mut self, queue: &str, error: String) {
        self.dead.push(DeadEntry { queue: queue.to_string(), header: None, error });
    }

    fn take(&mut self, queues: &[String], count: usize, consumer: &str) -> Vec<Delivery> {
        let mut out = Vec::new();
        for q in queues {
            while out.len() < count {
                let Some(e) = self.streams.get_mut(q).and_then(|s| s.pop_front()) else { break };
                match decode_header(&e.h) {
                    Ok(header) => {
                        out.push(Delivery {
                            queue: q.clone(),
                            stream_id: e.id.clone(),
                            header,
                            raw_header: e.h.clone(),
                            payload: e.p.clone(),
                            delivery_count: 1,
                        });
                        self.pending.insert(e.id.clone(), Pending {
                            queue: q.clone(),
                            entry: e,
                            owner: consumer.to_string(),
                            last_delivery: Instant::now(),
                            count: 1,
                        });
                    }
                    Err(err) => self.bury(q, err.to_string()),
                }
            }
        }
        out
    }
}

#[derive(Clone, Default)]
pub struct MemoryBroker {
    state: Arc<Mutex<State>>,
    notify: Arc<Notify>,
    failing: Arc<AtomicBool>,
    consumer: String,
}

impl MemoryBroker {
    /// A handle on the same state that acts as a different consumer (worker).
    pub fn with_consumer(&self, consumer: &str) -> Self {
        MemoryBroker { consumer: consumer.to_string(), ..self.clone() }
    }

    /// While set, every `Broker` call fails, simulating an outage.
    pub fn set_failing(&self, on: bool) {
        self.failing.store(on, Ordering::SeqCst);
    }

    fn check(&self) -> Result<()> {
        if self.failing.load(Ordering::SeqCst) {
            Err(Error::Broker("injected failure".into()))
        } else {
            Ok(())
        }
    }

    pub fn push(&self, header: &Header, payload: &[u8]) {
        let h = encode_header(header);
        {
            let mut s = self.state.lock().unwrap();
            match header.eta {
                Some(eta) if eta > now_ms() => {
                    s.delayed.entry(header.queue.clone()).or_default().push((eta, h, payload.to_vec()))
                }
                _ => s.push_stream(&header.queue, h, payload.to_vec()),
            }
        }
        self.notify.notify_waiters();
    }

    pub fn pending_len(&self) -> usize { self.state.lock().unwrap().pending.len() }
    pub fn stream_len(&self, q: &str) -> usize {
        self.state.lock().unwrap().streams.get(q).map_or(0, |s| s.len())
    }
    pub fn delayed_len(&self, q: &str) -> usize {
        self.state.lock().unwrap().delayed.get(q).map_or(0, |d| d.len())
    }
    pub fn dead(&self) -> Vec<DeadEntry> { self.state.lock().unwrap().dead.clone() }
    pub fn result(&self, id: &str) -> Option<Vec<u8>> { self.state.lock().unwrap().results.get(id).cloned() }
    pub fn delivery_count(&self, stream_id: &str) -> Option<u64> {
        self.state.lock().unwrap().pending.get(stream_id).map(|p| p.count)
    }
    /// The worker's heartbeat, if it has not expired.
    pub fn worker(&self, id: &str) -> Option<WorkerInfo> {
        let s = self.state.lock().unwrap();
        s.workers.get(id).filter(|(_, expires)| *expires > Instant::now()).map(|(w, _)| w.clone())
    }

    fn store(s: &mut State, r: Option<StoredResult>) {
        if let Some(r) = r { s.results.insert(r.id, r.record); }
    }
}

impl Broker for MemoryBroker {
    async fn ensure_queues(&self, _queues: &[String]) -> Result<()> { self.check() }

    async fn enqueue(&self, header: &Header, payload: &[u8]) -> Result<()> {
        self.check()?;
        self.push(header, payload);
        Ok(())
    }

    async fn fetch(&self, queues: &[String], count: usize, block_ms: u64) -> Result<Vec<Delivery>> {
        self.check()?;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(block_ms);
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let got = self.state.lock().unwrap().take(queues, count, &self.consumer);
            if !got.is_empty() { return Ok(got); }
            if tokio::time::timeout_at(deadline, notified).await.is_err() { return Ok(Vec::new()); }
        }
    }

    async fn ack(&self, d: &Delivery, result: Option<StoredResult>) -> Result<()> {
        self.check()?;
        let mut s = self.state.lock().unwrap();
        s.pending.remove(&d.stream_id);
        Self::store(&mut s, result);
        Ok(())
    }

    async fn retry(&self, d: &Delivery, header: &Header) -> Result<()> {
        self.check()?;
        let mut s = self.state.lock().unwrap();
        s.pending.remove(&d.stream_id);
        s.delayed.entry(d.queue.clone()).or_default()
            .push((header.eta.unwrap_or(0), encode_header(header), d.payload.clone()));
        Ok(())
    }

    async fn dead_letter(&self, d: &Delivery, error: &str, result: Option<StoredResult>) -> Result<()> {
        self.check()?;
        let mut s = self.state.lock().unwrap();
        s.pending.remove(&d.stream_id);
        s.dead.push(DeadEntry { queue: d.queue.clone(), header: Some(d.header.clone()), error: error.into() });
        Self::store(&mut s, result);
        Ok(())
    }

    async fn requeue(&self, d: &Delivery) -> Result<()> {
        self.check()?;
        {
            let mut s = self.state.lock().unwrap();
            s.pending.remove(&d.stream_id);
            s.push_stream(&d.queue, d.raw_header.clone(), d.payload.clone());
        }
        self.notify.notify_waiters();
        Ok(())
    }

    async fn promote(&self, queue: &str, now_ms: u64, limit: usize) -> Result<usize> {
        self.check()?;
        let n = {
            let mut s = self.state.lock().unwrap();
            let delayed = s.delayed.entry(queue.to_string()).or_default();
            delayed.sort_by_key(|e| e.0);
            let due = delayed.iter().take(limit).take_while(|e| e.0 <= now_ms).count();
            let moved: Vec<_> = delayed.drain(..due).collect();
            for (_, h, p) in moved { s.push_stream(queue, h, p); }
            due
        };
        if n > 0 { self.notify.notify_waiters(); }
        Ok(n)
    }

    async fn get_result(&self, id: &str) -> Result<Option<Vec<u8>>> {
        self.check()?;
        Ok(self.result(id))
    }

    async fn extend(&self, _queue: &str, stream_ids: &[String]) -> Result<Vec<String>> {
        self.check()?;
        let mut s = self.state.lock().unwrap();
        let now = Instant::now();
        let mut lost = Vec::new();
        for id in stream_ids {
            match s.pending.get_mut(id) {
                Some(p) if p.owner == self.consumer => p.last_delivery = now,
                _ => lost.push(id.clone()),
            }
        }
        Ok(lost)
    }

    async fn reclaim(&self, queue: &str, min_idle_ms: u64, _cursor: &str, count: usize) -> Result<Reclaimed> {
        self.check()?;
        let mut s = self.state.lock().unwrap();
        let min_idle = Duration::from_millis(min_idle_ms);
        let mut ids: Vec<String> = s.pending.iter()
            .filter(|(_, p)| p.queue == queue && p.last_delivery.elapsed() >= min_idle)
            .map(|(id, _)| id.clone())
            .collect();
        ids.sort_by_key(|id| id.split('-').next().and_then(|n| n.parse::<u64>().ok()).unwrap_or(0));
        ids.truncate(count);
        let mut deliveries = Vec::new();
        for id in ids {
            let p = s.pending.get_mut(&id).expect("listed above");
            p.owner = self.consumer.clone();
            p.last_delivery = Instant::now();
            p.count += 1;
            let (entry, delivery_count) = (p.entry.clone(), p.count);
            match decode_header(&entry.h) {
                Ok(header) => deliveries.push(Delivery {
                    queue: queue.to_string(),
                    stream_id: id,
                    header,
                    raw_header: entry.h,
                    payload: entry.p,
                    delivery_count,
                }),
                Err(err) => {
                    s.pending.remove(&id);
                    s.bury(queue, err.to_string());
                }
            }
        }
        // The memory broker scans everything each call, so the cursor always wraps.
        Ok(Reclaimed { next_cursor: "0-0".into(), deliveries })
    }

    async fn heartbeat(&self, info: &WorkerInfo, ttl_ms: u64) -> Result<()> {
        self.check()?;
        let expires = Instant::now() + Duration::from_millis(ttl_ms);
        self.state.lock().unwrap().workers.insert(info.worker_id.clone(), (info.clone(), expires));
        Ok(())
    }

    async fn unregister(&self, worker_id: &str) -> Result<()> {
        self.check()?;
        self.state.lock().unwrap().workers.remove(worker_id);
        Ok(())
    }

    /// The memory broker keeps no consumer registry, so there is nothing to prune.
    async fn prune_consumers(&self, _queue: &str, _min_idle_ms: u64) -> Result<usize> {
        self.check()?;
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
    }

    #[test]
    fn enqueue_fetch_ack() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            let h = Header::new("t", "q", 3, "ct");
            b.enqueue(&h, b"p").await.unwrap();
            let ds = b.fetch(&["q".into()], 10, 10).await.unwrap();
            assert_eq!(ds.len(), 1);
            assert_eq!(ds[0].header, h);
            assert_eq!(ds[0].payload, b"p");
            assert_eq!(b.pending_len(), 1);
            b.ack(&ds[0], Some(StoredResult { id: h.id.clone(), record: b"r".to_vec(), ttl_secs: 9 }))
                .await.unwrap();
            assert_eq!(b.pending_len(), 0);
            assert_eq!(b.result(&h.id), Some(b"r".to_vec()));
        });
    }

    #[test]
    fn fetch_times_out_empty() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            let t = std::time::Instant::now();
            assert!(b.fetch(&["q".into()], 1, 50).await.unwrap().is_empty());
            assert!(t.elapsed() >= std::time::Duration::from_millis(45));
        });
    }

    #[test]
    fn fetch_wakes_on_enqueue() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            let b2 = b.clone();
            let h = Header::new("t", "q", 0, "ct");
            let waiter = tokio::spawn(async move { b2.fetch(&["q".into()], 1, 5_000).await.unwrap() });
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            b.enqueue(&h, b"").await.unwrap();
            let got = tokio::time::timeout(std::time::Duration::from_secs(1), waiter).await.unwrap().unwrap();
            assert_eq!(got.len(), 1);
        });
    }

    #[test]
    fn delayed_then_promote() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            let mut h = Header::new("t", "q", 0, "ct");
            h.eta = Some(now_ms() + 60_000);
            b.enqueue(&h, b"").await.unwrap();
            assert_eq!(b.delayed_len("q"), 1);
            assert_eq!(b.promote("q", now_ms(), 100).await.unwrap(), 0);
            assert_eq!(b.promote("q", now_ms() + 120_000, 100).await.unwrap(), 1);
            assert_eq!(b.stream_len("q"), 1);
        });
    }

    #[test]
    fn retry_dead_requeue() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            for _ in 0..3 { b.enqueue(&Header::new("t", "q", 1, "ct"), b"").await.unwrap(); }
            let ds = b.fetch(&["q".into()], 3, 10).await.unwrap();
            let mut next = ds[0].header.clone();
            next.attempt = 1;
            next.eta = Some(0);
            b.retry(&ds[0], &next).await.unwrap();
            b.dead_letter(&ds[1], "bad", None).await.unwrap();
            b.requeue(&ds[2]).await.unwrap();
            assert_eq!(b.pending_len(), 0);
            assert_eq!(b.delayed_len("q"), 1);
            assert_eq!(b.dead().len(), 1);
            assert_eq!(b.dead()[0].error, "bad");
            assert_eq!(b.stream_len("q"), 1);
        });
    }

    #[test]
    fn failing_switch_fails_calls_until_cleared() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            let h = Header::new("t", "q", 0, "ct");
            b.set_failing(true);
            assert!(b.enqueue(&h, b"").await.is_err());
            assert!(b.fetch(&["q".into()], 1, 10).await.is_err());
            b.set_failing(false);
            b.enqueue(&h, b"").await.unwrap();
            assert_eq!(b.fetch(&["q".into()], 1, 10).await.unwrap().len(), 1);
        });
    }

    #[test]
    fn extend_keeps_ownership_and_reports_lost() {
        rt().block_on(async {
            let a = MemoryBroker::default().with_consumer("a");
            let b = a.with_consumer("b");
            a.enqueue(&Header::new("t", "q", 0, "ct"), b"").await.unwrap();
            let d = a.fetch(&["q".into()], 1, 10).await.unwrap().remove(0);
            assert_eq!(d.delivery_count, 1);
            assert!(a.extend("q", &[d.stream_id.clone()]).await.unwrap().is_empty());
            let r = b.reclaim("q", 0, "0-0", 10).await.unwrap();
            assert_eq!(r.deliveries.len(), 1);
            assert_eq!(r.deliveries[0].delivery_count, 2);
            assert_eq!(a.delivery_count(&d.stream_id), Some(2));
            assert_eq!(a.extend("q", &[d.stream_id.clone()]).await.unwrap(), vec![d.stream_id.clone()]);
        });
    }

    #[test]
    fn reclaim_respects_min_idle_and_queue() {
        rt().block_on(async {
            let a = MemoryBroker::default().with_consumer("a");
            let b = a.with_consumer("b");
            a.enqueue(&Header::new("t", "q", 0, "ct"), b"").await.unwrap();
            a.fetch(&["q".into()], 1, 10).await.unwrap();
            assert!(b.reclaim("q", 60_000, "0-0", 10).await.unwrap().deliveries.is_empty());
            assert!(b.reclaim("other", 0, "0-0", 10).await.unwrap().deliveries.is_empty());
            tokio::time::sleep(Duration::from_millis(30)).await;
            assert_eq!(b.reclaim("q", 20, "0-0", 10).await.unwrap().deliveries.len(), 1);
        });
    }

    #[test]
    fn heartbeat_expires_and_unregisters() {
        rt().block_on(async {
            let b = MemoryBroker::default();
            let info = WorkerInfo {
                worker_id: "w1".into(), hostname: "h".into(), pid: 1, queues: vec!["q".into()],
                concurrency: 2, inflight: 0, started_at: 0, last_seen: 0,
            };
            b.heartbeat(&info, 50).await.unwrap();
            assert_eq!(b.worker("w1"), Some(info.clone()));
            tokio::time::sleep(Duration::from_millis(80)).await;
            assert_eq!(b.worker("w1"), None);
            b.heartbeat(&info, 60_000).await.unwrap();
            b.unregister("w1").await.unwrap();
            assert_eq!(b.worker("w1"), None);
        });
    }
}
