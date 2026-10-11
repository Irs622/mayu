//! In-process `Broker` used to test the runtime without Redis.
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::broker::{Broker, Delivery, StoredResult};
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

#[derive(Default)]
struct State {
    seq: u64,
    streams: HashMap<String, VecDeque<Entry>>,
    pending: HashMap<String, Entry>,
    delayed: HashMap<String, Vec<(u64, Vec<u8>, Vec<u8>)>>,
    dead: Vec<DeadEntry>,
    results: HashMap<String, Vec<u8>>,
}

impl State {
    fn push_stream(&mut self, queue: &str, h: Vec<u8>, p: Vec<u8>) {
        self.seq += 1;
        let id = format!("{}-0", self.seq);
        self.streams.entry(queue.to_string()).or_default().push_back(Entry { id, h, p });
    }

    fn take(&mut self, queues: &[String], count: usize) -> Vec<Delivery> {
        let mut out = Vec::new();
        for q in queues {
            while out.len() < count {
                let Some(e) = self.streams.get_mut(q).and_then(|s| s.pop_front()) else { break };
                match decode_header(&e.h) {
                    Ok(header) => {
                        self.pending.insert(e.id.clone(), e.clone());
                        out.push(Delivery { queue: q.clone(), stream_id: e.id, header, raw_header: e.h, payload: e.p });
                    }
                    Err(err) => self.dead.push(DeadEntry { queue: q.clone(), header: None, error: err.to_string() }),
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
}

impl MemoryBroker {
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

    fn store(s: &mut State, r: Option<StoredResult>) {
        if let Some(r) = r { s.results.insert(r.id, r.record); }
    }
}

impl Broker for MemoryBroker {
    async fn ensure_queues(&self, _queues: &[String]) -> Result<()> {
        self.check()
    }

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
            let got = self.state.lock().unwrap().take(queues, count);
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
}
