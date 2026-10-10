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
}

#[derive(Debug, Clone)]
pub struct StoredResult {
    pub id: String,
    pub record: Vec<u8>,
    pub ttl_secs: u64,
}

/// The only component aware of the broker. Every state transition must be atomic.
pub trait Broker: Send + Sync + 'static {
    fn ensure_queues(&self, queues: &[String]) -> impl Future<Output = Result<()>> + Send;
    fn enqueue(&self, header: &Header, payload: &[u8]) -> impl Future<Output = Result<()>> + Send;
    fn fetch(&self, queues: &[String], count: usize, block_ms: u64)
        -> impl Future<Output = Result<Vec<Delivery>>> + Send;
    fn ack(&self, d: &Delivery, result: Option<StoredResult>) -> impl Future<Output = Result<()>> + Send;
    fn retry(&self, d: &Delivery, header: &Header) -> impl Future<Output = Result<()>> + Send;
    fn dead_letter(&self, d: &Delivery, error: &str, result: Option<StoredResult>)
        -> impl Future<Output = Result<()>> + Send;
    fn requeue(&self, d: &Delivery) -> impl Future<Output = Result<()>> + Send;
    fn promote(&self, queue: &str, now_ms: u64, limit: usize) -> impl Future<Output = Result<usize>> + Send;
    fn get_result(&self, id: &str) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send;
}
