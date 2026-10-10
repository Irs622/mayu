use mayu_core::{Broker, Header, StoredResult, now_ms};
use mayu_redis::{RedisBroker, stream_key};

fn url() -> String {
    std::env::var("MAYU_TEST_REDIS").unwrap_or_else(|_| "redis://127.0.0.1:6390/0".into())
}

fn uniq(prefix: &str) -> String { format!("{prefix}-{}", ulid::Ulid::generate()) }

async fn setup() -> (RedisBroker, String) {
    let q = uniq("q");
    let b = RedisBroker::connect(&url(), uniq("c")).await.expect("is Redis running? scripts/redis-up.sh");
    b.ensure_queues(&[q.clone()]).await.unwrap();
    (b, q)
}

#[tokio::test]
async fn enqueue_fetch_roundtrip() {
    let (b, q) = setup().await;
    let h = Header::new("t", &q, 3, "ct");
    b.enqueue(&h, b"payload").await.unwrap();
    let ds = b.fetch(&[q.clone()], 10, 100).await.unwrap();
    assert_eq!(ds.len(), 1);
    assert_eq!(ds[0].header, h);
    assert_eq!(ds[0].payload, b"payload");
    assert_eq!(ds[0].queue, q);
    assert_eq!(b.stats(&q).await.unwrap().pending, 1);
}

#[tokio::test]
async fn enqueue_before_group_exists_is_delivered() {
    let q = uniq("q");
    let b = RedisBroker::connect(&url(), uniq("c")).await.unwrap();
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap();
    b.ensure_queues(&[q.clone()]).await.unwrap();
    assert_eq!(b.fetch(&[q.clone()], 10, 100).await.unwrap().len(), 1);
}

#[tokio::test]
async fn ensure_queues_is_idempotent() {
    let (b, q) = setup().await;
    b.ensure_queues(&[q.clone()]).await.unwrap();
}

#[tokio::test]
async fn fetch_empty_returns_after_block() {
    let (b, q) = setup().await;
    let t = std::time::Instant::now();
    assert!(b.fetch(&[q], 1, 100).await.unwrap().is_empty());
    assert!(t.elapsed() >= std::time::Duration::from_millis(90));
}

#[tokio::test]
async fn ack_removes_entry_and_stores_result() {
    let (b, q) = setup().await;
    let h = Header::new("t", &q, 0, "ct");
    b.enqueue(&h, b"").await.unwrap();
    let d = b.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
    b.ack(&d, Some(StoredResult { id: h.id.clone(), record: b"rec".to_vec(), ttl_secs: 60 })).await.unwrap();
    let s = b.stats(&q).await.unwrap();
    assert_eq!((s.stream, s.pending), (0, 0));
    assert_eq!(b.get_result(&h.id).await.unwrap(), Some(b"rec".to_vec()));
}

#[tokio::test]
async fn delayed_enqueue_and_promote() {
    let (b, q) = setup().await;
    let mut h = Header::new("t", &q, 0, "ct");
    h.eta = Some(now_ms() + 60_000);
    b.enqueue(&h, b"p:with:colons").await.unwrap();
    assert_eq!(b.stats(&q).await.unwrap().delayed, 1);
    assert_eq!(b.promote(&q, now_ms(), 100).await.unwrap(), 0);
    assert_eq!(b.promote(&q, now_ms() + 120_000, 100).await.unwrap(), 1);
    let d = b.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
    assert_eq!(d.header, h);
    assert_eq!(d.payload, b"p:with:colons");
}

#[tokio::test]
async fn retry_moves_to_delayed_with_new_attempt() {
    let (b, q) = setup().await;
    b.enqueue(&Header::new("t", &q, 3, "ct"), b"x").await.unwrap();
    let d = b.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
    let mut next = d.header.clone();
    next.attempt = 1;
    next.eta = Some(now_ms());
    b.retry(&d, &next).await.unwrap();
    let s = b.stats(&q).await.unwrap();
    assert_eq!((s.stream, s.pending, s.delayed), (0, 0, 1));
    b.promote(&q, now_ms() + 1, 10).await.unwrap();
    let d2 = b.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
    assert_eq!(d2.header.attempt, 1);
    assert_eq!(d2.payload, b"x");
}

#[tokio::test]
async fn dead_letter_and_requeue() {
    let (b, q) = setup().await;
    let h = Header::new("t", &q, 0, "ct");
    b.enqueue(&h, b"").await.unwrap();
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap();
    let ds = b.fetch(&[q.clone()], 2, 100).await.unwrap();
    b.dead_letter(&ds[0], "boom", Some(StoredResult { id: h.id.clone(), record: b"f".to_vec(), ttl_secs: 60 }))
        .await.unwrap();
    b.requeue(&ds[1]).await.unwrap();
    let s = b.stats(&q).await.unwrap();
    assert_eq!((s.dead, s.pending, s.stream), (1, 0, 1));
    assert_eq!(b.get_result(&h.id).await.unwrap(), Some(b"f".to_vec()));
    assert_eq!(b.fetch(&[q], 1, 100).await.unwrap()[0].header, ds[1].header);
}

#[tokio::test]
async fn corrupt_header_is_dead_lettered() {
    let (b, q) = setup().await;
    let client = redis::Client::open(url()).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let _: String = redis::cmd("XADD").arg(stream_key(&q)).arg("*").arg("h").arg("garbage").arg("p").arg("x")
        .query_async(&mut c).await.unwrap();
    assert!(b.fetch(&[q.clone()], 10, 100).await.unwrap().is_empty());
    let s = b.stats(&q).await.unwrap();
    assert_eq!((s.dead, s.pending, s.stream), (1, 0, 0));
}

#[tokio::test]
async fn fetch_recovers_from_deleted_stream() {
    let (b, q) = setup().await;
    let client = redis::Client::open(url()).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let _: i64 = redis::cmd("DEL").arg(stream_key(&q)).query_async(&mut c).await.unwrap();
    assert!(b.fetch(&[q.clone()], 1, 50).await.unwrap().is_empty(), "NOGROUP handled");
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap();
    assert_eq!(b.fetch(&[q], 1, 100).await.unwrap().len(), 1);
}

#[tokio::test]
async fn concurrent_promote_never_duplicates() {
    let (b1, q) = setup().await;
    let b2 = RedisBroker::connect(&url(), uniq("c")).await.unwrap();
    for _ in 0..50 {
        let mut h = Header::new("t", &q, 0, "ct");
        h.eta = Some(now_ms() + 60_000);
        b1.enqueue(&h, b"").await.unwrap();
    }
    let later = now_ms() + 120_000;
    let (a, c) = tokio::join!(b1.promote(&q, later, 7), b2.promote(&q, later, 7));
    let mut total = a.unwrap() + c.unwrap();
    while total < 50 { total += b1.promote(&q, later, 7).await.unwrap(); }
    let s = b1.stats(&q).await.unwrap();
    assert_eq!((s.stream, s.delayed, total), (50, 0, 50));
}
