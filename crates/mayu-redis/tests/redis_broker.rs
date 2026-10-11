use std::time::Duration;
use mayu_core::{Broker, Header, StoredResult, WorkerInfo, now_ms};
use mayu_redis::{RedisBroker, stream_key, worker_key};
use redis::Value;

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

async fn raw() -> redis::aio::MultiplexedConnection {
    redis::Client::open(url()).unwrap().get_multiplexed_async_connection().await.unwrap()
}

async fn consumer_count(q: &str) -> usize {
    let v: Vec<Value> = redis::cmd("XINFO").arg("CONSUMERS").arg(stream_key(q)).arg("mayu")
        .query_async(&mut raw().await).await.unwrap();
    v.len()
}

#[tokio::test]
async fn fresh_fetch_has_delivery_count_one() {
    let (b, q) = setup().await;
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap();
    assert_eq!(b.fetch(&[q.clone()], 1, 100).await.unwrap()[0].delivery_count, 1);
}

#[tokio::test]
async fn extend_keeps_ownership_and_reports_lost() {
    let (a, q) = setup().await;
    let b = RedisBroker::connect(&url(), uniq("c")).await.unwrap();
    a.enqueue(&Header::new("t", &q, 0, "ct"), b"x").await.unwrap();
    let d = a.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(a.extend(&q, &[d.stream_id.clone()]).await.unwrap().is_empty());
    assert!(b.reclaim(&q, 100, "0-0", 10).await.unwrap().deliveries.is_empty(), "extend reset idle time");
    let r = b.reclaim(&q, 0, "0-0", 10).await.unwrap();
    assert_eq!(r.deliveries.len(), 1);
    assert_eq!(r.deliveries[0].delivery_count, 2);
    assert_eq!(r.deliveries[0].payload, b"x");
    assert_eq!(a.extend(&q, &[d.stream_id.clone()]).await.unwrap(), vec![d.stream_id.clone()]);
    assert!(a.extend(&q, &[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn reclaim_pages_with_cursor() {
    let (a, q) = setup().await;
    let b = RedisBroker::connect(&url(), uniq("c")).await.unwrap();
    for _ in 0..5 { a.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap(); }
    assert_eq!(a.fetch(&[q.clone()], 5, 100).await.unwrap().len(), 5);
    let (mut cursor, mut total) = ("0-0".to_string(), 0);
    for _ in 0..10 {
        let r = b.reclaim(&q, 0, &cursor, 2).await.unwrap();
        total += r.deliveries.len();
        cursor = r.next_cursor;
        if cursor == "0-0" { break; }
    }
    assert_eq!(total, 5);
}

#[tokio::test]
async fn reclaim_dead_letters_corrupt_header() {
    let (b, q) = setup().await;
    let mut c = raw().await;
    let _: String = redis::cmd("XADD").arg(stream_key(&q)).arg("*").arg("h").arg("garbage")
        .query_async(&mut c).await.unwrap();
    let _: Value = redis::cmd("XREADGROUP").arg("GROUP").arg("mayu").arg("someone-else")
        .arg("COUNT").arg(1).arg("STREAMS").arg(stream_key(&q)).arg(">")
        .query_async(&mut c).await.unwrap();
    assert!(b.reclaim(&q, 0, "0-0", 10).await.unwrap().deliveries.is_empty());
    let s = b.stats(&q).await.unwrap();
    assert_eq!((s.dead, s.pending), (1, 0));
}

#[tokio::test]
async fn heartbeat_roundtrip_ttl_and_unregister() {
    let (b, _q) = setup().await;
    let info = WorkerInfo {
        worker_id: uniq("w"), hostname: "host-1".into(), pid: 42, queues: vec!["a".into(), "b".into()],
        concurrency: 8, inflight: 3, started_at: 1_000, last_seen: 2_000,
    };
    b.heartbeat(&info, 300).await.unwrap();
    assert_eq!(b.worker_info(&info.worker_id).await.unwrap(), Some(info.clone()));
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(b.worker_info(&info.worker_id).await.unwrap(), None, "expired");
    b.heartbeat(&info, 60_000).await.unwrap();
    b.unregister(&info.worker_id).await.unwrap();
    assert_eq!(b.worker_info(&info.worker_id).await.unwrap(), None);
    assert!(worker_key("x").starts_with("mayu:workers:"));
}

#[tokio::test]
async fn prune_removes_only_idle_consumers_without_heartbeat_or_pending() {
    let q = uniq("q");
    let ghost_name = uniq("ghost");
    let live_name = uniq("live");
    let busy_name = uniq("busy");
    let ghost = RedisBroker::connect(&url(), ghost_name.clone()).await.unwrap();
    let live = RedisBroker::connect(&url(), live_name.clone()).await.unwrap();
    let busy = RedisBroker::connect(&url(), busy_name.clone()).await.unwrap();
    ghost.ensure_queues(&[q.clone()]).await.unwrap();
    for _ in 0..3 { ghost.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap(); }
    for b in [&ghost, &live] {
        let d = b.fetch(&[q.clone()], 1, 100).await.unwrap().remove(0);
        b.ack(&d, None).await.unwrap(); // consumer exists, 0 pending
    }
    busy.fetch(&[q.clone()], 1, 100).await.unwrap(); // 1 pending, no heartbeat
    live.heartbeat(&WorkerInfo {
        worker_id: live_name.clone(), hostname: "h".into(), pid: 1, queues: vec![q.clone()],
        concurrency: 1, inflight: 0, started_at: 0, last_seen: 0,
    }, 60_000).await.unwrap();
    assert_eq!(consumer_count(&q).await, 3);
    assert_eq!(ghost.prune_consumers(&q, 0).await.unwrap(), 1);
    assert_eq!(consumer_count(&q).await, 2, "live (heartbeat) and busy (pending) are kept");
    assert_eq!(ghost.prune_consumers(&q, 60_000).await.unwrap(), 0, "min idle respected");
    assert_eq!(ghost.prune_consumers(&uniq("nope"), 0).await.unwrap(), 0, "missing stream is fine");
}

#[tokio::test]
async fn fetch_takes_queues_in_order_and_caps_the_total() {
    let (b, q1) = setup().await;
    let q2 = uniq("q");
    b.ensure_queues(std::slice::from_ref(&q2)).await.unwrap();
    for q in [&q1, &q2] {
        for _ in 0..3 { b.enqueue(&Header::new("t", q, 0, "ct"), b"").await.unwrap(); }
    }
    let got = b.fetch(&[q2.clone(), q1.clone()], 4, 100).await.unwrap();
    let queues: Vec<&str> = got.iter().map(|d| d.queue.as_str()).collect();
    assert_eq!(queues, vec![q2.as_str(), q2.as_str(), q2.as_str(), q1.as_str()]);
    assert_eq!(b.stats(&q1).await.unwrap().pending, 1);
    assert_eq!(b.stats(&q2).await.unwrap().pending, 3);
}

#[tokio::test]
async fn blocking_fetch_across_queues_stays_within_count() {
    let (b, q1) = setup().await;
    let q2 = uniq("q");
    b.ensure_queues(std::slice::from_ref(&q2)).await.unwrap();
    let producer = RedisBroker::connect(&url(), uniq("p")).await.unwrap();
    let (qa, qb) = (q1.clone(), q2.clone());
    let push = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        for q in [&qa, &qb] {
            for _ in 0..3 { producer.enqueue(&Header::new("t", q, 0, "ct"), b"").await.unwrap(); }
        }
    });
    let got = b.fetch(&[q1.clone(), q2.clone()], 2, 2_000).await.unwrap();
    push.await.unwrap();
    assert!(!got.is_empty() && got.len() <= 2, "claimed {}", got.len());
}

#[tokio::test]
async fn fetch_keeps_claimed_entries_when_a_later_queue_lost_its_group() {
    let (b, q1) = setup().await;
    let q2 = uniq("q");
    b.ensure_queues(std::slice::from_ref(&q2)).await.unwrap();
    for q in [&q1, &q2] {
        b.enqueue(&Header::new("t", q, 0, "ct"), b"").await.unwrap();
    }
    // Deleting the stream drops its group; the next enqueue recreates the stream without one.
    let mut c = redis::Client::open(url().as_str()).unwrap().get_multiplexed_async_connection().await.unwrap();
    redis::cmd("DEL").arg(stream_key(&q2)).query_async::<i64>(&mut c).await.unwrap();
    b.enqueue(&Header::new("t", &q2, 0, "ct"), b"").await.unwrap();

    let got = b.fetch(&[q1.clone(), q2.clone()], 10, 100).await.unwrap();
    assert_eq!(got.iter().filter(|d| d.queue == q1).count(), 1, "q1's claimed entry must be returned");
    assert_eq!(b.stats(&q1).await.unwrap().pending, 1);

    let got = b.fetch(std::slice::from_ref(&q2), 10, 100).await.unwrap();
    assert_eq!(got.len(), 1, "the recreated group delivers q2's entry");
    assert_eq!(got[0].queue, q2);
}

#[tokio::test]
async fn fetch_with_zero_count_returns_nothing() {
    let (b, q) = setup().await;
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"").await.unwrap();
    assert!(b.fetch(std::slice::from_ref(&q), 0, 100).await.unwrap().is_empty());
    assert_eq!(b.stats(&q).await.unwrap().pending, 0);
}
