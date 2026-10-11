use std::time::{Duration, Instant};

use mayu_core::{Header, MemoryBroker, Outcome, ResultRecord, RetryPolicy, Runtime, RuntimeConfig, now_ms};

fn cfg(concurrency: usize, prefetch: usize) -> RuntimeConfig {
    RuntimeConfig {
        queues: vec!["q".into()],
        concurrency,
        prefetch_multiplier: prefetch,
        result_ttl_secs: Some(60),
        fetch_block_ms: 50,
        promote_interval_ms: 20,
        retry: RetryPolicy { base_ms: 10, cap_ms: 50 },
        visibility_timeout_ms: 10_000,
        reclaim_interval_ms: 50,
        heartbeat_interval_ms: 50,
        max_deliveries: 5,
        ..RuntimeConfig::default()
    }
}

fn start(mem: &MemoryBroker, c: RuntimeConfig) -> Runtime<MemoryBroker> {
    let m = mem.clone();
    Runtime::start(move || async move { Ok(m) }, c).unwrap()
}

fn wait_until(mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < Duration::from_secs(3), "condition not reached in time");
        std::thread::sleep(Duration::from_millis(10));
    }
}

const RECV: Duration = Duration::from_secs(3);

#[test]
fn success_acks_and_stores_result() {
    let mem = MemoryBroker::default();
    let h = Header::new("t", "q", 3, "application/msgpack");
    mem.push(&h, b"p");
    let rt = start(&mem, cfg(2, 2));
    let t = rt.recv_timeout(RECV).expect("task");
    assert_eq!((t.id.as_str(), t.task.as_str(), t.payload.as_slice()), (h.id.as_str(), "t", &b"p"[..]));
    rt.complete(t.token, Outcome::Success { result: Some(b"r".to_vec()) });
    wait_until(|| mem.pending_len() == 0 && mem.result(&h.id).is_some());
    let rec = ResultRecord::decode(&mem.result(&h.id).unwrap()).unwrap();
    assert_eq!(rec.status, "success");
    assert_eq!(rec.value.as_deref(), Some(&b"r"[..]));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn retry_redelivers_with_incremented_attempt() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("t", "q", 3, "ct"), b"");
    let rt = start(&mem, cfg(1, 1));
    let t = rt.recv_timeout(RECV).unwrap();
    rt.complete(t.token, Outcome::Retry { countdown_ms: Some(0), error: "e".into() });
    let t2 = rt.recv_timeout(RECV).expect("redelivered");
    assert_eq!(t2.id, t.id);
    assert_eq!(t2.attempt, 1);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn retry_exhausted_dead_letters_and_stores_failure() {
    let mem = MemoryBroker::default();
    let h = Header::new("t", "q", 0, "ct");
    mem.push(&h, b"");
    let rt = start(&mem, cfg(1, 1));
    let t = rt.recv_timeout(RECV).unwrap();
    rt.complete(t.token, Outcome::Retry { countdown_ms: None, error: "boom".into() });
    wait_until(|| mem.dead().len() == 1);
    assert!(mem.dead()[0].error.contains("retries exhausted"));
    let rec = ResultRecord::decode(&mem.result(&h.id).unwrap()).unwrap();
    assert_eq!(rec.status, "failure");
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn failure_dead_letters() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("t", "q", 5, "ct"), b"");
    let rt = start(&mem, cfg(1, 1));
    let t = rt.recv_timeout(RECV).unwrap();
    rt.complete(t.token, Outcome::Failure { error: "E".into(), traceback: "tb".into() });
    wait_until(|| mem.dead().len() == 1 && mem.pending_len() == 0);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn prefetch_caps_inflight() {
    let mem = MemoryBroker::default();
    for _ in 0..5 { mem.push(&Header::new("t", "q", 0, "ct"), b""); }
    let rt = start(&mem, cfg(1, 2));
    wait_until(|| mem.pending_len() == 2);
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(mem.pending_len(), 2, "must not claim beyond concurrency * prefetch");
    assert_eq!(mem.stream_len("q"), 3);
    let t = rt.recv_timeout(RECV).unwrap();
    rt.complete(t.token, Outcome::Success { result: None });
    wait_until(|| mem.stream_len("q") == 2 && mem.pending_len() == 2);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn unknown_token_is_ignored() {
    let mem = MemoryBroker::default();
    let rt = start(&mem, cfg(1, 1));
    rt.complete(999_999, Outcome::Success { result: None });
    mem.push(&Header::new("t", "q", 0, "ct"), b"");
    assert!(rt.recv_timeout(RECV).is_some(), "runtime still works after a stray completion");
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn delayed_task_is_promoted_when_due() {
    let mem = MemoryBroker::default();
    let mut h = Header::new("t", "q", 0, "ct");
    h.eta = Some(now_ms() + 300);
    mem.push(&h, b"");
    let rt = start(&mem, cfg(1, 1));
    assert!(rt.recv_timeout(Duration::from_millis(100)).is_none(), "not due yet");
    assert_eq!(rt.recv_timeout(RECV).expect("promoted").id, h.id);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn shutdown_requeues_unstarted() {
    let mem = MemoryBroker::default();
    for _ in 0..3 { mem.push(&Header::new("t", "q", 0, "ct"), b""); }
    let rt = start(&mem, cfg(1, 3));
    wait_until(|| mem.pending_len() == 3);
    let _running = rt.recv_timeout(RECV).unwrap(); // started, never completed
    rt.shutdown(Duration::from_millis(200));
    assert_eq!(mem.stream_len("q"), 2, "two unstarted tasks returned to the stream");
    assert_eq!(mem.pending_len(), 1, "the running task stays pending for reclaim");
    rt.shutdown(Duration::from_millis(10)); // idempotent
}

#[test]
fn rejects_zero_concurrency() {
    let mem = MemoryBroker::default();
    let m = mem.clone();
    assert!(Runtime::start(move || async move { Ok(m) }, cfg(0, 1)).is_err());
}

#[test]
fn shutdown_is_prompt_while_broker_is_failing() {
    let mem = MemoryBroker::default();
    let rt = start(&mem, cfg(1, 1));
    mem.set_failing(true);
    std::thread::sleep(Duration::from_millis(60)); // fetch loop is now in its 500ms error backoff
    let t = Instant::now();
    rt.shutdown(Duration::from_millis(100));
    assert!(t.elapsed() < Duration::from_millis(250), "shutdown took {:?}", t.elapsed());
}

use mayu_core::Broker;

fn rel_cfg(worker: &str, visibility_ms: u64, max_deliveries: u64) -> RuntimeConfig {
    RuntimeConfig {
        worker_id: worker.into(),
        visibility_timeout_ms: visibility_ms,
        reclaim_interval_ms: 30,
        heartbeat_interval_ms: 30,
        max_deliveries,
        ..cfg(1, 1)
    }
}

fn start_as(mem: &MemoryBroker, worker: &str, c: RuntimeConfig) -> Runtime<MemoryBroker> {
    let m = mem.with_consumer(worker);
    Runtime::start(move || async move { Ok(m) }, c).unwrap()
}

#[test]
fn running_task_is_kept_alive_and_not_stolen() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("t", "q", 0, "ct"), b"");
    let a = start_as(&mem, "a", rel_cfg("a", 200, 5));
    let t = a.recv_timeout(RECV).unwrap();
    let b = start_as(&mem, "b", rel_cfg("b", 200, 5));
    assert!(b.recv_timeout(Duration::from_millis(700)).is_none(), "b stole a task a is still running");
    a.complete(t.token, Outcome::Success { result: None });
    wait_until(|| mem.pending_len() == 0);
    a.shutdown(Duration::from_secs(1));
    b.shutdown(Duration::from_secs(1));
}

#[test]
fn crashed_workers_task_is_reclaimed() {
    let mem = MemoryBroker::default();
    let h = Header::new("t", "q", 0, "ct");
    mem.push(&h, b"");
    let a = start_as(&mem, "a", rel_cfg("a", 200, 5));
    let _never_completed = a.recv_timeout(RECV).unwrap();
    drop(a); // crash: no ack, no keepalive, no unregister
    let b = start_as(&mem, "b", rel_cfg("b", 200, 5));
    let t = b.recv_timeout(RECV).expect("reclaimed after visibility timeout");
    assert_eq!(t.id, h.id);
    b.complete(t.token, Outcome::Success { result: None });
    wait_until(|| mem.pending_len() == 0);
    b.shutdown(Duration::from_secs(1));
}

#[test]
fn poison_message_is_dead_lettered_without_running() {
    let mem = MemoryBroker::default();
    let h = Header::new("t", "q", 0, "ct");
    mem.push(&h, b"");
    let a = start_as(&mem, "a", rel_cfg("a", 200, 1));
    let _ = a.recv_timeout(RECV).unwrap();
    drop(a);
    let b = start_as(&mem, "b", rel_cfg("b", 200, 1));
    assert!(b.recv_timeout(Duration::from_millis(800)).is_none(), "poison message must not run");
    wait_until(|| mem.dead().len() == 1 && mem.pending_len() == 0);
    assert!(mem.dead()[0].error.contains("poison"));
    let rec = ResultRecord::decode(&mem.result(&h.id).unwrap()).unwrap();
    assert_eq!(rec.status, "failure");
    b.shutdown(Duration::from_secs(1));
}

#[test]
fn lost_ownership_still_completes() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("t", "q", 0, "ct"), b"");
    let a = start_as(&mem, "a", rel_cfg("a", 10_000, 5));
    let t = a.recv_timeout(RECV).unwrap();
    // Another worker steals the entry, as if `a` had stalled past the visibility timeout.
    let thief = mem.with_consumer("thief");
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    assert_eq!(rt.block_on(thief.reclaim("q", 0, "0-0", 10)).unwrap().deliveries.len(), 1);
    a.complete(t.token, Outcome::Success { result: None });
    wait_until(|| mem.pending_len() == 0);
    a.shutdown(Duration::from_secs(1));
}

#[test]
fn heartbeat_registers_while_running_and_unregisters_on_shutdown() {
    let mem = MemoryBroker::default();
    let rt = start_as(&mem, "hb", rel_cfg("hb", 10_000, 5));
    assert_eq!(rt.worker_id(), "hb");
    wait_until(|| mem.worker("hb").is_some());
    let w = mem.worker("hb").unwrap();
    assert_eq!(w.queues, vec!["q".to_string()]);
    assert_eq!(w.pid, std::process::id());
    assert_eq!(w.concurrency, 1);
    rt.shutdown(Duration::from_secs(1));
    assert!(mem.worker("hb").is_none());
}

#[test]
fn outcome_is_recorded_after_transient_broker_failure() {
    let mem = MemoryBroker::default();
    let h = Header::new("t", "q", 0, "ct");
    mem.push(&h, b"");
    let rt = start(&mem, cfg(1, 1));
    let t = rt.recv_timeout(RECV).unwrap();
    mem.set_failing(true);
    rt.complete(t.token, Outcome::Success { result: Some(b"r".to_vec()) });
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(mem.pending_len(), 1, "not recorded while the broker is down");
    mem.set_failing(false);
    wait_until(|| mem.pending_len() == 0 && mem.result(&h.id).is_some());
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn rejects_zero_intervals() {
    let mem = MemoryBroker::default();
    let m = mem.clone();
    let c = RuntimeConfig { visibility_timeout_ms: 0, ..cfg(1, 1) };
    assert!(Runtime::start(move || async move { Ok(m) }, c).is_err());
}
