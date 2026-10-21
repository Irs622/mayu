use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use mayu_core::{Error, Header, MemoryBroker, Outcome, Resources, RetryPolicy, Runtime, RuntimeConfig};

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
        metrics_addr: Some("127.0.0.1:0".parse().unwrap()),
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
        assert!(t.elapsed() < Duration::from_secs(5), "condition not reached in time");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// (status line, body)
fn request(addr: SocketAddr, raw: &str) -> (String, String) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(raw.as_bytes()).unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    let (head, body) = out.split_once("\r\n\r\n").expect("an HTTP response");
    (head.lines().next().unwrap().to_string(), body.to_string())
}

fn scrape(rt: &Runtime<MemoryBroker>) -> String {
    let (status, body) = request(rt.metrics_addr().unwrap(), "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
    assert_eq!(status, "HTTP/1.1 200 OK");
    body
}

fn value(body: &str, series: &str) -> Option<f64> {
    body.lines().find_map(|l| l.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
}

const RECV: Duration = Duration::from_secs(3);

#[test]
fn counts_received_settled_and_durations() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("t", "q", 0, "ct"), b"");
    mem.push(&Header::new("t", "q", 0, "ct"), b"");
    let rt = start(&mem, cfg(2, 2));
    let a = rt.recv_timeout(RECV).unwrap();
    let b = rt.recv_timeout(RECV).unwrap();
    rt.complete(a.token, Outcome::Success { result: None, links: vec![] });
    rt.complete(b.token, Outcome::Failure { error: "E".into(), traceback: String::new() });
    wait_until(|| mem.pending_len() == 0 && mem.dead().len() == 1);
    wait_until(|| value(&scrape(&rt), r#"mayu_tasks_settled_total{queue="q",task="t",outcome="dead"}"#) == Some(1.0));
    let body = scrape(&rt);
    assert_eq!(value(&body, r#"mayu_tasks_received_total{queue="q",task="t"}"#), Some(2.0));
    assert_eq!(value(&body, r#"mayu_tasks_settled_total{queue="q",task="t",outcome="success"}"#), Some(1.0));
    assert_eq!(value(&body, r#"mayu_task_duration_seconds_count{queue="q",task="t"}"#), Some(2.0));
    assert_eq!(value(&body, "mayu_worker_concurrency"), Some(2.0));
    assert_eq!(value(&body, r#"mayu_queue_dead{queue="q"}"#), Some(1.0));
    assert_eq!(value(&body, r#"mayu_queue_ready{queue="q"}"#), Some(0.0));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn inflight_and_running_gauges() {
    let mem = MemoryBroker::default();
    for _ in 0..3 {
        mem.push(&Header::new("t", "q", 0, "ct"), b"");
    }
    let rt = start(&mem, cfg(1, 4));
    let t = rt.recv_timeout(RECV).unwrap();
    wait_until(|| rt.inflight() == 3);
    let body = scrape(&rt);
    assert_eq!(value(&body, "mayu_tasks_inflight"), Some(3.0));
    assert_eq!(value(&body, "mayu_tasks_running"), Some(1.0));
    assert_eq!(value(&body, r#"mayu_queue_pending{queue="q"}"#), Some(3.0));
    rt.complete(t.token, Outcome::Success { result: None, links: vec![] });
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn deferred_rate_limited_tasks_are_counted() {
    let mem = MemoryBroker::default();
    for _ in 0..3 {
        mem.push(&Header::new("slow", "q", 0, "ct"), b"");
    }
    let mut c = cfg(4, 4);
    c.rate_limits = HashMap::from([("slow".to_string(), 0.5)]); // one start per 2 s
    let rt = start(&mem, c);
    wait_until(|| mem.delayed_len("q") == 2);
    wait_until(|| value(&scrape(&rt), r#"mayu_tasks_deferred_total{queue="q",task="slow"}"#) == Some(2.0));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn scrape_while_broker_is_down_still_answers() {
    let mem = MemoryBroker::default();
    let rt = start(&mem, cfg(1, 1));
    mem.set_failing(true);
    let t = Instant::now();
    let body = scrape(&rt);
    assert!(t.elapsed() < Duration::from_secs(3), "scrape took {:?}", t.elapsed());
    assert_eq!(value(&body, "mayu_worker_concurrency"), Some(1.0));
    assert!(!body.contains("mayu_queue_ready{"), "no queue gauges without the broker:\n{body}");
    assert!(value(&body, r#"mayu_broker_errors_total{op="stats"}"#).unwrap_or(0.0) >= 1.0);
    mem.set_failing(false);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn idle_connection_does_not_block_scrapes() {
    let mem = MemoryBroker::default();
    let rt = start(&mem, cfg(1, 1));
    let _idle = TcpStream::connect(rt.metrics_addr().unwrap()).unwrap(); // sends nothing
    let t = Instant::now();
    scrape(&rt);
    assert!(t.elapsed() < Duration::from_secs(2));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn other_paths_and_methods_are_rejected() {
    let mem = MemoryBroker::default();
    let rt = start(&mem, cfg(1, 1));
    let addr = rt.metrics_addr().unwrap();
    assert_eq!(request(addr, "GET / HTTP/1.1\r\n\r\n").0, "HTTP/1.1 404 Not Found");
    assert_eq!(request(addr, "POST /metrics HTTP/1.1\r\n\r\n").0, "HTTP/1.1 405 Method Not Allowed");
    assert_eq!(request(addr, "GET /metrics?x=1 HTTP/1.1\r\n\r\n").0, "HTTP/1.1 200 OK");
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn port_in_use_is_a_config_error() {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut c = cfg(1, 1);
    c.metrics_addr = Some(taken.local_addr().unwrap());
    let mem = MemoryBroker::default();
    match Runtime::start(move || async move { Ok(mem) }, c) {
        Ok(_) => panic!("binding a taken port must fail"),
        Err(Error::Config(msg)) => assert!(msg.contains("metrics"), "{msg}"),
        Err(e) => panic!("expected a config error, got {e}"),
    }
}

#[test]
fn no_metrics_addr_means_no_server() {
    let mem = MemoryBroker::default();
    let mut c = cfg(1, 1);
    c.metrics_addr = None;
    let rt = start(&mem, c);
    assert!(rt.metrics_addr().is_none());
    rt.shutdown(Duration::from_secs(1));
}

fn gpu_cfg() -> RuntimeConfig {
    RuntimeConfig {
        resources: Resources {
            capacity: [("gpu".to_string(), 1.0)].into(),
            requests: [("g".to_string(), [("gpu".to_string(), 0.5)].into())].into(),
        },
        ..cfg(4, 2)
    }
}

#[test]
fn resource_gauges_follow_running_tasks() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("g", "q", 0, "ct"), b"");
    let rt = start(&mem, gpu_cfg());
    let body = scrape(&rt);
    assert_eq!(value(&body, r#"mayu_resource_capacity{resource="gpu"}"#), Some(1.0));
    assert_eq!(value(&body, r#"mayu_resource_in_use{resource="gpu"}"#), Some(0.0));
    let t = rt.recv_timeout(RECV).unwrap();
    assert_eq!(value(&scrape(&rt), r#"mayu_resource_in_use{resource="gpu"}"#), Some(0.5));
    rt.complete(t.token, Outcome::Success { result: None, links: vec![] });
    assert_eq!(value(&scrape(&rt), r#"mayu_resource_in_use{resource="gpu"}"#), Some(0.0));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn resource_gauges_are_absent_when_none_are_configured() {
    let mem = MemoryBroker::default();
    let rt = start(&mem, cfg(1, 1));
    assert!(!scrape(&rt).contains("mayu_resource"));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn a_task_taken_after_stop_gives_its_resources_back() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("g", "q", 0, "ct"), b"");
    let rt = start(&mem, gpu_cfg());
    wait_until(|| rt.inflight() == 1);
    rt.stop_fetching();
    assert!(rt.recv_timeout(Duration::from_secs(1)).is_none(), "not started while stopping");
    assert_eq!(value(&scrape(&rt), r#"mayu_resource_in_use{resource="gpu"}"#), Some(0.0));
    rt.shutdown(Duration::from_secs(1));
}
