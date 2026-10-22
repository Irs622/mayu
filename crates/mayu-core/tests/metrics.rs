use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use mayu_core::{
    Broker, Error, GpuStat, Header, MemoryBroker, Outcome, Resources, RetryPolicy, Runtime, RuntimeConfig, Schedule,
    WorkflowNode, WorkflowSpec,
};

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
            ..Default::default()
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

fn lazy_cfg() -> RuntimeConfig {
    use mayu_core::dispatch::LazyPool;
    RuntimeConfig {
        resources: Resources {
            capacity: [("vram_gb".to_string(), 7.5), ("pool:gemma".to_string(), 1.0), ("pool:tts".to_string(), 1.0)].into(),
            requests: [
                ("g".to_string(), [("pool:gemma".to_string(), 1.0)].into()),
                ("t".to_string(), [("pool:tts".to_string(), 1.0)].into()),
            ].into(),
            lazy: [
                ("gemma".to_string(), LazyPool { claim: [("vram_gb".to_string(), 7.4)].into(), loaded: 1, ..Default::default() }),
                ("tts".to_string(), LazyPool { claim: [("vram_gb".to_string(), 7.2)].into(), loaded: 0, ..Default::default() }),
            ].into(),
        },
        schedule: Schedule::Oldest,
        ..cfg(4, 2)
    }
}

#[test]
fn lazy_pool_gauge_follows_loads_and_unloads() {
    let mem = MemoryBroker::default();
    mem.push(&Header::new("t", "q", 0, "ct"), b"");
    let rt = start(&mem, lazy_cfg());
    let body = scrape(&rt);
    assert_eq!(value(&body, r#"mayu_pool_loaded{pool="gemma"}"#), Some(1.0));
    assert_eq!(value(&body, r#"mayu_pool_loaded{pool="tts"}"#), Some(0.0));
    let t = rt.recv_timeout(RECV).unwrap();
    assert_eq!((t.loads, t.evicts), (vec!["tts".to_string()], vec!["gemma".to_string()]));
    rt.pool_evicted("gemma");
    rt.pool_loaded("tts");
    let body = scrape(&rt);
    assert_eq!(value(&body, r#"mayu_pool_loaded{pool="gemma"}"#), Some(0.0));
    assert_eq!(value(&body, r#"mayu_pool_loaded{pool="tts"}"#), Some(1.0));
    assert_eq!(value(&body, r#"mayu_pool_loads_total{pool="tts"}"#), Some(1.0));
    assert_eq!(value(&body, r#"mayu_pool_evictions_total{pool="gemma"}"#), Some(1.0));
    rt.unload_begin("tts").unwrap();
    assert_eq!(value(&scrape(&rt), r#"mayu_pool_loaded{pool="tts"}"#), Some(0.0));
    assert_eq!(value(&scrape(&rt), r#"mayu_resource_in_use{resource="vram_gb"}"#), Some(7.2));
    rt.complete(t.token, Outcome::Success { result: None, links: vec![] });
    rt.unload_done("tts").unwrap();
    assert_eq!(value(&scrape(&rt), r#"mayu_resource_in_use{resource="vram_gb"}"#), Some(0.0));
    assert!(matches!(rt.unload_done("tts"), Err(Error::Config(_))));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn eager_workers_have_no_lazy_pool_families() {
    let mem = MemoryBroker::default();
    let rt = start(&mem, gpu_cfg());
    assert!(!scrape(&rt).contains("mayu_pool_loaded"));
    rt.shutdown(Duration::from_secs(1));
}

fn wf_node(task: &str, parents: &[usize]) -> WorkflowNode {
    WorkflowNode { header: Header::new(task, "q", 0, "ct"), payload: Vec::new(), parents: parents.to_vec() }
}

fn submit(mem: &MemoryBroker, wf: &WorkflowSpec) {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(mem.submit_workflow(wf)).unwrap();
}

fn workflow_state(mem: &MemoryBroker, id: &str) -> &'static str {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(mem.workflow(id)).unwrap().unwrap().summary().state
}

#[test]
fn lookahead_demand_counts_the_pending_pool_nodes_of_workflows_this_worker_runs() {
    let mem = MemoryBroker::default();
    let wf = WorkflowSpec::new("w", 3600, vec![wf_node("a", &[]), wf_node("t", &[0])]);
    submit(&mem, &wf);
    let rt = start(&mem, RuntimeConfig { lookahead: true, ..lazy_cfg() });
    let a = rt.recv_timeout(RECV).expect("node a");
    assert_eq!(a.task, "a");
    // `t` is still waiting on `a`, and `a` is not done: the workflow's tts work counts as demand.
    wait_until(|| value(&scrape(&rt), r#"mayu_pool_demand{pool="tts"}"#) == Some(1.0));
    assert_eq!(value(&scrape(&rt), r#"mayu_pool_demand{pool="gemma"}"#), Some(0.0));
    rt.complete(a.token, Outcome::Success { result: None, links: vec![] });
    let t = rt.recv_timeout(RECV).expect("node t, released by a's ack");
    assert_eq!(t.task, "t");
    assert_eq!(value(&scrape(&rt), r#"mayu_pool_demand{pool="tts"}"#), Some(1.0), "queued still counts");
    rt.complete(t.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| workflow_state(&mem, &wf.id) == "done");
    wait_until(|| value(&scrape(&rt), r#"mayu_pool_demand{pool="tts"}"#) == Some(0.0));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn lookahead_demand_of_a_cancelled_workflow_returns_to_zero() {
    let mem = MemoryBroker::default();
    let wf = WorkflowSpec::new("w", 3600, vec![wf_node("a", &[]), wf_node("t", &[0])]);
    submit(&mem, &wf);
    let rt = start(&mem, RuntimeConfig { lookahead: true, ..lazy_cfg() });
    let a = rt.recv_timeout(RECV).expect("node a");
    wait_until(|| value(&scrape(&rt), r#"mayu_pool_demand{pool="tts"}"#) == Some(1.0));
    let tokio_rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    assert!(tokio_rt.block_on(mem.cancel_workflow(&wf.id)).unwrap());
    rt.complete(a.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| value(&scrape(&rt), r#"mayu_pool_demand{pool="tts"}"#) == Some(0.0));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn the_lookahead_loop_runs_only_with_lazy_pools_and_lookahead_or_linger() {
    use mayu_core::dispatch::LazyPool;
    let demand_family = |c: RuntimeConfig| {
        let rt = start(&MemoryBroker::default(), c);
        let body = scrape(&rt);
        rt.shutdown(Duration::from_secs(1));
        body.contains("mayu_pool_demand")
    };
    assert!(!demand_family(lazy_cfg()), "lazy pools alone don't need demand");
    assert!(demand_family(RuntimeConfig { lookahead: true, ..lazy_cfg() }));
    assert!(!demand_family(RuntimeConfig { lookahead: true, ..gpu_cfg() }), "no lazy pools, nothing to look ahead for");
    let mut lingering = lazy_cfg();
    lingering.resources.lazy.insert(
        "tts".into(),
        LazyPool { claim: [("vram_gb".to_string(), 7.2)].into(), linger: Some(Duration::from_secs(30)), ..Default::default() },
    );
    assert!(demand_family(lingering), "a lingering pool needs demand too");
}

#[test]
fn gpu_stats_are_rendered_from_the_last_push() {
    let mem = MemoryBroker::default();
    let rt = start(&mem, cfg(1, 1));
    assert!(!scrape(&rt).contains("mayu_gpu"));
    let stat = |index, util| GpuStat {
        index,
        uuid: format!("GPU-{index}"),
        utilization: util,
        memory_used: 1 << 30,
        memory_total: 8 << 30,
        process_memory: None,
        power_watts: Some(70.0),
        temperature_c: None,
    };
    rt.set_gpu_stats(vec![stat(0, 0.25)]);
    rt.set_gpu_stats(vec![stat(0, 0.75), stat(1, 0.5)]);
    let body = scrape(&rt);
    assert_eq!(value(&body, r#"mayu_gpu_utilization_ratio{gpu="0",uuid="GPU-0"}"#), Some(0.75));
    assert_eq!(value(&body, r#"mayu_gpu_utilization_ratio{gpu="1",uuid="GPU-1"}"#), Some(0.5));
    assert_eq!(value(&body, r#"mayu_gpu_memory_total_bytes{gpu="0",uuid="GPU-0"}"#), Some((8u64 << 30) as f64));
    assert_eq!(value(&body, r#"mayu_gpu_power_watts{gpu="1",uuid="GPU-1"}"#), Some(70.0));
    assert!(!body.contains("mayu_gpu_process_memory_bytes") && !body.contains("mayu_gpu_temperature_celsius"), "{body}");
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn lookahead_passthroughs_follow_the_ledger() {
    let mem = MemoryBroker::default();
    let rt = start(&mem, lazy_cfg());
    assert_eq!(rt.lookahead_begin(), None, "no demand");
    assert!(matches!(rt.lookahead_done("tts", true), Err(Error::Config(_))), "nothing is loading");
    assert_eq!(rt.blocked("vram_gb"), 0);
    assert!(rt.usage().iter().any(|(r, cap, _)| r == "vram_gb" && *cap == 7.5));
    rt.pool_lookahead_loaded("tts");
    assert_eq!(value(&scrape(&rt), r#"mayu_pool_lookahead_loads_total{pool="tts"}"#), Some(1.0));
    rt.shutdown(Duration::from_secs(1));
}

/// One lazy pool `tts` (the `voice` task) that parks 6.6 GB of host RAM and can be staged.
fn parked_cfg() -> RuntimeConfig {
    use mayu_core::dispatch::LazyPool;
    RuntimeConfig {
        resources: Resources {
            capacity: [
                ("vram_gb".to_string(), 7.5), ("ram_gb".to_string(), 16.0), ("pool:tts".to_string(), 1.0),
            ].into(),
            requests: [("voice".to_string(), [("pool:tts".to_string(), 1.0)].into())].into(),
            lazy: [(
                "tts".to_string(),
                LazyPool {
                    claim: [("vram_gb".to_string(), 7.2)].into(),
                    parked_claim: [("ram_gb".to_string(), 6.6)].into(),
                    stageable: true,
                    ..Default::default()
                },
            )].into(),
        },
        schedule: Schedule::Throughput,
        lookahead: true,
        ..cfg(4, 2)
    }
}

const NEW_FAMILIES: [&str; 7] = [
    "mayu_pool_parked", "mayu_pool_parks_total", "mayu_pool_unparks_total", "mayu_pool_drops_total",
    "mayu_pool_stages_total", "mayu_pool_move_seconds", "mayu_pool_next_need_seconds",
];

fn next_need(rt: &Runtime<MemoryBroker>) -> Option<f64> {
    value(&scrape(rt), r#"mayu_pool_next_need_seconds{pool="tts"}"#)
}

/// encode1/2/3 -> voice, all three encodes running and `voice` waiting; their means are 40, 20 and 10 s.
fn start_encodes(cfg: RuntimeConfig) -> (MemoryBroker, Runtime<MemoryBroker>, Vec<mayu_core::ReadyTask>) {
    let mem = MemoryBroker::default();
    let wf = WorkflowSpec::new("w", 3600, vec![
        wf_node("e1", &[]), wf_node("e2", &[]), wf_node("e3", &[]), wf_node("voice", &[0, 1, 2]),
    ]);
    submit(&mem, &wf);
    let rt = start(&mem, cfg);
    for (task, secs) in [("e1", 40.0), ("e2", 20.0), ("e3", 10.0)] {
        rt.seed_task_secs(task, secs);
    }
    let mut running: Vec<_> = (0..3).map(|_| rt.recv_timeout(RECV).expect("an encode")).collect();
    running.sort_by(|a, b| a.task.cmp(&b.task));
    (mem, rt, running)
}

fn succeed(rt: &Runtime<MemoryBroker>, t: &mayu_core::ReadyTask) {
    rt.complete(t.token, Outcome::Success { result: None, links: vec![] });
}

#[test]
fn next_need_falls_as_encodes_finish_and_a_stage_is_granted_inside_its_lead_while_a_task_runs() {
    let (_mem, rt, e) = start_encodes(parked_cfg());
    wait_until(|| next_need(&rt).is_some_and(|s| s > 35.0 && s <= 40.0));
    assert_eq!(rt.stage_begin(), None, "voice is needed in ~40 s, the lead is 15 s");
    succeed(&rt, &e[0]);
    wait_until(|| next_need(&rt).is_some_and(|s| s > 15.0 && s <= 20.0));
    assert_eq!(rt.stage_begin(), None, "~20 s away is still outside the lead");
    succeed(&rt, &e[1]);
    wait_until(|| next_need(&rt).is_some_and(|s| s > 5.0 && s <= 10.0));
    // e3 is still running: unlike a lookahead load, a stage uses no GPU resource and goes ahead.
    assert_eq!(rt.inflight(), 1);
    assert_eq!(rt.stage_begin().as_deref(), Some("tts"));
    assert_eq!(rt.stage_begin(), None, "one at a time");
    assert_eq!(rt.parked_usage(), vec![("tts".to_string(), 1)]);
    let body = scrape(&rt);
    assert_eq!(value(&body, r#"mayu_pool_parked{pool="tts"}"#), Some(1.0));
    assert_eq!(value(&body, r#"mayu_resource_in_use{resource="ram_gb"}"#), Some(6.6));
    rt.stage_done("tts", true).unwrap();
    rt.pool_staged("tts");
    assert_eq!(value(&scrape(&rt), r#"mayu_pool_stages_total{pool="tts"}"#), Some(1.0));
    assert!(matches!(rt.stage_done("tts", true), Err(Error::Config(_))), "nothing is staging");
    succeed(&rt, &e[2]);
    // voice is queued now, so tts is needed right away.
    wait_until(|| next_need(&rt) == Some(0.0));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn the_stage_lead_follows_the_reported_stage_and_unpark_times() {
    let (_mem, rt, e) = start_encodes(parked_cfg());
    rt.pool_timing("tts", mayu_core::MoveKind::Stage, 2.0).unwrap();
    rt.pool_timing("tts", mayu_core::MoveKind::Unpark, 1.0).unwrap();
    succeed(&rt, &e[0]);
    succeed(&rt, &e[1]);
    wait_until(|| next_need(&rt).is_some_and(|s| s > 5.0 && s <= 10.0));
    assert_eq!(rt.stage_begin(), None, "lead is 2 + 1 + 5 = 8 s, voice is ~10 s away");
    succeed(&rt, &e[2]);
    wait_until(|| next_need(&rt) == Some(0.0));
    assert_eq!(rt.stage_begin().as_deref(), Some("tts"));
    let body = scrape(&rt);
    assert_eq!(value(&body, r#"mayu_pool_move_seconds_sum{pool="tts",kind="stage"}"#), Some(2.0));
    assert_eq!(value(&body, r#"mayu_pool_move_seconds_count{pool="tts",kind="unpark"}"#), Some(1.0));
    assert!(matches!(rt.pool_timing("nope", mayu_core::MoveKind::Stage, 1.0), Err(Error::Config(_))));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn completions_teach_the_estimate() {
    let mem = MemoryBroker::default();
    let first = WorkflowSpec::new("w1", 3600, vec![wf_node("e1", &[])]);
    submit(&mem, &first);
    let rt = start(&mem, parked_cfg());
    let e = rt.recv_timeout(RECV).unwrap();
    std::thread::sleep(Duration::from_millis(400));
    succeed(&rt, &e);
    wait_until(|| workflow_state(&mem, &first.id) == "done");
    // A second workflow with the same task, not yet started: voice is a run of e1 away.
    submit(&mem, &WorkflowSpec::new("w2", 3600, vec![wf_node("e1", &[]), wf_node("voice", &[0])]));
    wait_until(|| next_need(&rt).is_some_and(|s| s > 0.3 && s < 1.5));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn a_parkable_worker_without_lookahead_reports_no_needs() {
    let (_mem, rt, _e) = start_encodes(RuntimeConfig { lookahead: false, ..parked_cfg() });
    let body = scrape(&rt);
    assert_eq!(value(&body, r#"mayu_pool_parked{pool="tts"}"#), Some(0.0), "the parkable pool reports its gauge");
    assert!(!body.contains("mayu_pool_next_need_seconds"), "{body}");
    assert_eq!(rt.stage_begin(), None, "nothing is due without needs");
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn a_lookahead_worker_without_parkable_pools_exposes_no_new_families() {
    let mem = MemoryBroker::default();
    let wf = WorkflowSpec::new("w", 3600, vec![wf_node("a", &[]), wf_node("t", &[0])]);
    submit(&mem, &wf);
    let rt = start(&mem, RuntimeConfig { lookahead: true, ..lazy_cfg() });
    let a = rt.recv_timeout(RECV).expect("node a");
    wait_until(|| value(&scrape(&rt), r#"mayu_pool_demand{pool="tts"}"#) == Some(1.0));
    rt.pool_loaded("tts");
    rt.pool_timing("tts", mayu_core::MoveKind::Build, 1.5).unwrap(); // a pool that can't park reports nothing
    assert!(matches!(rt.pool_timing("nope", mayu_core::MoveKind::Build, 1.5), Err(Error::Config(_))));
    succeed(&rt, &a);
    wait_until(|| value(&scrape(&rt), r#"mayu_tasks_settled_total{queue="q",task="a",outcome="success"}"#) == Some(1.0));
    let body = scrape(&rt);
    for name in NEW_FAMILIES {
        assert!(!body.contains(name), "{name} appears in:\n{body}");
    }
    let families: Vec<&str> = body.lines().filter_map(|l| l.strip_prefix("# TYPE ")?.split(' ').next()).collect();
    assert!(families.contains(&"mayu_pool_demand") && families.contains(&"mayu_pool_loads_total"), "{families:?}");
    rt.shutdown(Duration::from_secs(1));
}

// ---- lookahead ETA from task progress ----

/// A task running on another worker, as that worker's heartbeat publishes it.
fn remote_task(id: &str, remaining_s: Option<f64>) -> mayu_core::RunningTask {
    mayu_core::RunningTask {
        id: id.into(), task: "r".into(), queue: "rq".into(), workflow_id: None, node: None, started_ms: 1,
        done: remaining_s.map(|_| 3.0), total: remaining_s.map(|_| 10.0), fraction: remaining_s.map(|_| 0.3),
        remaining_s,
    }
}

/// Registers `worker` as alive with these tasks running.
fn publish(mem: &MemoryBroker, worker: &str, tasks: Vec<mayu_core::RunningTask>) {
    let info = mayu_core::WorkerInfo {
        worker_id: worker.into(), hostname: "h".into(), pid: 1, queues: vec!["rq".into()], concurrency: 1,
        inflight: tasks.len(), started_at: 1, last_seen: 1, tasks,
    };
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(mem.heartbeat(&info, 60_000)).unwrap();
}

/// `a` (queue q, this worker) and `r` (queue rq, which this worker doesn't serve; claimed by another consumer)
/// -> `voice`. Returns the workflow, with `r` in flight on "other".
fn remote_encode(mem: &MemoryBroker, pool_task: &str) -> WorkflowSpec {
    let mut r = wf_node("r", &[]);
    r.header.queue = "rq".into();
    let wf = WorkflowSpec::new("w", 3600, vec![wf_node("a", &[]), r, wf_node(pool_task, &[0, 1])]);
    submit(mem, &wf);
    let other = mem.with_consumer("other");
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let got = rt.block_on(other.fetch(&["rq".to_string()], 1, 0)).unwrap();
    assert_eq!(got.len(), 1, "the other worker claims r");
    wf
}

#[test]
fn next_need_tracks_the_reported_progress_instead_of_the_mean_and_a_stage_follows() {
    let mem = MemoryBroker::default();
    let wf = WorkflowSpec::new("w", 3600, vec![wf_node("enc", &[]), wf_node("voice", &[0])]);
    submit(&mem, &wf);
    let rt = start(&mem, parked_cfg());
    rt.seed_task_secs("enc", 3600.0); // nothing like the real run
    let enc = rt.recv_timeout(RECV).expect("the encode");
    // No report yet: the mean rule, an hour away.
    wait_until(|| next_need(&rt).is_some_and(|s| s > 3000.0));
    assert_eq!(rt.stage_begin(), None);
    // 3/10 after ~0.3 s: the encode is about 2 s in by the next look, so ~5 s are left.
    std::thread::sleep(Duration::from_millis(300));
    assert!(rt.progress(enc.token, 3.0, Some(10.0)));
    wait_until(|| next_need(&rt).is_some_and(|s| s < 100.0));
    let first = next_need(&rt).unwrap();
    assert!(first > 0.5 && first < 14.0, "{first}");
    // Inside the 15 s lead: the stage is granted although the mean says an hour.
    assert_eq!(rt.stage_begin().as_deref(), Some("tts"));
    rt.stage_done("tts", true).unwrap();
    // 9/10: nearly there, and the estimate follows.
    assert!(rt.progress(enc.token, 9.0, Some(10.0)));
    wait_until(|| next_need(&rt).is_some_and(|s| s < first / 2.0));
    let (secs, done, total) = rt.pool_need("tts").expect("tts is needed");
    assert!(secs < first / 2.0 && (done, total) == (0, 1), "{secs} {done} {total}");
    succeed(&rt, &enc);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn a_task_that_reports_too_little_keeps_the_mean_rule() {
    let mem = MemoryBroker::default();
    let wf = WorkflowSpec::new("w", 3600, vec![wf_node("enc", &[]), wf_node("voice", &[0])]);
    submit(&mem, &wf);
    let rt = start(&mem, parked_cfg());
    rt.seed_task_secs("enc", 3600.0);
    let enc = rt.recv_timeout(RECV).expect("the encode");
    // 1 % is below the fraction an estimate needs, so it isn't one.
    assert!(rt.progress(enc.token, 1.0, Some(100.0)));
    wait_until(|| next_need(&rt).is_some_and(|s| s > 3000.0));
    assert_eq!(rt.stage_begin(), None);
    succeed(&rt, &enc);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn a_node_running_on_another_worker_is_estimated_from_its_heartbeat() {
    let mem = MemoryBroker::default();
    let wf = remote_encode(&mem, "voice");
    publish(&mem, "other", vec![remote_task(&wf.nodes[1].header.id, Some(5.0))]);
    let rt = start(&mem, parked_cfg());
    rt.seed_task_secs("r", 3600.0);
    rt.seed_task_secs("a", 0.0);
    let a = rt.recv_timeout(RECV).expect("node a");
    succeed(&rt, &a);
    // voice waits on r alone, which has ~5 s left over there (and an hour by its mean).
    wait_until(|| next_need(&rt).is_some_and(|s| s > 0.0 && s <= 5.0));
    assert_eq!(rt.stage_begin().as_deref(), Some("tts"), "5 s is inside the lead");
    assert!(mem.running_all_calls() >= 1);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn a_remote_node_without_an_estimate_keeps_the_mean_rule_and_a_failed_read_is_counted() {
    let mem = MemoryBroker::default();
    let wf = remote_encode(&mem, "voice");
    publish(&mem, "other", vec![remote_task(&wf.nodes[1].header.id, None)]);
    let rt = start(&mem, parked_cfg());
    rt.seed_task_secs("r", 40.0);
    let a = rt.recv_timeout(RECV).expect("node a");
    succeed(&rt, &a);
    wait_until(|| next_need(&rt).is_some_and(|s| s > 30.0 && s <= 40.0));
    assert_eq!(rt.stage_begin(), None);
    assert_eq!(value(&scrape(&rt), r#"mayu_broker_errors_total{op="progress"}"#), None, "nothing failed");
    rt.shutdown(Duration::from_secs(1));

    // The broker can't list the other workers: the estimate falls back to the mean and the error is counted.
    let mem = MemoryBroker::default();
    let wf = remote_encode(&mem, "voice");
    publish(&mem, "other", vec![remote_task(&wf.nodes[1].header.id, Some(1.0))]);
    mem.fail_running_all(true);
    let rt = start(&mem, parked_cfg());
    rt.seed_task_secs("r", 40.0);
    let a = rt.recv_timeout(RECV).expect("node a");
    succeed(&rt, &a);
    wait_until(|| value(&scrape(&rt), r#"mayu_broker_errors_total{op="progress"}"#).is_some_and(|n| n >= 1.0));
    wait_until(|| next_need(&rt).is_some_and(|s| s > 30.0 && s <= 40.0));
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn a_task_this_worker_runs_is_not_overridden_by_a_heartbeat_listing_it() {
    let mem = MemoryBroker::default();
    let wf = remote_encode(&mem, "voice"); // r is on another worker, so the loop does read the heartbeats
    // That worker's heartbeat (stale, or confused) also lists the task this worker is running.
    publish(&mem, "other", vec![
        remote_task(&wf.nodes[0].header.id, Some(3000.0)), remote_task(&wf.nodes[1].header.id, Some(1.0)),
    ]);
    let rt = start(&mem, parked_cfg());
    rt.seed_task_secs("a", 10.0);
    let a = rt.recv_timeout(RECV).expect("node a");
    // Its own mean rule (10 s, a little of it spent), not 3000 s.
    wait_until(|| next_need(&rt).is_some_and(|s| s > 5.0 && s <= 10.0));
    assert!(mem.running_all_calls() >= 1, "the heartbeats were read");
    succeed(&rt, &a);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn other_workers_are_read_at_most_once_per_heartbeat() {
    let mem = MemoryBroker::default();
    let wf = remote_encode(&mem, "voice");
    publish(&mem, "other", vec![remote_task(&wf.nodes[1].header.id, Some(500.0))]);
    let rt = start(&mem, RuntimeConfig { heartbeat_interval_ms: 60_000, ..parked_cfg() });
    rt.seed_task_secs("a", 0.0);
    let a = rt.recv_timeout(RECV).expect("node a");
    succeed(&rt, &a);
    wait_until(|| next_need(&rt).is_some_and(|s| s > 400.0 && s <= 500.0));
    assert_eq!(mem.running_all_calls(), 1);
    // A second workflow wakes the loop; the estimate is recomputed (voice 2 is ~0.5 s away) without another read.
    let two = WorkflowSpec::new("w2", 3600, vec![wf_node("b", &[]), wf_node("voice", &[0])]);
    submit(&mem, &two);
    rt.seed_task_secs("b", 0.5);
    let b = rt.recv_timeout(RECV).expect("node b");
    wait_until(|| next_need(&rt).is_some_and(|s| s <= 0.5));
    assert_eq!(mem.running_all_calls(), 1);
    succeed(&rt, &b);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn workers_that_do_not_use_the_estimate_never_read_other_workers() {
    // Lookahead on, no pool that parks.
    let mem = MemoryBroker::default();
    let wf = remote_encode(&mem, "t");
    publish(&mem, "other", vec![remote_task(&wf.nodes[1].header.id, Some(5.0))]);
    let rt = start(&mem, RuntimeConfig { lookahead: true, ..lazy_cfg() });
    let a = rt.recv_timeout(RECV).expect("node a");
    // `voice` is waiting, so the loop has run over this workflow.
    wait_until(|| value(&scrape(&rt), r#"mayu_pool_demand{pool="tts"}"#) == Some(1.0));
    succeed(&rt, &a);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(mem.running_all_calls(), 0, "no parkable pool");
    rt.shutdown(Duration::from_secs(1));

    // A parkable pool without --lookahead.
    let mem = MemoryBroker::default();
    let wf = remote_encode(&mem, "voice");
    publish(&mem, "other", vec![remote_task(&wf.nodes[1].header.id, Some(5.0))]);
    let rt = start(&mem, RuntimeConfig { lookahead: false, ..parked_cfg() });
    let a = rt.recv_timeout(RECV).expect("node a");
    succeed(&rt, &a);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(mem.running_all_calls(), 0, "no --lookahead");
    assert_eq!(rt.pool_need("tts"), None);
    rt.shutdown(Duration::from_secs(1));
}
