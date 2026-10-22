use std::io::{Read, Write};
use std::time::{Duration, Instant};

use mayu_core::{
    Broker, Delivery, Error, Header, MemoryBroker, NodeState, Outcome, ResultRecord, RetryPolicy, Runtime,
    RuntimeConfig, StoredResult, WorkflowNode, WorkflowSpec, WorkflowState,
};

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

fn node(task: &str, parents: &[usize]) -> WorkflowNode {
    WorkflowNode {
        header: Header::new(task, "q", 0, "ct"),
        payload: task.as_bytes().to_vec(),
        parents: parents.to_vec(),
    }
}

/// A -> (B, C) -> D
fn diamond() -> WorkflowSpec {
    WorkflowSpec::new("diamond", 3600, vec![node("a", &[]), node("b", &[0]), node("c", &[0]), node("d", &[1, 2])])
}

async fn fetch_all(b: &MemoryBroker) -> Vec<Delivery> {
    b.fetch(&["q".into()], 10, 10).await.unwrap()
}

fn states(s: &WorkflowState) -> Vec<NodeState> {
    s.nodes.iter().map(|n| n.state).collect()
}

fn tasks(ds: &[Delivery]) -> Vec<&str> {
    let mut t: Vec<&str> = ds.iter().map(|d| d.header.task.as_str()).collect();
    t.sort();
    t
}

use NodeState::{Cancelled, Dead, Done, Queued, Waiting};

#[test]
fn new_stamps_ids_and_children_are_the_reverse_of_parents() {
    let wf = diamond();
    assert_eq!(wf.id.len(), 26);
    for (i, n) in wf.nodes.iter().enumerate() {
        assert_eq!(n.header.workflow_id.as_deref(), Some(wf.id.as_str()));
        assert_eq!(n.header.node, Some(i.to_string()));
    }
    assert_eq!(wf.children(), vec![vec![1, 2], vec![3], vec![3], vec![]]);
}

#[test]
fn parents_are_read_back() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        let wf = diamond();
        b.submit_workflow(&wf).await.unwrap();
        let s = b.workflow(&wf.id).await.unwrap().unwrap();
        let parents: Vec<_> = s.nodes.iter().map(|n| n.parents.clone()).collect();
        assert_eq!(parents, vec![vec![], vec![0], vec![0], vec![1, 2]]);
    });
}

#[test]
fn diamond_runs_in_dependency_order() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        let wf = diamond();
        b.submit_workflow(&wf).await.unwrap();

        let s = b.workflow(&wf.id).await.unwrap().unwrap();
        assert_eq!((s.id.as_str(), s.name.as_str(), s.cancelled), (wf.id.as_str(), "diamond", false));
        assert_eq!(states(&s), vec![Queued, Waiting, Waiting, Waiting]);
        assert_eq!(
            s.nodes.iter().map(|n| (n.node.as_str(), n.task.as_str(), n.queue.as_str())).collect::<Vec<_>>(),
            vec![("0", "a", "q"), ("1", "b", "q"), ("2", "c", "q"), ("3", "d", "q")]
        );
        for (n, spec) in s.nodes.iter().zip(&wf.nodes) {
            assert_eq!(n.task_id, spec.header.id);
        }
        let sum = s.summary();
        assert_eq!((sum.state, sum.waiting, sum.queued, sum.done, sum.dead, sum.cancelled), ("running", 3, 1, 0, 0, 0));

        let ds = fetch_all(&b).await;
        assert_eq!(tasks(&ds), vec!["a"]);
        assert_eq!(ds[0].payload, b"a");
        b.ack(&ds[0], None, &[]).await.unwrap();
        let s = b.workflow(&wf.id).await.unwrap().unwrap();
        assert_eq!(states(&s), vec![Done, Queued, Queued, Waiting]);

        let ds = fetch_all(&b).await;
        assert_eq!(tasks(&ds), vec!["b", "c"]);
        let (db, dc) = if ds[0].header.task == "b" { (&ds[0], &ds[1]) } else { (&ds[1], &ds[0]) };
        assert_eq!(db.payload, b"b");
        assert_eq!(db.header.id, wf.nodes[1].header.id, "released with the id the spec promised");
        assert_eq!(db.header.workflow_id.as_deref(), Some(wf.id.as_str()));
        assert_eq!(db.header.node.as_deref(), Some("1"));

        b.ack(db, None, &[]).await.unwrap();
        assert!(fetch_all(&b).await.is_empty(), "D waits for C");
        let s = b.workflow(&wf.id).await.unwrap().unwrap();
        assert_eq!(states(&s), vec![Done, Done, Queued, Waiting]);

        b.ack(dc, None, &[]).await.unwrap();
        let s = b.workflow(&wf.id).await.unwrap().unwrap();
        assert_eq!(states(&s), vec![Done, Done, Done, Queued]);
        let ds = fetch_all(&b).await;
        assert_eq!(tasks(&ds), vec!["d"]);
        b.ack(&ds[0], None, &[]).await.unwrap();

        let s = b.workflow(&wf.id).await.unwrap().unwrap();
        assert_eq!(states(&s), vec![Done; 4]);
        let sum = s.summary();
        assert_eq!((sum.state, sum.waiting, sum.queued, sum.done), ("done", 0, 0, 4));
        assert_eq!(b.pending_len(), 0);
    });
}

#[test]
fn ack_stores_the_result_and_release_still_happens() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        let wf = diamond();
        b.submit_workflow(&wf).await.unwrap();
        let d = fetch_all(&b).await.remove(0);
        let r = StoredResult { id: d.header.id.clone(), record: b"r".to_vec(), ttl_secs: 9 };
        b.ack(&d, Some(r), &[]).await.unwrap();
        assert_eq!(b.result(&d.header.id), Some(b"r".to_vec()));
        assert_eq!(b.stream_len("q"), 2);
    });
}

#[test]
fn duplicate_acks_and_duplicate_runs_release_once() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        let wf = diamond();
        b.submit_workflow(&wf).await.unwrap();
        let a = fetch_all(&b).await.remove(0);
        // A second delivery of A, as after a visibility-timeout reclaim of a task that did finish.
        b.push(&a.header, &a.payload);
        let a2 = fetch_all(&b).await.remove(0);

        b.ack(&a, None, &[]).await.unwrap();
        b.ack(&a, None, &[]).await.unwrap(); // a retried ack
        b.ack(&a2, None, &[]).await.unwrap(); // the duplicate run's ack

        assert_eq!(b.stream_len("q"), 2, "B and C were enqueued once each");
        let ds = fetch_all(&b).await;
        assert_eq!(tasks(&ds), vec!["b", "c"]);
        // Both ack, then re-ack B: D is released exactly once.
        for d in &ds {
            b.ack(d, None, &[]).await.unwrap();
        }
        b.ack(&ds[0], None, &[]).await.unwrap();
        assert_eq!(tasks(&fetch_all(&b).await), vec!["d"]);
        assert!(fetch_all(&b).await.is_empty());
    });
}

#[test]
fn dead_letter_blocks_descendants() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        let wf = diamond();
        b.submit_workflow(&wf).await.unwrap();
        let a = fetch_all(&b).await.remove(0);
        b.ack(&a, None, &[]).await.unwrap();
        let ds = fetch_all(&b).await;
        let (db, dc) = if ds[0].header.task == "b" { (&ds[0], &ds[1]) } else { (&ds[1], &ds[0]) };

        b.dead_letter(db, "boom", None).await.unwrap();
        b.ack(dc, None, &[]).await.unwrap();

        let s = b.workflow(&wf.id).await.unwrap().unwrap();
        assert_eq!(states(&s), vec![Done, Dead, Done, Waiting]);
        let sum = s.summary();
        assert_eq!((sum.state, sum.waiting, sum.dead, sum.done), ("failed", 1, 1, 2));
        assert!(fetch_all(&b).await.is_empty(), "D is never released");
        assert_eq!(b.dead().len(), 1);
    });
}

#[test]
fn dead_letter_does_not_undo_a_done_node() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        let wf = diamond();
        b.submit_workflow(&wf).await.unwrap();
        let a = fetch_all(&b).await.remove(0);
        b.push(&a.header, &a.payload);
        let a2 = fetch_all(&b).await.remove(0);
        b.ack(&a, None, &[]).await.unwrap();
        b.dead_letter(&a2, "late duplicate failed", None).await.unwrap();
        let s = b.workflow(&wf.id).await.unwrap().unwrap();
        assert_eq!(states(&s)[0], Done);
    });
}

#[test]
fn cancel_stops_the_rest() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        let wf = diamond();
        b.submit_workflow(&wf).await.unwrap();
        let a = fetch_all(&b).await.remove(0);
        b.ack(&a, None, &[]).await.unwrap();
        let ds = fetch_all(&b).await; // B and C are claimed

        assert!(b.cancel_workflow(&wf.id).await.unwrap());
        let s = b.workflow(&wf.id).await.unwrap().unwrap();
        assert!(s.cancelled);
        assert_eq!(s.summary().state, "cancelled");

        for d in &ds {
            b.ack(d, None, &[]).await.unwrap();
        }
        let s = b.workflow(&wf.id).await.unwrap().unwrap();
        assert_eq!(states(&s), vec![Done, Cancelled, Cancelled, Waiting]);
        let sum = s.summary();
        assert_eq!((sum.state, sum.cancelled, sum.done, sum.waiting), ("cancelled", 2, 1, 1));
        assert!(fetch_all(&b).await.is_empty(), "D is never queued");
        assert_eq!(b.pending_len(), 0);
    });
}

#[test]
fn cancel_is_false_when_unknown_or_all_done() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        assert!(!b.cancel_workflow("nope").await.unwrap());

        let wf = WorkflowSpec::new("one", 60, vec![node("a", &[])]);
        b.submit_workflow(&wf).await.unwrap();
        let a = fetch_all(&b).await.remove(0);
        b.ack(&a, None, &[]).await.unwrap();
        assert!(!b.cancel_workflow(&wf.id).await.unwrap());
        let s = b.workflow(&wf.id).await.unwrap().unwrap();
        assert!(!s.cancelled);
        assert_eq!(s.summary().state, "done");
    });
}

#[test]
fn workflow_is_none_for_an_unknown_id() {
    rt().block_on(async {
        assert!(MemoryBroker::default().workflow("nope").await.unwrap().is_none());
    });
}

#[test]
fn cancelled_workflows_returns_exactly_the_cancelled_ids() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        let (w1, w2, w3) = (diamond(), diamond(), diamond());
        for w in [&w1, &w2, &w3] {
            b.submit_workflow(w).await.unwrap();
        }
        b.cancel_workflow(&w1.id).await.unwrap();
        b.cancel_workflow(&w3.id).await.unwrap();
        let ids = vec![w1.id.clone(), w2.id.clone(), w3.id.clone(), "unknown".to_string()];
        assert_eq!(b.cancelled_workflows(&ids).await.unwrap(), vec![w1.id.clone(), w3.id.clone()]);
        assert!(b.cancelled_workflows(&[]).await.unwrap().is_empty());
    });
}

#[test]
fn ack_of_a_node_with_follow_ups_is_an_error() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        let wf = diamond();
        b.submit_workflow(&wf).await.unwrap();
        let a = fetch_all(&b).await.remove(0);
        let follow = [(Header::new("cb", "q", 0, "ct"), b"x".to_vec())];
        assert!(matches!(b.ack(&a, None, &follow).await, Err(Error::Broker(_))));
        assert_eq!(b.pending_len(), 1, "nothing was acked");
        assert_eq!(states(&b.workflow(&wf.id).await.unwrap().unwrap())[0], Queued);
    });
}

#[test]
fn submitting_the_same_workflow_twice_is_an_error() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        let wf = diamond();
        b.submit_workflow(&wf).await.unwrap();
        assert!(matches!(b.submit_workflow(&wf).await, Err(Error::Broker(_))));
        assert_eq!(b.stream_len("q"), 1);
    });
}

#[test]
fn submit_rejects_an_invalid_spec() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        let wf = WorkflowSpec::new("empty", 60, vec![]);
        assert!(matches!(b.submit_workflow(&wf).await, Err(Error::Config(_))));
    });
}

#[test]
fn workflow_calls_fail_while_the_broker_is_down() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        let wf = diamond();
        b.set_failing(true);
        assert!(b.submit_workflow(&wf).await.is_err());
        assert!(b.workflow(&wf.id).await.is_err());
        assert!(b.cancel_workflow(&wf.id).await.is_err());
        assert!(b.cancelled_workflows(std::slice::from_ref(&wf.id)).await.is_err());
    });
}

fn invalid(wf: WorkflowSpec) -> String {
    match wf.validate() {
        Err(Error::Config(m)) => m,
        other => panic!("expected Error::Config, got {other:?}"),
    }
}

#[test]
fn validate_accepts_the_diamond() {
    diamond().validate().unwrap();
}

#[test]
fn validate_rejects_bad_specs() {
    invalid(WorkflowSpec::new("w", 60, vec![]));
    invalid(WorkflowSpec::new("w", 0, vec![node("a", &[])]));
    invalid(WorkflowSpec::new("w", 60, vec![node("a", &[0])])); // self
    invalid(WorkflowSpec::new("w", 60, vec![node("a", &[1]), node("b", &[])])); // forward
    invalid(WorkflowSpec::new("w", 60, vec![node("a", &[]), node("b", &[5])])); // out of range
    invalid(WorkflowSpec::new("w", 60, vec![node("a", &[]), node("b", &[0, 0])])); // duplicate
    let mut eta = node("a", &[]);
    eta.header.eta = Some(1);
    invalid(WorkflowSpec::new("w", 60, vec![eta]));
}

#[test]
fn validate_rejects_headers_not_stamped_for_their_node() {
    let mut wf = diamond();
    wf.nodes[2].header.node = Some("1".into());
    invalid(wf);
    let mut wf = diamond();
    wf.nodes[1].header.workflow_id = None;
    invalid(wf);
}

#[test]
fn node_state_strings_roundtrip() {
    for s in [Waiting, Queued, Done, Dead, Cancelled] {
        assert_eq!(NodeState::parse(s.as_str()).unwrap(), s);
    }
    assert_eq!(Waiting.as_str(), "waiting");
    assert!(matches!(NodeState::parse("bogus"), Err(Error::Broker(_))));
}

#[test]
fn summary_precedence() {
    fn state(cancelled: bool, ss: &[NodeState]) -> WorkflowState {
        WorkflowState {
            id: "w".into(),
            name: "w".into(),
            created_at: 0,
            cancelled,
            nodes: ss
                .iter()
                .enumerate()
                .map(|(i, s)| mayu_core::NodeInfo {
                    node: i.to_string(),
                    task_id: i.to_string(),
                    task: "t".into(),
                    queue: "q".into(),
                    state: *s,
                    parents: Vec::new(),
                })
                .collect(),
        }
    }
    assert_eq!(state(true, &[Dead, Done]).summary().state, "cancelled");
    assert_eq!(state(true, &[Done, Done]).summary().state, "cancelled");
    assert_eq!(state(false, &[Done, Done]).summary().state, "done");
    assert_eq!(state(false, &[Dead, Waiting]).summary().state, "failed");
    assert_eq!(state(false, &[Done, Queued]).summary().state, "running");
}

fn runtime_cfg() -> RuntimeConfig {
    RuntimeConfig {
        queues: vec!["q".into()],
        concurrency: 2,
        prefetch_multiplier: 2,
        result_ttl_secs: Some(60),
        fetch_block_ms: 50,
        promote_interval_ms: 20,
        retry: RetryPolicy { base_ms: 10, cap_ms: 50 },
        visibility_timeout_ms: 10_000,
        reclaim_interval_ms: 50,
        heartbeat_interval_ms: 50,
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

fn scrape(rt: &Runtime<MemoryBroker>) -> String {
    let mut s = std::net::TcpStream::connect(rt.metrics_addr().unwrap()).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    out.split_once("\r\n\r\n").expect("an HTTP response").1.to_string()
}

fn series(body: &str, name: &str) -> Option<f64> {
    body.lines().find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
}

const RECV: Duration = Duration::from_secs(3);

#[test]
fn runtime_acks_cancelled_workflow_nodes_without_running_them() {
    let mem = MemoryBroker::default();
    let wf = WorkflowSpec::new("w", 3600, vec![node("a", &[]), node("b", &[0])]);
    let (a_id, b_id) = (wf.nodes[0].header.id.clone(), wf.nodes[1].header.id.clone());
    rt().block_on(async {
        mem.submit_workflow(&wf).await.unwrap();
        assert!(mem.cancel_workflow(&wf.id).await.unwrap());
    });

    let rt = start(&mem, runtime_cfg());
    wait_until(|| mem.result(&a_id).is_some() && mem.pending_len() == 0);
    assert!(rt.recv_timeout(Duration::from_millis(200)).is_none(), "a cancelled node must not be dispatched");

    let rec = ResultRecord::decode(&mem.result(&a_id).unwrap()).unwrap();
    assert_eq!((rec.status.as_str(), rec.error.as_deref()), ("failure", Some("workflow cancelled")));
    assert_eq!(rt.inflight(), 0);
    assert_eq!(mem.stream_len("q"), 0, "node b must never be queued");
    assert!(mem.result(&b_id).is_none());
    let s = rt_block(&mem, &wf.id);
    assert_eq!(states(&s), vec![Cancelled, Waiting]);

    let body = scrape(&rt);
    assert_eq!(series(&body, r#"mayu_tasks_settled_total{queue="q",task="a",outcome="cancelled"}"#), Some(1.0));
    assert_eq!(series(&body, r#"mayu_tasks_received_total{queue="q",task="a"}"#), Some(1.0));
    rt.shutdown(Duration::from_secs(1));
}

fn rt_block(mem: &MemoryBroker, id: &str) -> WorkflowState {
    rt().block_on(mem.workflow(id)).unwrap().unwrap()
}

#[test]
fn runtime_runs_ordinary_tasks_next_to_cancelled_nodes() {
    let mem = MemoryBroker::default();
    let wf = WorkflowSpec::new("w", 3600, vec![node("a", &[]), node("b", &[0])]);
    let live = Header::new("live", "q", 0, "ct");
    rt().block_on(async {
        mem.submit_workflow(&wf).await.unwrap();
        assert!(mem.cancel_workflow(&wf.id).await.unwrap());
    });
    mem.push(&live, b"");

    let rt = start(&mem, runtime_cfg());
    let t = rt.recv_timeout(RECV).expect("the ordinary task");
    assert_eq!(t.task, "live");
    rt.complete(t.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| mem.pending_len() == 0);
    assert_eq!(rt_block(&mem, &wf.id).nodes[0].state, Cancelled);
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn runtime_releases_children_of_a_live_workflow() {
    let mem = MemoryBroker::default();
    let wf = WorkflowSpec::new("w", 3600, vec![node("a", &[]), node("b", &[0])]);
    rt().block_on(mem.submit_workflow(&wf)).unwrap();

    let rt = start(&mem, runtime_cfg());
    let a = rt.recv_timeout(RECV).expect("node a");
    assert_eq!(a.task, "a");
    rt.complete(a.token, Outcome::Success { result: None, links: vec![] });
    let b = rt.recv_timeout(RECV).expect("node b, released by a's ack");
    assert_eq!(b.task, "b");
    rt.complete(b.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| rt_block(&mem, &wf.id).summary().state == "done");
    rt.shutdown(Duration::from_secs(1));
}

#[test]
fn runtime_dispatches_normally_when_the_cancelled_check_fails() {
    // The broker answers fetch but not cancelled_workflows.
    let mem = MemoryBroker::default();
    let wf = WorkflowSpec::new("w", 3600, vec![node("a", &[])]);
    rt().block_on(async {
        mem.submit_workflow(&wf).await.unwrap();
        assert!(mem.cancel_workflow(&wf.id).await.unwrap());
    });
    mem.fail_cancelled_checks(true);
    let rt = start(&mem, runtime_cfg());
    let a = rt.recv_timeout(RECV).expect("dispatched despite the failed check");
    assert_eq!(a.task, "a");
    rt.complete(a.token, Outcome::Success { result: None, links: vec![] });
    wait_until(|| mem.pending_len() == 0);
    assert_eq!(rt_block(&mem, &wf.id).nodes[0].state, Cancelled);
    rt.shutdown(Duration::from_secs(1));
}
