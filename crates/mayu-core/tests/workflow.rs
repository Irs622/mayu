use mayu_core::{
    Broker, Delivery, Error, Header, MemoryBroker, NodeState, StoredResult, WorkflowNode, WorkflowSpec,
    WorkflowState,
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
