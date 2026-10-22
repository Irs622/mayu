use std::time::Duration;

use mayu_core::{Broker, MemoryBroker, PublishedTasks, RunningTask, WorkerInfo};

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

fn task(id: &str, progress: bool) -> RunningTask {
    RunningTask {
        id: id.into(), task: "t".into(), queue: "q".into(), workflow_id: Some("wf".into()), node: Some("1".into()),
        started_ms: 1_000, done: progress.then_some(3.0), total: progress.then_some(12.0),
        fraction: progress.then_some(0.25), remaining_s: progress.then_some(90.0),
    }
}

fn info(id: &str, tasks: Vec<RunningTask>) -> WorkerInfo {
    WorkerInfo {
        worker_id: id.into(), hostname: "h".into(), pid: 1, queues: vec!["q".into()], concurrency: 2,
        inflight: tasks.len(), started_at: 1, last_seen: 2, tasks,
    }
}

#[test]
fn running_returns_what_the_last_heartbeat_published() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        assert!(b.running("w").await.unwrap().is_empty(), "unknown worker");
        let tasks = vec![task("a", true), task("b", false)];
        b.heartbeat(&info("w", tasks.clone()), 60_000).await.unwrap();
        assert_eq!(b.running("w").await.unwrap(), tasks);
        assert_eq!(b.worker("w").unwrap().tasks, tasks);
        // The next beat replaces them, and an idle beat clears them.
        b.heartbeat(&info("w", vec![task("b", true)]), 60_000).await.unwrap();
        assert_eq!(b.running("w").await.unwrap(), vec![task("b", true)]);
        b.heartbeat(&info("w", vec![]), 60_000).await.unwrap();
        assert!(b.running("w").await.unwrap().is_empty());
    });
}

#[test]
fn running_is_empty_once_the_heartbeat_expires_or_the_worker_leaves() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        b.heartbeat(&info("w", vec![task("a", false)]), 40).await.unwrap();
        b.heartbeat(&info("x", vec![task("b", false)]), 60_000).await.unwrap();
        assert_eq!(b.running("w").await.unwrap().len(), 1);
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(b.running("w").await.unwrap().is_empty(), "expired");
        b.unregister("x").await.unwrap();
        assert!(b.running("x").await.unwrap().is_empty(), "unregistered");
    });
}

#[test]
fn running_all_lists_live_workers_tasks_and_skips_expired_ones() {
    rt().block_on(async {
        let b = MemoryBroker::default();
        assert_eq!(b.running_all().await.unwrap(), PublishedTasks::default());
        b.heartbeat(&info("w2", vec![task("c", true)]), 60_000).await.unwrap();
        b.heartbeat(&info("w1", vec![task("a", true), task("b", false)]), 60_000).await.unwrap();
        b.heartbeat(&info("idle", vec![]), 60_000).await.unwrap();
        b.heartbeat(&info("gone", vec![task("d", true)]), 40).await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let all = b.running_all().await.unwrap();
        assert_eq!(all.malformed, 0);
        let got: Vec<(&str, &str)> = all.tasks.iter().map(|(w, t)| (w.as_str(), t.id.as_str())).collect();
        assert_eq!(got, vec![("w1", "a"), ("w1", "b"), ("w2", "c")], "sorted by worker, expired ones skipped");
        assert_eq!(all.tasks[0].1, task("a", true));
        b.fail_running_all(true);
        assert!(b.running_all().await.is_err());
        assert_eq!(b.running_all_calls(), 3);
    });
}
