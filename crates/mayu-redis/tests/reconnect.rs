//! Restarts a dedicated Redis container on port 6392.
//! Run with: cargo test -p mayu-redis --test reconnect -- --ignored
use std::process::Command;
use std::time::{Duration, Instant};

use mayu_core::{Broker, Header};
use mayu_redis::RedisBroker;

const NAME: &str = "mayu-redis-reconnect";
const URL: &str = "redis://127.0.0.1:6392/0";

fn docker(args: &[&str]) {
    let ok = Command::new("docker").args(args).status().expect("docker not found").success();
    assert!(ok, "docker {args:?} failed");
}

fn wait_ping() {
    let t = Instant::now();
    loop {
        let out = Command::new("docker").args(["exec", NAME, "redis-cli", "ping"]).output().unwrap();
        if String::from_utf8_lossy(&out.stdout).contains("PONG") {
            return;
        }
        assert!(t.elapsed() < Duration::from_secs(20), "redis did not come up");
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "restarts a docker container"]
async fn broker_survives_redis_restart() {
    let _ = Command::new("docker").args(["rm", "-f", NAME]).output();
    docker(&["run", "-d", "--rm", "--name", NAME, "-p", "6392:6379", "redis:8",
             "redis-server", "--appendonly", "yes"]);
    wait_ping();
    let q = format!("q-{}", ulid::Ulid::generate());
    let b = RedisBroker::connect(URL, "c1").await.unwrap();
    b.ensure_queues(&[q.clone()]).await.unwrap();
    b.enqueue(&Header::new("t", &q, 0, "ct"), b"before").await.unwrap();

    docker(&["restart", NAME]);
    wait_ping();

    // Calls may fail while the manager reconnects, then succeed on the same broker.
    let t = Instant::now();
    loop {
        match b.enqueue(&Header::new("t", &q, 0, "ct"), b"after").await {
            Ok(()) => break,
            Err(_) if t.elapsed() < Duration::from_secs(15) => tokio::time::sleep(Duration::from_millis(200)).await,
            Err(e) => panic!("never reconnected: {e}"),
        }
    }
    let mut got = Vec::new();
    let t = Instant::now();
    while got.len() < 2 && t.elapsed() < Duration::from_secs(15) {
        match b.fetch(&[q.clone()], 10, 200).await {
            Ok(mut d) => got.append(&mut d),
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    let payloads: Vec<Vec<u8>> = got.iter().map(|d| d.payload.clone()).collect();
    assert_eq!(payloads, vec![b"before".to_vec(), b"after".to_vec()]);
    docker(&["rm", "-f", NAME]);
}
