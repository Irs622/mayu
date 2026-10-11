use std::time::{Duration, Instant};

use mayu_core::{MemoryBroker, wait_result};

#[tokio::test]
async fn returns_none_after_timeout_when_no_result() {
    let mem = MemoryBroker::default();
    let t = Instant::now();
    let got = wait_result(&mem, "nope", Duration::from_millis(100)).await.unwrap();
    assert_eq!(got, None);
    assert!(t.elapsed() >= Duration::from_millis(100));
}

/// A Redis restart makes the first command on a stale connection fail; polling for a result is
/// idempotent, so the wait must ride out broker errors instead of surfacing them.
#[tokio::test]
async fn rides_out_broker_errors_until_a_result_appears() {
    let mem = MemoryBroker::default();
    mem.set_failing(true);
    let m = mem.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        m.set_failing(false);
        m.put_result("id1", b"done".to_vec());
    });
    let got = wait_result(&mem, "id1", Duration::from_secs(3)).await.unwrap();
    assert_eq!(got.as_deref(), Some(&b"done"[..]));
}

#[tokio::test]
async fn reports_the_broker_error_if_it_persists_to_the_deadline() {
    let mem = MemoryBroker::default();
    mem.set_failing(true);
    let err = wait_result(&mem, "id1", Duration::from_millis(150)).await.unwrap_err();
    assert!(err.to_string().contains("broker"), "{err}");
}
