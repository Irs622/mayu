use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::broker::Broker;
use crate::error::{Error, Result};

const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Polls for a stored result until it appears or `timeout` elapses (`Ok(None)`).
///
/// Reading a result is idempotent, so broker errors (e.g. the first command on a connection that
/// a Redis restart left stale) are retried until the deadline; only an error still present then
/// is returned.
pub async fn wait_result<B: Broker>(broker: &B, id: &str, timeout: Duration) -> Result<Option<Vec<u8>>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let err = match broker.get_result(id).await {
            Ok(Some(v)) => return Ok(Some(v)),
            Ok(None) => None,
            Err(e) => Some(e),
        };
        if tokio::time::Instant::now() >= deadline {
            return err.map_or(Ok(None), Err);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Stored at `mayu:result:{id}`. Encoded as a msgpack map.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultRecord {
    pub status: String,
    #[serde(with = "serde_bytes", default)]
    pub value: Option<Vec<u8>>,
    pub content_type: String,
    pub error: Option<String>,
    pub traceback: Option<String>,
}

impl ResultRecord {
    pub fn success(value: Vec<u8>, content_type: &str) -> Self {
        ResultRecord {
            status: "success".into(),
            value: Some(value),
            content_type: content_type.into(),
            error: None,
            traceback: None,
        }
    }

    pub fn failure(error: &str, traceback: &str, content_type: &str) -> Self {
        ResultRecord {
            status: "failure".into(),
            value: None,
            content_type: content_type.into(),
            error: Some(error.into()),
            traceback: Some(traceback.into()),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        rmp_serde::to_vec_named(self).expect("result record serialization cannot fail")
    }

    pub fn decode(b: &[u8]) -> Result<Self> {
        rmp_serde::from_slice(b).map_err(|e| Error::Envelope(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_roundtrip() {
        let r = ResultRecord::success(b"\x01\x02".to_vec(), "application/msgpack");
        let back = ResultRecord::decode(&r.encode()).unwrap();
        assert_eq!(back, r);
        assert_eq!(back.status, "success");
    }

    #[test]
    fn failure_roundtrip() {
        let r = ResultRecord::failure("ValueError('x')", "Traceback...", "application/json");
        let back = ResultRecord::decode(&r.encode()).unwrap();
        assert_eq!(back.status, "failure");
        assert_eq!(back.value, None);
        assert_eq!(back.error.as_deref(), Some("ValueError('x')"));
    }

    #[test]
    fn encodes_as_msgpack_map() {
        // fixmap marker 0x80..=0x8f: Python's msgpack.unpackb yields a dict
        let b = ResultRecord::success(vec![], "ct").encode();
        assert_eq!(b[0] & 0xf0, 0x80);
    }
}
