use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

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
