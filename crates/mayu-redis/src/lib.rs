//! Redis Streams implementation of `mayu_core::Broker`.
use std::collections::HashMap;
use std::time::Duration;

use mayu_core::{
    Broker, Delivery, Error, Header, Result, StoredResult, decode_header, encode_delayed_member,
    encode_header, now_ms,
};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::streams::StreamReadReply;
use redis::{Script, Value};

pub const GROUP: &str = "mayu";
/// XREADGROUP BLOCK is clamped below the fetch connection's response timeout.
const MAX_BLOCK_MS: u64 = 10_000;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

pub fn stream_key(q: &str) -> String { format!("mayu:{{{q}}}:stream") }
pub fn delayed_key(q: &str) -> String { format!("mayu:{{{q}}}:delayed") }
pub fn dead_key(q: &str) -> String { format!("mayu:{{{q}}}:dead") }
pub fn result_key(id: &str) -> String { format!("mayu:result:{id}") }

fn queue_from_stream_key(k: &str) -> &str {
    k.strip_prefix("mayu:{").and_then(|s| s.strip_suffix("}:stream")).unwrap_or(k)
}

fn be(e: impl std::fmt::Display) -> Error { Error::Broker(e.to_string()) }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueStats {
    pub stream: u64,
    pub pending: u64,
    pub delayed: u64,
    pub dead: u64,
}

struct Scripts {
    ack: Script,
    retry: Script,
    dead: Script,
    requeue: Script,
    promote: Script,
}

async fn manager(client: &redis::Client, response_timeout: Duration) -> Result<ConnectionManager> {
    let cfg = ConnectionManagerConfig::new()
        .set_connection_timeout(Some(COMMAND_TIMEOUT))
        .set_response_timeout(Some(response_timeout))
        .set_number_of_retries(2);
    ConnectionManager::new_with_config(client.clone(), cfg).await.map_err(be)
}

pub struct RedisBroker {
    conn: ConnectionManager,
    /// Dedicated connection: a blocking XREADGROUP would stall other commands pipelined behind it.
    fetch_conn: ConnectionManager,
    consumer: String,
    scripts: Scripts,
}

impl RedisBroker {
    pub async fn connect(url: &str, consumer: impl Into<String>) -> Result<Self> {
        let client = redis::Client::open(url).map_err(be)?;
        let conn = manager(&client, COMMAND_TIMEOUT).await?;
        // Dedicated: a blocking XREADGROUP would stall commands pipelined behind it.
        let fetch_conn = manager(&client, Duration::from_millis(MAX_BLOCK_MS) + COMMAND_TIMEOUT).await?;
        Ok(RedisBroker {
            conn,
            fetch_conn,
            consumer: consumer.into(),
            scripts: Scripts {
                ack: Script::new(include_str!("lua/ack.lua")),
                retry: Script::new(include_str!("lua/retry.lua")),
                dead: Script::new(include_str!("lua/dead.lua")),
                requeue: Script::new(include_str!("lua/requeue.lua")),
                promote: Script::new(include_str!("lua/promote.lua")),
            },
        })
    }

    pub async fn stats(&self, queue: &str) -> Result<QueueStats> {
        let mut c = self.conn.clone();
        let stream: u64 = redis::cmd("XLEN").arg(stream_key(queue)).query_async(&mut c).await.map_err(be)?;
        let delayed: u64 = redis::cmd("ZCARD").arg(delayed_key(queue)).query_async(&mut c).await.map_err(be)?;
        let dead: u64 = redis::cmd("XLEN").arg(dead_key(queue)).query_async(&mut c).await.map_err(be)?;
        let pending = match redis::cmd("XPENDING").arg(stream_key(queue)).arg(GROUP)
            .query_async::<Value>(&mut c).await
        {
            Ok(Value::Array(v)) => match v.first() { Some(Value::Int(n)) => *n as u64, _ => 0 },
            _ => 0, // no group yet
        };
        Ok(QueueStats { stream, pending, delayed, dead })
    }

    async fn dead_raw(&self, queue: &str, stream_id: &str, h: &[u8], p: &[u8], error: &str,
                      result: Option<StoredResult>) -> Result<()> {
        let (flag, id, record, ttl) = match result {
            Some(r) => ("1", r.id, r.record, r.ttl_secs),
            None => ("0", String::new(), Vec::new(), 0),
        };
        self.scripts.dead
            .key(stream_key(queue)).key(dead_key(queue)).key(result_key(&id))
            .arg(GROUP).arg(stream_id).arg(h).arg(p).arg(error).arg(flag).arg(record).arg(ttl)
            .invoke_async::<i64>(&mut self.conn.clone()).await.map_err(be)?;
        Ok(())
    }
}

fn take_bytes(map: &mut HashMap<String, Value>, k: &str) -> Option<Vec<u8>> {
    match map.remove(k)? {
        Value::BulkString(b) => Some(b),
        _ => None,
    }
}

impl Broker for RedisBroker {
    async fn ensure_queues(&self, queues: &[String]) -> Result<()> {
        let mut c = self.conn.clone();
        for q in queues {
            // Start at 0, not $, so messages enqueued before any worker existed are delivered.
            let r: redis::RedisResult<()> = redis::cmd("XGROUP").arg("CREATE").arg(stream_key(q))
                .arg(GROUP).arg("0").arg("MKSTREAM").query_async(&mut c).await;
            match r {
                Ok(()) => {}
                Err(e) if e.code() == Some("BUSYGROUP") => {}
                Err(e) => return Err(be(e)),
            }
        }
        Ok(())
    }

    async fn enqueue(&self, header: &Header, payload: &[u8]) -> Result<()> {
        let h = encode_header(header);
        let mut c = self.conn.clone();
        match header.eta {
            Some(eta) if eta > now_ms() => {
                redis::cmd("ZADD").arg(delayed_key(&header.queue)).arg(eta)
                    .arg(encode_delayed_member(&h, payload))
                    .query_async::<i64>(&mut c).await.map_err(be)?;
            }
            _ => {
                redis::cmd("XADD").arg(stream_key(&header.queue)).arg("*")
                    .arg("h").arg(&h).arg("p").arg(payload)
                    .query_async::<String>(&mut c).await.map_err(be)?;
            }
        }
        Ok(())
    }

    async fn fetch(&self, queues: &[String], count: usize, block_ms: u64) -> Result<Vec<Delivery>> {
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP").arg(GROUP).arg(&self.consumer)
            .arg("COUNT").arg(count).arg("BLOCK").arg(block_ms.min(MAX_BLOCK_MS)).arg("STREAMS");
        for q in queues { cmd.arg(stream_key(q)); }
        for _ in queues { cmd.arg(">"); }
        let reply: Option<StreamReadReply> = match cmd.query_async(&mut self.fetch_conn.clone()).await {
            Ok(r) => r,
            Err(e) if e.code() == Some("NOGROUP") => {
                self.ensure_queues(queues).await?;
                return Ok(Vec::new());
            }
            Err(e) => return Err(be(e)),
        };
        let mut out = Vec::new();
        for key in reply.map(|r| r.keys).unwrap_or_default() {
            let queue = queue_from_stream_key(&key.key).to_string();
            for entry in key.ids {
                let mut map = entry.map;
                let h = take_bytes(&mut map, "h");
                let p = take_bytes(&mut map, "p").unwrap_or_default();
                let decoded = h.as_deref().map(decode_header);
                match (h, decoded) {
                    (Some(raw_header), Some(Ok(header))) => out.push(Delivery {
                        queue: queue.clone(), stream_id: entry.id, header, raw_header, payload: p,
                    }),
                    (h, _) => {
                        self.dead_raw(&queue, &entry.id, &h.unwrap_or_default(), &p,
                                      "undecodable envelope header", None).await?;
                    }
                }
            }
        }
        Ok(out)
    }

    async fn ack(&self, d: &Delivery, result: Option<StoredResult>) -> Result<()> {
        let (flag, id, record, ttl) = match result {
            Some(r) => ("1", r.id, r.record, r.ttl_secs),
            None => ("0", String::new(), Vec::new(), 0),
        };
        self.scripts.ack
            .key(stream_key(&d.queue)).key(result_key(&id))
            .arg(GROUP).arg(&d.stream_id).arg(flag).arg(record).arg(ttl)
            .invoke_async::<i64>(&mut self.conn.clone()).await.map_err(be)?;
        Ok(())
    }

    async fn retry(&self, d: &Delivery, header: &Header) -> Result<()> {
        let member = encode_delayed_member(&encode_header(header), &d.payload);
        self.scripts.retry
            .key(stream_key(&d.queue)).key(delayed_key(&d.queue))
            .arg(GROUP).arg(&d.stream_id).arg(header.eta.unwrap_or(0)).arg(member)
            .invoke_async::<i64>(&mut self.conn.clone()).await.map_err(be)?;
        Ok(())
    }

    async fn dead_letter(&self, d: &Delivery, error: &str, result: Option<StoredResult>) -> Result<()> {
        self.dead_raw(&d.queue, &d.stream_id, &d.raw_header, &d.payload, error, result).await
    }

    async fn requeue(&self, d: &Delivery) -> Result<()> {
        self.scripts.requeue
            .key(stream_key(&d.queue))
            .arg(GROUP).arg(&d.stream_id).arg(&d.raw_header).arg(&d.payload)
            .invoke_async::<i64>(&mut self.conn.clone()).await.map_err(be)?;
        Ok(())
    }

    async fn promote(&self, queue: &str, now_ms: u64, limit: usize) -> Result<usize> {
        self.scripts.promote
            .key(delayed_key(queue)).key(stream_key(queue))
            .arg(now_ms).arg(limit)
            .invoke_async::<usize>(&mut self.conn.clone()).await.map_err(be)
    }

    async fn get_result(&self, id: &str) -> Result<Option<Vec<u8>>> {
        redis::cmd("GET").arg(result_key(id))
            .query_async(&mut self.conn.clone()).await.map_err(be)
    }
}
