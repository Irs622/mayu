//! Redis Streams implementation of `mayu_core::Broker`.
use std::collections::HashMap;
use std::time::Duration;

use mayu_core::{
    Broker, Delivery, Error, Header, QueueStats, Reclaimed, Result, StoredResult, WorkerInfo, decode_header,
    encode_delayed_member, encode_header, now_ms,
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
const WORKER_KEY_PREFIX: &str = "mayu:workers:";
pub fn worker_key(id: &str) -> String { format!("{WORKER_KEY_PREFIX}{id}") }

fn queue_from_stream_key(k: &str) -> &str {
    k.strip_prefix("mayu:{").and_then(|s| s.strip_suffix("}:stream")).unwrap_or(k)
}

fn be(e: impl std::fmt::Display) -> Error { Error::Broker(e.to_string()) }

struct Scripts {
    ack: Script,
    retry: Script,
    dead: Script,
    requeue: Script,
    promote: Script,
    extend: Script,
    reclaim: Script,
    prune: Script,
    fetch: Script,
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
                extend: Script::new(include_str!("lua/extend.lua")),
                reclaim: Script::new(include_str!("lua/reclaim.lua")),
                prune: Script::new(include_str!("lua/prune.lua")),
                fetch: Script::new(include_str!("lua/fetch.lua")),
            },
        })
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

    /// Non-blocking ordered claim of up to `count` entries (one round trip).
    async fn fetch_ordered(&self, queues: &[String], count: usize) -> Result<Vec<Delivery>> {
        let mut inv = self.scripts.fetch.prepare_invoke();
        for q in queues { inv.key(stream_key(q)); }
        inv.arg(GROUP).arg(&self.consumer).arg(count);
        let v: Value = match inv.invoke_async(&mut self.conn.clone()).await {
            Ok(v) => v,
            Err(e) if is_nogroup(&e) => {
                self.ensure_queues(queues).await?;
                return Ok(Vec::new());
            }
            Err(e) => return Err(be(e)),
        };
        let (claimed, missing) = parse_fetch(v)?;
        let mut out = Vec::new();
        for (i, id, mut fields) in claimed {
            let queue = queues.get(i).ok_or_else(|| be("fetch reply names an unknown queue"))?;
            let h = fields.remove("h");
            let p = fields.remove("p").unwrap_or_default();
            if let Some(d) = self.decode_or_bury(queue, id, h, p, 1).await? {
                out.push(d);
            }
        }
        if !missing.is_empty() {
            let gone = missing.iter().filter_map(|&i| queues.get(i).cloned()).collect::<Vec<_>>();
            self.ensure_queues(&gone).await?;
        }
        Ok(out)
    }

    /// Blocks on every stream until one has entries; `per_stream` caps each stream.
    async fn fetch_blocking(&self, queues: &[String], per_stream: usize, block_ms: u64) -> Result<Vec<Delivery>> {
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP").arg(GROUP).arg(&self.consumer)
            .arg("COUNT").arg(per_stream).arg("BLOCK").arg(block_ms.min(MAX_BLOCK_MS)).arg("STREAMS");
        for q in queues { cmd.arg(stream_key(q)); }
        for _ in queues { cmd.arg(">"); }
        let reply: Option<StreamReadReply> = match cmd.query_async(&mut self.fetch_conn.clone()).await {
            Ok(r) => r,
            Err(e) if is_nogroup(&e) => {
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
                if let Some(d) = self.decode_or_bury(&queue, entry.id, h, p, 1).await? {
                    out.push(d);
                }
            }
        }
        Ok(out)
    }

    /// A stream entry as a `Delivery`, or dead-lettered if its header cannot be decoded.
    async fn decode_or_bury(&self, queue: &str, stream_id: String, h: Option<Vec<u8>>, p: Vec<u8>,
                            delivery_count: u64) -> Result<Option<Delivery>> {
        let decoded = h.as_deref().map(decode_header);
        match (h, decoded) {
            (Some(raw_header), Some(Ok(header))) => Ok(Some(Delivery {
                queue: queue.to_string(), stream_id, header, raw_header, payload: p, delivery_count,
            })),
            (h, _) => {
                self.dead_raw(queue, &stream_id, &h.unwrap_or_default(), &p, "undecodable envelope header", None)
                    .await?;
                Ok(None)
            }
        }
    }

    pub async fn worker_info(&self, worker_id: &str) -> Result<Option<WorkerInfo>> {
        let map: HashMap<String, String> = redis::cmd("HGETALL").arg(worker_key(worker_id))
            .query_async(&mut self.conn.clone()).await.map_err(be)?;
        if map.is_empty() {
            return Ok(None);
        }
        let num = |k: &str| map.get(k).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
        Ok(Some(WorkerInfo {
            worker_id: worker_id.to_string(),
            hostname: map.get("hostname").cloned().unwrap_or_default(),
            pid: num("pid") as u32,
            queues: map.get("queues")
                .map(|q| q.split(',').filter(|s| !s.is_empty()).map(String::from).collect())
                .unwrap_or_default(),
            concurrency: num("concurrency") as usize,
            inflight: num("inflight") as usize,
            started_at: num("started_at"),
            last_seen: num("last_seen"),
        }))
    }
}

fn take_bytes(map: &mut HashMap<String, Value>, k: &str) -> Option<Vec<u8>> {
    match map.remove(k)? {
        Value::BulkString(b) => Some(b),
        _ => None,
    }
}

fn bulk(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::BulkString(b) => Some(b.clone()),
        Value::SimpleString(s) => Some(s.clone().into_bytes()),
        _ => None,
    }
}

fn text(v: &Value) -> Option<String> {
    bulk(v).map(|b| String::from_utf8_lossy(&b).into_owned())
}

struct RawEntry {
    id: String,
    fields: HashMap<String, Vec<u8>>,
    delivery_count: u64,
}

fn parse_reclaim(v: Value) -> Result<(String, Vec<RawEntry>)> {
    let Value::Array(top) = v else { return Err(be("unexpected reclaim reply")) };
    let mut top = top.into_iter();
    let cursor = top.next().as_ref().and_then(text).ok_or_else(|| be("reclaim reply missing cursor"))?;
    let entries = match top.next() {
        Some(Value::Array(e)) => e,
        _ => Vec::new(),
    };
    let mut out = Vec::new();
    for e in entries {
        let Value::Array(parts) = e else { continue };
        let mut parts = parts.into_iter();
        let (Some(id), Some(Value::Array(flat)), Some(count)) = (parts.next(), parts.next(), parts.next()) else {
            continue;
        };
        let Some(id) = text(&id) else { continue };
        let delivery_count = match count { Value::Int(n) => n.max(1) as u64, _ => 1 };
        let mut fields = HashMap::new();
        for pair in flat.chunks(2) {
            if let [k, v] = pair {
                if let (Some(k), Some(v)) = (text(k), bulk(v)) {
                    fields.insert(k, v);
                }
            }
        }
        out.push(RawEntry { id, fields, delivery_count });
    }
    Ok((cursor, out))
}

fn is_nogroup(e: &redis::RedisError) -> bool {
    e.code() == Some("NOGROUP") || e.to_string().contains("NOGROUP")
}

/// A claimed entry: (queue index, entry id, fields).
type Claimed = (usize, String, HashMap<String, Vec<u8>>);

/// `fetch.lua` reply `{claimed, missing}` -> (claimed entries, indexes of streams without the group).
///
/// Every element of `claimed` is already claimed in Redis, so a malformed one is an error,
/// never silently skipped.
fn parse_fetch(v: Value) -> Result<(Vec<Claimed>, Vec<usize>)> {
    let bad = || be("malformed fetch reply");
    let Value::Array(top) = v else { return Err(bad()) };
    let mut top = top.into_iter();
    let (Some(Value::Array(entries)), Some(Value::Array(missing))) = (top.next(), top.next()) else {
        return Err(bad());
    };
    let index = |v: Value| match v {
        Value::Int(i) if i >= 1 => Ok(i as usize - 1),
        _ => Err(bad()),
    };
    let mut out = Vec::new();
    for e in entries {
        let Value::Array(parts) = e else { return Err(bad()) };
        let mut parts = parts.into_iter();
        let (Some(i), Some(id), Some(Value::Array(flat))) = (parts.next(), parts.next(), parts.next())
        else { return Err(bad()) };
        let i = index(i)?;
        let id = text(&id).ok_or_else(bad)?;
        let mut fields = HashMap::new();
        for pair in flat.chunks(2) {
            let [k, v] = pair else { return Err(bad()) };
            fields.insert(text(k).ok_or_else(bad)?, bulk(v).ok_or_else(bad)?);
        }
        out.push((i, id, fields));
    }
    let missing = missing.into_iter().map(index).collect::<Result<Vec<_>>>()?;
    Ok((out, missing))
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
        if count == 0 {
            return Ok(Vec::new());
        }
        let got = self.fetch_ordered(queues, count).await?;
        if !got.is_empty() || block_ms == 0 || queues.is_empty() {
            return Ok(got);
        }
        // COUNT is per stream: spread `count` so a multi-queue wake-up stays near it.
        self.fetch_blocking(queues, (count / queues.len()).max(1), block_ms).await
    }

    async fn ack(&self, d: &Delivery, result: Option<StoredResult>, follow_ups: &[(Header, Vec<u8>)]) -> Result<()> {
        let (flag, id, record, ttl) = match result {
            Some(r) => ("1", r.id, r.record, r.ttl_secs),
            None => ("0", String::new(), Vec::new(), 0),
        };
        let mut inv = self.scripts.ack.prepare_invoke();
        inv.key(stream_key(&d.queue)).key(result_key(&id));
        for (h, _) in follow_ups { inv.key(stream_key(&h.queue)); }
        inv.arg(GROUP).arg(&d.stream_id).arg(flag).arg(record).arg(ttl);
        for (h, p) in follow_ups { inv.arg(encode_header(h)).arg(p); }
        inv.invoke_async::<i64>(&mut self.conn.clone()).await.map_err(be)?;
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

    async fn queue_stats(&self, queue: &str) -> Result<QueueStats> {
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

    async fn extend(&self, queue: &str, stream_ids: &[String]) -> Result<Vec<String>> {
        if stream_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut inv = self.scripts.extend.prepare_invoke();
        inv.key(stream_key(queue)).arg(GROUP).arg(&self.consumer);
        for id in stream_ids {
            inv.arg(id);
        }
        inv.invoke_async(&mut self.conn.clone()).await.map_err(be)
    }

    async fn reclaim(&self, queue: &str, min_idle_ms: u64, cursor: &str, count: usize) -> Result<Reclaimed> {
        let v: Value = self.scripts.reclaim
            .key(stream_key(queue))
            .arg(GROUP).arg(&self.consumer).arg(min_idle_ms).arg(cursor).arg(count.max(1))
            .invoke_async(&mut self.conn.clone()).await.map_err(be)?;
        let (next_cursor, entries) = parse_reclaim(v)?;
        let mut deliveries = Vec::new();
        for mut e in entries {
            let h = e.fields.remove("h");
            let p = e.fields.remove("p").unwrap_or_default();
            if let Some(d) = self.decode_or_bury(queue, e.id, h, p, e.delivery_count).await? {
                deliveries.push(d);
            }
        }
        Ok(Reclaimed { next_cursor, deliveries })
    }

    async fn heartbeat(&self, info: &WorkerInfo, ttl_ms: u64) -> Result<()> {
        let key = worker_key(&info.worker_id);
        redis::pipe()
            .atomic()
            .cmd("HSET").arg(&key)
                .arg("hostname").arg(&info.hostname)
                .arg("pid").arg(info.pid)
                .arg("queues").arg(info.queues.join(","))
                .arg("concurrency").arg(info.concurrency)
                .arg("inflight").arg(info.inflight)
                .arg("started_at").arg(info.started_at)
                .arg("last_seen").arg(info.last_seen)
                .ignore()
            .cmd("PEXPIRE").arg(&key).arg(ttl_ms).ignore()
            .query_async::<()>(&mut self.conn.clone()).await.map_err(be)
    }

    async fn unregister(&self, worker_id: &str) -> Result<()> {
        redis::cmd("DEL").arg(worker_key(worker_id))
            .query_async::<i64>(&mut self.conn.clone()).await.map_err(be)?;
        Ok(())
    }

    async fn prune_consumers(&self, queue: &str, min_idle_ms: u64) -> Result<usize> {
        self.scripts.prune
            .key(stream_key(queue))
            .arg(GROUP).arg(min_idle_ms).arg(WORKER_KEY_PREFIX)
            .invoke_async(&mut self.conn.clone()).await.map_err(be)
    }
}
