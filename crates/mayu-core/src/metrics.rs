//! Worker metrics in the Prometheus text exposition format (0.0.4).
//!
//! Counters and the duration histogram are updated by the runtime's loops; gauges are sampled when
//! `/metrics` is scraped. Everything is per worker process.
use std::collections::BTreeMap;
use std::fmt::{Display, Write as _};
use std::sync::Mutex;
use std::time::Duration;

use crate::broker::QueueStats;

/// Upper bounds (seconds) of the task duration histogram; `+Inf` is implied.
pub const DURATION_BUCKETS: [f64; 14] =
    [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0];

/// How a claimed task was settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Settled {
    Success,
    Retry,
    Dead,
}

impl Settled {
    pub fn as_str(self) -> &'static str {
        match self {
            Settled::Success => "success",
            Settled::Retry => "retry",
            Settled::Dead => "dead",
        }
    }
}

#[derive(Default)]
struct Histogram {
    /// Cumulative: `buckets[i]` counts observations `<= DURATION_BUCKETS[i]`.
    buckets: [u64; DURATION_BUCKETS.len()],
    count: u64,
    sum: f64,
}

/// (queue, task)
type Key = (String, String);

#[derive(Default)]
struct Counters {
    received: BTreeMap<Key, u64>,
    settled: BTreeMap<(String, String, Settled), u64>,
    durations: BTreeMap<Key, Histogram>,
    deferred: BTreeMap<Key, u64>,
    reclaimed: BTreeMap<String, u64>,
    broker_errors: BTreeMap<&'static str, u64>,
}

/// Sampled when `/metrics` is scraped.
#[derive(Debug, Clone, Default)]
pub struct Gauges {
    pub concurrency: usize,
    /// Claimed and not yet settled (prefetched or running).
    pub inflight: usize,
    /// Handed to a Python thread and not yet settled.
    pub running: usize,
    /// Per-queue broker state; empty when the broker could not be read.
    pub queues: Vec<(String, QueueStats)>,
}

#[derive(Default)]
pub struct Metrics {
    c: Mutex<Counters>,
}

fn key(queue: &str, task: &str) -> Key {
    (queue.to_string(), task.to_string())
}

impl Metrics {
    pub fn received(&self, queue: &str, task: &str) {
        *self.c.lock().unwrap().received.entry(key(queue, task)).or_default() += 1;
    }

    pub fn settled(&self, queue: &str, task: &str, how: Settled, duration: Option<Duration>) {
        let mut c = self.c.lock().unwrap();
        *c.settled.entry((queue.to_string(), task.to_string(), how)).or_default() += 1;
        if let Some(d) = duration {
            let secs = d.as_secs_f64();
            let h = c.durations.entry(key(queue, task)).or_default();
            for (i, le) in DURATION_BUCKETS.iter().enumerate() {
                if secs <= *le {
                    h.buckets[i] += 1;
                }
            }
            h.count += 1;
            h.sum += secs;
        }
    }

    pub fn deferred(&self, queue: &str, task: &str) {
        *self.c.lock().unwrap().deferred.entry(key(queue, task)).or_default() += 1;
    }

    pub fn reclaimed(&self, queue: &str) {
        *self.c.lock().unwrap().reclaimed.entry(queue.to_string()).or_default() += 1;
    }

    pub fn broker_error(&self, op: &'static str) {
        *self.c.lock().unwrap().broker_errors.entry(op).or_default() += 1;
    }

    pub fn render(&self, g: &Gauges) -> String {
        let c = self.c.lock().unwrap();
        let mut out = String::with_capacity(4096);

        family(&mut out, "mayu_tasks_received_total", "counter", "Tasks claimed from the broker, new or reclaimed.");
        for ((q, t), n) in &c.received {
            sample(&mut out, "mayu_tasks_received_total", &[("queue", q), ("task", t)], n);
        }
        family(&mut out, "mayu_tasks_settled_total", "counter", "Task outcomes recorded in the broker.");
        for ((q, t, how), n) in &c.settled {
            sample(&mut out, "mayu_tasks_settled_total", &[("queue", q), ("task", t), ("outcome", how.as_str())], n);
        }
        family(&mut out, "mayu_task_duration_seconds", "histogram", "Time from a thread taking a task to its outcome being recorded.");
        for ((q, t), h) in &c.durations {
            for (i, le) in DURATION_BUCKETS.iter().enumerate() {
                let le = le.to_string();
                sample(&mut out, "mayu_task_duration_seconds_bucket", &[("queue", q), ("task", t), ("le", &le)], h.buckets[i]);
            }
            sample(&mut out, "mayu_task_duration_seconds_bucket", &[("queue", q), ("task", t), ("le", "+Inf")], h.count);
            sample(&mut out, "mayu_task_duration_seconds_sum", &[("queue", q), ("task", t)], h.sum);
            sample(&mut out, "mayu_task_duration_seconds_count", &[("queue", q), ("task", t)], h.count);
        }
        family(&mut out, "mayu_tasks_deferred_total", "counter", "Rate-limited tasks handed back to the broker until their slot.");
        for ((q, t), n) in &c.deferred {
            sample(&mut out, "mayu_tasks_deferred_total", &[("queue", q), ("task", t)], n);
        }
        family(&mut out, "mayu_tasks_reclaimed_total", "counter", "Tasks reclaimed from workers that stopped responding.");
        for (q, n) in &c.reclaimed {
            sample(&mut out, "mayu_tasks_reclaimed_total", &[("queue", q)], n);
        }
        family(&mut out, "mayu_broker_errors_total", "counter", "Failed broker operations, by operation.");
        for (op, n) in &c.broker_errors {
            sample(&mut out, "mayu_broker_errors_total", &[("op", op)], n);
        }
        drop(c);

        family(&mut out, "mayu_worker_concurrency", "gauge", "Task threads in this worker process.");
        sample(&mut out, "mayu_worker_concurrency", &[], g.concurrency);
        family(&mut out, "mayu_tasks_inflight", "gauge", "Tasks claimed by this process and not yet settled.");
        sample(&mut out, "mayu_tasks_inflight", &[], g.inflight);
        family(&mut out, "mayu_tasks_running", "gauge", "Tasks running on a thread of this process.");
        sample(&mut out, "mayu_tasks_running", &[], g.running);
        let per_queue: [(&str, &str, fn(&QueueStats) -> u64); 4] = [
            ("mayu_queue_ready", "Entries no worker has claimed yet.", |s| s.ready()),
            ("mayu_queue_pending", "Entries claimed by a worker and not yet settled.", |s| s.pending),
            ("mayu_queue_delayed", "Entries waiting for their countdown, eta or retry backoff.", |s| s.delayed),
            ("mayu_queue_dead", "Dead-lettered entries.", |s| s.dead),
        ];
        for (name, help, get) in per_queue {
            family(&mut out, name, "gauge", help);
            for (q, s) in &g.queues {
                sample(&mut out, name, &[("queue", q)], get(s));
            }
        }
        out
    }
}

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn sample(out: &mut String, name: &str, labels: &[(&str, &str)], value: impl Display) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            let _ = write!(out, "{k}=\"{}\"", escape(v));
        }
        out.push('}');
    }
    let _ = writeln!(out, " {value}");
}

/// Label value escaping per the exposition format: backslash, double quote, newline.
fn escape(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::QueueStats;

    fn value(body: &str, series: &str) -> Option<f64> {
        body.lines().find_map(|l| l.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
    }

    #[test]
    fn counters_render_with_labels() {
        let m = Metrics::default();
        m.received("q", "t");
        m.received("q", "t");
        m.received("q2", "t");
        m.deferred("q", "t");
        m.reclaimed("q");
        m.broker_error("fetch");
        let b = m.render(&Gauges::default());
        assert_eq!(value(&b, r#"mayu_tasks_received_total{queue="q",task="t"}"#), Some(2.0));
        assert_eq!(value(&b, r#"mayu_tasks_received_total{queue="q2",task="t"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_tasks_deferred_total{queue="q",task="t"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_tasks_reclaimed_total{queue="q"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_broker_errors_total{op="fetch"}"#), Some(1.0));
    }

    #[test]
    fn settled_counts_by_outcome_and_observes_cumulative_durations() {
        let m = Metrics::default();
        m.settled("q", "t", Settled::Success, Some(Duration::from_millis(30)));
        m.settled("q", "t", Settled::Dead, Some(Duration::from_secs(2)));
        m.settled("q", "t", Settled::Dead, None); // poison: counted, not timed
        let b = m.render(&Gauges::default());
        assert_eq!(value(&b, r#"mayu_tasks_settled_total{queue="q",task="t",outcome="success"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_tasks_settled_total{queue="q",task="t",outcome="dead"}"#), Some(2.0));
        let bucket = |le: &str| value(&b, &format!(r#"mayu_task_duration_seconds_bucket{{queue="q",task="t",le="{le}"}}"#));
        assert_eq!(bucket("0.025"), Some(0.0));
        assert_eq!(bucket("0.05"), Some(1.0));
        assert_eq!(bucket("1"), Some(1.0));
        assert_eq!(bucket("2.5"), Some(2.0));
        assert_eq!(bucket("+Inf"), Some(2.0));
        assert_eq!(value(&b, r#"mayu_task_duration_seconds_count{queue="q",task="t"}"#), Some(2.0));
        let sum = value(&b, r#"mayu_task_duration_seconds_sum{queue="q",task="t"}"#).unwrap();
        assert!((sum - 2.03).abs() < 1e-9, "sum was {sum}");
    }

    #[test]
    fn label_values_are_escaped() {
        let m = Metrics::default();
        m.received("q", "a\"b\\c\nd");
        let b = m.render(&Gauges::default());
        assert!(b.contains(r#"mayu_tasks_received_total{queue="q",task="a\"b\\c\nd"} 1"#), "{b}");
    }

    #[test]
    fn gauges_render_with_ready_derived_from_stream_and_pending() {
        let g = Gauges {
            concurrency: 8,
            inflight: 5,
            running: 3,
            queues: vec![
                ("a".into(), QueueStats { stream: 10, pending: 4, delayed: 2, dead: 1 }),
                ("b".into(), QueueStats { stream: 1, pending: 3, delayed: 0, dead: 0 }), // racy read
            ],
        };
        let b = Metrics::default().render(&g);
        assert_eq!(value(&b, "mayu_worker_concurrency"), Some(8.0));
        assert_eq!(value(&b, "mayu_tasks_inflight"), Some(5.0));
        assert_eq!(value(&b, "mayu_tasks_running"), Some(3.0));
        assert_eq!(value(&b, r#"mayu_queue_ready{queue="a"}"#), Some(6.0));
        assert_eq!(value(&b, r#"mayu_queue_pending{queue="a"}"#), Some(4.0));
        assert_eq!(value(&b, r#"mayu_queue_delayed{queue="a"}"#), Some(2.0));
        assert_eq!(value(&b, r#"mayu_queue_dead{queue="a"}"#), Some(1.0));
        assert_eq!(value(&b, r#"mayu_queue_ready{queue="b"}"#), Some(0.0));
    }

    #[test]
    fn every_sample_follows_its_type_line() {
        let m = Metrics::default();
        m.received("q", "t");
        m.settled("q", "t", Settled::Retry, Some(Duration::from_millis(1)));
        let g = Gauges { queues: vec![("q".into(), QueueStats::default())], ..Gauges::default() };
        let b = m.render(&g);
        let mut typed: Vec<String> = Vec::new();
        for line in b.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                typed.push(rest.split(' ').next().unwrap().to_string());
                continue;
            }
            if line.starts_with('#') || line.is_empty() { continue; }
            let name = line.split(['{', ' ']).next().unwrap();
            let family = ["_bucket", "_sum", "_count"].iter()
                .find_map(|s| name.strip_suffix(s).filter(|f| typed.iter().any(|t| t == f)))
                .unwrap_or(name);
            assert!(typed.iter().any(|t| t == family), "sample {line:?} has no preceding TYPE");
            assert!(line.rsplit(' ').next().unwrap().parse::<f64>().is_ok(), "bad value in {line:?}");
        }
    }
}
