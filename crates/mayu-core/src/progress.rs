use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Most running tasks one heartbeat publishes, oldest-started first.
pub const MAX_PUBLISHED: usize = 64;

/// Fraction change two reports must differ by before their rate is trusted.
const MIN_RATE_FRACTION: f64 = 0.01;
/// Time two reports must be apart before their rate is trusted.
const MIN_RATE_WINDOW: Duration = Duration::from_secs(1);
/// Fraction from which `elapsed x (1 - f) / f` is a usable guess.
const MIN_ELAPSED_FRACTION: f64 = 0.05;

/// What a running task last reported, kept in its in-flight entry.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Progress {
    /// In `[0, 1]`.
    pub fraction: f64,
    pub done: f64,
    pub total: Option<f64>,
    /// The report the rate is measured from; reset when progress goes backwards.
    pub first: (Instant, f64),
    pub last: (Instant, f64),
}

/// The fraction a report stands for, or None for numbers that can't be one.
fn fraction_of(done: f64, total: Option<f64>) -> Option<f64> {
    if !done.is_finite() {
        return None;
    }
    let f = match total {
        None => done,
        Some(t) if t.is_finite() && t > 0.0 => done / t,
        Some(_) => return None,
    };
    Some(f.clamp(0.0, 1.0))
}

impl Progress {
    /// The first report; None if the numbers are unusable (the caller validates, this keeps Rust safe).
    pub fn new(now: Instant, done: f64, total: Option<f64>) -> Option<Self> {
        let f = fraction_of(done, total)?;
        Some(Progress { fraction: f, done, total, first: (now, f), last: (now, f) })
    }

    /// Records a later report. False (and nothing stored) if the numbers are unusable.
    pub fn report(&mut self, now: Instant, done: f64, total: Option<f64>) -> bool {
        let Some(f) = fraction_of(done, total) else { return false };
        if f < self.last.1 {
            // A retry inside the task or a new phase: estimate from here, not from the old run.
            self.first = (now, f);
        }
        self.fraction = f;
        self.done = done;
        self.total = total;
        self.last = (now, f);
        true
    }

    /// Seconds left, or None while there's too little to go on. `started` is when the task began running.
    pub fn remaining_s(&self, started: Instant, now: Instant) -> Option<f64> {
        let f = self.fraction;
        if f >= 1.0 {
            return Some(0.0);
        }
        let dt = self.last.0.saturating_duration_since(self.first.0);
        let df = self.last.1 - self.first.1;
        if df >= MIN_RATE_FRACTION && dt >= MIN_RATE_WINDOW {
            // From the first report, so model loading and setup before the loop don't count as slow work.
            return Some((1.0 - f) * dt.as_secs_f64() / df);
        }
        if f >= MIN_ELAPSED_FRACTION {
            return Some(now.saturating_duration_since(started).as_secs_f64() * (1.0 - f) / f);
        }
        None
    }
}

/// A task running on this worker, as published in the heartbeat.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunningTask {
    pub id: String,
    pub task: String,
    pub queue: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// The node's index in its workflow, as a decimal string (as in `Header`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Unix ms the task started running.
    pub started_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub done: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fraction: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining_s: Option<f64>,
}

/// The heartbeat's `tasks` field: a msgpack list of maps with named keys and no None fields, so later versions
/// can add keys (readers ignore unknown ones) and older writers can omit optional ones.
pub fn encode_running(tasks: &[RunningTask]) -> Vec<u8> {
    rmp_serde::to_vec_named(tasks).expect("running task serialization cannot fail")
}

pub fn decode_running(b: &[u8]) -> Result<Vec<RunningTask>> {
    rmp_serde::from_slice(b).map_err(|e| Error::Envelope(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn close(a: Option<f64>, b: f64) {
        let a = a.expect("an estimate");
        assert!((a - b).abs() < 1e-9, "{a} != {b}");
    }

    #[test]
    fn steady_rate_gives_the_remaining_time() {
        let t0 = Instant::now();
        let mut p = Progress::new(t0, 10.0, Some(100.0)).unwrap();
        // 10% per 10 s -> 1 %/s; at 40 % there are 60 s left.
        for i in 1..=3 {
            assert!(p.report(t0 + secs(10 * i), 10.0 + 10.0 * i as f64, Some(100.0)));
        }
        close(p.remaining_s(t0, t0 + secs(30)), 60.0);
    }

    #[test]
    fn setup_before_the_first_report_is_ignored() {
        let t0 = Instant::now();
        // Loaded a model for 100 s, then ran at 1 %/s.
        let mut p = Progress::new(t0 + secs(100), 0.1, None).unwrap();
        p.report(t0 + secs(110), 0.2, None);
        p.report(t0 + secs(120), 0.3, None);
        // The elapsed-time fallback would say 100+ s elapsed x 0.7/0.3 = 280 s; the rate says 70 s.
        close(p.remaining_s(t0, t0 + secs(120)), 70.0);
    }

    #[test]
    fn going_backwards_restarts_the_estimate() {
        let t0 = Instant::now();
        let mut p = Progress::new(t0, 0.0, None).unwrap();
        p.report(t0 + secs(10), 0.8, None);
        p.report(t0 + secs(20), 0.1, None);
        assert_eq!(p.first, (t0 + secs(20), 0.1));
        assert_eq!(p.fraction, 0.1);
        // One report since the reset: no rate yet, so the elapsed-time guess applies.
        close(p.remaining_s(t0, t0 + secs(20)), 20.0 * 0.9 / 0.1);
        p.report(t0 + secs(30), 0.3, None);
        assert_eq!(p.first, (t0 + secs(20), 0.1));
        close(p.remaining_s(t0, t0 + secs(30)), 0.7 * 10.0 / 0.2);
    }

    #[test]
    fn unknown_below_five_percent_with_one_report() {
        let t0 = Instant::now();
        let p = Progress::new(t0 + secs(5), 4.0, Some(100.0)).unwrap();
        assert_eq!(p.remaining_s(t0, t0 + secs(30)), None);
        let p = Progress::new(t0 + secs(5), 0.0, None).unwrap();
        assert_eq!(p.remaining_s(t0, t0 + secs(30)), None);
    }

    #[test]
    fn complete_is_zero_seconds_left() {
        let t0 = Instant::now();
        let p = Progress::new(t0, 7.0, Some(7.0)).unwrap();
        assert_eq!(p.remaining_s(t0, t0), Some(0.0));
    }

    #[test]
    fn one_report_at_five_percent_uses_elapsed_time() {
        let t0 = Instant::now();
        let p = Progress::new(t0 + secs(20), 0.25, None).unwrap();
        close(p.remaining_s(t0, t0 + secs(40)), 40.0 * 0.75 / 0.25);
    }

    #[test]
    fn rate_needs_a_big_enough_step_and_window() {
        let t0 = Instant::now();
        // 0.5 % in 100 s: too small a step, and still under 5 % overall.
        let mut p = Progress::new(t0, 0.0, None).unwrap();
        p.report(t0 + secs(100), 0.005, None);
        assert_eq!(p.remaining_s(t0, t0 + secs(100)), None);
        // 2 % in 0.5 s: too short a window, so the elapsed guess is used (6 %).
        let mut p = Progress::new(t0, 0.04, None).unwrap();
        p.report(t0 + Duration::from_millis(500), 0.06, None);
        close(p.remaining_s(t0, t0 + secs(10)), 10.0 * 0.94 / 0.06);
    }

    #[test]
    fn unusable_numbers_are_not_stored() {
        let t0 = Instant::now();
        assert!(Progress::new(t0, f64::NAN, None).is_none());
        assert!(Progress::new(t0, f64::INFINITY, Some(10.0)).is_none());
        assert!(Progress::new(t0, 1.0, Some(0.0)).is_none());
        assert!(Progress::new(t0, 1.0, Some(f64::NAN)).is_none());
        let mut p = Progress::new(t0, 0.5, None).unwrap();
        let before = p;
        assert!(!p.report(t0 + secs(1), f64::NAN, None));
        assert!(!p.report(t0 + secs(1), 1.0, Some(-1.0)));
        assert_eq!(p, before);
    }

    #[test]
    fn fraction_is_clamped() {
        let t0 = Instant::now();
        assert_eq!(Progress::new(t0, 12.0, Some(10.0)).unwrap().fraction, 1.0);
        assert_eq!(Progress::new(t0, -3.0, None).unwrap().fraction, 0.0);
        assert_eq!(Progress::new(t0, 3.0, None).unwrap().fraction, 1.0);
    }

    fn task(id: &str) -> RunningTask {
        RunningTask {
            id: id.into(), task: "t".into(), queue: "q".into(), workflow_id: None, node: None,
            started_ms: 1_700_000_000_000, done: None, total: None, fraction: None, remaining_s: None,
        }
    }

    #[test]
    fn running_tasks_round_trip_with_and_without_optional_fields() {
        let full = RunningTask {
            workflow_id: Some("wf".into()), node: Some("2".into()), done: Some(7.0), total: Some(25.0),
            fraction: Some(0.28), remaining_s: Some(250.5), ..task("a")
        };
        let list = vec![full, task("b")];
        assert_eq!(decode_running(&encode_running(&list)).unwrap(), list);
        assert_eq!(decode_running(&encode_running(&[])).unwrap(), vec![]);
    }

    #[test]
    fn none_fields_are_left_out_of_the_wire_form() {
        use serde::de::IgnoredAny;
        let raw: Vec<std::collections::BTreeMap<String, IgnoredAny>> =
            rmp_serde::from_slice(&encode_running(&[task("a")])).unwrap();
        let keys: Vec<_> = raw[0].keys().cloned().collect();
        assert_eq!(keys, ["id", "queue", "started_ms", "task"]);
    }

    #[test]
    fn decode_ignores_unknown_keys_and_accepts_missing_optional_ones() {
        #[derive(Serialize)]
        struct Newer {
            id: &'static str, task: &'static str, queue: &'static str, started_ms: u64, gpu: &'static str, extra: Vec<u8>,
        }
        let bytes = rmp_serde::to_vec_named(&[Newer {
            id: "a", task: "t", queue: "q", started_ms: 5, gpu: "0", extra: vec![1, 2],
        }]).unwrap();
        let got = decode_running(&bytes).unwrap();
        assert_eq!(got, vec![RunningTask { started_ms: 5, ..task("a") }]);
    }

    #[test]
    fn malformed_bytes_are_an_error() {
        assert!(decode_running(b"").is_err());
        assert!(decode_running(b"\xff\xfe not msgpack").is_err());
        // A map where a list is expected, and a list of the wrong things.
        assert!(decode_running(&rmp_serde::to_vec_named(&task("a")).unwrap()).is_err());
        assert!(decode_running(&rmp_serde::to_vec(&[1, 2, 3]).unwrap()).is_err());
    }
}
