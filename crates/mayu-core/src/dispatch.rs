//! Orders claimed tasks for the worker threads: queue priority first, then per-task rate limits.
use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::runtime::ReadyTask;

/// A throttled task whose slot is further out than this goes back to the broker instead of holding
/// a prefetch slot, so a rate-limited backlog cannot starve other tasks.
pub const HOLD_HORIZON: Duration = Duration::from_secs(1);

/// Token bucket of capacity 1 as virtual scheduling: each admitted task reserves the next slot.
struct Throttle {
    interval: Duration,
    /// Earliest start for the next admitted task.
    next: Option<Instant>,
    /// Advisory: spaces deferred tasks' return times one interval apart. Reserves nothing.
    next_deferred: Option<Instant>,
}

pub enum Admit {
    /// Held here until it is due.
    Held,
    /// Too far out: the caller must return it to the broker to run after `delay`.
    Defer { task: ReadyTask, delay: Duration },
}

struct Held {
    level: u32,
    seq: u64,
    at: Instant,
    task: ReadyTask,
}

pub struct Dispatcher {
    /// Queue -> priority level, lower first. Empty when all queues rank equally.
    levels: HashMap<String, u32>,
    throttles: HashMap<String, Throttle>,
    held: Vec<Held>,
    seq: u64,
}

impl Dispatcher {
    /// With `strict`, earlier `queues` run first; otherwise every queue ranks equally (FIFO).
    /// `rate_limits` maps task name to starts per second, for this worker.
    pub fn new(queues: &[String], strict: bool, rate_limits: &HashMap<String, f64>) -> Result<Self> {
        let levels = if strict {
            queues.iter().enumerate().map(|(i, q)| (q.clone(), i as u32)).collect()
        } else {
            HashMap::new()
        };
        let mut throttles = HashMap::new();
        for (name, &rate) in rate_limits {
            let interval = (rate.is_finite() && rate > 0.0)
                .then(|| Duration::try_from_secs_f64(1.0 / rate).ok())
                .flatten()
                .ok_or_else(|| Error::Config(format!("rate limit for {name:?} must be a positive rate, got {rate}")))?;
            throttles.insert(name.clone(), Throttle { interval, next: None, next_deferred: None });
        }
        Ok(Dispatcher { levels, throttles, held: Vec::new(), seq: 0 })
    }

    pub fn push(&mut self, task: ReadyTask, now: Instant) -> Admit {
        let at = match self.throttles.get_mut(&task.task) {
            None => now,
            Some(t) => {
                let slot = t.next.map_or(now, |n| n.max(now));
                if slot - now > HOLD_HORIZON {
                    let eta = t.next_deferred.map_or(slot, |d| d.max(slot));
                    t.next_deferred = Some(eta + t.interval);
                    return Admit::Defer { task, delay: eta - now };
                }
                t.next = Some(slot + t.interval);
                slot
            }
        };
        self.hold(task, at);
        Admit::Held
    }

    /// Holds `task` until `at` without consulting its rate limit.
    pub fn hold(&mut self, task: ReadyTask, at: Instant) {
        let level = self.levels.get(&task.queue).copied().unwrap_or(0);
        self.seq += 1;
        self.held.push(Held { level, seq: self.seq, at, task });
    }

    /// The highest-priority task that is due, oldest first within a priority.
    pub fn pop(&mut self, now: Instant) -> Option<ReadyTask> {
        let i = self.held.iter().enumerate()
            .filter(|(_, h)| h.at <= now)
            .min_by_key(|(_, h)| (h.level, h.seq))
            .map(|(i, _)| i)?;
        Some(self.held.swap_remove(i).task)
    }

    pub fn next_due(&self) -> Option<Instant> {
        self.held.iter().map(|h| h.at).min()
    }

    pub fn drain(&mut self) -> Vec<ReadyTask> {
        self.held.drain(..).map(|h| h.task).collect()
    }

    pub fn len(&self) -> usize {
        self.held.len()
    }

    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }
}

/// The dispatcher shared by the fetch loop (push) and the Python threads (blocking pop).
pub(crate) struct ReadyQueue {
    inner: Mutex<Dispatcher>,
    cv: Condvar,
}

impl ReadyQueue {
    pub(crate) fn new(d: Dispatcher) -> Self {
        ReadyQueue { inner: Mutex::new(d), cv: Condvar::new() }
    }

    pub(crate) fn push(&self, task: ReadyTask, now: Instant) -> Admit {
        let admit = self.inner.lock().unwrap().push(task, now);
        if matches!(admit, Admit::Held) {
            self.cv.notify_one();
        }
        admit
    }

    pub(crate) fn hold(&self, task: ReadyTask, at: Instant) {
        self.inner.lock().unwrap().hold(task, at);
        self.cv.notify_one();
    }

    /// Blocks until a task is due or `timeout` passes. Waiters wake early for held tasks coming due.
    pub(crate) fn recv_timeout(&self, timeout: Duration) -> Option<ReadyTask> {
        let deadline = Instant::now() + timeout;
        let mut d = self.inner.lock().unwrap();
        loop {
            let now = Instant::now();
            if let Some(t) = d.pop(now) {
                if !d.is_empty() {
                    self.cv.notify_one(); // let another thread look at what is left
                }
                return Some(t);
            }
            if now >= deadline {
                if !d.is_empty() {
                    self.cv.notify_one(); // we may have consumed a wake meant for a newly held task
                }
                return None;
            }
            let wake = d.next_due().map_or(deadline, |n| n.min(deadline));
            d = self.cv.wait_timeout(d, wake.saturating_duration_since(now)).unwrap().0;
        }
    }

    pub(crate) fn drain(&self) -> Vec<ReadyTask> {
        self.inner.lock().unwrap().drain()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn task(token: u64, name: &str, queue: &str) -> ReadyTask {
        ReadyTask {
            token,
            id: format!("id{token}"),
            task: name.into(),
            queue: queue.into(),
            attempt: 0,
            max_retries: 0,
            content_type: "ct".into(),
            payload: vec![],
        }
    }

    fn qs(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn limits(pairs: &[(&str, f64)]) -> HashMap<String, f64> {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    fn tokens(d: &mut Dispatcher, now: Instant) -> Vec<u64> {
        std::iter::from_fn(|| d.pop(now).map(|t| t.token)).collect()
    }

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn strict_priority_pops_earlier_queues_first_fifo_within() {
        let mut d = Dispatcher::new(&qs(&["hi", "lo"]), true, &HashMap::new()).unwrap();
        let now = Instant::now();
        for (tok, q) in [(1, "lo"), (2, "hi"), (3, "lo"), (4, "hi")] {
            assert!(matches!(d.push(task(tok, "t", q), now), Admit::Held));
        }
        assert_eq!(tokens(&mut d, now), vec![2, 4, 1, 3]);
    }

    #[test]
    fn equal_priority_is_fifo_across_queues() {
        let mut d = Dispatcher::new(&qs(&["hi", "lo"]), false, &HashMap::new()).unwrap();
        let now = Instant::now();
        for (tok, q) in [(1, "lo"), (2, "hi"), (3, "lo")] {
            d.push(task(tok, "t", q), now);
        }
        assert_eq!(tokens(&mut d, now), vec![1, 2, 3]);
    }

    #[test]
    fn rate_limit_spaces_tasks_one_interval_apart() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 10.0)])).unwrap();
        let t0 = Instant::now();
        for tok in 1..=3 {
            assert!(matches!(d.push(task(tok, "slow", "q"), t0), Admit::Held));
        }
        assert_eq!(tokens(&mut d, t0), vec![1]);
        assert_eq!(d.next_due(), Some(t0 + 100 * MS));
        assert_eq!(tokens(&mut d, t0 + 99 * MS), Vec::<u64>::new());
        assert_eq!(tokens(&mut d, t0 + 100 * MS), vec![2]);
        assert_eq!(tokens(&mut d, t0 + 200 * MS), vec![3]);
        assert!(d.is_empty());
    }

    #[test]
    fn throttled_task_does_not_block_others() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 10.0)])).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        d.push(task(2, "slow", "q"), t0);
        d.push(task(3, "fast", "q"), t0);
        assert_eq!(tokens(&mut d, t0), vec![1, 3]);
    }

    #[test]
    fn throttled_higher_priority_task_lets_lower_priority_run() {
        let mut d = Dispatcher::new(&qs(&["hi", "lo"]), true, &limits(&[("slow", 10.0)])).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "hi"), t0);
        d.push(task(2, "slow", "hi"), t0);
        d.push(task(3, "fast", "lo"), t0);
        assert_eq!(tokens(&mut d, t0), vec![1, 3]);
        assert_eq!(tokens(&mut d, t0 + 100 * MS), vec![2]);
    }

    #[test]
    fn idle_bucket_refills() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 10.0)])).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        let later = t0 + Duration::from_secs(5);
        d.push(task(2, "slow", "q"), later);
        assert_eq!(tokens(&mut d, later), vec![1, 2]);
    }

    #[test]
    fn beyond_horizon_defers_spread_one_interval_apart() {
        // 0.5/s = one start every 2 s, past the 1 s horizon
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 0.5)])).unwrap();
        let t0 = Instant::now();
        assert!(matches!(d.push(task(1, "slow", "q"), t0), Admit::Held));
        let delays: Vec<Duration> = (2..=4)
            .map(|tok| match d.push(task(tok, "slow", "q"), t0) {
                Admit::Defer { task, delay } => {
                    assert_eq!(task.token, tok);
                    delay
                }
                Admit::Held => panic!("task {tok} should be deferred"),
            })
            .collect();
        let s = Duration::from_secs(1);
        assert_eq!(delays, vec![2 * s, 4 * s, 6 * s]);
        assert_eq!(d.len(), 1, "deferred tasks are not held");
    }

    #[test]
    fn deferred_task_returning_on_time_is_admitted() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 0.5)])).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        assert!(matches!(d.push(task(2, "slow", "q"), t0), Admit::Defer { .. }));
        let back = t0 + Duration::from_secs(2);
        assert!(matches!(d.push(task(2, "slow", "q"), back), Admit::Held), "deferral reserved no slot");
        assert_eq!(tokens(&mut d, back), vec![1, 2]);
    }

    #[test]
    fn slot_exactly_at_the_horizon_is_held() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 1.0)])).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        assert!(matches!(d.push(task(2, "slow", "q"), t0), Admit::Held));
    }

    #[test]
    fn hold_ignores_the_rate_limit_and_waits_until_at() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 0.5)])).unwrap();
        let t0 = Instant::now();
        let at = t0 + Duration::from_secs(3);
        d.hold(task(1, "slow", "q"), at);
        assert_eq!(d.pop(t0).map(|t| t.token), None);
        assert_eq!(d.next_due(), Some(at));
        assert_eq!(tokens(&mut d, at), vec![1]);
    }

    #[test]
    fn drain_returns_everything_held() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 10.0)])).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        d.push(task(2, "slow", "q"), t0); // not due yet
        let mut got: Vec<u64> = d.drain().into_iter().map(|t| t.token).collect();
        got.sort();
        assert_eq!(got, vec![1, 2]);
        assert!(d.is_empty() && d.next_due().is_none());
    }

    #[test]
    fn rejects_unusable_rate_limits() {
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e-300] {
            assert!(
                matches!(Dispatcher::new(&qs(&["q"]), false, &limits(&[("t", bad)])), Err(Error::Config(_))),
                "rate {bad} must be rejected"
            );
        }
    }

    fn empty_queue() -> ReadyQueue {
        ReadyQueue::new(Dispatcher::new(&qs(&["q"]), false, &HashMap::new()).unwrap())
    }

    #[test]
    fn ready_queue_recv_waits_for_a_held_task_to_come_due() {
        let q = empty_queue();
        q.hold(task(1, "t", "q"), Instant::now() + 80 * MS);
        let t = Instant::now();
        assert_eq!(q.recv_timeout(Duration::from_secs(2)).map(|t| t.token), Some(1));
        let waited = t.elapsed();
        assert!(waited >= 70 * MS && waited < Duration::from_secs(1), "waited {waited:?}");
    }

    #[test]
    fn ready_queue_recv_times_out_empty() {
        let q = empty_queue();
        let t = Instant::now();
        assert!(q.recv_timeout(50 * MS).is_none());
        assert!(t.elapsed() >= 45 * MS);
    }

    #[test]
    fn ready_queue_push_wakes_a_blocked_receiver() {
        let q = Arc::new(empty_queue());
        let q2 = q.clone();
        let h = std::thread::spawn(move || q2.recv_timeout(Duration::from_secs(5)).map(|t| t.token));
        std::thread::sleep(30 * MS);
        let t = Instant::now();
        q.push(task(7, "t", "q"), Instant::now());
        assert_eq!(h.join().unwrap(), Some(7));
        assert!(t.elapsed() < Duration::from_secs(1));
    }
}
