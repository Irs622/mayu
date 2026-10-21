//! Orders claimed tasks for the worker threads: queue priority first, then per-task rate limits.
use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::runtime::ReadyTask;

/// A throttled task whose slot is further out than this goes back to the broker instead of holding
/// a prefetch slot, so a rate-limited backlog cannot starve other tasks.
pub const HOLD_HORIZON: Duration = Duration::from_secs(1);

/// Once the head (the first due task that doesn't fit) has waited this long, later tasks may start
/// only if they request none of the resources the head requests.
pub const RESERVE_AFTER: Duration = Duration::from_secs(5);

/// Ledger units per whole unit: amounts are integer milli-units so repeated acquire/release never drifts.
const MILLI: f64 = 1000.0;
/// Larger amounts could overflow the ledger sums; far beyond any real resource.
const MAX_MILLI: f64 = 1e15;

/// What a worker has and what its tasks use, in the units the user picked (GPUs, GB, cores).
#[derive(Debug, Clone, Default)]
pub struct Resources {
    /// Resource -> amount this worker process has.
    pub capacity: HashMap<String, f64>,
    /// Task name -> resource -> amount one running instance uses.
    pub requests: HashMap<String, HashMap<String, f64>>,
    /// Pool name -> lazily loaded model pool. Empty unless the worker runs pools in lazy mode.
    pub lazy: HashMap<String, LazyPool>,
}

/// A model pool whose instances are loaded when a task needs one and evicted when the room is needed.
/// Needs a `pool:<name>` resource (the most instances at once) and holds `claim` per loaded instance.
#[derive(Debug, Clone, Default)]
pub struct LazyPool {
    /// Resource -> amount one loaded instance holds, idle or not.
    pub claim: HashMap<String, f64>,
    /// Instances already loaded when the dispatcher starts (preloaded).
    pub loaded: u32,
}

fn milli(amount: f64) -> Option<u64> {
    let m = (amount * MILLI).round();
    (m.is_finite() && (0.0..MAX_MILLI).contains(&m)).then_some(m as u64)
}

/// (resource index, milli-units)
type Claim = Vec<(usize, u64)>;

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
    Defer { task: Box<ReadyTask>, delay: Duration },
}

/// One lazy pool's residency, as far as the ledger knows.
struct LazyState {
    name: String,
    /// Ledger index of `pool:<name>`; its `in_use` is the number of busy instances.
    res: usize,
    /// What one loaded instance holds in the ledger.
    claim: Claim,
    /// Instances loaded or being loaded (idle or busy).
    loaded: u32,
    /// Unloads begun and not done: the memory is still in `in_use`.
    unloading: u32,
    /// When a task for this pool was last popped; None until then.
    last_used: Option<Instant>,
}

/// The lazy pools, sorted by name. Empty for eager and non-pool workers, which never look at it.
#[derive(Default)]
struct Lazy {
    pools: Vec<LazyState>,
    /// Ledger resource index -> the lazy pool whose `pool:<name>` it is.
    of_res: Vec<Option<usize>>,
}

/// What popping a task changes in a lazy worker: pools (indexes into `Lazy::pools`), one entry per instance.
#[derive(Default)]
struct Plan {
    loads: Vec<usize>,
    evicts: Vec<usize>,
}

impl Lazy {
    fn idle(&self, p: usize, in_use: &[u64]) -> u32 {
        let busy = (in_use[self.pools[p].res] / MILLI as u64) as u32;
        self.pools[p].loaded.saturating_sub(busy)
    }

    /// How many instances `m` milli-units of `pool:<name>` asks for.
    fn instances(m: u64) -> u32 {
        (m / MILLI as u64).max(1) as u32
    }

    /// Pools to load for `claim`: one entry per instance the task needs beyond the idle ones. A pool already
    /// at its instance limit gets no load: the task waits for `pool:<name>`, not for room to build one.
    fn loads_for(&self, claim: &Claim, in_use: &[u64], capacity: &[u64]) -> Vec<usize> {
        let mut loads = Vec::new();
        for &(r, m) in claim {
            let Some(p) = self.of_res[r] else { continue };
            let room = ((capacity[r] / MILLI as u64) as u32).saturating_sub(self.pools[p].loaded);
            let missing = Self::instances(m).saturating_sub(self.idle(p, in_use));
            loads.extend(std::iter::repeat_n(p, missing.min(room) as usize));
        }
        loads
    }

    /// Whether `claim` can start now, loading what is missing and evicting idle instances (least recently
    /// used pool first) for the room; the plan says how. None if it cannot, even with every idle instance gone.
    fn plan(&self, claim: &Claim, in_use: &[u64], capacity: &[u64]) -> Option<Plan> {
        let loads = self.loads_for(claim, in_use, capacity);
        if loads.is_empty() && claim.iter().all(|&(r, m)| in_use[r] + m <= capacity[r]) {
            return Some(Plan::default());
        }
        let mut add = vec![0u64; capacity.len()];
        for &(r, m) in claim {
            add[r] += m;
        }
        for &p in &loads {
            for &(r, m) in &self.pools[p].claim {
                add[r] += m;
            }
        }
        // Only what this task touches counts: a resource over capacity after a shrink must not block it.
        let mut short: Vec<u64> = (0..capacity.len())
            .map(|r| if add[r] > 0 { (in_use[r] + add[r]).saturating_sub(capacity[r]) } else { 0 })
            .collect();
        // The instances this task takes are not victims.
        let mut avail: Vec<u32> = (0..self.pools.len()).map(|p| self.idle(p, in_use)).collect();
        for &(r, m) in claim {
            if let Some(p) = self.of_res[r] {
                avail[p] = avail[p].saturating_sub(Self::instances(m));
            }
        }
        let mut order: Vec<usize> = (0..self.pools.len()).filter(|&p| avail[p] > 0).collect();
        order.sort_by_key(|&p| (self.pools[p].last_used, p));
        let mut evicts = Vec::new();
        while short.iter().any(|&s| s > 0) {
            // A pool that holds none of what is short would be closed for nothing.
            let p = order.iter().copied()
                .find(|&p| avail[p] > 0 && self.pools[p].claim.iter().any(|&(r, _)| short[r] > 0))?;
            avail[p] -= 1;
            evicts.push(p);
            for &(r, m) in &self.pools[p].claim {
                short[r] = short[r].saturating_sub(m);
            }
        }
        Some(Plan { loads, evicts })
    }

    /// Resources a waiting head claim wants beyond its own: those of its loads, and `pool:<name>` of every
    /// pool holding any of what it needs, so tasks that would keep such a pool busy stop backfilling.
    fn wanted_beyond(&self, claim: &Claim, in_use: &[u64], capacity: &[u64]) -> Vec<usize> {
        let mut wanted: Vec<usize> = self.loads_for(claim, in_use, capacity).into_iter()
            .flat_map(|p| self.pools[p].claim.iter().map(|&(r, _)| r))
            .collect();
        let needs = |r: usize| wanted.contains(&r) || claim.iter().any(|&(c, _)| c == r);
        let holders: Vec<usize> = self.pools.iter()
            .filter(|p| p.claim.iter().any(|&(r, _)| needs(r)))
            .map(|p| p.res)
            .collect();
        wanted.extend(holders);
        wanted
    }

    /// Applies `plan` for the task being popped; `claim` is already in `in_use`.
    fn commit(&mut self, claim: &Claim, plan: Plan, in_use: &mut [u64], task: &mut ReadyTask, now: Instant) {
        // The victims' room goes straight to the loads: the task closes them before it builds.
        for p in plan.evicts {
            let pool = &mut self.pools[p];
            pool.loaded -= 1;
            for &(r, m) in &pool.claim {
                in_use[r] = in_use[r].saturating_sub(m);
            }
            task.evicts.push(pool.name.clone());
        }
        for p in plan.loads {
            let pool = &mut self.pools[p];
            pool.loaded += 1;
            for &(r, m) in &pool.claim {
                in_use[r] += m;
            }
            task.loads.push(pool.name.clone());
        }
        for &(r, _) in claim {
            if let Some(p) = self.of_res[r] {
                self.pools[p].last_used = Some(now);
            }
        }
    }

    fn pool_mut(&mut self, name: &str) -> Result<&mut LazyState> {
        self.pools.iter_mut().find(|p| p.name == name)
            .ok_or_else(|| Error::Config(format!("pool {name:?} is not a lazy pool of this worker")))
    }
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
    /// Resource names, sorted; the other ledger vectors are indexed by position here.
    resource_names: Vec<String>,
    capacity: Vec<u64>,
    in_use: Vec<u64>,
    /// Task name -> what one instance claims. Empty when no served task requests anything.
    requests: HashMap<String, Claim>,
    /// Token -> what that running task holds, until `release`.
    held_by: HashMap<u64, Claim>,
    lazy: Lazy,
}

impl Dispatcher {
    /// With `strict`, earlier `queues` run first; otherwise every queue ranks equally (FIFO).
    /// `rate_limits` maps task name to starts per second, for this worker.
    pub fn new(
        queues: &[String],
        strict: bool,
        rate_limits: &HashMap<String, f64>,
        resources: &Resources,
    ) -> Result<Self> {
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
        let mut resource_names: Vec<String> = resources.capacity.keys().cloned().collect();
        resource_names.sort();
        let mut capacity = Vec::with_capacity(resource_names.len());
        for name in &resource_names {
            let amount = resources.capacity[name];
            capacity.push(milli(amount).ok_or_else(|| {
                Error::Config(format!("resource {name:?} must have a finite capacity >= 0, got {amount}"))
            })?);
        }
        let mut requests = HashMap::new();
        for (task, wants) in &resources.requests {
            let mut claim: Claim = Vec::with_capacity(wants.len());
            for (name, &amount) in wants {
                let i = resource_names.binary_search(name).map_err(|_| {
                    Error::Config(format!("task {task:?} requests resource {name:?}, which this worker does not declare"))
                })?;
                let m = milli(amount).filter(|&m| m > 0).ok_or_else(|| {
                    Error::Config(format!("task {task:?} requests {amount} of {name:?}; it must be a finite amount of at least 0.001"))
                })?;
                if m > capacity[i] {
                    return Err(Error::Config(format!(
                        "task {task:?} requests {amount} of {name:?}, but this worker has only {}",
                        capacity[i] as f64 / MILLI
                    )));
                }
                claim.push((i, m));
            }
            if !claim.is_empty() {
                claim.sort_unstable();
                requests.insert(task.clone(), claim);
            }
        }
        let mut in_use = vec![0u64; capacity.len()];
        let mut lazy = Lazy { pools: Vec::new(), of_res: vec![None; capacity.len()] };
        let mut lazy_names: Vec<&String> = resources.lazy.keys().collect();
        lazy_names.sort();
        for name in lazy_names {
            let pool = &resources.lazy[name];
            let key = format!("pool:{name}");
            let res = resource_names.binary_search(&key).map_err(|_| {
                Error::Config(format!("lazy pool {name:?} needs the resource {key:?}, which this worker does not declare"))
            })?;
            if u64::from(pool.loaded) * MILLI as u64 > capacity[res] {
                return Err(Error::Config(format!(
                    "lazy pool {name:?} starts with {} loaded but allows only {}",
                    pool.loaded, capacity[res] as f64 / MILLI
                )));
            }
            let mut claim: Claim = Vec::with_capacity(pool.claim.len());
            for (rname, &amount) in &pool.claim {
                let i = resource_names.binary_search(rname).map_err(|_| {
                    Error::Config(format!("lazy pool {name:?} uses resource {rname:?}, which this worker does not declare"))
                })?;
                let m = milli(amount).filter(|&m| m > 0).ok_or_else(|| {
                    Error::Config(format!("lazy pool {name:?} uses {amount} of {rname:?}; it must be a finite amount of at least 0.001"))
                })?;
                let total = u64::from(pool.loaded).checked_mul(m).and_then(|t| t.checked_add(in_use[i]))
                    .filter(|&t| t <= capacity[i])
                    .ok_or_else(|| Error::Config(format!(
                        "lazy pool {name:?} with {} loaded needs more {rname:?} than this worker has ({})",
                        pool.loaded, capacity[i] as f64 / MILLI
                    )))?;
                in_use[i] = total;
                claim.push((i, m));
            }
            claim.sort_unstable();
            lazy.of_res[res] = Some(lazy.pools.len());
            lazy.pools.push(LazyState {
                name: name.clone(), res, claim, loaded: pool.loaded, unloading: 0, last_used: None,
            });
        }
        Ok(Dispatcher {
            levels, throttles, held: Vec::new(), seq: 0,
            resource_names, capacity, in_use, requests, held_by: HashMap::new(), lazy,
        })
    }

    pub fn push(&mut self, task: ReadyTask, now: Instant) -> Admit {
        let at = match self.throttles.get_mut(&task.task) {
            None => now,
            Some(t) => {
                let slot = t.next.map_or(now, |n| n.max(now));
                if slot - now > HOLD_HORIZON {
                    let eta = t.next_deferred.map_or(slot, |d| d.max(slot));
                    t.next_deferred = Some(eta + t.interval);
                    return Admit::Defer { task: Box::new(task), delay: eta - now };
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

    /// The highest-priority task that is due and fits the free resources, oldest first within a priority.
    pub fn pop(&mut self, now: Instant) -> Option<ReadyTask> {
        if self.requests.is_empty() {
            let i = self.held.iter().enumerate()
                .filter(|(_, h)| h.at <= now)
                .min_by_key(|(_, h)| (h.level, h.seq))
                .map(|(i, _)| i)?;
            return Some(self.held.swap_remove(i).task);
        }
        let (i, plan) = self.pop_fitting(now)?;
        let mut task = self.held.swap_remove(i).task;
        if let Some(claim) = self.requests.get(&task.task) {
            for &(r, m) in claim {
                self.in_use[r] += m;
            }
            self.held_by.insert(task.token, claim.clone());
            if !self.lazy.pools.is_empty() {
                self.lazy.commit(claim, plan, &mut self.in_use, &mut task, now);
            }
        }
        Some(task)
    }

    fn fits(&self, claim: &Claim) -> bool {
        claim.iter().all(|&(r, m)| self.in_use[r] + m <= self.capacity[r])
    }

    /// Index of the task to start (and its loads and evictions, empty unless the worker has lazy pools):
    /// walks due tasks in priority order. The first one that doesn't fit is the head; later ones may
    /// backfill while the head is young, and afterwards only if they request nothing the head requests,
    /// so a big request cannot be starved by a stream of small ones.
    fn pop_fitting(&self, now: Instant) -> Option<(usize, Plan)> {
        let mut due: Vec<usize> = (0..self.held.len()).filter(|&i| self.held[i].at <= now).collect();
        due.sort_unstable_by_key(|&i| (self.held[i].level, self.held[i].seq));
        let mut head: Option<(&Claim, Instant)> = None;
        // Lazy workers: what else the head wants (see `Lazy::wanted_beyond`).
        let mut head_beyond: Vec<usize> = Vec::new();
        for i in due {
            let h = &self.held[i];
            let claim = self.requests.get(&h.task.task);
            let plan = match claim {
                None => Some(Plan::default()),
                Some(c) if self.lazy.pools.is_empty() => self.fits(c).then(Plan::default),
                Some(c) => self.lazy.plan(c, &self.in_use, &self.capacity),
            };
            if let Some(plan) = plan {
                let backfill = match head {
                    None => true,
                    Some((_, since)) if now.saturating_duration_since(since) < RESERVE_AFTER => true,
                    Some((wanted, _)) => claim.is_none_or(|c| {
                        c.iter().all(|(r, _)| wanted.iter().all(|(w, _)| w != r) && !head_beyond.contains(r))
                    }),
                };
                if backfill {
                    return Some((i, plan));
                }
            } else if head.is_none() {
                head = claim.map(|c| (c, h.at));
                if let Some(c) = claim.filter(|_| !self.lazy.pools.is_empty()) {
                    head_beyond = self.lazy.wanted_beyond(c, &self.in_use, &self.capacity);
                }
            }
        }
        None
    }

    /// Returns what the task `token` was holding. A no-op for tasks that held nothing or were released.
    /// Returns whether anything was freed.
    pub fn release(&mut self, token: u64) -> bool {
        let Some(claim) = self.held_by.remove(&token) else { return false };
        for (r, m) in claim {
            self.in_use[r] -= m;
        }
        true
    }

    /// Changes a resource's capacity by `delta` (negative shrinks). Capacity may drop below what is in
    /// use: nothing new fits until enough is released, and `release` still subtracts what was taken.
    /// Returns whether it grew, which is when waiters may now fit.
    pub fn adjust_capacity(&mut self, resource: &str, delta: f64) -> Result<bool> {
        let i = self.resource_names.binary_search_by(|n| n.as_str().cmp(resource)).map_err(|_| {
            Error::Config(format!("cannot adjust resource {resource:?}, which this worker does not declare"))
        })?;
        let d = (delta * MILLI).round();
        if !d.is_finite() || d.abs() >= MAX_MILLI {
            return Err(Error::Config(format!("cannot adjust resource {resource:?} by {delta}")));
        }
        let d = d as i64;
        // |d| < 1e15 and capacity < 1e15, so the sum cannot overflow an i64.
        let new = self.capacity[i] as i64 + d;
        if new < 0 {
            return Err(Error::Config(format!(
                "cannot adjust resource {resource:?} by {delta}: it has only {}",
                self.capacity[i] as f64 / MILLI
            )));
        }
        if new as f64 >= MAX_MILLI {
            return Err(Error::Config(format!("cannot adjust resource {resource:?} by {delta}")));
        }
        self.capacity[i] = new as u64;
        Ok(d > 0)
    }

    /// A loaded instance of lazy pool `pool` is going away (its memory is still held until `unload_done`):
    /// it no longer counts as loaded, so releasing its `pool:<name>` does not make it look idle.
    pub fn unload_begin(&mut self, pool: &str) -> Result<()> {
        let p = self.lazy.pool_mut(pool)?;
        if p.loaded == 0 {
            return Err(Error::Config(format!("pool {pool:?} has no loaded instance to unload")));
        }
        p.loaded -= 1;
        p.unloading += 1;
        Ok(())
    }

    /// The instance from `unload_begin` is closed: gives its memory back.
    pub fn unload_done(&mut self, pool: &str) -> Result<()> {
        let p = self.lazy.pool_mut(pool)?;
        if p.unloading == 0 {
            return Err(Error::Config(format!("pool {pool:?} has no unload in progress")));
        }
        p.unloading -= 1;
        for &(r, m) in &p.claim {
            self.in_use[r] = self.in_use[r].saturating_sub(m);
        }
        Ok(())
    }

    /// (pool, instances loaded or being loaded) for each lazy pool, for metrics.
    pub fn lazy_usage(&self) -> Vec<(String, u32)> {
        self.lazy.pools.iter().map(|p| (p.name.clone(), p.loaded)).collect()
    }

    /// (resource, capacity, in use), for metrics.
    pub fn usage(&self) -> Vec<(String, f64, f64)> {
        self.resource_names.iter().enumerate()
            .map(|(i, n)| (n.clone(), self.capacity[i] as f64 / MILLI, self.in_use[i] as f64 / MILLI))
            .collect()
    }

    /// The earliest time after `now` that a held task comes due. A task already due but waiting for
    /// resources is not counted: `release` wakes it, and counting it would make waiters spin.
    pub fn next_due(&self, now: Instant) -> Option<Instant> {
        self.held.iter().map(|h| h.at).filter(|&at| at > now).min()
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
    /// Whether any task can hold resources; when not, `release` skips the lock.
    gated: bool,
}

impl ReadyQueue {
    pub(crate) fn new(d: Dispatcher) -> Self {
        let gated = !d.requests.is_empty() || !d.lazy.pools.is_empty();
        ReadyQueue { inner: Mutex::new(d), cv: Condvar::new(), gated }
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

    /// See `Dispatcher::adjust_capacity`. Growth wakes the waiters, several of which may now fit.
    pub(crate) fn adjust_capacity(&self, resource: &str, delta: f64) -> Result<()> {
        let grew = self.inner.lock().unwrap().adjust_capacity(resource, delta)?;
        if grew {
            self.cv.notify_all();
        }
        Ok(())
    }

    /// See `Dispatcher::unload_begin`. Fewer loaded instances can change what a waiter may evict.
    pub(crate) fn unload_begin(&self, pool: &str) -> Result<()> {
        self.inner.lock().unwrap().unload_begin(pool)?;
        self.cv.notify_all();
        Ok(())
    }

    /// See `Dispatcher::unload_done`. The freed memory may let several waiters fit.
    pub(crate) fn unload_done(&self, pool: &str) -> Result<()> {
        self.inner.lock().unwrap().unload_done(pool)?;
        self.cv.notify_all();
        Ok(())
    }

    pub(crate) fn lazy_usage(&self) -> Vec<(String, u32)> {
        self.inner.lock().unwrap().lazy_usage()
    }

    /// (resource, capacity, in use), for metrics.
    pub(crate) fn usage(&self) -> Vec<(String, f64, f64)> {
        self.inner.lock().unwrap().usage()
    }

    /// Frees what `token` held and wakes the waiters, several of which may now fit.
    pub(crate) fn release(&self, token: u64) {
        if self.gated && self.inner.lock().unwrap().release(token) {
            self.cv.notify_all();
        }
    }

    /// Blocks until a task is due or `timeout` passes. Waiters wake early for held tasks coming due
    /// and for released resources.
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
            let wake = d.next_due(now).map_or(deadline, |n| n.min(deadline));
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
            trace_ctx: None,
            loads: vec![],
            evicts: vec![],
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
        let mut d = Dispatcher::new(&qs(&["hi", "lo"]), true, &HashMap::new(), &Resources::default()).unwrap();
        let now = Instant::now();
        for (tok, q) in [(1, "lo"), (2, "hi"), (3, "lo"), (4, "hi")] {
            assert!(matches!(d.push(task(tok, "t", q), now), Admit::Held));
        }
        assert_eq!(tokens(&mut d, now), vec![2, 4, 1, 3]);
    }

    #[test]
    fn equal_priority_is_fifo_across_queues() {
        let mut d = Dispatcher::new(&qs(&["hi", "lo"]), false, &HashMap::new(), &Resources::default()).unwrap();
        let now = Instant::now();
        for (tok, q) in [(1, "lo"), (2, "hi"), (3, "lo")] {
            d.push(task(tok, "t", q), now);
        }
        assert_eq!(tokens(&mut d, now), vec![1, 2, 3]);
    }

    #[test]
    fn rate_limit_spaces_tasks_one_interval_apart() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 10.0)]), &Resources::default()).unwrap();
        let t0 = Instant::now();
        for tok in 1..=3 {
            assert!(matches!(d.push(task(tok, "slow", "q"), t0), Admit::Held));
        }
        assert_eq!(tokens(&mut d, t0), vec![1]);
        assert_eq!(d.next_due(t0), Some(t0 + 100 * MS));
        assert_eq!(tokens(&mut d, t0 + 99 * MS), Vec::<u64>::new());
        assert_eq!(tokens(&mut d, t0 + 100 * MS), vec![2]);
        assert_eq!(tokens(&mut d, t0 + 200 * MS), vec![3]);
        assert!(d.is_empty());
    }

    #[test]
    fn throttled_task_does_not_block_others() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 10.0)]), &Resources::default()).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        d.push(task(2, "slow", "q"), t0);
        d.push(task(3, "fast", "q"), t0);
        assert_eq!(tokens(&mut d, t0), vec![1, 3]);
    }

    #[test]
    fn throttled_higher_priority_task_lets_lower_priority_run() {
        let mut d = Dispatcher::new(&qs(&["hi", "lo"]), true, &limits(&[("slow", 10.0)]), &Resources::default()).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "hi"), t0);
        d.push(task(2, "slow", "hi"), t0);
        d.push(task(3, "fast", "lo"), t0);
        assert_eq!(tokens(&mut d, t0), vec![1, 3]);
        assert_eq!(tokens(&mut d, t0 + 100 * MS), vec![2]);
    }

    #[test]
    fn idle_bucket_refills() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 10.0)]), &Resources::default()).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        let later = t0 + Duration::from_secs(5);
        d.push(task(2, "slow", "q"), later);
        assert_eq!(tokens(&mut d, later), vec![1, 2]);
    }

    #[test]
    fn beyond_horizon_defers_spread_one_interval_apart() {
        // 0.5/s = one start every 2 s, past the 1 s horizon
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 0.5)]), &Resources::default()).unwrap();
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
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 0.5)]), &Resources::default()).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        assert!(matches!(d.push(task(2, "slow", "q"), t0), Admit::Defer { .. }));
        let back = t0 + Duration::from_secs(2);
        assert!(matches!(d.push(task(2, "slow", "q"), back), Admit::Held), "deferral reserved no slot");
        assert_eq!(tokens(&mut d, back), vec![1, 2]);
    }

    #[test]
    fn slot_exactly_at_the_horizon_is_held() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 1.0)]), &Resources::default()).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        assert!(matches!(d.push(task(2, "slow", "q"), t0), Admit::Held));
    }

    #[test]
    fn hold_ignores_the_rate_limit_and_waits_until_at() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 0.5)]), &Resources::default()).unwrap();
        let t0 = Instant::now();
        let at = t0 + Duration::from_secs(3);
        d.hold(task(1, "slow", "q"), at);
        assert_eq!(d.pop(t0).map(|t| t.token), None);
        assert_eq!(d.next_due(t0), Some(at));
        assert_eq!(tokens(&mut d, at), vec![1]);
    }

    #[test]
    fn drain_returns_everything_held() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 10.0)]), &Resources::default()).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        d.push(task(2, "slow", "q"), t0); // not due yet
        let mut got: Vec<u64> = d.drain().into_iter().map(|t| t.token).collect();
        got.sort();
        assert_eq!(got, vec![1, 2]);
        assert!(d.is_empty() && d.next_due(t0).is_none());
    }

    #[test]
    fn rejects_unusable_rate_limits() {
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e-300] {
            assert!(
                matches!(Dispatcher::new(&qs(&["q"]), false, &limits(&[("t", bad)]), &Resources::default()), Err(Error::Config(_))),
                "rate {bad} must be rejected"
            );
        }
    }

    fn empty_queue() -> ReadyQueue {
        ReadyQueue::new(Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &Resources::default()).unwrap())
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

    fn res(caps: &[(&str, f64)], reqs: &[(&str, &[(&str, f64)])]) -> Resources {
        Resources {
            capacity: caps.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
            requests: reqs.iter().map(|(t, r)| (t.to_string(), limits(r))).collect(),
            ..Default::default()
        }
    }

    fn with_res(r: &Resources) -> Dispatcher {
        Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), r).unwrap()
    }

    fn in_use(d: &Dispatcher, name: &str) -> f64 {
        d.usage().into_iter().find(|(n, ..)| n == name).unwrap().2
    }

    #[test]
    fn requests_that_fit_run_together_and_the_rest_wait_for_a_release() {
        let mut d = with_res(&res(&[("gpu", 1.0)], &[("g", &[("gpu", 0.5)])]));
        let t0 = Instant::now();
        for tok in 1..=3 {
            d.push(task(tok, "g", "q"), t0);
        }
        assert_eq!(tokens(&mut d, t0), vec![1, 2]);
        assert_eq!(d.len(), 1);
        d.release(1);
        assert_eq!(tokens(&mut d, t0), vec![3]);
    }

    #[test]
    fn rejects_unusable_resources() {
        let bad = |caps: &[(&str, f64)], reqs: &[(&str, &[(&str, f64)])]| {
            let r = res(caps, reqs);
            matches!(Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &r), Err(Error::Config(_)))
        };
        assert!(bad(&[("gpu", 1.0)], &[("g", &[("vram", 1.0)])]), "undeclared resource");
        assert!(bad(&[], &[("g", &[("gpu", 1.0)])]), "no resources at all");
        assert!(bad(&[("gpu", 1.0)], &[("g", &[("gpu", 1.5)])]), "over capacity");
        assert!(bad(&[("gpu", 1.0)], &[("g", &[("gpu", 0.0004)])]), "rounds to zero");
        for amount in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(bad(&[("gpu", 1.0)], &[("g", &[("gpu", amount)])]), "request {amount}");
        }
        for cap in [-1.0, f64::NAN, f64::INFINITY] {
            assert!(bad(&[("gpu", cap)], &[]), "capacity {cap}");
        }
        assert!(!bad(&[("gpu", 1.0)], &[("g", &[("gpu", 1.0)])]), "a request equal to capacity is fine");
        assert!(!bad(&[("gpu", 0.0)], &[]), "zero capacity is allowed when nothing requests it");
    }

    #[test]
    fn config_error_names_the_task_and_resource() {
        let r = res(&[("gpu", 1.0)], &[("train", &[("vram", 1.0)])]);
        let Err(Error::Config(m)) = Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &r) else { panic!() };
        assert!(m.contains("train") && m.contains("vram"), "{m}");
    }

    fn backfill_setup(t0: Instant) -> Dispatcher {
        let mut d = with_res(&res(
            &[("gpu", 1.0), ("cpu", 4.0)],
            &[("half", &[("gpu", 0.5)]), ("big", &[("gpu", 1.0)]), ("mid", &[("gpu", 0.3)]), ("small", &[("cpu", 0.5)])],
        ));
        d.push(task(1, "half", "q"), t0);
        assert_eq!(tokens(&mut d, t0), vec![1]);
        d.push(task(2, "big", "q"), t0); // the head: needs the whole gpu
        d.push(task(3, "mid", "q"), t0);
        d.push(task(4, "small", "q"), t0);
        d
    }

    #[test]
    fn backfill_before_the_reservation_lets_later_tasks_that_fit_start() {
        let t0 = Instant::now();
        let mut d = backfill_setup(t0);
        let just_before = t0 + RESERVE_AFTER - MS;
        assert_eq!(tokens(&mut d, just_before), vec![3, 4]);
        assert_eq!(d.len(), 1, "the head still waits");
    }

    #[test]
    fn reservation_blocks_later_tasks_that_share_a_resource_with_the_head() {
        let t0 = Instant::now();
        let mut d = backfill_setup(t0);
        let reserved = t0 + RESERVE_AFTER;
        assert_eq!(tokens(&mut d, reserved), vec![4], "cpu-only task still backfills, gpu task does not");
        assert_eq!(d.len(), 2);
        // The held-back gpu tasks start once the gpu frees up, head first.
        d.release(1);
        assert_eq!(tokens(&mut d, reserved), vec![2]);
        d.release(2);
        assert_eq!(tokens(&mut d, reserved), vec![3]);
    }

    #[test]
    fn priority_order_is_kept_among_tasks_that_fit() {
        let mut d = Dispatcher::new(
            &qs(&["hi", "lo"]), true, &HashMap::new(),
            &res(&[("gpu", 1.0)], &[("g", &[("gpu", 0.25)])]),
        ).unwrap();
        let t0 = Instant::now();
        for (tok, q) in [(1, "lo"), (2, "hi"), (3, "lo"), (4, "hi")] {
            d.push(task(tok, "g", q), t0);
        }
        assert_eq!(tokens(&mut d, t0), vec![2, 4, 1, 3]);
    }

    #[test]
    fn a_blocked_high_priority_task_lets_a_lower_one_that_fits_start() {
        let mut d = Dispatcher::new(
            &qs(&["hi", "lo"]), true, &HashMap::new(),
            &res(&[("gpu", 1.0)], &[("g", &[("gpu", 0.75)]), ("h", &[("gpu", 0.25)])]),
        ).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "g", "lo"), t0);
        assert_eq!(tokens(&mut d, t0), vec![1]);
        d.push(task(2, "g", "hi"), t0); // needs 0.75, only 0.25 left
        d.push(task(3, "h", "lo"), t0);
        assert_eq!(tokens(&mut d, t0), vec![3]);
    }

    #[test]
    fn tasks_without_a_request_ignore_a_full_ledger() {
        let mut d = with_res(&res(&[("gpu", 1.0)], &[("g", &[("gpu", 1.0)])]));
        let t0 = Instant::now();
        d.push(task(1, "g", "q"), t0);
        assert_eq!(tokens(&mut d, t0), vec![1]);
        d.push(task(2, "g", "q"), t0);
        d.push(task(3, "free", "q"), t0 + RESERVE_AFTER * 2);
        assert_eq!(tokens(&mut d, t0 + RESERVE_AFTER * 2), vec![3], "even past the reservation");
        d.release(3); // no request: nothing held
        assert_eq!(in_use(&d, "gpu"), 1.0);
    }

    #[test]
    fn release_of_an_unknown_or_already_released_token_is_a_no_op() {
        let mut d = with_res(&res(&[("gpu", 1.0)], &[("g", &[("gpu", 0.5)])]));
        let t0 = Instant::now();
        d.release(99);
        d.push(task(1, "g", "q"), t0);
        d.push(task(2, "g", "q"), t0);
        assert_eq!(tokens(&mut d, t0), vec![1, 2]);
        d.release(1);
        d.release(1);
        assert_eq!(in_use(&d, "gpu"), 0.5, "the second release must not return task 2's share");
    }

    #[test]
    fn repeated_acquire_and_release_does_not_drift() {
        let mut d = with_res(&res(&[("gpu", 1.0)], &[("g", &[("gpu", 0.1)])]));
        let t0 = Instant::now();
        let mut tok = 0;
        for _ in 0..1000 {
            tok += 1;
            d.push(task(tok, "g", "q"), t0);
            assert_eq!(tokens(&mut d, t0), vec![tok]);
            d.release(tok);
        }
        assert_eq!(in_use(&d, "gpu"), 0.0);
        // Ten of 0.1 fill 1.0 exactly; the eleventh waits.
        for i in 1..=11 {
            d.push(task(10_000 + i, "g", "q"), t0);
        }
        assert_eq!(tokens(&mut d, t0).len(), 10);
        assert_eq!(in_use(&d, "gpu"), 1.0);
    }

    #[test]
    fn next_due_ignores_tasks_that_are_due_but_blocked() {
        let mut d = with_res(&res(&[("gpu", 1.0)], &[("g", &[("gpu", 1.0)])]));
        let t0 = Instant::now();
        d.push(task(1, "g", "q"), t0);
        d.push(task(2, "g", "q"), t0);
        assert_eq!(tokens(&mut d, t0), vec![1]);
        assert_eq!(d.next_due(t0), None, "task 2 is due but waits for a release, not for the clock");
        d.hold(task(3, "g", "q"), t0 + 50 * MS);
        assert_eq!(d.next_due(t0), Some(t0 + 50 * MS));
    }

    #[test]
    fn usage_reports_capacity_and_in_use_by_resource() {
        let mut d = with_res(&res(&[("gpu", 1.0), ("vram_gb", 7.5)], &[("g", &[("gpu", 0.5), ("vram_gb", 1.2)])]));
        assert_eq!(d.usage(), vec![("gpu".to_string(), 1.0, 0.0), ("vram_gb".to_string(), 7.5, 0.0)]);
        d.push(task(1, "g", "q"), Instant::now());
        assert!(d.pop(Instant::now()).is_some());
        assert_eq!(d.usage(), vec![("gpu".to_string(), 1.0, 0.5), ("vram_gb".to_string(), 7.5, 1.2)]);
    }

    fn gpu_queue() -> ReadyQueue {
        ReadyQueue::new(with_res(&res(&[("gpu", 1.0)], &[("g", &[("gpu", 1.0)])])))
    }

    #[test]
    fn ready_queue_blocked_receiver_waits_for_release_not_the_clock() {
        let q = Arc::new(gpu_queue());
        q.push(task(1, "g", "q"), Instant::now());
        q.push(task(2, "g", "q"), Instant::now());
        assert_eq!(q.recv_timeout(Duration::from_secs(1)).map(|t| t.token), Some(1));

        let t = Instant::now();
        assert!(q.recv_timeout(200 * MS).is_none(), "task 2 is due but does not fit");
        let waited = t.elapsed();
        assert!(waited >= 190 * MS && waited < Duration::from_secs(1), "waited {waited:?}");

        let q2 = q.clone();
        let h = std::thread::spawn(move || q2.recv_timeout(Duration::from_secs(5)).map(|t| t.token));
        std::thread::sleep(50 * MS);
        let t = Instant::now();
        q.release(1);
        assert_eq!(h.join().unwrap(), Some(2));
        assert!(t.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn ready_queue_release_wakes_every_waiter_that_now_fits() {
        let q = Arc::new(ReadyQueue::new(with_res(&res(&[("gpu", 1.0)], &[("g", &[("gpu", 0.5)])]))));
        for tok in 1..=2 {
            q.push(task(tok, "g", "q"), Instant::now());
        }
        let first: Vec<u64> = (0..2).map(|_| q.recv_timeout(Duration::from_secs(1)).unwrap().token).collect();
        for tok in 3..=4 {
            q.push(task(tok, "g", "q"), Instant::now());
        }
        let waiters: Vec<_> = (0..2)
            .map(|_| {
                let q = q.clone();
                std::thread::spawn(move || q.recv_timeout(Duration::from_secs(5)).map(|t| t.token))
            })
            .collect();
        std::thread::sleep(50 * MS);
        let t = Instant::now();
        for tok in first {
            q.release(tok);
        }
        let mut got: Vec<_> = waiters.into_iter().map(|h| h.join().unwrap()).collect();
        got.sort();
        assert_eq!(got, vec![Some(3), Some(4)]);
        assert!(t.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn shrinking_below_in_use_blocks_admission_until_enough_is_released() {
        let mut d = with_res(&res(&[("pool:m", 3.0)], &[("g", &[("pool:m", 1.0)])]));
        let t0 = Instant::now();
        for tok in 1..=4 {
            d.push(task(tok, "g", "q"), t0);
        }
        assert_eq!(tokens(&mut d, t0), vec![1, 2, 3]);
        d.adjust_capacity("pool:m", -2.0).unwrap(); // capacity 1, three in use
        assert_eq!(in_use(&d, "pool:m"), 3.0);
        d.release(1);
        assert_eq!(tokens(&mut d, t0), Vec::<u64>::new(), "2 in use, capacity 1");
        d.release(2);
        assert_eq!(tokens(&mut d, t0), Vec::<u64>::new(), "1 in use, capacity 1");
        d.release(3);
        assert_eq!(tokens(&mut d, t0), vec![4]);
        assert_eq!(in_use(&d, "pool:m"), 1.0);
    }

    #[test]
    fn growing_lets_a_waiting_task_start() {
        let mut d = with_res(&res(&[("pool:m", 1.0)], &[("g", &[("pool:m", 1.0)])]));
        let t0 = Instant::now();
        d.push(task(1, "g", "q"), t0);
        d.push(task(2, "g", "q"), t0);
        assert_eq!(tokens(&mut d, t0), vec![1]);
        d.adjust_capacity("pool:m", 1.0).unwrap();
        assert_eq!(tokens(&mut d, t0), vec![2]);
        assert_eq!(d.usage(), vec![("pool:m".to_string(), 2.0, 2.0)]);
    }

    #[test]
    fn adjust_capacity_rejects_unusable_changes_and_leaves_capacity_alone() {
        let mut d = with_res(&res(&[("pool:m", 2.0)], &[]));
        for (name, delta) in [("nope", 1.0), ("pool:m", -2.5), ("pool:m", f64::NAN), ("pool:m", f64::INFINITY),
            ("pool:m", f64::NEG_INFINITY), ("pool:m", 1e300)] {
            assert!(matches!(d.adjust_capacity(name, delta), Err(Error::Config(_))), "{name} {delta}");
        }
        assert_eq!(d.usage(), vec![("pool:m".to_string(), 2.0, 0.0)]);
        d.adjust_capacity("pool:m", -2.0).unwrap(); // down to exactly zero is fine
        assert_eq!(d.usage(), vec![("pool:m".to_string(), 0.0, 0.0)]);
    }

    #[test]
    fn adjust_capacity_works_in_milli_units() {
        let mut d = with_res(&res(&[("vram_gb", 8.0)], &[]));
        d.adjust_capacity("vram_gb", -5.8).unwrap();
        d.adjust_capacity("vram_gb", 0.1).unwrap();
        assert_eq!(d.usage(), vec![("vram_gb".to_string(), 2.3, 0.0)]);
    }

    #[test]
    fn ready_queue_growth_wakes_a_blocked_receiver() {
        let q = Arc::new(gpu_queue());
        q.push(task(1, "g", "q"), Instant::now());
        q.push(task(2, "g", "q"), Instant::now());
        assert_eq!(q.recv_timeout(Duration::from_secs(1)).map(|t| t.token), Some(1));
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let q2 = q.clone();
        let h = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            q2.recv_timeout(Duration::from_secs(10)).map(|t| t.token)
        });
        started_rx.recv().unwrap();
        // The receiver may or may not be parked yet; growth must wake it either way.
        q.adjust_capacity("gpu", 1.0).unwrap();
        assert_eq!(h.join().unwrap(), Some(2));
    }

    #[test]
    fn ready_queue_adjust_capacity_reports_errors() {
        let q = gpu_queue();
        assert!(matches!(q.adjust_capacity("gpu", -2.0), Err(Error::Config(_))));
        assert_eq!(q.usage(), vec![("gpu".to_string(), 1.0, 0.0)]);
    }
    // ---- lazy pools ----

    /// (name, what one instance claims, instances loaded at start)
    type PoolSpec<'a> = (&'a str, &'a [(&'a str, f64)], u32);

    fn lazy_res(caps: &[(&str, f64)], reqs: &[(&str, &[(&str, f64)])], pools: &[PoolSpec]) -> Resources {
        Resources {
            lazy: pools.iter()
                .map(|(n, c, loaded)| (n.to_string(), LazyPool { claim: limits(c), loaded: *loaded }))
                .collect(),
            ..res(caps, reqs)
        }
    }

    fn lazy(caps: &[(&str, f64)], reqs: &[(&str, &[(&str, f64)])], pools: &[PoolSpec]) -> Dispatcher {
        Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &lazy_res(caps, reqs, pools)).unwrap()
    }

    fn loaded(d: &Dispatcher) -> Vec<(String, u32)> {
        d.lazy_usage()
    }

    /// vram 7.5 (one model at a time), gemma 7.4 and tts 7.2, one instance each at most.
    fn two_models(gemma_loaded: u32, tts_loaded: u32) -> Dispatcher {
        lazy(
            &[("vram_gb", 7.5), ("pool:gemma", 1.0), ("pool:tts", 1.0)],
            &[("g", &[("pool:gemma", 1.0)]), ("t", &[("pool:tts", 1.0)])],
            &[("gemma", &[("vram_gb", 7.4)], gemma_loaded), ("tts", &[("vram_gb", 7.2)], tts_loaded)],
        )
    }

    #[test]
    fn a_resident_task_loads_and_evicts_nothing() {
        let mut d = two_models(1, 0);
        let t0 = Instant::now();
        d.push(task(1, "g", "q"), t0);
        let t = d.pop(t0).unwrap();
        assert!(t.loads.is_empty() && t.evicts.is_empty(), "{:?} {:?}", t.loads, t.evicts);
        assert_eq!(in_use(&d, "vram_gb"), 7.4, "the preloaded instance is counted once");
        assert_eq!(in_use(&d, "pool:gemma"), 1.0);
        assert_eq!(loaded(&d), vec![("gemma".to_string(), 1), ("tts".to_string(), 0)]);
    }

    #[test]
    fn a_non_resident_task_loads_its_model() {
        let mut d = two_models(0, 0);
        let t0 = Instant::now();
        d.push(task(1, "g", "q"), t0);
        let t = d.pop(t0).unwrap();
        assert_eq!(t.loads, vec!["gemma".to_string()]);
        assert!(t.evicts.is_empty());
        assert_eq!(in_use(&d, "vram_gb"), 7.4);
        assert_eq!(loaded(&d), vec![("gemma".to_string(), 1), ("tts".to_string(), 0)]);
        d.release(1);
        assert_eq!(in_use(&d, "vram_gb"), 7.4, "the model stays loaded after its task ends");
    }

    #[test]
    fn a_task_that_needs_the_room_evicts_an_idle_model() {
        let mut d = two_models(1, 0);
        let t0 = Instant::now();
        d.push(task(1, "t", "q"), t0);
        let t = d.pop(t0).unwrap();
        assert_eq!(t.evicts, vec!["gemma".to_string()]);
        assert_eq!(t.loads, vec!["tts".to_string()]);
        assert_eq!(in_use(&d, "vram_gb"), 7.2);
        assert_eq!(loaded(&d), vec![("gemma".to_string(), 0), ("tts".to_string(), 1)]);
    }

    #[test]
    fn a_busy_instance_is_never_a_victim() {
        let mut d = two_models(1, 0);
        let t0 = Instant::now();
        d.push(task(1, "g", "q"), t0);
        assert!(d.pop(t0).unwrap().evicts.is_empty());
        d.push(task(2, "t", "q"), t0);
        assert!(d.pop(t0).is_none(), "gemma is running; tts must wait");
        assert_eq!(loaded(&d)[0], ("gemma".to_string(), 1));
        d.release(1);
        let t = d.pop(t0).unwrap();
        assert_eq!((t.loads, t.evicts), (vec!["tts".to_string()], vec!["gemma".to_string()]));
    }

    #[test]
    fn the_least_recently_popped_idle_model_is_evicted_first() {
        let mk = |loaded| lazy(
            &[("vram_gb", 6.0), ("pool:a", 1.0), ("pool:b", 1.0), ("pool:c", 1.0), ("pool:d", 1.0)],
            &[("ta", &[("pool:a", 1.0)]), ("tb", &[("pool:b", 1.0)]), ("tc", &[("pool:c", 1.0)]),
              ("td", &[("pool:d", 1.0)])],
            &[("a", &[("vram_gb", 2.0)], loaded), ("b", &[("vram_gb", 2.0)], loaded),
              ("c", &[("vram_gb", 2.0)], loaded), ("d", &[("vram_gb", 2.0)], 0)],
        );
        let mut d = mk(1);
        let t0 = Instant::now();
        // Use b, then a, then c: a is not the oldest by name, b is by use.
        for (tok, name, at) in [(1, "tb", 0), (2, "ta", 1), (3, "tc", 2)] {
            d.push(task(tok, name, "q"), t0 + at * MS);
            assert!(d.pop(t0 + at * MS).is_some());
            d.release(tok);
        }
        d.push(task(4, "td", "q"), t0 + 3 * MS);
        let t = d.pop(t0 + 3 * MS).unwrap();
        assert_eq!((t.loads, t.evicts), (vec!["d".to_string()], vec!["b".to_string()]));
        assert_eq!(loaded(&d), vec![("a".into(), 1), ("b".into(), 0), ("c".into(), 1), ("d".into(), 1)]);
    }

    #[test]
    fn a_model_that_frees_nothing_the_task_lacks_is_not_evicted() {
        // "x" is the oldest idle model but only holds cpu; the shortfall is vram.
        let mut d = lazy(
            &[("vram_gb", 4.0), ("cpu", 4.0), ("pool:a", 1.0), ("pool:x", 1.0), ("pool:d", 1.0)],
            &[("td", &[("pool:d", 1.0)])],
            &[("a", &[("vram_gb", 2.0)], 1), ("x", &[("cpu", 4.0)], 1), ("d", &[("vram_gb", 3.0)], 0)],
        );
        let t0 = Instant::now();
        d.push(task(1, "td", "q"), t0);
        let t = d.pop(t0).unwrap();
        assert_eq!((t.loads, t.evicts), (vec!["d".to_string()], vec!["a".to_string()]));
        assert_eq!(in_use(&d, "cpu"), 4.0);
    }

    #[test]
    fn a_plain_task_evicts_an_idle_model_only_when_it_must() {
        let reqs: &[(&str, &[(&str, f64)])] = &[("render", &[("vram_gb", 0.6)])];
        let mut tight = lazy(&[("vram_gb", 7.5), ("pool:gemma", 1.0)], reqs, &[("gemma", &[("vram_gb", 7.4)], 1)]);
        let t0 = Instant::now();
        tight.push(task(1, "render", "q"), t0);
        let t = tight.pop(t0).unwrap();
        assert_eq!((t.loads, t.evicts), (vec![], vec!["gemma".to_string()]));
        assert_eq!(in_use(&tight, "vram_gb"), 0.6);

        let mut roomy = lazy(&[("vram_gb", 8.0), ("pool:gemma", 1.0)], reqs, &[("gemma", &[("vram_gb", 7.4)], 1)]);
        roomy.push(task(1, "render", "q"), t0);
        let t = roomy.pop(t0).unwrap();
        assert!(t.loads.is_empty() && t.evicts.is_empty());
        assert_eq!(loaded(&roomy), vec![("gemma".to_string(), 1)]);
    }

    #[test]
    fn a_pool_at_capacity_does_not_load_even_with_room() {
        let mut d = lazy(
            &[("vram_gb", 20.0), ("pool:gemma", 1.0)],
            &[("g", &[("pool:gemma", 1.0)])],
            &[("gemma", &[("vram_gb", 7.4)], 0)],
        );
        let t0 = Instant::now();
        d.push(task(1, "g", "q"), t0);
        d.push(task(2, "g", "q"), t0);
        assert_eq!(d.pop(t0).unwrap().loads, vec!["gemma".to_string()]);
        assert!(d.pop(t0).is_none());
        assert_eq!(in_use(&d, "vram_gb"), 7.4);
    }

    #[test]
    fn a_pool_of_two_loads_a_second_instance_while_the_first_is_busy() {
        let mut d = lazy(
            &[("vram_gb", 20.0), ("pool:gemma", 2.0)],
            &[("g", &[("pool:gemma", 1.0)])],
            &[("gemma", &[("vram_gb", 7.4)], 0)],
        );
        let t0 = Instant::now();
        for tok in 1..=3 {
            d.push(task(tok, "g", "q"), t0);
        }
        assert_eq!(d.pop(t0).unwrap().loads, vec!["gemma".to_string()]);
        assert_eq!(d.pop(t0).unwrap().loads, vec!["gemma".to_string()]);
        assert!(d.pop(t0).is_none(), "both instances are busy");
        assert_eq!(in_use(&d, "vram_gb"), 14.8);
        d.release(1);
        let t = d.pop(t0).unwrap();
        assert!(t.loads.is_empty() && t.evicts.is_empty(), "the idle instance is reused");
        assert_eq!(loaded(&d), vec![("gemma".to_string(), 2)]);
    }

    #[test]
    fn unload_begin_keeps_the_memory_used_and_unload_done_frees_it() {
        let mut d = two_models(1, 0);
        d.unload_begin("gemma").unwrap();
        assert_eq!(loaded(&d)[0], ("gemma".to_string(), 0));
        assert_eq!(in_use(&d, "vram_gb"), 7.4, "the instance is still in memory");
        let t0 = Instant::now();
        d.push(task(1, "t", "q"), t0);
        assert!(d.pop(t0).is_none(), "nothing to evict and the memory is not free yet");
        d.unload_done("gemma").unwrap();
        assert_eq!(in_use(&d, "vram_gb"), 0.0);
        let t = d.pop(t0).unwrap();
        assert_eq!((t.loads, t.evicts), (vec!["tts".to_string()], vec![]));
    }

    #[test]
    fn unload_calls_reject_what_cannot_be_unloaded() {
        let mut d = two_models(1, 0);
        let bad = |r: Result<()>| matches!(r, Err(Error::Config(_)));
        assert!(bad(d.unload_begin("nope")));
        assert!(bad(d.unload_done("nope")));
        assert!(bad(d.unload_begin("tts")), "nothing loaded");
        assert!(bad(d.unload_done("gemma")), "no unload in progress");
        d.unload_begin("gemma").unwrap();
        assert!(bad(d.unload_begin("gemma")), "only one instance was loaded");
        d.unload_done("gemma").unwrap();
        assert!(bad(d.unload_done("gemma")), "freed once");
        assert_eq!(in_use(&d, "vram_gb"), 0.0);
        let mut eager = with_res(&res(&[("gpu", 1.0), ("pool:m", 1.0)], &[]));
        assert!(bad(eager.unload_begin("m")), "not a lazy pool");
        assert!(bad(eager.unload_done("m")));
    }

    #[test]
    fn ready_queue_unload_done_wakes_a_blocked_receiver() {
        let q = Arc::new(ReadyQueue::new(two_models(1, 0)));
        q.push(task(1, "g", "q"), Instant::now());
        assert_eq!(q.recv_timeout(Duration::from_secs(1)).map(|t| t.token), Some(1));
        q.unload_begin("gemma").unwrap(); // the task is discarding its instance
        q.release(1);
        q.push(task(2, "t", "q"), Instant::now());
        assert!(q.recv_timeout(100 * MS).is_none(), "the discarded instance still holds the memory");

        let q2 = q.clone();
        let h = std::thread::spawn(move || q2.recv_timeout(Duration::from_secs(5)).map(|t| t.token));
        std::thread::sleep(50 * MS);
        let t = Instant::now();
        q.unload_done("gemma").unwrap();
        assert_eq!(h.join().unwrap(), Some(2));
        assert!(t.elapsed() < Duration::from_secs(1));
        assert_eq!(q.lazy_usage(), vec![("gemma".to_string(), 0), ("tts".to_string(), 1)]);
    }

    #[test]
    fn ready_queue_release_wakes_waiters_in_a_lazy_worker() {
        // The ready queue of a lazy worker takes the lock on release.
        let q = Arc::new(ReadyQueue::new(two_models(0, 0)));
        q.push(task(1, "g", "q"), Instant::now());
        q.push(task(2, "g", "q"), Instant::now());
        assert_eq!(q.recv_timeout(Duration::from_secs(1)).map(|t| t.token), Some(1));
        let q2 = q.clone();
        let h = std::thread::spawn(move || q2.recv_timeout(Duration::from_secs(5)).map(|t| t.token));
        std::thread::sleep(50 * MS);
        q.release(1);
        assert_eq!(h.join().unwrap(), Some(2));
    }

    /// vram 16, two gemma instances (14.8) and a tts needing 9.0: tts must evict both.
    fn reservation_setup(t0: Instant) -> Dispatcher {
        let mut d = lazy(
            &[("vram_gb", 16.0), ("pool:gemma", 2.0), ("pool:tts", 1.0)],
            &[("g", &[("pool:gemma", 1.0)]), ("t", &[("pool:tts", 1.0)])],
            &[("gemma", &[("vram_gb", 7.4)], 2), ("tts", &[("vram_gb", 9.0)], 0)],
        );
        d.push(task(1, "g", "q"), t0);
        assert!(d.pop(t0).is_some()); // one gemma instance is running, the other idle
        d.push(task(2, "t", "q"), t0); // the head: needs both gemma instances gone
        d.push(task(3, "g", "q"), t0);
        d
    }

    #[test]
    fn before_the_reservation_gemma_tasks_keep_backfilling() {
        let t0 = Instant::now();
        let mut d = reservation_setup(t0);
        assert_eq!(d.pop(t0 + RESERVE_AFTER - MS).map(|t| t.token), Some(3));
        assert!(d.pop(t0 + RESERVE_AFTER - MS).is_none(), "the head still waits");
    }

    #[test]
    fn the_reserved_head_stops_tasks_that_would_keep_a_victim_busy() {
        let t0 = Instant::now();
        let mut d = reservation_setup(t0);
        let reserved = t0 + RESERVE_AFTER;
        assert!(d.pop(reserved).is_none(), "gemma task 3 would keep the idle instance busy");
        d.release(1);
        let t = d.pop(reserved).unwrap();
        assert_eq!(t.token, 2);
        assert_eq!((t.loads, t.evicts), (vec!["tts".to_string()], vec!["gemma".to_string(), "gemma".to_string()]));
        assert_eq!(in_use(&d, "vram_gb"), 9.0);
    }

    #[test]
    fn a_head_waiting_for_its_own_busy_pool_does_not_hold_back_other_models() {
        // Room for both models: gemma's single instance is busy, so the gemma head waits for pool:gemma only.
        let mut d = lazy(
            &[("vram_gb", 16.0), ("pool:gemma", 1.0), ("pool:tts", 1.0)],
            &[("g", &[("pool:gemma", 1.0)]), ("t", &[("pool:tts", 1.0)])],
            &[("gemma", &[("vram_gb", 7.4)], 1), ("tts", &[("vram_gb", 7.2)], 1)],
        );
        let t0 = Instant::now();
        d.push(task(1, "g", "q"), t0);
        assert!(d.pop(t0).is_some());
        d.push(task(2, "g", "q"), t0);
        d.push(task(3, "t", "q"), t0);
        let reserved = t0 + RESERVE_AFTER;
        let t = d.pop(reserved).unwrap();
        assert_eq!(t.token, 3, "tts shares nothing the gemma head waits for");
        assert!(t.loads.is_empty() && t.evicts.is_empty());
    }

    #[test]
    fn preloaded_instances_count_in_the_ledger_from_the_start() {
        let d = two_models(1, 0);
        assert_eq!(in_use(&d, "vram_gb"), 7.4);
        assert_eq!(in_use(&d, "pool:gemma"), 0.0, "idle: no task holds the pool");
        assert_eq!(loaded(&d), vec![("gemma".to_string(), 1), ("tts".to_string(), 0)]);
    }

    #[test]
    fn rejects_unusable_lazy_pools() {
        let bad = |r: Resources| matches!(Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &r), Err(Error::Config(_)));
        let caps = [("vram_gb", 7.5), ("pool:gemma", 1.0), ("pool:tts", 1.0)];
        let gemma: &[(&str, f64)] = &[("vram_gb", 7.4)];
        let tts: &[(&str, f64)] = &[("vram_gb", 7.2)];
        assert!(bad(lazy_res(&caps, &[], &[("gemma", gemma, 1), ("tts", tts, 1)])), "preload does not fit together");
        assert!(bad(lazy_res(&caps, &[], &[("gemma", gemma, 2)])), "more loaded than pool:gemma allows");
        assert!(bad(lazy_res(&[("vram_gb", 7.5)], &[], &[("gemma", gemma, 0)])), "pool:gemma is not declared");
        assert!(bad(lazy_res(&caps, &[], &[("gemma", &[("cpu", 1.0)], 0)])), "undeclared claim resource");
        assert!(bad(lazy_res(&caps, &[], &[("gemma", &[("vram_gb", 0.0)], 0)])), "zero claim");
        assert!(bad(lazy_res(&caps, &[], &[("gemma", &[("vram_gb", 8.0)], 1)])), "claim over capacity");
        assert!(!bad(lazy_res(&caps, &[], &[("gemma", gemma, 1), ("tts", tts, 0)])));
    }

    #[test]
    fn a_non_lazy_worker_hands_out_empty_plans() {
        let mut d = with_res(&res(&[("gpu", 1.0)], &[("g", &[("gpu", 0.5)])]));
        let t0 = Instant::now();
        d.push(task(1, "g", "q"), t0);
        let t = d.pop(t0).unwrap();
        assert!(t.loads.is_empty() && t.evicts.is_empty());
        assert!(d.lazy_usage().is_empty());
    }
}
