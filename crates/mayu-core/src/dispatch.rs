//! Orders claimed tasks for the worker threads: queue priority first, then per-task rate limits.
use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::envelope::now_ms;
use crate::error::{Error, Result};
use crate::runtime::ReadyTask;

/// A throttled task whose slot is further out than this goes back to the broker instead of holding
/// a prefetch slot, so a rate-limited backlog cannot starve other tasks.
pub const HOLD_HORIZON: Duration = Duration::from_secs(1);

/// Once the head (the first due task that doesn't fit) has waited this long, later tasks may start
/// only if they request none of the resources the head requests.
pub const RESERVE_AFTER: Duration = Duration::from_secs(5);

/// How much earlier than it arrived a resident task counts under `Schedule::Throughput`: it may run ahead of
/// non-resident work that arrived up to this long before it, which bounds how long that work can wait.
pub const THROUGHPUT_BONUS: Duration = Duration::from_secs(60);

/// How much earlier than its workflow started a resident task counts under `Schedule::Oldest`, about one
/// model load.
pub const OLDEST_BONUS: Duration = Duration::from_secs(10);

/// After a failed lookahead load of a pool, no new one is granted for it for this long.
pub const LOOKAHEAD_BACKOFF: Duration = Duration::from_secs(60);

/// A workflow is never counted as older than this, so a bogus id cannot rank a task ahead of everything.
const MAX_ORIGIN_AGE: Duration = Duration::from_secs(24 * 3600);

/// Ledger units per whole unit: amounts are integer milli-units so repeated acquire/release never drifts.
const MILLI: f64 = 1000.0;
/// Larger amounts could overflow the ledger sums; far beyond any real resource.
const MAX_MILLI: f64 = 1e15;

/// How a worker with lazy pools orders the tasks it could start. Strict queue priority always comes first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Schedule {
    /// Arrival order. The only choice without lazy pools.
    #[default]
    Fifo,
    /// Tasks whose model is loaded jump ahead of work that arrived up to `THROUGHPUT_BONUS` before them.
    Throughput,
    /// Oldest workflow first (other tasks by arrival); a loaded model is worth `OLDEST_BONUS`.
    Oldest,
}

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
    /// Keep an idle instance loaded up to this long after its last use while its workflows still have work
    /// for it. None: it may be evicted as soon as it is idle.
    pub linger: Option<Duration>,
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
    /// Instances loaded or being loaded (idle, busy or loading).
    loaded: u32,
    /// Instances a lookahead is building: counted in `loaded` and in the ledger, but not usable yet.
    loading: u32,
    /// Unloads begun and not done: the memory is still in `in_use`.
    unloading: u32,
    /// When a task for this pool was last popped; None until then.
    last_used: Option<Instant>,
    /// When the pool last had no task running on it (its last task was released, or a lookahead instance
    /// was built); None until then. `linger` counts from here, not from `last_used`, so a long task doesn't
    /// use up the idle time.
    idle_since: Option<Instant>,
    /// Keep an idle instance this long after `idle_since` while `demand > 0`.
    linger: Option<Duration>,
    /// Tasks of this pool still to come in the workflows this worker has seen, as last reported.
    demand: u32,
    /// When a lookahead load of this pool last failed.
    lookahead_failed: Option<Instant>,
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
        self.pools[p].loaded.saturating_sub(busy).saturating_sub(self.pools[p].loading)
    }

    /// Whether `p`'s idle instances are off limits as victims at `now`: its workflows still want it.
    fn lingering(&self, p: usize, now: Instant) -> bool {
        let pool = &self.pools[p];
        match (pool.linger, pool.idle_since) {
            (Some(linger), Some(since)) => pool.demand > 0 && now < since + linger,
            _ => false,
        }
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
    /// used pool first, and never one that is lingering) for the room; the plan says how. None if it cannot, even
    /// with every idle instance gone.
    fn plan(&self, claim: &Claim, in_use: &[u64], capacity: &[u64], now: Instant) -> Option<Plan> {
        let loads = self.loads_for(claim, in_use, capacity);
        // An instance that is still loading fits the ledger but cannot run the task, and the pool may allow
        // no other: wait for it.
        for &(r, m) in claim {
            if let Some(p) = self.of_res[r] {
                let coming = loads.iter().filter(|&&l| l == p).count() as u32;
                if self.idle(p, in_use) + coming < Self::instances(m) {
                    return None;
                }
            }
        }
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
        let mut order: Vec<usize> = (0..self.pools.len())
            .filter(|&p| avail[p] > 0 && !self.lingering(p, now))
            .collect();
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
    /// When the task's workflow started (`at` for other tasks, and unless the schedule is `Oldest`).
    origin: Instant,
    task: ReadyTask,
}

/// A due task with its rank (level, age with bonus - older first -, arrival), claim and plan, when scoring.
struct Ranked<'a> {
    key: (u32, Reverse<Duration>, u64),
    i: usize,
    claim: Option<&'a Claim>,
    plan: Option<Plan>,
}

/// The instant a workflow that started at wall-clock `started_ms` corresponds to, for a task held at `at`.
/// A start in the future (clock skew) or one before `at` can be represented only roughly: both clamp.
fn origin_of(at: Instant, started_ms: u64, wall_ms: u64) -> Instant {
    let age = Duration::from_millis(wall_ms.saturating_sub(started_ms)).min(MAX_ORIGIN_AGE);
    // An Instant close to the clock's epoch (just after boot) cannot go back that far.
    at.checked_sub(age).unwrap_or(at)
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
    /// `Fifo` exactly when `lazy` has no pools.
    schedule: Schedule,
}

impl Dispatcher {
    /// With `strict`, earlier `queues` run first; otherwise every queue ranks equally (FIFO).
    /// `rate_limits` maps task name to starts per second, for this worker. `schedule` orders the tasks of
    /// a worker with lazy pools within a priority; it is `Fifo` exactly when there are none.
    pub fn new(
        queues: &[String],
        strict: bool,
        rate_limits: &HashMap<String, f64>,
        resources: &Resources,
        schedule: Schedule,
    ) -> Result<Self> {
        match (resources.lazy.is_empty(), schedule) {
            (false, Schedule::Fifo) => {
                return Err(Error::Config("lazy pools need a schedule, 'throughput' or 'oldest'".into()));
            }
            (true, Schedule::Throughput | Schedule::Oldest) => {
                return Err(Error::Config("a schedule other than fifo needs lazy pools".into()));
            }
            _ => {}
        }
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
            if pool.linger == Some(Duration::ZERO) {
                return Err(Error::Config(format!("lazy pool {name:?} has a linger of zero; use None for no linger")));
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
                name: name.clone(), res, claim, loaded: pool.loaded, loading: 0, unloading: 0, last_used: None,
                idle_since: None, linger: pool.linger, demand: 0, lookahead_failed: None,
            });
        }
        Ok(Dispatcher {
            levels, throttles, held: Vec::new(), seq: 0,
            resource_names, capacity, in_use, requests, held_by: HashMap::new(), lazy, schedule,
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
        let origin = match task.workflow_started_ms {
            Some(started) if self.schedule == Schedule::Oldest => origin_of(at, started, now_ms()),
            _ => at,
        };
        self.held.push(Held { level, seq: self.seq, at, origin, task });
    }

    /// The highest-priority task that is due and fits the free resources, oldest first within a priority.
    pub fn pop(&mut self, now: Instant) -> Option<ReadyTask> {
        if self.requests.is_empty() && self.schedule == Schedule::Fifo {
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
    /// walks due tasks in priority order, and within a priority by the schedule. The first one that doesn't
    /// fit is the head; later ones may backfill while the head is young, and afterwards only if they request
    /// nothing the head requests, so a big request cannot be starved by a stream of small ones.
    fn pop_fitting(&self, now: Instant) -> Option<(usize, Plan)> {
        let mut due: Vec<usize> = (0..self.held.len()).filter(|&i| self.held[i].at <= now).collect();
        if self.schedule == Schedule::Fifo {
            due.sort_unstable_by_key(|&i| (self.held[i].level, self.held[i].seq));
            // Plans are made as the walk reaches each task, so it stops at the first that can start.
            return self.walk(now, due.into_iter().map(|i| {
                let claim = self.requests.get(&self.held[i].task.task);
                let plan = match claim {
                    None => Some(Plan::default()),
                    Some(c) => self.fits(c).then(Plan::default),
                };
                (i, claim, plan)
            }));
        }
        // Scoring: whether a task is resident (its plan is empty) is part of its rank, so every due task is
        // planned up front. Age counts back from `now`, which cannot underflow an `Instant` the way
        // subtracting the bonus from a task's instant could.
        let (basis, bonus): (fn(&Held) -> Instant, Duration) = match self.schedule {
            Schedule::Oldest => (|h| h.origin, OLDEST_BONUS),
            _ => (|h| h.at, THROUGHPUT_BONUS),
        };
        let mut ranked: Vec<Ranked> = due.into_iter()
            .map(|i| {
                let h = &self.held[i];
                let claim = self.requests.get(&h.task.task);
                let plan = match claim {
                    None => Some(Plan::default()),
                    Some(c) => self.lazy.plan(c, &self.in_use, &self.capacity, now),
                };
                let resident = plan.as_ref().is_some_and(|p| p.loads.is_empty() && p.evicts.is_empty());
                let mut age = now.saturating_duration_since(basis(h));
                if resident {
                    age = age.saturating_add(bonus);
                }
                Ranked { key: (h.level, Reverse(age), h.seq), i, claim, plan }
            })
            .collect();
        ranked.sort_unstable_by_key(|r| r.key);
        self.walk(now, ranked.into_iter().map(|r| (r.i, r.claim, r.plan)))
    }

    /// The first of `candidates` (task index, its claim, its plan if it can start) that may start now.
    fn walk<'a>(
        &'a self,
        now: Instant,
        candidates: impl Iterator<Item = (usize, Option<&'a Claim>, Option<Plan>)>,
    ) -> Option<(usize, Plan)> {
        let mut head: Option<(&Claim, Instant)> = None;
        // Lazy workers: what else the head wants (see `Lazy::wanted_beyond`).
        let mut head_beyond: Vec<usize> = Vec::new();
        for (i, claim, plan) in candidates {
            let h = &self.held[i];
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
        if self.lazy.pools.is_empty() {
            // Nothing lingers, so the release path stays free of a clock read.
            let Some(claim) = self.held_by.remove(&token) else { return false };
            for (r, m) in claim {
                self.in_use[r] -= m;
            }
            return true;
        }
        self.release_at(token, Instant::now())
    }

    /// `release` at `now`, which is when the lazy pools the task held start to count as idle (for linger).
    pub fn release_at(&mut self, token: u64, now: Instant) -> bool {
        let Some(claim) = self.held_by.remove(&token) else { return false };
        for (r, m) in claim {
            self.in_use[r] -= m;
            if let Some(p) = self.lazy.of_res[r] {
                self.lazy.pools[p].idle_since = Some(now);
            }
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

    /// Sets the demand of the lazy pools named (tasks their workflows still have pending); other pools keep
    /// theirs and unknown names are ignored. Returns whether a pool's demand dropped to zero, when a lingering
    /// idle instance may become a victim.
    pub fn set_demand(&mut self, demand: &[(String, u32)]) -> bool {
        let mut dropped = false;
        for (name, n) in demand {
            if let Some(p) = self.lazy.pools.iter_mut().find(|p| &p.name == name) {
                dropped |= p.demand > 0 && *n == 0;
                p.demand = *n;
            }
        }
        dropped
    }

    /// (pool, demand) for each lazy pool, for metrics.
    pub fn demand(&self) -> Vec<(String, u32)> {
        self.lazy.pools.iter().map(|p| (p.name.clone(), p.demand)).collect()
    }

    /// Grants a speculative load: the first lazy pool (by name) whose workflows want it, that has no idle or
    /// loading instance, may have another and has room for one without evicting anything. Only a worker with
    /// nothing due does this, and not again for a pool soon after a failed load. The caller builds the instance
    /// and calls `lookahead_done`; until then the ledger counts it as loaded but not idle.
    pub fn lookahead_begin(&mut self, now: Instant) -> Option<String> {
        if self.held.iter().any(|h| h.at <= now) {
            return None;
        }
        let p = (0..self.lazy.pools.len()).find(|&p| {
            let pool = &self.lazy.pools[p];
            pool.demand > 0
                && pool.loading == 0
                && self.lazy.idle(p, &self.in_use) == 0
                && pool.loaded < (self.capacity[pool.res] / MILLI as u64) as u32
                && self.fits(&pool.claim)
                && pool.lookahead_failed.is_none_or(|f| now.saturating_duration_since(f) >= LOOKAHEAD_BACKOFF)
        })?;
        let pool = &mut self.lazy.pools[p];
        pool.loaded += 1;
        pool.loading += 1;
        // So it isn't the least recently used pool, and the next victim, as soon as it exists.
        pool.last_used = Some(now);
        for &(r, m) in &pool.claim {
            self.in_use[r] += m;
        }
        Some(pool.name.clone())
    }

    /// The instance from `lookahead_begin` is built (`ok`) and idle, or failed and gives its room back.
    pub fn lookahead_done(&mut self, pool: &str, ok: bool, now: Instant) -> Result<()> {
        let p = self.lazy.pool_mut(pool)?;
        if p.loading == 0 {
            return Err(Error::Config(format!("pool {pool:?} has no lookahead load in progress")));
        }
        p.loading -= 1;
        if ok {
            p.idle_since = Some(now);
        } else {
            p.loaded = p.loaded.saturating_sub(1);
            p.lookahead_failed = Some(now);
            for &(r, m) in &p.claim {
                self.in_use[r] = self.in_use[r].saturating_sub(m);
            }
        }
        Ok(())
    }

    /// How many tasks due at `now` request `resource` and don't fit now. 0 for a resource not declared.
    pub fn blocked(&self, resource: &str, now: Instant) -> usize {
        let Ok(i) = self.resource_names.binary_search_by(|n| n.as_str().cmp(resource)) else { return 0 };
        self.held.iter()
            .filter(|h| h.at <= now)
            .filter_map(|h| self.requests.get(&h.task.task))
            .filter(|c| c.iter().any(|&(r, _)| r == i) && !self.fits(c))
            .count()
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

    /// The earliest time after `now` that a held task comes due, or that an idle instance stops lingering
    /// (that may let a waiter evict it). A task already due but waiting for resources is not counted:
    /// `release` wakes it, and counting it would make waiters spin.
    pub fn next_due(&self, now: Instant) -> Option<Instant> {
        let lingers = self.lazy.pools.iter().enumerate()
            .filter(|(p, pool)| pool.demand > 0 && self.lazy.idle(*p, &self.in_use) > 0)
            .filter_map(|(_, pool)| Some(pool.idle_since? + pool.linger?));
        self.held.iter().map(|h| h.at).chain(lingers).filter(|&at| at > now).min()
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

    /// See `Dispatcher::set_demand`. A pool whose demand ended may now be evicted, which waiters may need.
    #[allow(dead_code)] // the runtime's lookahead loop calls it
    pub(crate) fn set_demand(&self, demand: &[(String, u32)]) {
        if self.inner.lock().unwrap().set_demand(demand) {
            self.cv.notify_all();
        }
    }

    #[allow(dead_code)] // the runtime's metrics call it
    pub(crate) fn demand(&self) -> Vec<(String, u32)> {
        self.inner.lock().unwrap().demand()
    }

    /// See `Dispatcher::lookahead_begin`.
    #[allow(dead_code)] // the runtime's lookahead binding calls it
    pub(crate) fn lookahead_begin(&self) -> Option<String> {
        self.inner.lock().unwrap().lookahead_begin(Instant::now())
    }

    /// See `Dispatcher::lookahead_done`. The instance may be what a waiter needs, or its room is free again.
    #[allow(dead_code)] // the runtime's lookahead binding calls it
    pub(crate) fn lookahead_done(&self, pool: &str, ok: bool) -> Result<()> {
        self.inner.lock().unwrap().lookahead_done(pool, ok, Instant::now())?;
        self.cv.notify_all();
        Ok(())
    }

    /// See `Dispatcher::blocked`.
    #[allow(dead_code)] // the runtime's tuner binding calls it
    pub(crate) fn blocked(&self, resource: &str) -> usize {
        self.inner.lock().unwrap().blocked(resource, Instant::now())
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
            workflow_started_ms: None,
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
        let mut d = Dispatcher::new(&qs(&["hi", "lo"]), true, &HashMap::new(), &Resources::default(), Schedule::Fifo).unwrap();
        let now = Instant::now();
        for (tok, q) in [(1, "lo"), (2, "hi"), (3, "lo"), (4, "hi")] {
            assert!(matches!(d.push(task(tok, "t", q), now), Admit::Held));
        }
        assert_eq!(tokens(&mut d, now), vec![2, 4, 1, 3]);
    }

    #[test]
    fn equal_priority_is_fifo_across_queues() {
        let mut d = Dispatcher::new(&qs(&["hi", "lo"]), false, &HashMap::new(), &Resources::default(), Schedule::Fifo).unwrap();
        let now = Instant::now();
        for (tok, q) in [(1, "lo"), (2, "hi"), (3, "lo")] {
            d.push(task(tok, "t", q), now);
        }
        assert_eq!(tokens(&mut d, now), vec![1, 2, 3]);
    }

    #[test]
    fn rate_limit_spaces_tasks_one_interval_apart() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 10.0)]), &Resources::default(), Schedule::Fifo).unwrap();
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
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 10.0)]), &Resources::default(), Schedule::Fifo).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        d.push(task(2, "slow", "q"), t0);
        d.push(task(3, "fast", "q"), t0);
        assert_eq!(tokens(&mut d, t0), vec![1, 3]);
    }

    #[test]
    fn throttled_higher_priority_task_lets_lower_priority_run() {
        let mut d = Dispatcher::new(&qs(&["hi", "lo"]), true, &limits(&[("slow", 10.0)]), &Resources::default(), Schedule::Fifo).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "hi"), t0);
        d.push(task(2, "slow", "hi"), t0);
        d.push(task(3, "fast", "lo"), t0);
        assert_eq!(tokens(&mut d, t0), vec![1, 3]);
        assert_eq!(tokens(&mut d, t0 + 100 * MS), vec![2]);
    }

    #[test]
    fn idle_bucket_refills() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 10.0)]), &Resources::default(), Schedule::Fifo).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        let later = t0 + Duration::from_secs(5);
        d.push(task(2, "slow", "q"), later);
        assert_eq!(tokens(&mut d, later), vec![1, 2]);
    }

    #[test]
    fn beyond_horizon_defers_spread_one_interval_apart() {
        // 0.5/s = one start every 2 s, past the 1 s horizon
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 0.5)]), &Resources::default(), Schedule::Fifo).unwrap();
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
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 0.5)]), &Resources::default(), Schedule::Fifo).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        assert!(matches!(d.push(task(2, "slow", "q"), t0), Admit::Defer { .. }));
        let back = t0 + Duration::from_secs(2);
        assert!(matches!(d.push(task(2, "slow", "q"), back), Admit::Held), "deferral reserved no slot");
        assert_eq!(tokens(&mut d, back), vec![1, 2]);
    }

    #[test]
    fn slot_exactly_at_the_horizon_is_held() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 1.0)]), &Resources::default(), Schedule::Fifo).unwrap();
        let t0 = Instant::now();
        d.push(task(1, "slow", "q"), t0);
        assert!(matches!(d.push(task(2, "slow", "q"), t0), Admit::Held));
    }

    #[test]
    fn hold_ignores_the_rate_limit_and_waits_until_at() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 0.5)]), &Resources::default(), Schedule::Fifo).unwrap();
        let t0 = Instant::now();
        let at = t0 + Duration::from_secs(3);
        d.hold(task(1, "slow", "q"), at);
        assert_eq!(d.pop(t0).map(|t| t.token), None);
        assert_eq!(d.next_due(t0), Some(at));
        assert_eq!(tokens(&mut d, at), vec![1]);
    }

    #[test]
    fn drain_returns_everything_held() {
        let mut d = Dispatcher::new(&qs(&["q"]), false, &limits(&[("slow", 10.0)]), &Resources::default(), Schedule::Fifo).unwrap();
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
                matches!(Dispatcher::new(&qs(&["q"]), false, &limits(&[("t", bad)]), &Resources::default(), Schedule::Fifo), Err(Error::Config(_))),
                "rate {bad} must be rejected"
            );
        }
    }

    fn empty_queue() -> ReadyQueue {
        ReadyQueue::new(Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &Resources::default(), Schedule::Fifo).unwrap())
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
        Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), r, Schedule::Fifo).unwrap()
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
            matches!(Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &r, Schedule::Fifo), Err(Error::Config(_)))
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
        let Err(Error::Config(m)) = Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &r, Schedule::Fifo) else { panic!() };
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
            &res(&[("gpu", 1.0)], &[("g", &[("gpu", 0.25)])]), Schedule::Fifo,
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
            &res(&[("gpu", 1.0)], &[("g", &[("gpu", 0.75)]), ("h", &[("gpu", 0.25)])]), Schedule::Fifo,
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
                .map(|(n, c, loaded)| (n.to_string(), LazyPool { claim: limits(c), loaded: *loaded, ..Default::default() }))
                .collect(),
            ..res(caps, reqs)
        }
    }

    fn lazy(caps: &[(&str, f64)], reqs: &[(&str, &[(&str, f64)])], pools: &[PoolSpec]) -> Dispatcher {
        Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &lazy_res(caps, reqs, pools), Schedule::Oldest).unwrap()
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

    /// vram 16, two gemma instances (14.8) and a tts needing 9.0: tts must evict both. The gemma task 3
    /// arrives `late` after the others.
    fn reservation_setup(t0: Instant, late: Duration) -> Dispatcher {
        let mut d = lazy(
            &[("vram_gb", 16.0), ("pool:gemma", 2.0), ("pool:tts", 1.0)],
            &[("g", &[("pool:gemma", 1.0)]), ("t", &[("pool:tts", 1.0)])],
            &[("gemma", &[("vram_gb", 7.4)], 2), ("tts", &[("vram_gb", 9.0)], 0)],
        );
        d.push(task(1, "g", "q"), t0);
        assert!(d.pop(t0).is_some()); // one gemma instance is running, the other idle
        d.push(task(2, "t", "q"), t0); // the head: needs both gemma instances gone
        d.push(task(3, "g", "q"), t0 + late);
        d
    }

    #[test]
    fn before_the_reservation_gemma_tasks_keep_backfilling() {
        let t0 = Instant::now();
        let mut d = reservation_setup(t0, Duration::ZERO);
        assert_eq!(d.pop(t0 + RESERVE_AFTER - MS).map(|t| t.token), Some(3));
        assert!(d.pop(t0 + RESERVE_AFTER - MS).is_none(), "the head still waits");
    }

    #[test]
    fn the_reserved_head_stops_tasks_that_would_keep_a_victim_busy() {
        let t0 = Instant::now();
        // A resident task outranks a swap for `OLDEST_BONUS`, so the tts head is the head only once it is older.
        let late = OLDEST_BONUS + MS;
        let mut d = reservation_setup(t0, late);
        let reserved = t0 + late;
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
        let bad = |r: Resources| {
            let schedule = if r.lazy.is_empty() { Schedule::Fifo } else { Schedule::Oldest };
            matches!(Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &r, schedule), Err(Error::Config(_)))
        };
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

    // ---- scheduling ----

    fn lazy_sched(schedule: Schedule) -> Dispatcher {
        Dispatcher::new(
            &qs(&["q"]), false, &HashMap::new(),
            &lazy_res(
                &[("vram_gb", 7.5), ("pool:gemma", 1.0), ("pool:tts", 1.0)],
                &[("g", &[("pool:gemma", 1.0)]), ("t", &[("pool:tts", 1.0)])],
                &[("gemma", &[("vram_gb", 7.4)], 1), ("tts", &[("vram_gb", 7.2)], 0)],
            ),
            schedule,
        ).unwrap()
    }

    /// An instant far enough from the clock's epoch to subtract a day from.
    fn far() -> Instant {
        Instant::now() + Duration::from_secs(48 * 3600)
    }

    fn in_workflow(token: u64, name: &str, started_ms: u64) -> ReadyTask {
        ReadyTask { workflow_started_ms: Some(started_ms), ..task(token, name, "q") }
    }

    #[test]
    fn throughput_groups_resident_tasks_before_a_swap() {
        let mut d = lazy_sched(Schedule::Throughput);
        let t0 = Instant::now();
        for (tok, name) in [(1, "g"), (2, "t"), (3, "g"), (4, "t"), (5, "g"), (6, "t")] {
            d.push(task(tok, name, "q"), t0);
        }
        let mut order = Vec::new();
        while let Some(t) = d.pop(t0) {
            order.push(t.token);
            assert_eq!(d.pop(t0).map(|t| t.token), None, "the model is busy: nothing else can start");
            d.release(t.token);
        }
        assert_eq!(order, vec![1, 3, 5, 2, 4, 6]);
    }

    #[test]
    fn throughput_resident_bonus_is_bounded_by_the_wait() {
        let first = |gap: Duration| {
            let mut d = lazy_sched(Schedule::Throughput);
            let t0 = Instant::now();
            d.push(task(1, "t", "q"), t0);
            d.push(task(2, "g", "q"), t0 + gap);
            d.pop(t0 + gap).unwrap().token
        };
        assert_eq!(first(THROUGHPUT_BONUS - MS), 2, "t arrived less than the bonus before g");
        assert_eq!(first(THROUGHPUT_BONUS + MS), 1, "t waited longer than the bonus");
    }

    #[test]
    fn oldest_runs_an_older_workflow_before_a_younger_resident_task() {
        let mut d = lazy_sched(Schedule::Oldest);
        let t0 = far();
        let wall = now_ms();
        d.push(in_workflow(1, "g", wall - 1_000), t0);
        d.push(in_workflow(2, "t", wall - 600_000), t0);
        let t = d.pop(t0).unwrap();
        assert_eq!((t.token, t.evicts), (2, vec!["gemma".to_string()]));
    }

    #[test]
    fn oldest_prefers_a_resident_task_over_a_swap_that_is_only_slightly_older() {
        let first = |t_started_ago: u64| {
            let mut d = lazy_sched(Schedule::Oldest);
            let t0 = far();
            let wall = now_ms();
            d.push(in_workflow(1, "t", wall - t_started_ago), t0);
            d.push(in_workflow(2, "g", wall - 10_000), t0);
            d.pop(t0).unwrap().token
        };
        assert_eq!(first(14_000), 2, "4 s older is less than the bonus");
        assert_eq!(first(30_000), 1, "20 s older is more");
    }

    #[test]
    fn oldest_ranks_tasks_outside_workflows_by_arrival() {
        let first = |gap: Duration| {
            let mut d = lazy_sched(Schedule::Oldest);
            let t0 = Instant::now();
            d.push(task(1, "t", "q"), t0);
            d.push(task(2, "g", "q"), t0 + gap);
            d.pop(t0 + gap).unwrap().token
        };
        assert_eq!(first(OLDEST_BONUS - MS), 2);
        assert_eq!(first(OLDEST_BONUS + MS), 1);
    }

    #[test]
    fn strict_priority_still_beats_scoring() {
        for schedule in [Schedule::Throughput, Schedule::Oldest] {
            let mut d = Dispatcher::new(
                &qs(&["hi", "lo"]), true, &HashMap::new(),
                &lazy_res(
                    &[("vram_gb", 7.5), ("pool:gemma", 1.0), ("pool:tts", 1.0)],
                    &[("g", &[("pool:gemma", 1.0)]), ("t", &[("pool:tts", 1.0)])],
                    &[("gemma", &[("vram_gb", 7.4)], 1), ("tts", &[("vram_gb", 7.2)], 0)],
                ),
                schedule,
            ).unwrap();
            let t0 = far();
            // The gemma task is resident but in the lower queue; the tts one needs a swap and is far older.
            d.hold(ReadyTask { queue: "lo".into(), ..in_workflow(1, "g", now_ms() - 1_000) }, t0);
            d.hold(ReadyTask { queue: "hi".into(), ..in_workflow(2, "t", now_ms() - 600_000) }, t0);
            assert_eq!(d.pop(t0).unwrap().token, 2, "{schedule:?}");
        }
    }

    #[test]
    fn a_task_that_cannot_start_does_not_block_others_when_scoring() {
        let mut d = lazy(
            &[("vram_gb", 7.5), ("pool:gemma", 1.0)],
            &[("g", &[("pool:gemma", 1.0)]), ("p", &[("vram_gb", 0.5)])],
            &[("gemma", &[("vram_gb", 7.4)], 0)],
        );
        let t0 = Instant::now();
        d.push(task(1, "g", "q"), t0);
        assert!(d.pop(t0).is_some());
        d.push(task(2, "g", "q"), t0);
        d.push(task(3, "plain", "q"), t0);
        assert_eq!(d.pop(t0).unwrap().token, 3, "the second gemma task waits for pool:gemma");
    }

    #[test]
    fn schedule_and_lazy_pools_must_agree() {
        let lazy_r = lazy_res(
            &[("vram_gb", 7.5), ("pool:gemma", 1.0)], &[], &[("gemma", &[("vram_gb", 7.4)], 0)],
        );
        let new = |r: &Resources, s| Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), r, s).map(|_| ());
        assert!(matches!(new(&lazy_r, Schedule::Fifo), Err(Error::Config(_))));
        assert!(new(&lazy_r, Schedule::Throughput).is_ok() && new(&lazy_r, Schedule::Oldest).is_ok());
        let plain = res(&[("gpu", 1.0)], &[]);
        assert!(matches!(new(&plain, Schedule::Throughput), Err(Error::Config(_))));
        assert!(matches!(new(&plain, Schedule::Oldest), Err(Error::Config(_))));
        assert!(new(&plain, Schedule::Fifo).is_ok());
    }

    #[test]
    fn only_the_oldest_schedule_looks_at_the_workflow_start() {
        let origin_age = |schedule| {
            let mut d = lazy_sched(schedule);
            let t0 = far();
            d.push(in_workflow(1, "g", now_ms() - 30_000), t0);
            t0 - d.held[0].origin
        };
        assert_eq!(origin_age(Schedule::Throughput), Duration::ZERO);
        let age = origin_age(Schedule::Oldest);
        assert!(age >= Duration::from_secs(29) && age <= Duration::from_secs(31), "{age:?}");
    }

    #[test]
    fn origin_clamps_a_future_start_and_a_very_old_one() {
        let at = far();
        assert_eq!(origin_of(at, 5_000, 4_000), at, "started in the future (clock skew)");
        assert_eq!(origin_of(at, 5_000, 5_000), at);
        assert_eq!(origin_of(at, 1_000, 11_000), at - Duration::from_secs(10));
        assert_eq!(origin_of(at, 0, u64::MAX), at - MAX_ORIGIN_AGE, "capped at a day");
    }

    #[test]
    fn origin_falls_back_to_at_when_the_instant_cannot_go_back() {
        // An Instant can't be moved before the clock's epoch; a task held right after boot must not panic.
        let at = Instant::now();
        let origin = origin_of(at, 0, 3_600_000);
        assert!(origin == at || at - origin == Duration::from_secs(3600));
    }

    // ---- linger, lookahead, demand ----

    const LINGER: Duration = Duration::from_secs(30);

    /// two_models(0, 1) where tts has `linger` and was last used at `t0` (its task has finished); gemma's task
    /// `g` is the one that would have to evict it.
    fn lingering_tts(linger: Option<Duration>, t0: Instant) -> Dispatcher {
        let mut r = lazy_res(
            &[("vram_gb", 7.5), ("pool:gemma", 1.0), ("pool:tts", 1.0)],
            &[("g", &[("pool:gemma", 1.0)]), ("t", &[("pool:tts", 1.0)])],
            &[("gemma", &[("vram_gb", 7.4)], 0), ("tts", &[("vram_gb", 7.2)], 1)],
        );
        r.lazy.get_mut("tts").unwrap().linger = linger;
        let mut d = Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &r, Schedule::Oldest).unwrap();
        d.push(task(1, "t", "q"), t0);
        assert!(d.pop(t0).is_some());
        d.release_at(1, t0);
        d
    }

    #[test]
    fn a_lingering_idle_model_is_not_evicted_while_its_workflow_has_demand() {
        let t0 = Instant::now();
        let mut d = lingering_tts(Some(LINGER), t0);
        d.set_demand(&[("tts".to_string(), 1)]);
        d.push(task(2, "g", "q"), t0 + 10 * Duration::from_secs(1));
        assert!(d.pop(t0 + Duration::from_secs(10)).is_none(), "tts is lingering");
        assert!(d.pop(t0 + LINGER - MS).is_none());
        let t = d.pop(t0 + LINGER + Duration::from_secs(1)).unwrap();
        assert_eq!((t.loads, t.evicts), (vec!["gemma".to_string()], vec!["tts".to_string()]));
    }

    #[test]
    fn linger_counts_from_when_the_model_went_idle_not_from_when_its_task_started() {
        let t0 = Instant::now();
        let secs = Duration::from_secs;
        let mut r = lazy_res(
            &[("vram_gb", 7.5), ("pool:gemma", 1.0), ("pool:tts", 1.0)],
            &[("g", &[("pool:gemma", 1.0)]), ("t", &[("pool:tts", 1.0)])],
            &[("gemma", &[("vram_gb", 7.4)], 0), ("tts", &[("vram_gb", 7.2)], 1)],
        );
        r.lazy.get_mut("tts").unwrap().linger = Some(LINGER);
        let mut d = Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &r, Schedule::Oldest).unwrap();
        d.push(task(1, "t", "q"), t0);
        assert!(d.pop(t0).is_some());
        d.set_demand(&[("tts".to_string(), 1)]);
        d.push(task(2, "g", "q"), t0 + secs(1));
        assert!(d.pop(t0 + secs(10)).is_none(), "tts is busy");
        d.release_at(1, t0 + secs(20));
        assert!(d.pop(t0 + secs(45)).is_none(), "idle for 25 s of 30");
        assert_eq!(d.next_due(t0 + secs(45)), Some(t0 + secs(50)));
        assert_eq!(d.pop(t0 + secs(51)).unwrap().evicts, vec!["tts".to_string()]);
    }

    #[test]
    fn a_built_lookahead_instance_lingers_from_when_it_was_built() {
        let t0 = Instant::now();
        let mut r = lazy_res(
            &[("vram_gb", 7.5), ("pool:gemma", 1.0), ("pool:tts", 1.0)],
            &[("g", &[("pool:gemma", 1.0)])],
            &[("gemma", &[("vram_gb", 7.4)], 0), ("tts", &[("vram_gb", 7.2)], 0)],
        );
        r.lazy.get_mut("tts").unwrap().linger = Some(LINGER);
        let mut d = Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &r, Schedule::Oldest).unwrap();
        d.set_demand(&[("tts".to_string(), 1)]);
        assert_eq!(d.lookahead_begin(t0).as_deref(), Some("tts"));
        let built = t0 + Duration::from_secs(8);
        d.lookahead_done("tts", true, built).unwrap();
        d.push(task(1, "g", "q"), built);
        assert!(d.pop(built + LINGER - MS).is_none());
        assert_eq!(d.pop(built + LINGER).unwrap().evicts, vec!["tts".to_string()]);
    }

    #[test]
    fn linger_does_not_protect_without_demand_or_without_linger() {
        let t0 = Instant::now();
        let at = t0 + Duration::from_secs(10);
        let mut no_demand = lingering_tts(Some(LINGER), t0);
        no_demand.push(task(2, "g", "q"), at);
        assert_eq!(no_demand.pop(at).unwrap().evicts, vec!["tts".to_string()]);

        let mut dropped = lingering_tts(Some(LINGER), t0);
        dropped.set_demand(&[("tts".to_string(), 2)]);
        dropped.set_demand(&[("tts".to_string(), 0)]);
        dropped.push(task(2, "g", "q"), at);
        assert_eq!(dropped.pop(at).unwrap().evicts, vec!["tts".to_string()]);

        let mut no_linger = lingering_tts(None, t0);
        no_linger.set_demand(&[("tts".to_string(), 1)]);
        no_linger.push(task(2, "g", "q"), at);
        assert_eq!(no_linger.pop(at).unwrap().evicts, vec!["tts".to_string()]);
    }

    #[test]
    fn a_model_never_used_is_not_protected_by_linger() {
        let mut r = lazy_res(
            &[("vram_gb", 7.5), ("pool:gemma", 1.0), ("pool:tts", 1.0)],
            &[("g", &[("pool:gemma", 1.0)])],
            &[("gemma", &[("vram_gb", 7.4)], 0), ("tts", &[("vram_gb", 7.2)], 1)],
        );
        r.lazy.get_mut("tts").unwrap().linger = Some(LINGER);
        let mut d = Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &r, Schedule::Oldest).unwrap();
        d.set_demand(&[("tts".to_string(), 1)]);
        let t0 = Instant::now();
        d.push(task(1, "g", "q"), t0);
        assert_eq!(d.pop(t0).unwrap().evicts, vec!["tts".to_string()]);
    }

    #[test]
    fn next_due_includes_the_linger_expiry_only_while_it_matters() {
        let t0 = Instant::now();
        let mut d = lingering_tts(Some(LINGER), t0);
        d.set_demand(&[("tts".to_string(), 1)]);
        let at = t0 + Duration::from_secs(10);
        d.push(task(2, "g", "q"), at);
        assert!(d.pop(at).is_none());
        assert_eq!(d.next_due(at), Some(t0 + LINGER));
        assert_eq!(d.next_due(t0 + LINGER), None, "only instants after now");
        d.set_demand(&[("tts".to_string(), 0)]);
        assert_eq!(d.next_due(at), None);

        let mut busy = lingering_tts(Some(LINGER), t0);
        busy.set_demand(&[("tts".to_string(), 1)]);
        busy.push(task(3, "t", "q"), t0);
        assert!(busy.pop(t0).is_some());
        assert_eq!(busy.next_due(t0), None, "no idle instance: nothing is waiting on the linger");
    }

    #[test]
    fn set_demand_ignores_unknown_pools_and_reports_a_drop_to_zero() {
        let mut d = two_models(0, 0);
        assert_eq!(d.demand(), vec![("gemma".to_string(), 0), ("tts".to_string(), 0)]);
        assert!(!d.set_demand(&[("nope".to_string(), 3), ("gemma".to_string(), 2)]));
        assert_eq!(d.demand(), vec![("gemma".to_string(), 2), ("tts".to_string(), 0)]);
        assert!(!d.set_demand(&[("gemma".to_string(), 1)]), "lower but not zero");
        assert!(!d.set_demand(&[("tts".to_string(), 0)]), "already zero");
        assert!(d.set_demand(&[("gemma".to_string(), 0)]));
        let mut eager = with_res(&res(&[("gpu", 1.0)], &[]));
        assert!(!eager.set_demand(&[("gpu".to_string(), 1)]));
        assert!(eager.demand().is_empty());
    }

    #[test]
    fn lookahead_loads_a_model_with_demand_and_counts_it_as_loading() {
        let mut d = two_models(0, 0);
        let t0 = Instant::now();
        assert_eq!(d.lookahead_begin(t0), None, "no demand");
        d.set_demand(&[("gemma".to_string(), 1)]);
        assert_eq!(d.lookahead_begin(t0).as_deref(), Some("gemma"));
        assert_eq!(in_use(&d, "vram_gb"), 7.4);
        assert_eq!(in_use(&d, "pool:gemma"), 0.0);
        assert_eq!(loaded(&d), vec![("gemma".to_string(), 1), ("tts".to_string(), 0)]);
        assert_eq!(d.lookahead_begin(t0), None, "one is already loading");

        // The instance is not built yet, so the task cannot use it, and pool:gemma allows no second one.
        d.push(task(1, "g", "q"), t0);
        assert!(d.pop(t0).is_none());
        d.lookahead_done("gemma", true, t0).unwrap();
        let t = d.pop(t0).unwrap();
        assert!(t.loads.is_empty() && t.evicts.is_empty(), "the lookahead instance is reused");
        assert_eq!(in_use(&d, "vram_gb"), 7.4);
    }

    #[test]
    fn a_task_may_load_another_instance_while_one_is_loading_if_the_pool_allows() {
        let mut d = lazy(
            &[("vram_gb", 20.0), ("pool:gemma", 2.0)],
            &[("g", &[("pool:gemma", 1.0)])],
            &[("gemma", &[("vram_gb", 7.4)], 0)],
        );
        let t0 = Instant::now();
        d.set_demand(&[("gemma".to_string(), 1)]);
        assert!(d.lookahead_begin(t0).is_some());
        d.push(task(1, "g", "q"), t0);
        assert_eq!(d.pop(t0).unwrap().loads, vec!["gemma".to_string()]);
        assert_eq!(loaded(&d), vec![("gemma".to_string(), 2)]);
    }

    #[test]
    fn a_failed_lookahead_gives_the_room_back_and_backs_off() {
        let mut d = two_models(0, 0);
        let t0 = Instant::now();
        d.set_demand(&[("gemma".to_string(), 1)]);
        assert!(d.lookahead_begin(t0).is_some());
        d.lookahead_done("gemma", false, t0).unwrap();
        assert_eq!(in_use(&d, "vram_gb"), 0.0);
        assert_eq!(loaded(&d), vec![("gemma".to_string(), 0), ("tts".to_string(), 0)]);
        assert_eq!(d.lookahead_begin(t0 + LOOKAHEAD_BACKOFF - MS), None);
        assert_eq!(d.lookahead_begin(t0 + LOOKAHEAD_BACKOFF).as_deref(), Some("gemma"));
        // A task can still load the model itself during the backoff.
        d.lookahead_done("gemma", true, t0 + LOOKAHEAD_BACKOFF).unwrap();
        assert_eq!(in_use(&d, "vram_gb"), 7.4);
    }

    #[test]
    fn lookahead_grants_nothing_unless_every_condition_holds() {
        let t0 = Instant::now();
        let demand = |d: &mut Dispatcher| {
            d.set_demand(&[("gemma".to_string(), 1), ("tts".to_string(), 1)]);
        };
        // An idle instance exists (gemma); tts would need the room gemma holds.
        let mut d = two_models(1, 0);
        demand(&mut d);
        assert_eq!(d.lookahead_begin(t0), None, "gemma is idle and tts needs an eviction");
        // Only an eviction would make room.
        let mut d = two_models(0, 1);
        d.set_demand(&[("gemma".to_string(), 1)]);
        assert_eq!(d.lookahead_begin(t0), None);
        // A due held task: busy workers don't speculate; one that is not due yet does not count.
        let mut d = two_models(0, 0);
        demand(&mut d);
        d.hold(task(1, "g", "q"), t0 + Duration::from_secs(5));
        assert_eq!(d.lookahead_begin(t0).as_deref(), Some("gemma"));
        let mut d = two_models(0, 0);
        demand(&mut d);
        d.push(task(1, "g", "q"), t0);
        assert_eq!(d.lookahead_begin(t0), None);
        // pool:gemma is at its limit (one busy instance), even though the room is there.
        let mk = |instances: f64| lazy(
            &[("vram_gb", 20.0), ("pool:gemma", instances)],
            &[("g", &[("pool:gemma", 1.0)])],
            &[("gemma", &[("vram_gb", 7.4)], 0)],
        );
        let mut d = mk(1.0);
        d.push(task(1, "g", "q"), t0);
        assert!(d.pop(t0).is_some());
        d.set_demand(&[("gemma".to_string(), 1)]);
        assert_eq!(d.lookahead_begin(t0), None);
        let mut d = mk(2.0);
        d.push(task(1, "g", "q"), t0);
        assert!(d.pop(t0).is_some());
        d.set_demand(&[("gemma".to_string(), 1)]);
        assert_eq!(d.lookahead_begin(t0).as_deref(), Some("gemma"), "a second instance is allowed");
    }

    #[test]
    fn lookahead_grants_one_pool_per_call_in_name_order() {
        let mut d = lazy(
            &[("vram_gb", 20.0), ("pool:gemma", 1.0), ("pool:tts", 1.0)],
            &[],
            &[("gemma", &[("vram_gb", 7.4)], 0), ("tts", &[("vram_gb", 7.2)], 0)],
        );
        let t0 = Instant::now();
        d.set_demand(&[("tts".to_string(), 1), ("gemma".to_string(), 1)]);
        assert_eq!(d.lookahead_begin(t0).as_deref(), Some("gemma"));
        assert_eq!(d.lookahead_begin(t0).as_deref(), Some("tts"));
        assert_eq!(d.lookahead_begin(t0), None);
        assert_eq!(in_use(&d, "vram_gb"), 14.6);
    }

    #[test]
    fn a_lookahead_model_is_not_the_next_victim() {
        // Granted at t0 it counts as used at t0, so an older idle model is evicted before it.
        let mut d = lazy(
            &[("vram_gb", 15.0), ("pool:a", 1.0), ("pool:b", 1.0), ("pool:c", 1.0)],
            &[("ta", &[("pool:a", 1.0)]), ("tc", &[("pool:c", 1.0)])],
            &[("a", &[("vram_gb", 5.0)], 1), ("b", &[("vram_gb", 5.0)], 0), ("c", &[("vram_gb", 8.0)], 0)],
        );
        let t0 = far();
        d.push(task(1, "ta", "q"), t0 - 10 * Duration::from_secs(1));
        assert!(d.pop(t0 - 10 * Duration::from_secs(1)).is_some());
        d.release(1);
        d.set_demand(&[("b".to_string(), 1)]);
        assert_eq!(d.lookahead_begin(t0).as_deref(), Some("b"));
        d.lookahead_done("b", true, t0).unwrap();
        d.push(task(2, "tc", "q"), t0);
        assert_eq!(d.pop(t0).unwrap().evicts, vec!["a".to_string()]);
    }

    #[test]
    fn blocked_counts_due_tasks_that_do_not_fit_and_use_the_resource() {
        let mut d = with_res(&res(
            &[("gpu", 1.0), ("cpu", 4.0)],
            &[("half", &[("gpu", 0.5)]), ("big", &[("gpu", 1.0)]), ("small", &[("cpu", 0.5)])],
        ));
        let t0 = Instant::now();
        d.push(task(1, "half", "q"), t0);
        assert_eq!(tokens(&mut d, t0), vec![1]);
        d.push(task(2, "big", "q"), t0);
        d.push(task(3, "half", "q"), t0 + 10 * MS); // not due
        d.push(task(4, "small", "q"), t0); // fits
        d.push(task(5, "big", "q"), t0);
        assert_eq!(d.blocked("gpu", t0), 2);
        assert_eq!(d.blocked("cpu", t0), 0);
        assert_eq!(d.blocked("nope", t0), 0);
        assert_eq!(d.blocked("gpu", t0 + 10 * MS), 2, "the half task fits and is not counted");
        d.release(1);
        assert_eq!(d.blocked("gpu", t0), 0, "everything fits again");
    }

    #[test]
    fn lookahead_and_unload_calls_reject_unknown_pools() {
        let mut d = two_models(0, 0);
        let t0 = Instant::now();
        let bad = |r: Result<()>| matches!(r, Err(Error::Config(_)));
        assert!(bad(d.lookahead_done("nope", true, t0)));
        assert!(bad(d.lookahead_done("gemma", true, t0)), "nothing is loading");
        assert!(bad(d.lookahead_done("gemma", false, t0)));
        assert_eq!(loaded(&d), vec![("gemma".to_string(), 0), ("tts".to_string(), 0)]);
        d.set_demand(&[("gemma".to_string(), 1)]);
        assert!(d.lookahead_begin(t0).is_some());
        d.lookahead_done("gemma", true, t0).unwrap();
        assert!(bad(d.lookahead_done("gemma", true, t0)), "finished once");
        let mut eager = with_res(&res(&[("gpu", 1.0), ("pool:m", 1.0)], &[]));
        assert!(bad(eager.lookahead_done("m", true, t0)), "not a lazy pool");
        assert_eq!(eager.lookahead_begin(t0), None);
    }

    #[test]
    fn a_zero_linger_is_rejected() {
        let mk = |linger| {
            let mut r = lazy_res(
                &[("vram_gb", 7.5), ("pool:gemma", 1.0)], &[], &[("gemma", &[("vram_gb", 7.4)], 0)],
            );
            r.lazy.get_mut("gemma").unwrap().linger = linger;
            Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &r, Schedule::Oldest).map(|_| ())
        };
        assert!(matches!(mk(Some(Duration::ZERO)), Err(Error::Config(_))));
        assert!(mk(Some(MS)).is_ok() && mk(None).is_ok());
    }

    #[test]
    fn ready_queue_set_demand_dropping_to_zero_wakes_a_waiter() {
        let mut r = lazy_res(
            &[("vram_gb", 7.5), ("pool:gemma", 1.0), ("pool:tts", 1.0)],
            &[("g", &[("pool:gemma", 1.0)]), ("t", &[("pool:tts", 1.0)])],
            &[("gemma", &[("vram_gb", 7.4)], 0), ("tts", &[("vram_gb", 7.2)], 1)],
        );
        r.lazy.get_mut("tts").unwrap().linger = Some(Duration::from_secs(60));
        let q = Arc::new(ReadyQueue::new(
            Dispatcher::new(&qs(&["q"]), false, &HashMap::new(), &r, Schedule::Oldest).unwrap(),
        ));
        q.push(task(1, "t", "q"), Instant::now());
        assert_eq!(q.recv_timeout(Duration::from_secs(1)).map(|t| t.token), Some(1));
        q.release(1);
        q.set_demand(&[("tts".to_string(), 1)]);
        assert_eq!(q.demand(), vec![("gemma".to_string(), 0), ("tts".to_string(), 1)]);
        q.push(task(2, "g", "q"), Instant::now());
        assert!(q.recv_timeout(100 * MS).is_none(), "tts lingers");

        let q2 = q.clone();
        let h = std::thread::spawn(move || q2.recv_timeout(Duration::from_secs(5)).map(|t| t.token));
        std::thread::sleep(50 * MS);
        let t = Instant::now();
        q.set_demand(&[("tts".to_string(), 0)]);
        assert_eq!(h.join().unwrap(), Some(2));
        assert!(t.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn ready_queue_lookahead_done_wakes_a_waiter_and_blocked_counts_now() {
        let q = Arc::new(ReadyQueue::new(two_models(0, 0)));
        q.set_demand(&[("gemma".to_string(), 1)]);
        assert_eq!(q.lookahead_begin().as_deref(), Some("gemma"));
        q.push(task(1, "g", "q"), Instant::now());
        assert!(q.recv_timeout(100 * MS).is_none(), "the instance is still loading");
        assert_eq!(q.blocked("pool:gemma"), 0, "pool:gemma is free; the task waits for the load, not the pool");

        let q2 = q.clone();
        let h = std::thread::spawn(move || q2.recv_timeout(Duration::from_secs(5)).map(|t| t.token));
        std::thread::sleep(50 * MS);
        let t = Instant::now();
        q.lookahead_done("gemma", true).unwrap();
        assert_eq!(h.join().unwrap(), Some(1));
        assert!(t.elapsed() < Duration::from_secs(1));
        assert!(matches!(q.lookahead_done("gemma", true), Err(Error::Config(_))));
    }
}
