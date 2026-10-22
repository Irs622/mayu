//! Estimates when the nodes of a workflow will be needed, from this worker's own run times.
//!
//! Pure: the runtime feeds it workflows read from the broker, the run times it has seen and how long the
//! running nodes have been going. Nothing here touches the dispatcher or the clock.
use std::collections::HashMap;

use crate::workflow::{NodeState, WorkflowState};

/// Weight of the newest sample in the moving average of a duration.
pub const EWMA_ALPHA: f64 = 0.3;

/// What a lazy pool instance move is timed as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MoveKind {
    /// The factory ran.
    Build,
    /// An instance was built straight into the parked state.
    Stage,
    /// An instance moved from the GPU to host RAM.
    Park,
    /// An instance moved from host RAM to the GPU.
    Unpark,
}

impl MoveKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MoveKind::Build => "build",
            MoveKind::Stage => "stage",
            MoveKind::Park => "park",
            MoveKind::Unpark => "unpark",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "build" => MoveKind::Build,
            "stage" => MoveKind::Stage,
            "park" => MoveKind::Park,
            "unpark" => MoveKind::Unpark,
            _ => return None,
        })
    }
}

fn blend(old: Option<f64>, sample: f64) -> f64 {
    match old {
        Some(old) => EWMA_ALPHA * sample + (1.0 - EWMA_ALPHA) * old,
        None => sample,
    }
}

/// Moving averages (seconds) of how long tasks run, per task name, and of pool moves, per (pool, kind).
#[derive(Debug, Clone, Default)]
pub struct Durations {
    tasks: HashMap<String, f64>,
    moves: HashMap<(String, MoveKind), f64>,
}

impl Durations {
    /// Folds in one run of `task`. Samples that aren't a finite, non-negative number of seconds are ignored.
    pub fn record_task(&mut self, task: &str, secs: f64) {
        if secs.is_finite() && secs >= 0.0 {
            let old = self.tasks.get(task).copied();
            self.tasks.insert(task.to_string(), blend(old, secs));
        }
    }

    /// Mean run time of `task`; 0 until one run is seen, which makes estimates optimistic.
    pub fn task_secs(&self, task: &str) -> f64 {
        self.tasks.get(task).copied().unwrap_or(0.0)
    }

    /// Folds in one move and returns the new average, or None for a sample that isn't a usable duration.
    pub fn record_move(&mut self, pool: &str, kind: MoveKind, secs: f64) -> Option<f64> {
        if !(secs.is_finite() && secs >= 0.0) {
            return None;
        }
        let slot = self.moves.entry((pool.to_string(), kind)).or_insert(secs);
        // A fresh slot already holds the sample; blending it again would change nothing.
        *slot = blend(Some(*slot), secs);
        Some(*slot)
    }

    pub fn move_secs(&self, pool: &str, kind: MoveKind) -> Option<f64> {
        self.moves.get(&(pool.to_string(), kind)).copied()
    }
}

/// When one workflow node starts and finishes, in seconds from now.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NodeTimes {
    /// Done, dead or cancelled: nothing left to wait for.
    pub finished: bool,
    pub start: f64,
    pub finish: f64,
}

/// A task already running, by task id in the map `node_times` takes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Running {
    /// Seconds since it started, as this worker saw it. 0 for a task on another worker (not used there: it has
    /// a `remaining` or isn't listed).
    pub elapsed: f64,
    /// Seconds left according to the task's own progress reports; None when it reports none or too few.
    pub remaining: Option<f64>,
}

impl Running {
    /// What is left of the task: its progress estimate if it has one, else what is left of the mean.
    fn left(&self, mean: f64) -> f64 {
        match self.remaining {
            Some(r) if r.is_finite() => r.max(0.0),
            _ => (mean - self.elapsed).max(0.0),
        }
    }
}

/// The forward pass over one workflow. Nodes are in topological order (parents precede children), so one
/// sweep gives every node its times:
/// - finished nodes: nothing left, both 0;
/// - a queued node starts now, and finishes after its mean run time; if it is `running` (by task id) only what
///   is left: its own progress estimate when it has one, else what is left of the mean, never negative;
/// - a waiting node starts when the last of its unfinished parents finishes (the longest path) and finishes
///   one mean run time later.
pub fn node_times(wf: &WorkflowState, durations: &Durations, running: &HashMap<String, Running>) -> Vec<NodeTimes> {
    let mut times: Vec<NodeTimes> = Vec::with_capacity(wf.nodes.len());
    for node in &wf.nodes {
        let t = match node.state {
            NodeState::Done | NodeState::Dead | NodeState::Cancelled => {
                NodeTimes { finished: true, start: 0.0, finish: 0.0 }
            }
            NodeState::Queued => {
                let mean = durations.task_secs(&node.task);
                let left = running.get(&node.task_id).map_or(mean, |r| r.left(mean));
                NodeTimes { finished: false, start: 0.0, finish: left }
            }
            NodeState::Waiting => {
                // A parent that isn't earlier than the node (a malformed list) is ignored.
                let start = node.parents.iter()
                    .filter_map(|&p| times.get(p))
                    .filter(|t| !t.finished)
                    .fold(0.0, |latest: f64, t| latest.max(t.finish));
                NodeTimes { finished: false, start, finish: start + durations.task_secs(&node.task) }
            }
        };
        times.push(t);
    }
    times
}

/// What the lazy pool's workflow nodes look like across the workflows seen.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PoolEta {
    /// Seconds until the earliest unfinished node using the pool starts; None when none is unfinished.
    pub next_need: Option<f64>,
    /// Nodes using the pool that are done, and all of them.
    pub done: u32,
    pub total: u32,
}

/// Adds the nodes of `wf` to `acc`, one entry per pool its tasks use (`task_pools`: task name -> pools).
pub fn add_workflow(
    acc: &mut HashMap<String, PoolEta>,
    wf: &WorkflowState,
    durations: &Durations,
    running: &HashMap<String, Running>,
    task_pools: &HashMap<String, Vec<String>>,
) {
    if !wf.nodes.iter().any(|n| task_pools.contains_key(&n.task)) {
        return;
    }
    let times = node_times(wf, durations, running);
    for (node, t) in wf.nodes.iter().zip(&times) {
        for pool in task_pools.get(&node.task).into_iter().flatten() {
            let e = acc.entry(pool.clone()).or_default();
            e.total += 1;
            if node.state == NodeState::Done {
                e.done += 1;
            }
            if !t.finished {
                e.next_need = Some(e.next_need.map_or(t.start, |n| n.min(t.start)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::NodeInfo;
    use NodeState::{Cancelled, Dead, Done, Queued, Waiting};

    /// (task, state, parents) per node; the task id is the index.
    fn wf(nodes: &[(&str, NodeState, &[usize])]) -> WorkflowState {
        WorkflowState {
            id: "w".into(),
            name: "w".into(),
            created_at: 0,
            cancelled: false,
            nodes: nodes.iter().enumerate().map(|(i, (task, state, parents))| NodeInfo {
                node: i.to_string(),
                task_id: i.to_string(),
                task: task.to_string(),
                queue: "q".into(),
                state: *state,
                parents: parents.to_vec(),
            }).collect(),
        }
    }

    fn durs(pairs: &[(&str, f64)]) -> Durations {
        let mut d = Durations::default();
        for (t, s) in pairs {
            d.record_task(t, *s);
        }
        d
    }

    fn starts(w: &WorkflowState, d: &Durations, running: &HashMap<String, Running>) -> Vec<f64> {
        node_times(w, d, running).iter().map(|t| t.start).collect()
    }

    /// Tasks running for the given seconds, with no progress estimate.
    fn since(pairs: &[(&str, f64)]) -> HashMap<String, Running> {
        pairs.iter().map(|(id, e)| (id.to_string(), Running { elapsed: *e, remaining: None })).collect()
    }

    /// Tasks with a progress estimate of the given seconds left (elapsed 0, as for a task on another worker).
    fn left(pairs: &[(&str, f64)]) -> HashMap<String, Running> {
        pairs.iter().map(|(id, r)| (id.to_string(), Running { elapsed: 0.0, remaining: Some(*r) })).collect()
    }

    fn pools(pairs: &[(&str, &str)]) -> HashMap<String, Vec<String>> {
        pairs.iter().map(|(t, p)| (t.to_string(), vec![p.to_string()])).collect()
    }

    #[test]
    fn ewma_starts_at_the_first_sample_and_blends_after() {
        let mut d = Durations::default();
        assert_eq!(d.task_secs("t"), 0.0, "unknown is 0");
        d.record_task("t", 10.0);
        assert_eq!(d.task_secs("t"), 10.0);
        d.record_task("t", 20.0);
        assert!((d.task_secs("t") - 13.0).abs() < 1e-9);
        d.record_task("t", f64::NAN);
        d.record_task("t", -1.0);
        assert!((d.task_secs("t") - 13.0).abs() < 1e-9, "unusable samples are ignored");
    }

    #[test]
    fn moves_are_averaged_per_pool_and_kind() {
        let mut d = Durations::default();
        assert_eq!(d.move_secs("tts", MoveKind::Stage), None);
        assert_eq!(d.record_move("tts", MoveKind::Stage, 10.0), Some(10.0));
        let blended = d.record_move("tts", MoveKind::Stage, 20.0).unwrap();
        assert!((blended - 13.0).abs() < 1e-9);
        assert_eq!(d.move_secs("tts", MoveKind::Stage), Some(blended));
        assert_eq!(d.move_secs("tts", MoveKind::Unpark), None);
        assert_eq!(d.move_secs("gemma", MoveKind::Stage), None);
        assert_eq!(d.record_move("tts", MoveKind::Park, f64::INFINITY), None);
        for k in [MoveKind::Build, MoveKind::Stage, MoveKind::Park, MoveKind::Unpark] {
            assert_eq!(MoveKind::parse(k.as_str()), Some(k));
        }
        assert_eq!(MoveKind::parse("bogus"), None);
    }

    #[test]
    fn a_chain_adds_up() {
        let w = wf(&[("a", Queued, &[]), ("b", Waiting, &[0]), ("c", Waiting, &[1])]);
        let d = durs(&[("a", 10.0), ("b", 5.0), ("c", 2.0)]);
        let t = node_times(&w, &d, &HashMap::new());
        assert_eq!(t.iter().map(|t| t.start).collect::<Vec<_>>(), vec![0.0, 10.0, 15.0]);
        assert_eq!(t.iter().map(|t| t.finish).collect::<Vec<_>>(), vec![10.0, 15.0, 17.0]);
    }

    #[test]
    fn a_diamond_waits_for_its_slowest_parent() {
        let w = wf(&[("a", Done, &[]), ("b", Queued, &[0]), ("c", Queued, &[0]), ("d", Waiting, &[1, 2])]);
        let d = durs(&[("a", 1.0), ("b", 4.0), ("c", 9.0), ("d", 3.0)]);
        let t = node_times(&w, &d, &HashMap::new());
        assert_eq!(t[3].start, 9.0);
        assert_eq!(t[3].finish, 12.0);
    }

    #[test]
    fn a_running_node_has_what_is_left_of_its_mean() {
        let w = wf(&[("a", Queued, &[]), ("b", Waiting, &[0])]);
        let d = durs(&[("a", 10.0)]);
        assert_eq!(starts(&w, &d, &since(&[("0", 4.0)])), vec![0.0, 6.0]);
        assert_eq!(starts(&w, &d, &since(&[("0", 25.0)])), vec![0.0, 0.0], "never negative");
        // Another node's elapsed time says nothing about this one.
        assert_eq!(starts(&w, &d, &since(&[("7", 4.0)])), vec![0.0, 10.0]);
    }

    #[test]
    fn a_running_node_with_a_progress_estimate_uses_it_instead_of_the_mean() {
        let w = wf(&[("a", Queued, &[]), ("b", Waiting, &[0])]);
        let d = durs(&[("a", 10.0)]);
        // 4 s in against a 10 s mean would leave 6 s; its own reports say 30 s (and, below, 1 s).
        let slow = HashMap::from([("0".to_string(), Running { elapsed: 4.0, remaining: Some(30.0) })]);
        assert_eq!(starts(&w, &d, &slow), vec![0.0, 30.0]);
        let fast = HashMap::from([("0".to_string(), Running { elapsed: 4.0, remaining: Some(1.0) })]);
        assert_eq!(starts(&w, &d, &fast), vec![0.0, 1.0]);
        // Nothing is learned from an estimate of 0 or less, or one that isn't a number, except what it says.
        let done = HashMap::from([("0".to_string(), Running { elapsed: 4.0, remaining: Some(0.0) })]);
        assert_eq!(starts(&w, &d, &done), vec![0.0, 0.0]);
        let junk = HashMap::from([("0".to_string(), Running { elapsed: 4.0, remaining: Some(f64::NAN) })]);
        assert_eq!(starts(&w, &d, &junk), vec![0.0, 6.0], "falls back to the mean");
    }

    #[test]
    fn without_an_estimate_the_mean_rule_is_unchanged() {
        let w = wf(&[("a", Queued, &[]), ("b", Waiting, &[0])]);
        let d = durs(&[("a", 10.0)]);
        let none = HashMap::from([("0".to_string(), Running { elapsed: 4.0, remaining: None })]);
        assert_eq!(starts(&w, &d, &none), starts(&w, &d, &since(&[("0", 4.0)])));
        assert_eq!(starts(&w, &d, &none), vec![0.0, 6.0]);
    }

    #[test]
    fn a_node_running_on_another_worker_uses_its_estimate_and_a_longer_chain_adds_up() {
        let w = wf(&[("a", Queued, &[]), ("b", Waiting, &[0]), ("c", Waiting, &[1])]);
        let d = durs(&[("a", 10.0), ("b", 5.0), ("c", 2.0)]);
        let t = node_times(&w, &d, &left(&[("0", 3.0)]));
        assert_eq!(t.iter().map(|t| t.start).collect::<Vec<_>>(), vec![0.0, 3.0, 8.0]);
    }

    #[test]
    fn unknown_durations_are_zero() {
        let w = wf(&[("a", Queued, &[]), ("b", Waiting, &[0]), ("c", Waiting, &[1])]);
        assert_eq!(starts(&w, &Durations::default(), &HashMap::new()), vec![0.0, 0.0, 0.0]);
        // One known link only counts for itself.
        assert_eq!(starts(&w, &durs(&[("b", 5.0)]), &HashMap::new()), vec![0.0, 0.0, 5.0]);
    }

    #[test]
    fn finished_parents_cost_nothing() {
        let w = wf(&[
            ("a", Done, &[]), ("b", Dead, &[]), ("c", Cancelled, &[]), ("d", Waiting, &[0, 1, 2]),
        ]);
        let d = durs(&[("a", 30.0), ("b", 30.0), ("c", 30.0), ("d", 2.0)]);
        let t = node_times(&w, &d, &HashMap::new());
        assert!(t[..3].iter().all(|t| t.finished && t.finish == 0.0));
        assert_eq!((t[3].start, t[3].finish), (0.0, 2.0));
    }

    #[test]
    fn waiting_with_an_unfinished_parent_starts_when_it_finishes() {
        let w = wf(&[("a", Done, &[]), ("b", Queued, &[]), ("c", Waiting, &[0, 1])]);
        let d = durs(&[("b", 7.0), ("c", 1.0)]);
        let t = node_times(&w, &d, &HashMap::new());
        assert_eq!((t[2].start, t[2].finish), (7.0, 8.0));
    }

    #[test]
    fn a_parent_that_is_not_earlier_is_ignored() {
        let w = wf(&[("a", Waiting, &[1, 5]), ("b", Queued, &[])]);
        assert_eq!(starts(&w, &durs(&[("b", 3.0)]), &HashMap::new()), vec![0.0, 0.0]);
    }

    #[test]
    fn pool_eta_takes_the_earliest_start_and_counts_progress() {
        // encode x3 -> voice(tts), with one encode done
        let w = wf(&[
            ("encode", Done, &[]), ("encode", Queued, &[]), ("encode", Queued, &[]), ("voice", Waiting, &[0, 1, 2]),
        ]);
        let d = durs(&[("encode", 10.0), ("voice", 4.0)]);
        let running = since(&[("1", 3.0)]);
        let tp = pools(&[("voice", "tts"), ("encode", "gemma")]);
        let mut acc = HashMap::new();
        add_workflow(&mut acc, &w, &d, &running, &tp);
        assert_eq!(acc["tts"], PoolEta { next_need: Some(10.0), done: 0, total: 1 });
        // gemma's encodes: the running one has 7 s left but the other is queued and starts now
        assert_eq!(acc["gemma"], PoolEta { next_need: Some(0.0), done: 1, total: 3 });
    }

    #[test]
    fn pool_eta_is_the_minimum_across_workflows_and_none_when_all_finished() {
        let slow = wf(&[("a", Queued, &[]), ("v", Waiting, &[0])]);
        let fast = wf(&[("a", Done, &[]), ("v", Waiting, &[0])]);
        let done = wf(&[("v", Done, &[])]);
        let d = durs(&[("a", 20.0)]);
        let tp = pools(&[("v", "tts")]);
        let mut acc = HashMap::new();
        add_workflow(&mut acc, &slow, &d, &HashMap::new(), &tp);
        assert_eq!(acc["tts"].next_need, Some(20.0));
        add_workflow(&mut acc, &fast, &d, &HashMap::new(), &tp);
        assert_eq!(acc["tts"].next_need, Some(0.0));
        let mut only_done = HashMap::new();
        add_workflow(&mut only_done, &done, &d, &HashMap::new(), &tp);
        assert_eq!(only_done["tts"], PoolEta { next_need: None, done: 1, total: 1 });
        // A workflow with no pool task adds nothing.
        let mut none = HashMap::new();
        add_workflow(&mut none, &wf(&[("a", Queued, &[])]), &d, &HashMap::new(), &tp);
        assert!(none.is_empty());
    }
}
