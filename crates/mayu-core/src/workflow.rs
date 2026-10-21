//! Workflow types: a DAG of tasks submitted as one unit.
use crate::envelope::Header;
use crate::error::{Error, Result};

/// One task in a workflow. `parents` are indexes of earlier nodes in the spec.
#[derive(Debug, Clone)]
pub struct WorkflowNode {
    pub header: Header,
    pub payload: Vec<u8>,
    pub parents: Vec<usize>,
}

#[derive(Debug, Clone)]
pub struct WorkflowSpec {
    pub id: String,
    pub name: String,
    pub ttl_secs: u64,
    pub nodes: Vec<WorkflowNode>,
}

impl WorkflowSpec {
    /// Generates the workflow id and stamps every header with it and the node's index.
    pub fn new(name: impl Into<String>, ttl_secs: u64, mut nodes: Vec<WorkflowNode>) -> Self {
        let id = ulid::Ulid::generate().to_string();
        for (i, n) in nodes.iter_mut().enumerate() {
            n.header.workflow_id = Some(id.clone());
            n.header.node = Some(i.to_string());
        }
        WorkflowSpec { id, name: name.into(), ttl_secs, nodes }
    }

    /// Parents must precede their children, which makes the graph acyclic by construction.
    pub fn validate(&self) -> Result<()> {
        let bad = |m: String| Err(Error::Config(m));
        if self.nodes.is_empty() {
            return bad("a workflow needs at least one node".into());
        }
        if self.ttl_secs == 0 {
            return bad("workflow ttl must be positive".into());
        }
        for (i, n) in self.nodes.iter().enumerate() {
            if n.header.eta.is_some() {
                return bad(format!("workflow node {i} ({}) has an eta; nodes can't be delayed", n.header.task));
            }
            let stamped = n.header.workflow_id.as_deref() == Some(self.id.as_str())
                && n.header.node.as_deref() == Some(i.to_string().as_str());
            if !stamped {
                return bad(format!("workflow node {i} isn't stamped for it (build the spec with WorkflowSpec::new)"));
            }
            for (j, &p) in n.parents.iter().enumerate() {
                if p >= i {
                    return bad(format!("workflow node {i} lists parent {p}, which isn't an earlier node"));
                }
                if n.parents[..j].contains(&p) {
                    return bad(format!("workflow node {i} lists parent {p} twice"));
                }
            }
        }
        Ok(())
    }

    /// For each node, the nodes that list it as a parent.
    pub fn children(&self) -> Vec<Vec<usize>> {
        let mut out = vec![Vec::new(); self.nodes.len()];
        for (i, n) in self.nodes.iter().enumerate() {
            for &p in &n.parents {
                if let Some(c) = out.get_mut(p) {
                    c.push(i);
                }
            }
        }
        out
    }
}

/// `Waiting` -> `Queued` (in the stream) -> `Done` | `Dead` | `Cancelled`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Waiting,
    Queued,
    Done,
    Dead,
    Cancelled,
}

impl NodeState {
    /// The string stored in the broker.
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeState::Waiting => "waiting",
            NodeState::Queued => "queued",
            NodeState::Done => "done",
            NodeState::Dead => "dead",
            NodeState::Cancelled => "cancelled",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "waiting" => NodeState::Waiting,
            "queued" => NodeState::Queued,
            "done" => NodeState::Done,
            "dead" => NodeState::Dead,
            "cancelled" => NodeState::Cancelled,
            _ => return Err(Error::Broker(format!("unknown workflow node state {s:?}"))),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    /// The node's index in add order, as a string.
    pub node: String,
    pub task_id: String,
    pub task: String,
    pub queue: String,
    pub state: NodeState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowState {
    pub id: String,
    pub name: String,
    pub created_at: u64,
    pub cancelled: bool,
    pub nodes: Vec<NodeInfo>,
}

/// `state` is `cancelled`, `done`, `failed` or `running`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkflowSummary {
    pub state: &'static str,
    pub waiting: usize,
    pub queued: usize,
    pub done: usize,
    pub dead: usize,
    pub cancelled: usize,
}

impl WorkflowState {
    pub fn summary(&self) -> WorkflowSummary {
        let count = |s: NodeState| self.nodes.iter().filter(|n| n.state == s).count();
        let mut sum = WorkflowSummary {
            state: "running",
            waiting: count(NodeState::Waiting),
            queued: count(NodeState::Queued),
            done: count(NodeState::Done),
            dead: count(NodeState::Dead),
            cancelled: count(NodeState::Cancelled),
        };
        sum.state = if self.cancelled {
            "cancelled"
        } else if sum.done == self.nodes.len() {
            "done"
        } else if sum.dead > 0 {
            "failed"
        } else {
            "running"
        };
        sum
    }
}
