"""Workflows: a DAG of tasks submitted as one unit. A node runs once all its parents have succeeded."""
import math
import threading
import time

from . import otel
from .handle import TaskHandle
from .task import Signature, Task

_POLL_INTERVAL = 0.1
_FINISHED = frozenset({"done", "failed", "cancelled", "expired"})


class Node:
    """A task added to a workflow; pass it as ``after=`` to run another node once it has succeeded."""

    def __init__(self, workflow: "Workflow", index: int, task: Task):
        self.workflow = workflow
        self.index = index
        self.task = task

    def __repr__(self) -> str:
        return f"<Node {self.index} {self.task.name}>"


class Workflow:
    """Builds a DAG with ``add`` and writes it to the broker with ``send``.

    A ``Workflow`` is a builder: don't share it between threads while adding to it. ``send`` is guarded, so two
    threads sending the same workflow submit it once and the other gets a ``ValueError``.
    """

    def __init__(self, app, name: str, ttl: float):
        if not isinstance(name, str):
            raise TypeError(f"workflow name must be a string, got {type(name).__name__}")
        if isinstance(ttl, bool) or not isinstance(ttl, (int, float)):
            raise TypeError(f"workflow ttl must be a number of seconds, got {ttl!r}")
        if not math.isfinite(ttl) or ttl <= 0:
            raise ValueError(f"workflow ttl must be a positive number of seconds, got {ttl!r}")
        self.app = app
        self.name = name
        self.ttl = ttl
        self.id: str | None = None  # set by send()
        self._nodes: list[tuple[Node, Signature, list[int]]] = []
        self._sent = False
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        return f"<Workflow {self.name!r} nodes={len(self._nodes)}{' sent' if self._sent else ''}>"

    def add(self, sig, after=()) -> Node:
        """Adds a task (or ``task.s(...)``) that runs once every node in ``after`` has succeeded."""
        if isinstance(sig, Task):
            sig = Signature(sig)
        elif not isinstance(sig, Signature):
            raise TypeError(f"add() takes a task or task.s(...), got {sig!r}")
        if sig.task.app is not self.app:
            raise ValueError(f"{sig.task.name} belongs to a different App than this workflow")
        sig._check_node()
        parents = self._parents(after)
        with self._lock:
            if self._sent:
                raise ValueError("this workflow was already sent; build a new one with app.workflow()")
            node = Node(self, len(self._nodes), sig.task)
            self._nodes.append((node, sig, parents))
        return node

    def _parents(self, after) -> list[int]:
        items = [after] if isinstance(after, Node) else after
        try:
            items = list(items)
        except TypeError:
            raise TypeError(f"after must be a node or a list of nodes, got {after!r}") from None
        out: list[int] = []
        for n in items:
            if not isinstance(n, Node):
                raise TypeError(f"after must be a node or a list of nodes, got {n!r} in the list")
            if n.workflow is not self:
                raise ValueError(f"{n!r} belongs to a different workflow; nodes can only depend on this one's")
            if n.index not in out:
                out.append(n.index)
        return out

    def send(self) -> "WorkflowHandle":
        """Writes the workflow to the broker and enqueues the nodes that have no parents."""
        with self._lock:
            if self._sent:
                raise ValueError("this workflow was already sent; build a new one with app.workflow()")
            if not self._nodes:
                raise ValueError("a workflow needs at least one task; call add() before send()")
            serializer = self.app.serializer
            nodes = []
            for _, sig, parents in self._nodes:
                payload = serializer.dumps([list(sig.args), sig.kwargs])
                nodes.append((sig.task.name, sig.task.queue, payload, serializer.content_type, sig.task.max_retries,
                              parents, None))
            queues = sorted({sig.task.queue for _, sig, _ in self._nodes})
            span, traceparent = otel.start_send("workflow:" + self.name, ",".join(queues))
            if traceparent is not None:
                nodes = [n[:-1] + (traceparent,) for n in nodes]
            try:
                wf_id, task_ids = self.app.client.submit_workflow(self.name, max(1, math.ceil(self.ttl)), nodes)
            except BaseException as e:
                otel.end(span, e)
                raise
            otel.set_message_id(span, wf_id)
            otel.end(span)
            self._sent = True
            self.id = wf_id
        return WorkflowHandle(self.app, wf_id, task_ids)


class WorkflowHandle:
    """A sent workflow: ``nodes`` has a ``TaskHandle`` per node in add order."""

    def __init__(self, app, workflow_id: str, task_ids: list[str]):
        self.app = app
        self.id = workflow_id
        self.nodes = [TaskHandle(app, t) for t in task_ids]

    def __repr__(self) -> str:
        return f"WorkflowHandle({self.id!r})"

    def node(self, node: Node) -> TaskHandle:
        """The ``TaskHandle`` of a node of this workflow."""
        if not isinstance(node, Node) or node.workflow.id != self.id:
            raise ValueError(f"{node!r} is not a node of workflow {self.id}")
        return self.nodes[node.index]

    def status(self) -> dict:
        """The workflow's ``state`` (running, done, failed, cancelled or expired), node counts and per-node states."""
        found = self.app.client.workflow(self.id)
        if found is None:
            return {"id": self.id, "state": "expired", "nodes": []}
        return found

    def cancel(self) -> bool:
        """Stops the workflow from releasing more nodes; False if it is unknown or already finished."""
        return self.app.client.cancel_workflow(self.id)

    def wait(self, timeout: float | None = None) -> str:
        """Blocks until the workflow is done, failed, cancelled or expired, and returns that state."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            state = self.status()["state"]
            if state in _FINISHED:
                return state
            if deadline is None:
                time.sleep(_POLL_INTERVAL)
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"workflow {self.id} did not finish within {timeout}s (state: {state})")
            time.sleep(min(_POLL_INTERVAL, remaining))
