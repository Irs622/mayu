import time

import msgpack

from .exceptions import TaskFailed
from .progress import Progress
from .serializers import for_content_type

_POLL_CHUNK_MS = 1000  # keep get_result calls short so Ctrl-C stays responsive


class TaskHandle:
    def __init__(self, app, task_id: str):
        self.app = app
        self.id = task_id

    def __repr__(self) -> str:
        return f"TaskHandle({self.id!r})"

    def result(self, timeout: float | None = None):
        if self.app.result_ttl is None:
            raise RuntimeError("results are disabled; pass result_ttl= to App to enable them")
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if deadline is None:
                chunk = _POLL_CHUNK_MS
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"task {self.id} did not finish within {timeout}s")
                chunk = min(_POLL_CHUNK_MS, max(1, int(remaining * 1000)))
            raw = self.app.client.get_result(self.id, chunk)
            if raw is not None:
                break
        record = msgpack.unpackb(raw, raw=False)
        if record["status"] == "success":
            serializer = for_content_type(record["content_type"], allow_pickle=self.app.allow_pickle)
            return serializer.loads(record["value"])
        raise TaskFailed(self.id, record.get("error") or "", record.get("traceback") or "")

    def progress(self) -> Progress | None:
        """What the running task last reported, or None when it isn't running. Scans live workers: for admin and
        UIs, not tight loops.

        A running task that hasn't called `mayu.progress` yet gives a `Progress` whose numbers are None.
        """
        found = None
        for w in self.app.client.workers():
            for t in w["tasks"]:
                # a reclaimed task can run on two workers for a while; the most recent heartbeat wins
                if t["id"] == self.id and (found is None or w["last_seen"] > found[0]["last_seen"]):
                    found = (w, t)
        if found is None:
            return None
        w, t = found
        return Progress(t["done"], t["total"], t["fraction"], t["remaining_s"], w["worker_id"], w["last_seen"] / 1000)
