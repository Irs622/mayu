"""`mayu inspect` and `mayu dead`: queue depths, live workers, and dead-letter replay, read
straight from Redis."""
import datetime as dt

_PAGE = 100


def _queues(app, explicit) -> list[str]:
    if explicit:
        return list(explicit)
    return sorted(set(app.queues()) | set(app.client.known_queues()))


def _workflows(client) -> list[dict]:
    """Workflows that haven't finished (done ones expire on their TTL), without the per-node detail."""
    keep = ("id", "name", "state", "created_at", "waiting", "queued", "done", "dead", "cancelled_nodes")
    return [{k: w[k] for k in keep} for w in client.workflows() if w["state"] != "done"]


def _running(workers: list[dict]) -> list[dict]:
    """Tasks the workers last published as running, oldest first (as fresh as each worker's last heartbeat)."""
    keys = ("task", "id", "done", "total", "fraction", "remaining_s", "workflow_id", "node", "started_ms")
    out = [{"worker": w["worker_id"], **{k: t[k] for k in keys}} for w in workers for t in w["tasks"]]
    return sorted(out, key=lambda t: t["started_ms"])


def inspect(app, queues: list[str] | None = None) -> dict:
    client = app.client
    workers = client.workers()
    out = {"queues": {}, "workers": workers, "running": _running(workers), "workflows": _workflows(client)}
    for q in _queues(app, queues):
        s = client.queue_stats(q)
        out["queues"][q] = {"ready": max(0, s["stream"] - s["pending"]), "claimed": s["pending"],
                            "delayed": s["delayed"], "dead": s["dead"]}
    return out


def _table(rows: list[list[str]]) -> str:
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    return "\n".join("  ".join(c.ljust(w) for c, w in zip(r, widths)).rstrip() for r in rows)


def _ago(ms: int) -> str:
    s = max(0, ms) // 1000
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60}s"
    return f"{s // 3600}h{s % 3600 // 60}m"


def _progress_cell(t: dict) -> str:
    if t["fraction"] is None:
        return "-"
    pct = f"{round(t['fraction'] * 100)} %"
    return f"{t['done']:g}/{t['total']:g} ({pct})" if t["total"] is not None else pct


def format_inspect(data: dict, now_ms: int) -> str:
    rows = [["QUEUE", "READY", "CLAIMED", "DELAYED", "DEAD"]]
    for q, s in data["queues"].items():
        rows.append([q, str(s["ready"]), str(s["claimed"]), str(s["delayed"]), str(s["dead"])])
    parts = [_table(rows), "", f"WORKERS ({len(data['workers'])})"]
    if data["workers"]:
        wrows = [["ID", "HOST", "PID", "QUEUES", "INFLIGHT", "UP", "LAST SEEN"]]
        for w in data["workers"]:
            wrows.append([w["worker_id"], w["hostname"], str(w["pid"]), ",".join(w["queues"]),
                          f"{w['inflight']}/{w['concurrency']}", _ago(now_ms - w["started_at"]),
                          _ago(now_ms - w["last_seen"]) + " ago"])
        parts.append(_table(wrows))
    else:
        parts.append("(none)")
    if data.get("running"):  # only when something runs, so an idle `inspect` reads as it always did
        parts += ["", f"RUNNING ({len(data['running'])})"]
        rrows = [["TASK", "ID", "WORKER", "PROGRESS", "LEFT", "WORKFLOW"]]
        for t in data["running"]:
            left = "-" if t["remaining_s"] is None else "~" + _ago(int(t["remaining_s"] * 1000))
            rrows.append([t["task"], t["id"], t["worker"], _progress_cell(t), left, t["workflow_id"] or "-"])
        parts.append(_table(rrows))
    parts += ["", f"WORKFLOWS ({len(data['workflows'])})"]
    if data["workflows"]:
        frows = [["ID", "NAME", "STATE", "WAITING", "QUEUED", "DONE", "DEAD", "AGE"]]
        for w in data["workflows"]:
            frows.append([w["id"], w["name"], w["state"], str(w["waiting"]), str(w["queued"]), str(w["done"]),
                          str(w["dead"]), _ago(now_ms - w["created_at"])])
        parts.append(_table(frows))
    else:
        parts.append("(none)")
    return "\n".join(parts)


def dead_list(app, queue: str, limit: int) -> list[dict]:
    return app.client.dead_letters(queue, None, None, max(1, limit))


def format_dead(entries: list[dict]) -> str:
    if not entries:
        return "(no dead letters)"
    lines = []
    for e in entries:
        when = dt.datetime.fromtimestamp(e["dead_at_ms"] / 1000, dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        error = (e["error"].splitlines() or [""])[0][:120]
        task = e["task"] or "<undecodable>"
        attempt = "-" if e["attempt"] is None else str(e["attempt"])
        lines.append(f"{when}  {e['id'] or '-'}  {task}  attempt {attempt}  {error}")
    return "\n".join(lines)


def dead_replay(app, queue: str, *, limit: int | None = None, ids=None) -> tuple[int, int]:
    """Replays the dead letters that exist now (tasks that fail again meanwhile are not picked up).
    Returns (replayed, skipped because undecodable)."""
    client = app.client
    until = client.last_dead_id(queue)
    wanted = set(ids or ())
    replayed = skipped = 0
    after = None
    while until is not None and (limit is None or replayed < limit):
        page = client.dead_letters(queue, after, until, _PAGE)
        if not page:
            break
        after = page[-1]["entry_id"]
        picks = []
        for e in page:
            if e["id"] is None:
                skipped += 1
            elif not wanted or e["id"] in wanted:
                picks.append(e["entry_id"])
        if limit is not None:
            picks = picks[: limit - replayed]
        if picks:
            replayed += client.replay_dead(queue, picks)
    return replayed, skipped
