import time

import pytest

from mayu.task import parse_rate
from mayu.worker import WorkerRunner


@pytest.mark.parametrize("rate,per_sec", [
    ("100/s", 100.0), ("30/m", 0.5), ("36/h", 0.01), ("2", 2.0), (" 6 / m ", 0.1), (10, 10.0), (0.5, 0.5),
])
def test_parse_rate(rate, per_sec):
    assert parse_rate(rate) == pytest.approx(per_sec)


@pytest.mark.parametrize("rate", ["0/s", "-1/s", "abc", "10/d", "", "/s", "nan/s", "inf", 0, True])
def test_parse_rate_rejects_bad_rates(rate):
    with pytest.raises(ValueError):
        parse_rate(rate)


def test_bad_rate_limit_rejected_at_decoration(app):
    with pytest.raises(ValueError, match="rate_limit"):
        @app.task(rate_limit="fast")
        def f():
            pass

    @app.task(rate_limit="10/m")
    def g():
        pass

    assert (g.rate_limit, g.rate_per_sec) == ("10/m", pytest.approx(10 / 60))


def test_rate_limit_spaces_task_starts(app, running):
    starts = []

    @app.task(rate_limit="5/s")
    def tick():
        starts.append(time.monotonic())

    with running(app, threads=4):
        for h in [tick.send() for _ in range(4)]:
            h.result(timeout=10)
    starts.sort()
    gaps = [b - a for a, b in zip(starts, starts[1:])]
    assert min(gaps) >= 0.15, gaps


def test_rate_limited_backlog_does_not_starve_other_tasks(app, running):
    @app.task(rate_limit="1/m")
    def slow_lane():
        pass

    @app.task
    def quick():
        return "ok"

    for _ in range(10):
        slow_lane.send()
    with running(app, threads=1, prefetch_multiplier=1):
        assert quick.send().result(timeout=5) == "ok"
        assert app.client.queue_stats(app.default_queue)["delayed"] == 9


def test_explicit_queue_order_is_strict_priority(app, running, queue):
    hi, lo = f"{queue}-hi", f"{queue}-lo"
    order = []

    @app.task(queue=hi)
    def high(i):
        order.append("hi")

    @app.task(queue=lo)
    def low(i):
        order.append("lo")

    handles = [low.send(i) for i in range(4)] + [high.send(i) for i in range(4)]
    with running(app, threads=1, queues=[hi, lo], prefetch_multiplier=1) as w:
        assert w.strict_priority
        for h in handles:
            h.result(timeout=10)
    assert order == ["hi"] * 4 + ["lo"] * 4


def test_default_queues_share_fairly(app, queue):
    hi, lo = f"{queue}-hi", f"{queue}-lo"
    order = []

    @app.task(queue=hi)
    def high(i):
        order.append("hi")

    @app.task(queue=lo)
    def low(i):
        order.append("lo")

    handles = [high.send(i) for i in range(4)] + [low.send(i) for i in range(4)]
    w = WorkerRunner(app, threads=1, prefetch_multiplier=1)  # every app queue, equal priority
    assert not w.strict_priority
    w.start()
    try:
        for h in handles:
            h.result(timeout=10)
    finally:
        w.stop()
        w.join()
    assert "lo" in order[:4], order
