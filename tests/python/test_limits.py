from mayu import SoftTimeLimitExceeded
from mayu.limits import TimeLimits


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def make():
    sent = []
    clock = FakeClock()
    tl = TimeLimits(raise_in_thread=lambda ident, exc: sent.append((ident, exc)) or 1, clock=clock)
    return tl, sent, clock


def test_no_limits_is_not_tracked():
    tl, _, _ = make()
    assert tl.arm(1, 111, None, None) is None
    assert len(tl) == 0


def test_soft_sent_once_then_hard_expires():
    tl, sent, clock = make()
    run = tl.arm(1, 111, 1.0, 2.0, context="ctx")
    assert tl.tick() == [] and sent == []
    clock.t = 1.0
    assert tl.tick() == []
    assert sent == [(111, SoftTimeLimitExceeded)]
    clock.t = 1.5
    tl.tick()
    assert len(sent) == 1, "soft limit is delivered once"
    clock.t = 2.0
    assert tl.tick() == [run]
    assert run.context == "ctx"
    assert len(tl) == 0
    assert tl.disarm(run) is False, "the supervisor owns a hard-expired run"


def test_disarm_owns_outcome_and_clears_pending_soft():
    tl, sent, clock = make()
    run = tl.arm(7, 222, 1.0, None)
    clock.t = 1.0
    tl.tick()
    assert tl.disarm(run) is True
    assert sent[-1] == (222, None), "pending async exception cleared"
    assert tl.disarm(run) is True, "idempotent"


def test_disarm_before_deadline_prevents_soft():
    tl, sent, clock = make()
    run = tl.arm(1, 111, 1.0, 2.0)
    assert tl.disarm(run) is True
    clock.t = 5.0
    assert tl.tick() == []
    assert sent == []


def test_hard_wins_when_both_due():
    tl, sent, clock = make()
    run = tl.arm(1, 111, 1.0, 1.5)
    clock.t = 2.0
    assert tl.tick() == [run]
    assert sent == []
