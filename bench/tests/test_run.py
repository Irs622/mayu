import sys

import pytest
import redis

import run
import workload


class FakeProc:
    def __init__(self, code=None):
        self.returncode = code

    def poll(self):
        return self.returncode


def fake_clock(step=0.01):
    t = [0.0]

    def clock():
        t[0] += step
        return t[0]
    return clock


def _no_sleep(_):
    pass


def test_drive_returns_when_count_reaches_n():
    counts = iter([0, 3, 7, 10])
    samples, peak = run.drive(FakeProc(), lambda: next(counts), 10, timeout=5,
                              memory=lambda: (100, 50), clock=fake_clock(), sleep=_no_sleep)
    assert [c for _, c in samples] == [0, 3, 7, 10]
    assert peak == (100, 50)


def test_drive_fails_fast_when_workers_exit():
    with pytest.raises(run.BenchError, match="exited with code 3 after 0 of 10 tasks"):
        run.drive(FakeProc(3), lambda: 0, 10, timeout=5, clock=fake_clock(), sleep=_no_sleep)


def test_drive_times_out():
    with pytest.raises(run.BenchError, match="timed out after 1s: 0 of 10 tasks done"):
        run.drive(FakeProc(), lambda: 0, 10, timeout=1, clock=fake_clock(0.3), sleep=_no_sleep)


@pytest.fixture
def scratch_db():
    r = redis.Redis.from_url(workload.URL.rsplit("/", 1)[0] + "/14")
    r.flushdb()
    yield r
    r.flushdb()


def test_claim_db_refuses_foreign_keys(scratch_db):
    scratch_db.set("someone:else", "keep me")
    with pytest.raises(run.BenchError, match="refusing to flush"):
        run.claim_db(scratch_db)
    assert scratch_db.get("someone:else") == b"keep me"


def test_claim_db_empties_its_own_db(scratch_db):
    scratch_db.set(run.OWNER_KEY, "1")
    scratch_db.set("bench:done", "7")
    run.claim_db(scratch_db)
    assert scratch_db.keys() == [run.OWNER_KEY.encode()]


def test_claim_db_takes_an_empty_db(scratch_db):
    run.claim_db(scratch_db)
    assert scratch_db.exists(run.OWNER_KEY)


def test_require_nogil_raises_when_gil_enabled(monkeypatch):
    monkeypatch.setattr(workload, "_free_threaded_build", lambda: True)
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: True)
    with pytest.raises(RuntimeError, match="GIL"):
        workload.require_nogil()


def test_require_nogil_is_quiet_when_gil_disabled(monkeypatch):
    monkeypatch.setattr(workload, "_free_threaded_build", lambda: True)
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False)
    workload.require_nogil()


def test_concurrency_must_be_even(capsys):
    with pytest.raises(SystemExit) as e:
        run.main(["mayu-process", "noop", "--concurrency", "7"])
    assert e.value.code == 2
    assert "--concurrency must be an even number" in capsys.readouterr().err
