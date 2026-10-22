import importlib
import os
import pathlib
import sys
import time

import pytest

import mayu
from mayu import App, Env, TaskFailed
from mayu import env as env_module
from mayu.worker import WorkerRunner

HERE = pathlib.Path(__file__).parent
ROOT = HERE.parents[1]
WAIT = 20


def _until(fn, what, timeout=WAIT):
    deadline = time.monotonic() + timeout
    while True:
        value = fn()
        if value:
            return value
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.02)


def _other_python():
    """An interpreter of another minor version than this one, if the dev venvs have one."""
    for venv in (".venv-313", ".venv"):
        python = ROOT / venv / "bin" / "python"
        if python.exists():
            out = os.popen(f"{python} -c 'import sys; print(*sys.version_info[:2])'").read().split()
            if out and tuple(map(int, out)) != tuple(sys.version_info[:2]):
                return str(python), tuple(map(int, out))
    return None, None


@pytest.fixture
def env_app(monkeypatch, redis_url, queue, tmp_path):
    """`env_app` imported afresh for this test, on its own queue; its children inherit the environment."""
    monkeypatch.setenv("MAYU_BROKER", redis_url)
    monkeypatch.setenv("MAYU_DEFAULT_QUEUE", queue)
    monkeypatch.setenv("MAYU_TEST_FILE", str(tmp_path / "log"))
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(filter(None, [str(HERE), os.environ.get("PYTHONPATH")])))
    for name in ("env_app", "env_app_impl"):
        sys.modules.pop(name, None)
    yield importlib.import_module("env_app")
    for name in ("env_app", "env_app_impl"):
        sys.modules.pop(name, None)


@pytest.fixture
def worker(env_app, running):
    def start(**kwargs):
        kwargs.setdefault("heartbeat_interval", 0.05)
        kwargs.setdefault("shutdown_grace", 10)
        kwargs.setdefault("pools", {"model": 1})  # env_app has tasks that use it
        kwargs.setdefault("app_path", "env_app:app")
        return running(env_app.app, **kwargs)

    return start


def _logged(env_app):
    path = pathlib.Path(os.environ["MAYU_TEST_FILE"])
    return path.read_text().splitlines() if path.exists() else []


# -- declaring (no worker) -------------------------------------------------------------------------------------------

def test_env_is_registered_and_exported(app):
    legacy = app.env("legacy", python="/opt/py313/bin/python", imports=["a.b"])
    assert isinstance(legacy, Env) and mayu.Env is Env
    assert (legacy.name, legacy.python, legacy.imports) == ("legacy", "/opt/py313/bin/python", ("a.b",))
    assert app.envs == {"legacy": legacy}
    assert repr(legacy) == "<Env 'legacy' python='/opt/py313/bin/python'>"
    assert app.env("plain", python="python").imports == ()
    with pytest.raises(AttributeError):
        legacy.python = "other"


@pytest.mark.parametrize("name, kwargs, exc, message", [
    ("a,b", {"python": "p"}, ValueError, "bad env name"),
    ("a=b", {"python": "p"}, ValueError, "bad env name"),
    ("a b", {"python": "p"}, ValueError, "bad env name"),
    ("", {"python": "p"}, ValueError, "bad env name"),
    (3, {"python": "p"}, TypeError, "env name must be a string"),
    ("x", {"python": ""}, ValueError, "needs python="),
    ("x", {"python": None}, ValueError, "needs python="),
    ("x", {"python": "p", "imports": "mod"}, TypeError, "imports of env 'x' must be a list"),
    ("x", {"python": "p", "imports": [3]}, TypeError, "imports of env 'x' must be a list"),
])
def test_bad_env_declarations_are_rejected(app, name, kwargs, exc, message):
    with pytest.raises(exc, match=message):
        app.env(name, **kwargs)
    assert app.envs == {}


def test_duplicate_env_is_rejected(app):
    app.env("legacy", python="p")
    with pytest.raises(ValueError, match="env 'legacy' is already registered"):
        app.env("legacy", python="q")


def test_task_and_pool_take_an_env_or_its_name(app):
    legacy = app.env("legacy", python="p")

    @app.task(env=legacy)
    def by_object(): ...

    @app.task(env="legacy")
    def by_name(): ...

    @app.task(env="not-declared-yet")
    def forward(): ...

    @app.task
    def plain(): ...

    declared = app.declare(env=legacy, name="declared")(lambda: None)
    assert (by_object.env, by_name.env, forward.env, plain.env, declared.env) == (
        "legacy", "legacy", "not-declared-yet", None, "legacy")
    assert app.pool("m", env=legacy)(lambda ctx: None).env == "legacy"
    assert app.pool("n", env="legacy")(lambda ctx: None).env == "legacy"
    assert app.pool("o")(lambda ctx: None).env is None


def test_bad_env_options_are_rejected(app):
    other = App(broker=app.broker_url).env("legacy", python="p")
    with pytest.raises(ValueError, match="was not made by this app"):
        app.task(env=other)
    with pytest.raises(TypeError, match="env must be an Env"):
        app.task(env=3)
    with pytest.raises(ValueError, match="bad env name"):
        app.task(env="a,b")
    with pytest.raises(ValueError, match="eager only"):
        app.pool("m", env="legacy", linger=5)
    with pytest.raises(ValueError, match="eager only"):
        app.pool("m", env="legacy", parked={"ram_gb": 1})
    assert app.pools == {}


def test_stub_with_an_env_counts_as_implemented(app):
    app.env("legacy", python="p")
    app.declare(name="remote", env="legacy", queue="q-env")(lambda: None)
    app.declare(name="elsewhere", queue="q-other")(lambda: None)
    assert "q-env" in app.queues(implemented=True)
    assert "q-other" not in app.queues(implemented=True)


# -- the handshake (no worker) ---------------------------------------------------------------------------------------

def test_hello_check_accepts_another_python_and_a_patch_release():
    mine = env_module.hello()
    assert env_module.check_hello({**mine, "python": "3.9.1", "gil": True}) is None
    major, minor, patch = mine["mayu"].split(".")[:3]
    assert env_module.check_hello({**mine, "mayu": f"{major}.{minor}.99"}) is None


def test_hello_check_refuses_another_mayu_or_protocol():
    mine = env_module.hello()
    major, minor = (int(x) for x in mine["mayu"].split(".")[:2])
    problem = env_module.check_hello({**mine, "mayu": f"{major}.{minor + 1}.0"})
    assert mine["mayu"] in problem and f"{major}.{minor + 1}.0" in problem
    assert env_module.check_hello({**mine, "mayu": f"{major + 1}.{minor}.0"})
    assert "protocol" in env_module.check_hello({**mine, "protocol": mine["protocol"] + 1})


# -- worker validation (no child is started) ---------------------------------------------------------------------------

def _runner(app, **kwargs):
    return WorkerRunner(app, threads=1, queues=[app.default_queue], **kwargs)


def test_unknown_env_is_refused_when_the_worker_is_built(app):
    @app.task(env="nope")
    def t(): ...

    with pytest.raises(ValueError, match=r"task '.*t' runs in env 'nope', which this process does not define"):
        _runner(app, app_path="m:app")


def test_unserved_queue_with_an_unknown_env_is_not_checked(app):
    @app.task(env="nope", queue="elsewhere")
    def t(): ...

    @app.task
    def ok(): ...

    assert _runner(app)._env_children == {}


def test_env_tasks_need_an_app_path(app):
    legacy = app.env("legacy", python="p")

    @app.task(env=legacy)
    def t(): ...

    with pytest.raises(ValueError, match="pass app_path='module:app'"):
        _runner(app)


def test_envs_override_must_name_a_declared_env(app):
    app.env("legacy", python="p")
    with pytest.raises(ValueError, match="--envs names env 'other', which this process does not define"):
        _runner(app, envs={"other": "/x/python"})
    with pytest.raises(ValueError, match="interpreter of 'legacy' is empty"):
        _runner(app, envs={"legacy": " "})
    with pytest.raises(TypeError, match="envs must be a dict"):
        _runner(app, envs=["legacy"])


def test_envs_override_replaces_the_declared_python(app):
    legacy = app.env("legacy", python="/declared/python")

    @app.task(env=legacy)
    def t(): ...

    assert _runner(app, app_path="m:app")._env_children["legacy"].python == "/declared/python"
    assert _runner(app, app_path="m:app", envs={"legacy": "/here/python"})._env_children["legacy"].python == \
        "/here/python"


def test_a_task_and_its_pools_must_share_an_env(app):
    legacy = app.env("legacy", python="p")

    @app.pool("outer")
    def outer(ctx): ...

    @app.pool("inner", env=legacy)
    def inner(ctx): ...

    @app.task(env=legacy)
    def wants_outer(m=app.use("outer")): ...

    with pytest.raises(ValueError, match="runs in env 'legacy' but pool 'outer' is in the worker"):
        _runner(app, app_path="m:app", pools={"outer": 1})

    app2 = App(broker=app.broker_url, default_queue=app.default_queue)
    legacy2 = app2.env("legacy", python="p")
    app2.pool("inner", env=legacy2)(lambda ctx: None)

    @app2.task
    def wants_inner(m=app2.use("inner")): ...

    with pytest.raises(ValueError, match="runs in the worker but pool 'inner' is in env 'legacy'"):
        _runner(app2, app_path="m:app", pools={"inner": 1})


def test_env_pools_need_eager_mode(app):
    legacy = app.env("legacy", python="p")
    app.pool("inner", env=legacy)(lambda ctx: None)

    @app.task(env=legacy)
    def t(m=app.use("inner")): ...

    with pytest.raises(ValueError, match="pools in an env need --pool-mode eager"):
        _runner(app, app_path="m:app", pools={"inner": 1}, pool_mode="lazy")
    w = _runner(app, app_path="m:app", pools={"inner": 2})
    assert w.capacity == {"pool:inner": 2.0}
    assert w._env_children["legacy"].pools == {"inner": 2}


def test_env_pools_reserve_resources_like_any_pool(app):
    legacy = app.env("legacy", python="p")
    app.pool("inner", env=legacy, vram_gb=3)(lambda ctx: None)

    @app.task(env=legacy, resources={"vram_gb": 1})
    def t(m=app.use("inner")): ...

    w = _runner(app, app_path="m:app", pools={"inner": 2}, resources={"vram_gb": 8})
    assert w.capacity == {"vram_gb": 2.0, "pool:inner": 2.0}
    with pytest.raises(ValueError, match="pools reserve vram_gb=9"):
        _runner(app, app_path="m:app", pools={"inner": 3}, resources={"vram_gb": 8})


def test_a_worker_without_env_tasks_has_no_children(app, running):
    app.env("legacy", python="/does/not/exist")

    @app.task
    def plain():
        return os.getpid()

    with running(app, threads=1) as w:
        assert w._env_children == {}
        assert plain.send().result(timeout=WAIT) == os.getpid()


# -- running in the child --------------------------------------------------------------------------------------------

def test_task_runs_in_a_child_process(env_app, worker):
    with worker() as w:
        assert set(w._env_children) == {"legacy"}
        assert env_app.add.send(2, 3).result(timeout=WAIT) == 5
        pid, version = env_app.where.send().result(timeout=WAIT)
        assert pid != os.getpid() and tuple(version) == tuple(sys.version_info[:2])
        assert env_app.local.send().result(timeout=WAIT) == os.getpid()  # tasks without env stay in the worker
        assert env_app.where.send().result(timeout=WAIT)[0] == pid  # one long-lived child


def test_arguments_and_results_round_trip(env_app, worker):
    with worker():
        assert env_app.add.send(a=40, b=2).result(timeout=WAIT) == 42
        assert env_app.add.send(7).result(timeout=WAIT) == 7
        assert env_app.aadd.send(1, 2).result(timeout=WAIT) == 3


def test_declared_task_is_implemented_in_the_child(env_app, worker):
    assert not env_app.declared.implemented
    with worker():
        doubled, pid = env_app.declared.send(21).result(timeout=WAIT)
        assert doubled == 42 and pid != os.getpid()


def test_link_callbacks_are_computed_in_the_child(env_app, worker):
    with worker():
        env_app.add.send(2, 3, link=env_app.record)
        _until(lambda: _logged(env_app) == ["5"], "the link callback to run")


def test_failure_has_the_childs_traceback(env_app, worker):
    with worker():
        with pytest.raises(TaskFailed, match="ValueError") as e:
            env_app.fail.send().result(timeout=WAIT)
        assert "legacy failure" in e.value.traceback and "env_app.py" in e.value.traceback


def test_unserializable_result_fails_the_task(env_app, worker):
    with worker():
        with pytest.raises(TaskFailed, match="result not serializable"):
            env_app.unpicklable.send().result(timeout=WAIT)


def test_retry_and_retry_on_apply(env_app, worker, tmp_path):
    with worker():
        assert env_app.retry_later.send(str(tmp_path / "a")).result(timeout=WAIT) == "second try"
        assert env_app.flaky.send(str(tmp_path / "b")).result(timeout=WAIT) == "retried"


def test_progress_is_visible_on_the_handle(env_app, worker, tmp_path):
    go = tmp_path / "go"
    with worker():
        handle = env_app.progressing.send(str(go))
        p = _until(lambda: (lambda p: p if p is not None and p.fraction == 0.5 else None)(handle.progress()),
                   "progress 1/2")
        assert (p.done, p.total) == (1, 2)
        go.touch()
        assert handle.result(timeout=WAIT) == "done"


def test_soft_limit_is_raised_in_the_child(env_app, worker):
    with worker():
        assert env_app.soft_caught.send().result(timeout=WAIT) == "caught in the child"
        with pytest.raises(TaskFailed, match="SoftTimeLimitExceeded"):
            env_app.soft_uncaught.send().result(timeout=WAIT)


def test_hard_limit_kills_the_child_and_the_next_task_gets_a_new_one(env_app, worker):
    with worker():
        before = env_app.where.send().result(timeout=WAIT)[0]
        t = time.monotonic()
        with pytest.raises(TaskFailed, match="TimeLimitExceeded"):
            env_app.hang.send().result(timeout=WAIT)
        assert time.monotonic() - t < 10
        after = env_app.where.send().result(timeout=WAIT)[0]
        assert after != before


def test_a_crashing_child_means_the_task_runs_again(env_app, worker, tmp_path):
    with worker():
        before = env_app.where.send().result(timeout=WAIT)[0]
        assert env_app.crash_once.send(str(tmp_path / "flag")).result(timeout=WAIT) == "recovered"
        assert env_app.where.send().result(timeout=WAIT)[0] != before


def test_a_crash_reruns_the_other_tasks_in_flight(env_app, worker, tmp_path):
    started, go = tmp_path / "started", tmp_path / "go"
    with worker():
        waiting = env_app.wait_for.send(str(started), str(go))
        _until(started.exists, "the first task to start in the child")
        started.unlink()
        assert env_app.crash_once.send(str(tmp_path / "flag")).result(timeout=WAIT) == "recovered"
        _until(started.exists, "the first task to start again")
        go.touch()
        assert waiting.result(timeout=WAIT) == "finished"


def test_non_threadsafe_env_task_runs_one_at_a_time(env_app, worker):
    with worker(threads=4):
        handles = [env_app.exclusive.send() for _ in range(6)]
        assert [h.result(timeout=WAIT) for h in handles] == [False] * 6


def test_pool_is_built_in_the_child_and_closed_at_stop(env_app, worker):
    with worker(pools={"model": 2}) as w:
        assert w._states == {}  # the worker holds no instance
        assert w.capacity == {"pool:model": 2.0}
        seen = [env_app.use_model.send().result(timeout=WAIT) for _ in range(6)]
        assert {m["pid"] for m in seen} != {os.getpid()} and len({m["pid"] for m in seen}) == 1
        assert {m["index"] for m in seen} <= {0, 1}
    assert sorted(_logged(env_app)) == ["closed 0", "closed 1"]


def test_discarded_pool_instance_is_rebuilt_in_the_child(env_app, worker):
    with worker(pools={"model": 1}):
        first = env_app.use_model.send().result(timeout=WAIT)
        with pytest.raises(TaskFailed, match="DiscardInstance"):
            env_app.discard_model.send().result(timeout=WAIT)
        # the ledger gave the slot back after the rebuild: this would wait forever otherwise
        second = _until(lambda: env_app.use_model.send().result(timeout=WAIT), "a rebuilt instance")
        assert second["pid"] == first["pid"] and second["serial"] != first["serial"]


def test_pools_are_rebuilt_after_a_crash(env_app, worker, tmp_path):
    with worker(pools={"model": 1}):
        first = env_app.use_model.send().result(timeout=WAIT)
        assert env_app.crash_once.send(str(tmp_path / "flag")).result(timeout=WAIT) == "recovered"
        second = env_app.use_model.send().result(timeout=WAIT)
        assert second["pid"] != first["pid"]


def test_worker_with_a_missing_interpreter_refuses_to_start(worker):
    with pytest.raises(RuntimeError, match="could not start the env 'legacy' runner"):
        with worker(envs={"legacy": "/does/not/exist/python"}):
            pytest.fail("the worker started")


def test_worker_whose_app_does_not_import_in_the_child_refuses_to_start(worker):
    with pytest.raises(RuntimeError, match="failed to start"):
        with worker(app_path="no_such_module:app"):
            pytest.fail("the worker started")


@pytest.mark.skipif(_other_python()[0] is None, reason="needs the .venv / .venv-313 interpreters")
def test_env_can_be_another_python_version(env_app, worker):
    python, version = _other_python()
    with worker(envs={"legacy": python}):
        pid, got = env_app.where.send().result(timeout=60)
        assert tuple(got) == version != tuple(sys.version_info[:2])
        assert env_app.add.send(1, 2).result(timeout=WAIT) == 3


def test_child_inherits_the_workers_log_configuration(app, monkeypatch):
    from mayu import envproc, log

    seen = {}

    def popen(argv, **kwargs):
        seen.update(kwargs["env"])
        raise OSError("not really starting it")

    monkeypatch.setattr(log, "_configured", ("json", "DEBUG"))
    monkeypatch.setattr(envproc.subprocess, "Popen", popen)
    child = envproc.EnvChild(None, app.env("legacy", python="p"), "p", "m:app", {}, True, False)
    with pytest.raises(envproc.EnvSpawnError, match="not really starting it"):
        child.start()
    assert (seen["MAYU_LOG_FORMAT"], seen["MAYU_LOG_LEVEL"]) == ("json", "DEBUG")
