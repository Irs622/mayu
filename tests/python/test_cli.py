import os
import pathlib
import signal
import subprocess
import sys

import pytest

from mayu import App
from mayu.cli import load_app, parse_gpus, parse_pools, parse_preload, parse_resources

HERE = pathlib.Path(__file__).parent


def _env(redis_url, queue):
    return {**os.environ, "MAYU_BROKER": redis_url, "MAYU_DEFAULT_QUEUE": queue,
            "PYTHONPATH": str(HERE), "MAYU_GPU_MONITOR": "off"}  # off: the machine's real GPU stays out of the tests


def test_worker_cli_runs_tasks_and_exits_cleanly_on_sigterm(redis_url, queue):
    proc = subprocess.Popen(
        [sys.executable, "-m", "mayu", "worker", "sample_app:app", "--mode", "thread", "--threads", "2"],
        env=_env(redis_url, queue), stderr=subprocess.PIPE, text=True,
    )
    try:
        producer = App(broker=redis_url, result_ttl=60, default_queue=queue)

        @producer.task(name="sample.add")
        def add(a, b):
            pass

        assert add.send(20, 22).result(timeout=15) == 42
    finally:
        proc.send_signal(signal.SIGTERM)
        _, err = proc.communicate(timeout=15)
    assert proc.returncode == 0, err


def test_bad_app_path_exits_2(redis_url, queue):
    r = subprocess.run([sys.executable, "-m", "mayu", "worker", "nope_module:app"],
                       env=_env(redis_url, queue), capture_output=True, text=True, timeout=30)
    assert r.returncode == 2
    assert "nope_module" in r.stderr


def test_load_app_rejects_non_app(monkeypatch, redis_url, queue):
    monkeypatch.syspath_prepend(str(HERE))
    monkeypatch.setenv("MAYU_BROKER", redis_url)
    with pytest.raises(TypeError):
        load_app("sample_app:add")
    with pytest.raises(ValueError):
        load_app("sample_app")


def test_worker_cli_rejects_queue_with_unimplemented_tasks(tmp_path, redis_url, queue):
    (tmp_path / "decl_app.py").write_text(
        "from mayu import App\n"
        "app = App()\n"
        "@app.declare(name='decl.gpu', queue='gpu-q')\n"
        "def gpu(): ...\n"
    )
    cmd = [sys.executable, "-m", "mayu", "worker", "decl_app:app", "--mode", "thread", "--queues", "gpu-q"]
    r = subprocess.run(cmd, env={**_env(redis_url, queue), "PYTHONPATH": str(tmp_path)}, capture_output=True,
                       text=True, timeout=30)
    assert r.returncode == 2
    assert "mayu: queue 'gpu-q' has tasks declared but not implemented in this process: decl.gpu" in r.stderr


def _run_worker(tmp_path, redis_url, queue, source, *extra, env=None):
    (tmp_path / "res_app.py").write_text(source)
    cmd = [sys.executable, "-m", "mayu", "worker", "res_app:app", "--mode", "thread", *extra]
    return subprocess.run(cmd, env={**_env(redis_url, queue), "PYTHONPATH": str(tmp_path), **(env or {})},
                          capture_output=True, text=True, timeout=30)


GPU_APP = (
    "from mayu import App\n"
    "app = App()\n"
    "@app.task(name='res.train', resources={'gpu': 1})\n"
    "def train(): ...\n"
)


@pytest.mark.parametrize("value", ["gpu", "gpu=x", "gpu=-1", "=1", "gpu=1,gpu=2"])
def test_worker_cli_rejects_malformed_resources(tmp_path, redis_url, queue, value):
    r = _run_worker(tmp_path, redis_url, queue, GPU_APP, "--resources", value)
    assert r.returncode == 2
    assert "mayu: bad --resources" in r.stderr, r.stderr


def test_worker_cli_refuses_to_start_when_a_task_requests_more_than_the_worker_has(tmp_path, redis_url, queue):
    r = _run_worker(tmp_path, redis_url, queue, GPU_APP, "--resources", "cpu=4")
    assert r.returncode == 2
    assert ("mayu: task 'res.train' requests 'gpu' but this worker declares no such resource; "
            "pass --resources gpu=...") in r.stderr
    r = _run_worker(tmp_path, redis_url, queue, GPU_APP)
    assert r.returncode == 2 and "pass --resources gpu=..." in r.stderr


def test_worker_cli_reads_resources_from_the_environment(tmp_path, redis_url, queue):
    r = _run_worker(tmp_path, redis_url, queue, GPU_APP, env={"MAYU_RESOURCES": "gpu=0.5"})
    assert r.returncode == 2
    assert "requests gpu=1 but this worker has gpu=0.5" in r.stderr


@pytest.mark.parametrize("text,expected", [
    (None, None), ("", None), ("0", ["0"]), ("0,1", ["0", "1"]), (" 2 , GPU-ab12 ", ["2", "GPU-ab12"]),
])
def test_parse_gpus(text, expected):
    assert parse_gpus(text) == expected


@pytest.mark.parametrize("text", ["0,", ",0", "0,,1", "0,0", " , "])
def test_parse_gpus_rejects_empty_and_duplicate_ids(text):
    with pytest.raises(ValueError, match="--gpus"):
        parse_gpus(text)


@pytest.mark.parametrize("extra,message", [
    (["--gpus", "0,1", "--processes", "3"], "--gpus lists 2 GPU(s), one process each; --processes 3"),
    (["--gpus", "0,0"], "listed more than once"),
    (["--gpus", "0,"], "bad --gpus"),
])
def test_worker_cli_rejects_bad_gpus(tmp_path, redis_url, queue, extra, message):
    r = _run_worker(tmp_path, redis_url, queue, GPU_APP, *extra)
    assert r.returncode == 2
    assert message in r.stderr, r.stderr


ENV_APP = (
    "import os\n"
    "from mayu import App\n"
    "SEEN_AT_IMPORT = os.environ.get('CUDA_VISIBLE_DEVICES')\n"
    "app = App(result_ttl=60)\n"
    "@app.task(name='res.env')\n"
    "def env(pause=0):\n"
    "    import time\n"
    "    time.sleep(pause)\n"
    "    return [SEEN_AT_IMPORT, os.environ.get('CUDA_VISIBLE_DEVICES')]\n"
)


def _gpu_worker(tmp_path, redis_url, queue, *flags):
    (tmp_path / "res_app.py").write_text(ENV_APP)
    env = {**_env(redis_url, queue), "PYTHONPATH": str(tmp_path)}
    env.pop("CUDA_VISIBLE_DEVICES", None)
    return subprocess.Popen([sys.executable, "-m", "mayu", "worker", "res_app:app", "--mode", "thread",
                             "--prefetch-multiplier", "1", *flags], env=env, stderr=subprocess.PIPE, text=True)


def _stop(proc):
    proc.send_signal(signal.SIGTERM)
    _, err = proc.communicate(timeout=30)
    assert proc.returncode == 0, err


def test_one_gpu_is_set_before_the_app_is_imported(tmp_path, redis_url, queue):
    proc = _gpu_worker(tmp_path, redis_url, queue, "--gpus", "GPU-abc", "--threads", "1")
    try:
        producer = App(broker=redis_url, result_ttl=60, default_queue=queue)
        assert producer.send("res.env").result(timeout=30) == ["GPU-abc", "GPU-abc"]
    finally:
        _stop(proc)


def test_each_gpu_gets_its_own_worker_process(tmp_path, redis_url, queue):
    proc = _gpu_worker(tmp_path, redis_url, queue, "--gpus", "4,7", "--threads", "1")
    try:
        producer = App(broker=redis_url, result_ttl=60, default_queue=queue)
        handles = [producer.send("res.env", 0.3) for _ in range(24)]
        seen = {tuple(h.result(timeout=60)) for h in handles}
    finally:
        _stop(proc)
    assert seen == {("4", "4"), ("7", "7")}


POOL_APP = (
    "from mayu import App\n"
    "app = App(result_ttl=60)\n"
    "@app.pool('gemma')\n"
    "def gemma(ctx):\n"
    "    if %s:\n"
    "        raise OSError('no weights')\n"
    "    return f'gemma-{ctx.index}'\n"
    "@app.task(name='pool.use')\n"
    "def use(model=app.use('gemma')):\n"
    "    return model\n"
)


@pytest.mark.parametrize("text,expected", [
    (None, None), ("", None), ("  ", None), ("gemma=2", {"gemma": 2}), ("gemma=0", {"gemma": 0}),
    ("gemma=2, tts = 3", {"gemma": 2, "tts": 3}),
])
def test_parse_pools(text, expected):
    assert parse_pools(text) == expected


@pytest.mark.parametrize("text", [
    "gemma", "=1", "gemma=", "gemma=x", "gemma=-1", "gemma=1.5", "gemma=+1", "gemma=1,,tts=2", "gemma=1,gemma=2",
    "ge mma=1", "gemma=1=2",
])
def test_parse_pools_rejects_malformed_input(text):
    with pytest.raises(ValueError, match="--pools"):
        parse_pools(text)


@pytest.mark.parametrize("extra", [[], ["--mode", "process", "--processes", "2"]])
def test_worker_cli_rejects_malformed_pools_and_unsized_pools(tmp_path, redis_url, queue, extra):
    app_source = POOL_APP % "False"
    r = _run_worker(tmp_path, redis_url, queue, app_source, "--pools", "gemma=x", *extra)
    assert r.returncode == 2
    assert "mayu: bad --pools entry 'gemma=x'" in r.stderr, r.stderr
    r = _run_worker(tmp_path, redis_url, queue, app_source, *extra)
    assert r.returncode == 2
    assert ("mayu: task 'pool.use' uses pool 'gemma' but this worker has no instances of it; "
            "pass --pools gemma=N") in r.stderr, r.stderr
    r = _run_worker(tmp_path, redis_url, queue, app_source, "--pools", "gemma=0", *extra)
    assert r.returncode == 2 and "pass --pools gemma=N" in r.stderr
    r = _run_worker(tmp_path, redis_url, queue, app_source, "--pools", "gemma=1,ghost=1", *extra)
    assert r.returncode == 2 and "--pools names pool 'ghost'" in r.stderr, r.stderr


def test_worker_cli_reports_a_pool_that_fails_to_load(tmp_path, redis_url, queue):
    r = _run_worker(tmp_path, redis_url, queue, POOL_APP % "True", "--pools", "gemma=1")
    assert r.returncode == 1
    assert "mayu: cannot start worker: pool 'gemma' instance 0 failed to load: OSError('no weights')" in r.stderr


def test_worker_cli_loads_pools_from_the_environment(tmp_path, redis_url, queue):
    (tmp_path / "res_app.py").write_text(POOL_APP % "False")
    env = {**_env(redis_url, queue), "PYTHONPATH": str(tmp_path), "MAYU_POOLS": "gemma=1"}
    proc = subprocess.Popen([sys.executable, "-m", "mayu", "worker", "res_app:app", "--mode", "thread",
                             "--threads", "2"], env=env, stderr=subprocess.PIPE, text=True)
    try:
        producer = App(broker=redis_url, result_ttl=60, default_queue=queue)

        @producer.declare(name="pool.use")
        def use(model=producer.use("gemma")): ...

        assert use.send().result(timeout=30) == "gemma-0"
    finally:
        _stop(proc)


LAZY_APP = (
    "from mayu import App\n"
    "app = App(result_ttl=60)\n"
    "@app.pool('gemma', vram_gb=7.4)\n"
    "def gemma(ctx):\n"
    "    return f'gemma-{ctx.index}'\n"
    "@app.task(name='lazy.use')\n"
    "def use(model=app.use('gemma')):\n"
    "    return model\n"
)


@pytest.mark.parametrize("text,expected", [
    (None, None), ("", None), ("  ", None), ("gemma", ["gemma"]), ("tts, gemma ", ["tts", "gemma"]),
])
def test_parse_preload(text, expected):
    assert parse_preload(text) == expected


@pytest.mark.parametrize("text", ["gemma,", ",gemma", "gemma,,tts", "ge mma", "gemma=1", "gemma,gemma"])
def test_parse_preload_rejects_malformed_input(text):
    with pytest.raises(ValueError, match="--preload"):
        parse_preload(text)


@pytest.mark.parametrize("extra,env,message", [
    (("--preload", "gemma"), None, "--preload needs --pool-mode lazy"),
    (("--schedule", "oldest"), None, "--schedule needs --pool-mode lazy"),
    ((), {"MAYU_PRELOAD": "gemma"}, "--preload needs --pool-mode lazy"),
    ((), {"MAYU_SCHEDULE": "throughput"}, "--schedule needs --pool-mode lazy"),
    ((), {"MAYU_POOL_MODE": "sometimes"}, "pool_mode must be 'eager' or 'lazy'"),
    ((), {"MAYU_POOL_MODE": "lazy", "MAYU_SCHEDULE": "fastest"}, "schedule must be 'throughput' or 'oldest'"),
    (("--pool-mode", "lazy", "--preload", "gemma,,tts"), None, "mayu: bad --preload 'gemma,,tts'"),
    (("--pool-mode", "lazy", "--pools", "gemma=1", "--resources", "vram_gb=5"), None,
     "task 'lazy.use' needs vram_gb=7.4 (gemma 7.4) but this worker has vram_gb=5"),
])
def test_worker_cli_rejects_bad_lazy_options(tmp_path, redis_url, queue, extra, env, message):
    r = _run_worker(tmp_path, redis_url, queue, LAZY_APP, *extra, env=env)
    assert r.returncode == 2 and message in r.stderr, r.stderr


def test_worker_cli_rejects_a_bad_pool_mode_flag(tmp_path, redis_url, queue):
    r = _run_worker(tmp_path, redis_url, queue, LAZY_APP, "--pool-mode", "sometimes")
    assert r.returncode == 2 and "--pool-mode" in r.stderr, r.stderr


def test_worker_cli_runs_lazy_pools_from_flags_and_the_environment(tmp_path, redis_url, queue):
    (tmp_path / "res_app.py").write_text(LAZY_APP)
    env = {**_env(redis_url, queue), "PYTHONPATH": str(tmp_path), "MAYU_POOLS": "gemma=1",
           "MAYU_RESOURCES": "vram_gb=7.5", "MAYU_POOL_MODE": "lazy", "MAYU_SCHEDULE": "throughput",
           "MAYU_PRELOAD": "gemma"}
    proc = subprocess.Popen([sys.executable, "-m", "mayu", "worker", "res_app:app", "--mode", "thread",
                             "--threads", "2"], env=env, stderr=subprocess.PIPE, text=True)
    try:
        producer = App(broker=redis_url, result_ttl=60, default_queue=queue)

        @producer.declare(name="lazy.use")
        def use(model=producer.use("gemma")): ...

        assert use.send().result(timeout=30) == "gemma-0"
    finally:
        _stop(proc)


@pytest.mark.parametrize("extra,env", [(("--thp", "bad"), None), ((), {"MAYU_THP": "bad"})])
def test_worker_cli_rejects_a_bad_thp_mode(tmp_path, redis_url, queue, extra, env):
    r = _run_worker(tmp_path, redis_url, queue, GPU_APP, *extra, env=env)
    assert r.returncode == 2
    assert "mayu: bad thp mode 'bad': use one of python, keep, off" in r.stderr, r.stderr


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="THP is a Linux feature")
@pytest.mark.parametrize("extra,env", [(("--thp", "off"), None), ((), {"MAYU_THP": "off"})])
def test_worker_cli_applies_thp_before_the_app_is_imported(tmp_path, redis_url, queue, extra, env):
    source = (
        "from mayu import App\n"
        "app = App()\n"
        "print('IMPORT-THP', [l.split()[1] for l in open('/proc/self/status') if l.startswith('THP_enabled')])\n"
        "raise SystemExit(0)\n"
    )
    r = _run_worker(tmp_path, redis_url, queue, source, *extra, env=env)
    assert "IMPORT-THP" in r.stdout, r.stderr
    line = [l for l in r.stdout.splitlines() if l.startswith("IMPORT-THP")][0]
    assert line in ("IMPORT-THP ['0']", "IMPORT-THP []"), line  # [] on kernels that do not report it


@pytest.mark.parametrize("text,expected", [
    ("gpu=auto", {"gpu": "auto"}),
    ("gpu=auto,vram_gb=auto,cpu=4", {"gpu": "auto", "vram_gb": "auto", "cpu": 4.0}),
    ("vram_gb=7.5,gpu=auto", {"vram_gb": 7.5, "gpu": "auto"}),
])
def test_parse_resources_takes_auto_for_gpu_and_vram_gb(text, expected):
    assert parse_resources(text) == expected


@pytest.mark.parametrize("text", ["cpu=auto", "=auto", "gpu=auto,gpu=1", "gpu=Auto"])
def test_parse_resources_rejects_auto_elsewhere(text):
    with pytest.raises(ValueError, match="--resources"):
        parse_resources(text)


@pytest.mark.parametrize("extra,env,message", [
    (("--gpu-monitor", "sometimes"), None, "--gpu-monitor"),
    ((), {"MAYU_GPU_MONITOR": "sometimes"}, "gpu_monitor must be 'auto', 'on' or 'off', got 'sometimes'"),
    (("--gpu-interval", "0"), None, "gpu_interval must be a positive number of seconds, got 0.0"),
    (("--gpu-interval", "fast"), None, "--gpu-interval"),
    ((), {"MAYU_GPU_INTERVAL": "-1"}, "gpu_interval must be a positive number of seconds, got -1.0"),
    ((), {"MAYU_GPU_INTERVAL": "soon"}, "--gpu-interval"),
    (("--resources", "cpu=auto"), None, "mayu: bad --resources entry 'cpu=auto': only gpu and vram_gb can be auto"),
    (("--resources", "gpu=auto", "--gpu-monitor", "off"), None, "--gpu-monitor off turns away"),
    # no visible device: --gpu-monitor on and auto resources fail whether or not NVML is installed
    (("--gpu-monitor", "on"), {"CUDA_VISIBLE_DEVICES": ""}, "--gpu-monitor on "),
    (("--resources", "gpu=auto"), {"MAYU_GPU_MONITOR": "auto", "CUDA_VISIBLE_DEVICES": ""}, "gpu=auto "),
])
def test_worker_cli_rejects_bad_gpu_monitoring_options(tmp_path, redis_url, queue, extra, env, message):
    r = _run_worker(tmp_path, redis_url, queue, GPU_APP, *extra, env=env)
    assert r.returncode == 2 and message in r.stderr, r.stderr


def test_worker_cli_runs_with_the_gpu_options_from_flags_and_the_environment(tmp_path, redis_url, queue):
    (tmp_path / "res_app.py").write_text(
        "from mayu import App\n"
        "app = App(result_ttl=60)\n"
        "@app.task(name='res.ok')\n"
        "def ok():\n"
        "    return 'ok'\n"
    )
    for extra, env in [(("--gpu-monitor", "off", "--gpu-interval", "0.5"), None),
                       ((), {"MAYU_GPU_MONITOR": "off", "MAYU_GPU_INTERVAL": "0.5"})]:
        proc = subprocess.Popen([sys.executable, "-m", "mayu", "worker", "res_app:app", "--mode", "thread",
                                 "--threads", "1", *extra],
                                env={**_env(redis_url, queue), "PYTHONPATH": str(tmp_path), **(env or {})},
                                stderr=subprocess.PIPE, text=True)
        try:
            producer = App(broker=redis_url, result_ttl=60, default_queue=queue)
            assert producer.send("res.ok").result(timeout=30) == "ok"
        finally:
            _stop(proc)
