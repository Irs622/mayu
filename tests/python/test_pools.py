import pytest

import mayu
from mayu import App, DiscardInstance, Pool
from mayu.pool import PoolContext, Use
from mayu.task import check_resources


def test_pool_decorator_returns_a_pool(app):
    @app.pool("gemma", vram_gb=5.8)
    def gemma(ctx):
        return ctx.index

    assert isinstance(gemma, Pool)
    assert app.pools == {"gemma": gemma}
    assert gemma.name == "gemma" and gemma.resources == {"vram_gb": 5.8} and gemma.discard_on == ()
    assert gemma.factory(PoolContext("gemma", 3, 4)) == 3
    assert gemma.closer is None
    assert repr(gemma) == "<Pool 'gemma'>"


def test_close_registers_once_and_returns_the_function(app):
    @app.pool("tts")
    def tts(ctx): ...

    def unload(instance): ...

    assert tts.close(unload) is unload
    assert tts.closer is unload
    with pytest.raises(ValueError, match="already has a close function"):
        tts.close(lambda instance: None)
    with pytest.raises(TypeError, match="callable"):
        app.pool("other")(lambda ctx: None).close("nope")


def test_duplicate_pool_name_is_rejected(app):
    app.pool("gemma")(lambda ctx: None)
    with pytest.raises(ValueError, match="pool 'gemma' is already registered"):
        app.pool("gemma")(lambda ctx: None)


@pytest.mark.parametrize("name", ["", "a,b", "a=1", "a b", "a\tb", "a\n"])
def test_bad_pool_names(app, name):
    with pytest.raises(ValueError, match="pool name"):
        app.pool(name)
    with pytest.raises(ValueError, match="pool name"):
        app.use(name)


def test_non_string_pool_name(app):
    with pytest.raises(TypeError, match="pool name"):
        app.pool(3)
    with pytest.raises(TypeError, match="pool name"):
        app.use(None)


def test_discard_on_takes_an_exception_class_or_a_tuple(app):
    assert app.pool("a", discard_on=KeyError)(lambda ctx: None).discard_on == (KeyError,)
    assert app.pool("b", discard_on=(KeyError, OSError))(lambda ctx: None).discard_on == (KeyError, OSError)
    for bad in ("KeyError", 3, (KeyError, "x"), [KeyError], len):
        with pytest.raises(TypeError, match="discard_on"):
            app.pool("c", discard_on=bad)
    assert "c" not in app.pools


def test_pool_factory_must_be_callable(app):
    with pytest.raises(TypeError, match="callable"):
        app.pool("a")("not a function")


def test_pool_resources_are_validated(app):
    with pytest.raises(ValueError, match="at least 0.001"):
        app.pool("a", vram_gb=0)
    with pytest.raises(TypeError, match="must be a number"):
        app.pool("a", vram_gb="lots")


def test_pool_prefixed_resource_names_are_reserved(app):
    with pytest.raises(ValueError, match="reserved for pools"):
        app.pool("a", **{"pool:x": 1})
    with pytest.raises(ValueError, match="reserved for pools"):
        app.task(resources={"pool:x": 1})(lambda: None)
    with pytest.raises(ValueError, match="reserved for pools"):
        check_resources("resources", {"pool:gemma": 1}, allow_zero=True)
    assert check_resources("resources", {"pooled": 1}) == {"pooled": 1.0}


def test_use_compares_and_hashes_by_pool_name(app):
    assert app.use("gemma") == app.use("gemma") == Use("gemma")
    assert app.use("gemma") != app.use("tts")
    assert app.use("gemma") != "gemma"
    assert len({app.use("gemma"), app.use("gemma"), app.use("tts")}) == 2
    assert repr(app.use("gemma")) == "app.use('gemma')"
    assert app.use("gemma").name == "gemma"


def test_declare_and_implement_match_on_use(app):
    @app.declare(name="pools.encode")
    def encode(run, model=app.use("gemma")): ...

    assert encode.pools == {"model": "gemma"}

    @encode.implement
    def _(run, model=app.use("gemma")):
        return model

    assert encode.implemented and encode.pools == {"model": "gemma"}


def test_implement_with_another_pool_or_a_plain_default_fails(app):
    @app.declare(name="pools.a")
    def a(run, model=app.use("gemma")): ...

    for bad in (lambda run, model=app.use("tts"): None, lambda run, model=None: None, lambda run, model: None):
        with pytest.raises(TypeError, match="must match its declaration"):
            a.implement(bad)
    assert not a.implemented


def test_implement_rejects_async_with_a_pool(app):
    @app.declare(name="pools.b")
    def b(model=app.use("gemma")): ...

    async def impl(model=app.use("gemma")): ...

    # an async implementation differs from the sync declaration only in kind-of-function, so the shape matches
    with pytest.raises(TypeError, match="pools are for sync tasks"):
        b.implement(impl)
    assert not b.implemented and b.pools == {"model": "gemma"}


def test_task_pools_contents(app):
    @app.task
    def plain(x): ...

    @app.task
    def two(x, model=app.use("gemma"), *, voice=app.use("tts"), n=1): ...

    @app.task(threadsafe=False)
    def serial(model=app.use("gemma")): ...

    assert plain.pools == {}
    assert two.pools == {"model": "gemma", "voice": "tts"}
    assert serial.pools == {"model": "gemma"}


def test_pool_parameter_must_be_keyword_capable(app):
    with pytest.raises(TypeError, match="positional-only"):
        @app.task
        def bad(model=app.use("gemma"), /): ...


def test_one_pool_in_two_parameters_is_rejected(app):
    with pytest.raises(TypeError, match="one instance per pool"):
        @app.task
        def bad(a=app.use("gemma"), b=app.use("gemma")): ...


def test_async_task_cannot_use_a_pool(app):
    with pytest.raises(TypeError, match="pools are for sync tasks"):
        @app.task
        async def bad(model=app.use("gemma")): ...

    @app.task
    async def fine(x=1): ...

    assert fine.pools == {}


def test_producer_declares_a_pool_task_without_the_pool(app):
    @app.declare(name="pools.encode")
    def encode(run, model=app.use("gemma")): ...

    assert app.pools == {}
    assert encode.send("r1").id
    assert app.send("pools.encode", "r2").id
    assert encode.send("r3", link=encode.s()).id


def test_send_rejects_the_pool_parameter(app):
    @app.task
    def encode(run, model=app.use("gemma")): ...

    with pytest.raises(TypeError, match=r"parameter 'model' is filled from pool 'gemma' on the worker; don't pass it"):
        encode.send("r", model=1)
    with pytest.raises(TypeError, match="parameter 'model' is filled from pool 'gemma'"):
        encode.send("r", 1)
    with pytest.raises(TypeError, match="parameter 'model' is filled from pool 'gemma'"):
        app.send(encode.name, "r", model=1)
    assert encode.send("r").id


def test_workflow_node_rejects_the_pool_parameter(app):
    @app.task
    def encode(run, model=app.use("gemma")): ...

    wf = app.workflow("w")
    with pytest.raises(TypeError, match=r"cannot be a workflow node: parameter 'model' is filled from pool 'gemma'"):
        wf.add(encode.s("r", model=1))
    with pytest.raises(TypeError, match="missing a required argument: 'run'"):
        wf.add(encode)  # other binding errors are still reported
    assert wf.add(encode.s("r")).index == 0


def test_link_rejects_the_pool_parameter(app):
    @app.task
    def encode(run, model=app.use("gemma")): ...

    @app.task
    def first_is_pool(model=app.use("gemma")): ...

    @app.task
    def parent(x): ...

    with pytest.raises(TypeError, match=r"cannot be a link callback: parameter 'model' is filled from pool 'gemma'"):
        parent.send(1, link=encode.s(model=1))
    # the parent's result lands in the first slot, which here is the pool parameter
    with pytest.raises(TypeError, match="parameter 'model' is filled from pool 'gemma'"):
        parent.send(1, link=first_is_pool)
    assert parent.send(1, link=encode).id
    assert parent.send(1, link=[encode.s(), parent.s()]).id


def test_exports():
    assert mayu.Pool is Pool and mayu.DiscardInstance is DiscardInstance
    assert mayu.__all__ == sorted(mayu.__all__, key=lambda n: (n[0].islower(), n))


def test_discard_instance():
    assert issubclass(DiscardInstance, Exception)
    e = DiscardInstance("gone", pool="gemma")
    assert str(e) == "gone" and e.pool == "gemma"
    assert DiscardInstance().pool is None
    with pytest.raises(TypeError):
        DiscardInstance("gone", "gemma")


def test_pool_context_is_frozen():
    ctx = PoolContext("gemma", 1, 2)
    assert (ctx.name, ctx.index, ctx.size) == ("gemma", 1, 2)
    with pytest.raises(AttributeError):
        ctx.index = 0
