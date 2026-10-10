import json
import pickle

import msgpack

MSGPACK = "application/msgpack"
JSON = "application/json"
PICKLE = "application/x-python-pickle"


class MsgpackSerializer:
    content_type = MSGPACK

    def dumps(self, obj) -> bytes:
        return msgpack.packb(obj, use_bin_type=True)

    def loads(self, data: bytes):
        return msgpack.unpackb(data, raw=False)


class JsonSerializer:
    content_type = JSON

    def dumps(self, obj) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode()

    def loads(self, data: bytes):
        return json.loads(data)


class PickleSerializer:
    """Unsafe with untrusted producers: unpickling runs arbitrary code. Opt-in only."""

    content_type = PICKLE

    def dumps(self, obj) -> bytes:
        return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

    def loads(self, data: bytes):
        return pickle.loads(data)


_BY_NAME = {"msgpack": MsgpackSerializer(), "json": JsonSerializer(), "pickle": PickleSerializer()}
_BY_CONTENT_TYPE = {s.content_type: s for s in _BY_NAME.values()}


def get_serializer(name: str):
    try:
        return _BY_NAME[name]
    except KeyError:
        raise ValueError(f"unknown serializer {name!r}; choose from {sorted(_BY_NAME)}") from None


def for_content_type(content_type: str, *, allow_pickle: bool):
    if content_type == PICKLE and not allow_pickle:
        raise ValueError("refusing pickle payload: this app is not configured with serializer='pickle'")
    try:
        return _BY_CONTENT_TYPE[content_type]
    except KeyError:
        raise ValueError(f"unknown content type {content_type!r}") from None
