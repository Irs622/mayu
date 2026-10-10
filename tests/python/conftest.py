import os
import uuid

import pytest

REDIS_URL = os.environ.get("MAYU_TEST_REDIS", "redis://127.0.0.1:6390/0")


@pytest.fixture
def redis_url():
    return REDIS_URL


@pytest.fixture
def queue():
    return f"t-{uuid.uuid4().hex[:12]}"
