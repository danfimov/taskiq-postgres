import pytest
import uvloop


@pytest.fixture(scope="session")
def event_loop_policy():
    # Read for more details: https://pytest-asyncio.readthedocs.io/en/stable/how-to-guides/uvloop.html
    return uvloop.EventLoopPolicy()
