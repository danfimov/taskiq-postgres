import uvloop


def pytest_asyncio_loop_factories(config, item):  # noqa: ARG001
    return {
        "uvloop": uvloop.new_event_loop,
    }
