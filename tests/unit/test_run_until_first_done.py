import asyncio

import pytest

from taskiq_pg._internal.tasks import run_until_first_done


async def forever(cancelled: list[str], name: str) -> None:
    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        cancelled.append(name)
        raise


@pytest.mark.unit
async def test_when_one_coroutine_finishes__then_the_others_are_cancelled() -> None:
    # given
    cancelled: list[str] = []

    # when
    await run_until_first_done(asyncio.sleep(0.01), forever(cancelled, "a"), forever(cancelled, "b"))

    # then
    assert sorted(cancelled) == ["a", "b"]


@pytest.mark.unit
async def test_when_one_coroutine_fails__then_its_error_is_raised_and_the_others_are_cancelled() -> None:
    # given
    cancelled: list[str] = []

    async def fail() -> None:
        await asyncio.sleep(0.01)
        msg = "connection lost"
        raise ConnectionError(msg)

    # when / then
    with pytest.raises(ConnectionError, match="connection lost"):
        await run_until_first_done(fail(), forever(cancelled, "a"))
    assert cancelled == ["a"]


@pytest.mark.unit
async def test_when_caller_is_cancelled__then_all_coroutines_are_cancelled() -> None:
    # given
    cancelled: list[str] = []
    runner = asyncio.create_task(run_until_first_done(forever(cancelled, "a"), forever(cancelled, "b")))
    await asyncio.sleep(0.01)

    # when
    runner.cancel()
    await asyncio.gather(runner, return_exceptions=True)

    # then
    assert runner.cancelled()
    assert sorted(cancelled) == ["a", "b"]
