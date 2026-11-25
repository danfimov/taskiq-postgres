import asyncio

import pytest

from taskiq_pg._internal.tasks import cancel_and_wait


async def run_forever() -> None:
    await asyncio.Future()


async def slow_to_cancel() -> None:
    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        await asyncio.sleep(0.5)
        raise


@pytest.mark.unit
async def test_when_task_is_running__then_it_is_cancelled_and_awaited() -> None:
    # given
    task = asyncio.create_task(run_forever())
    await asyncio.sleep(0)

    # when
    await cancel_and_wait(task)

    # then
    assert task.cancelled()


@pytest.mark.unit
async def test_when_task_is_already_done__then_nothing_is_raised() -> None:
    # given
    task = asyncio.create_task(asyncio.sleep(0))
    await task

    # when / then
    await cancel_and_wait(task)


@pytest.mark.unit
async def test_when_caller_is_cancelled_while_waiting__then_cancellation_is_propagated() -> None:
    # given
    task = asyncio.create_task(slow_to_cancel())
    await asyncio.sleep(0)
    waiter = asyncio.create_task(cancel_and_wait(task))
    await asyncio.sleep(0.1)

    # when
    waiter.cancel()
    await asyncio.gather(waiter, return_exceptions=True)

    # then
    assert waiter.cancelled()


@pytest.mark.unit
async def test_when_task_fails__then_its_error_is_raised() -> None:
    # given
    async def fail_on_cancel() -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            msg = "cleanup failed"
            raise RuntimeError(msg) from None

    task = asyncio.create_task(fail_on_cancel())
    await asyncio.sleep(0)

    # when / then
    with pytest.raises(RuntimeError, match="cleanup failed"):
        await cancel_and_wait(task)
