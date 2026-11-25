import asyncio
import typing as tp


async def cancel_and_wait(task: asyncio.Task[tp.Any]) -> None:
    """
    Cancel a task and wait until it finishes.

    The task's own cancellation is expected and suppressed. A cancellation of the caller
    while it is waiting is re-raised, unlike with `contextlib.suppress(asyncio.CancelledError)`.
    """
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            raise
