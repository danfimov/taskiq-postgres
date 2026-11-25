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


async def run_until_first_done(*coroutines: tp.Coroutine[tp.Any, tp.Any, tp.Any]) -> None:
    """
    Run coroutines concurrently until the first one finishes, then cancel the others.

    The exception of the first finished coroutine, if any, is raised. If the caller is cancelled,
    all coroutines are cancelled and awaited before the cancellation propagates.
    """
    tasks = [asyncio.create_task(coroutine) for coroutine in coroutines]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        pending = [task for task in tasks if not task.done()]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        task.result()
