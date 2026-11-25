from collections.abc import Iterator


def retry_delays(initial: float = 1.0, maximum: float = 30.0, factor: float = 2.0) -> Iterator[float]:
    """
    Yield delays between retry attempts: exponentially growing and capped at `maximum`.

    Args:
        initial: The first delay in seconds.
        maximum: The upper bound for a delay in seconds.
        factor: The multiplier applied to the delay after each attempt.
    """
    delay = initial
    while True:
        yield delay
        delay = min(delay * factor, maximum)
