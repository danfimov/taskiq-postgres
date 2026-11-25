import random
from collections.abc import Iterator


def retry_delays(
    initial: float = 1.0,
    maximum: float = 30.0,
    factor: float = 2.0,
    jitter: float = 0.5,
    rng: random.Random | None = None,
) -> Iterator[float]:
    """
    Yield delays between retry attempts: exponentially growing and capped at `maximum`.

    Each delay is randomly reduced by up to `jitter` of its value, so that many clients
    that lost the database at the same moment do not retry in lockstep.

    Args:
        initial: The first delay in seconds.
        maximum: The upper bound for a delay in seconds.
        factor: The multiplier applied to the delay after each attempt.
        jitter: The largest share of a delay that may be cut off at random, from 0 to 1.
        rng: Random number generator, for reproducible delays in tests.
    """
    if not 0 <= jitter <= 1:
        msg = "jitter must be between 0 and 1"
        raise ValueError(msg)
    rand = rng or random.Random()  # noqa: S311 - not used for cryptography
    delay = initial
    while True:
        yield delay * (1 - jitter * rand.random())
        delay = min(delay * factor, maximum)
