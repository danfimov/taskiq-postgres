import itertools
import random

import pytest

from taskiq_pg._internal.retry import retry_delays


@pytest.mark.unit
def test_when_retrying_without_jitter__then_delays_grow_exponentially_up_to_maximum() -> None:
    # when
    delays = list(itertools.islice(retry_delays(initial=1.0, maximum=30.0, factor=2.0, jitter=0), 8))

    # then
    assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]


@pytest.mark.unit
def test_when_two_sequences_are_created__then_they_are_independent() -> None:
    # given
    first = retry_delays(jitter=0)
    next(first)
    next(first)

    # when
    second = retry_delays(jitter=0)

    # then
    assert next(second) == 1.0
    assert next(first) == 4.0


@pytest.mark.unit
def test_when_retrying_with_jitter__then_delays_stay_within_bounds() -> None:
    # given
    expected = [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]

    for seed in range(50):
        # when
        delays = list(itertools.islice(retry_delays(jitter=0.5, rng=random.Random(seed)), len(expected)))

        # then
        for delay, upper in zip(delays, expected, strict=True):
            assert upper * 0.5 <= delay <= upper


@pytest.mark.unit
def test_when_retrying_with_jitter__then_delays_differ_between_clients() -> None:
    # when
    first = list(itertools.islice(retry_delays(rng=random.Random(1)), 5))
    second = list(itertools.islice(retry_delays(rng=random.Random(2)), 5))

    # then
    assert first != second


@pytest.mark.unit
@pytest.mark.parametrize("jitter", [-0.1, 1.5])
def test_when_jitter_is_out_of_range__then_error_is_raised(jitter: float) -> None:
    # when / then
    with pytest.raises(ValueError, match="jitter"):
        next(retry_delays(jitter=jitter))
