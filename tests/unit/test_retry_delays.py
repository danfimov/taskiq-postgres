import itertools

import pytest

from taskiq_pg._internal.retry import retry_delays


@pytest.mark.unit
def test_when_retrying__then_delays_grow_exponentially_up_to_maximum() -> None:
    # when
    delays = list(itertools.islice(retry_delays(initial=1.0, maximum=30.0, factor=2.0), 8))

    # then
    assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]


@pytest.mark.unit
def test_when_two_sequences_are_created__then_they_are_independent() -> None:
    # given
    first = retry_delays()
    next(first)
    next(first)

    # when
    second = retry_delays()

    # then
    assert next(second) == 1.0
    assert next(first) == 4.0
