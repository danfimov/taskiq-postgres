import typing as tp

import pytest

from taskiq_pg.asyncpg import AsyncpgBroker
from taskiq_pg.psqlpy import PSQLPyBroker
from taskiq_pg.psycopg import PsycopgBroker


BROKERS: list[tp.Any] = [AsyncpgBroker, PsycopgBroker, PSQLPyBroker]


@pytest.mark.unit
@pytest.mark.parametrize("broker_class", BROKERS)
def test_when_broker_is_created__then_keepalive_is_enabled_by_default(broker_class: tp.Any) -> None:
    # when
    broker = broker_class()

    # then
    assert broker.listen_keepalive_interval == 30.0
    assert broker.listen_keepalive_timeout == 5.0


@pytest.mark.unit
@pytest.mark.parametrize("broker_class", BROKERS)
def test_when_keepalive_interval_is_none__then_keepalive_is_disabled(broker_class: tp.Any) -> None:
    # when
    broker = broker_class(listen_keepalive_interval=None)

    # then
    assert broker.listen_keepalive_interval is None


@pytest.mark.unit
@pytest.mark.parametrize("broker_class", BROKERS)
@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"listen_keepalive_interval": 0}, "listen_keepalive_interval"),
        ({"listen_keepalive_interval": -1}, "listen_keepalive_interval"),
        ({"listen_keepalive_timeout": 0}, "listen_keepalive_timeout"),
    ],
)
def test_when_keepalive_settings_are_invalid__then_error_is_raised(
    broker_class: tp.Any,
    kwargs: dict[str, float],
    match: str,
) -> None:
    # when / then
    with pytest.raises(ValueError, match=match):
        broker_class(**kwargs)
