---
title: Delivery and Failures
---

This page explains how a message travels from `kick()` to a worker, what happens when a connection or a
worker fails, and which broker parameters control that.

## How a message is delivered

1. `kick()` inserts the message into the messages table (`taskiq_messages` by default) with the status `pending`
   and sends `NOTIFY <channel>, '<message id>'`.
2. A listening worker receives the notification and claims the message: it changes the status to `processing`
   only if it is still `pending`, so exactly one worker gets it.
3. When the task is done, the worker acknowledges the message, and the row is deleted.

A message with a `delay` label (in seconds) is inserted right away, but becomes available only after its delay.

`NOTIFY` is fast but not reliable: PostgreSQL drops a notification nobody is listening to. To make sure no message
is left behind, every listening worker also **polls** the table:

- right after it starts listening, which picks up messages kicked while no worker was running (e.g. during a deploy);
- right after its LISTEN connection is restored, which picks up messages kicked while it was reconnecting;
- every `poll_interval` seconds (5 by default), which also picks up delayed messages whose producer exited before
  sending the delayed notification.

Polling claims the oldest ready message with `FOR UPDATE SKIP LOCKED`, so workers never claim the same row, and a
backlog is drained without waiting between messages.

## Connection failures

Each broker keeps one connection for `LISTEN` and a pool for everything else. When the LISTEN connection is
lost (PostgreSQL restart, `pg_terminate_backend`, a network problem), the broker reconnects with randomized
delays of up to 30 seconds and polls the table right after reconnecting.

Some failures do not close the connection:

- a proxy (pgbouncer, HAProxy, a cloud load balancer) or `idle_session_timeout` closes connections that send no
  queries, and a LISTEN connection only receives data;
- a connection can hang without being closed, for example after a network partition.

So the broker also runs a small keepalive query, `SELECT 1 /* taskiq-pg LISTEN keepalive */`, on its LISTEN
connection every `listen_keepalive_interval` seconds (30 by default). If the query does not answer within
`listen_keepalive_timeout` seconds (5 by default), the connection is treated as lost. Set
`listen_keepalive_interval` below the idle timeout of your proxy, or to `None` to disable the check.

!!! note
    A `read_connection` passed by you is not reconnected and not checked with keepalive queries:
    its lifecycle is your responsibility. The asyncpg and aiopg brokers log a warning when it is closed, and the
    psycopg broker raises from `listen()`.

## Worker failures and at-least-once delivery

If a worker dies after claiming a message but before acknowledging it (for example, `kill -9`), or the
acknowledgement does not reach the database, the message stays in `processing`.

By default, such a message is **not** delivered again (at-most-once delivery). To deliver it again (at-least-once
delivery), set `processing_timeout`:

```python
broker = AsyncpgBroker(
    dsn,
    processing_timeout=600,  # seconds; longer than your longest task
    max_retry_attempts=5,
)
```

A message claimed longer than `processing_timeout` seconds ago is returned to the queue and delivered again,
up to `max_retry_attempts` claims in total. After that, its status becomes `failed`, a warning is logged, and the
row stays in the table for you to inspect.

!!! warning
    With `processing_timeout`, a task can run more than once:

    - make tasks idempotent;
    - make the timeout longer than your longest task, otherwise a task that is still running is delivered again.

    When a worker acknowledges a message after it was delivered again, the acknowledgement is ignored: the row is
    deleted only by the worker that claimed it last.

## Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `poll_interval` | `5.0` | Seconds between polls for pending messages whose notification was missed. |
| `listen_keepalive_interval` | `30.0` | Seconds between keepalive queries on the broker's own LISTEN connection; `None` disables them. |
| `listen_keepalive_timeout` | `5.0` | Seconds to wait for a keepalive query before reconnecting. |
| `processing_timeout` | `None` | Seconds after which an unacknowledged message is delivered again; `None` keeps at-most-once delivery. |
| `max_retry_attempts` | `5` | With `processing_timeout`, how many times a message may be claimed before it is marked as `failed`. |

## Schema setup

On startup, the broker creates the parts of its schema that are missing: the messages table and its columns,
a partial index on pending messages (`<table>_pending_idx`) and, with `processing_timeout`, a partial index on
processing messages (`<table>_processing_idx`).

Schema setup is serialized with an advisory lock, so many workers can start at the same time. The broker
only runs the statements for the parts that are missing, so an application role with only `SELECT`, `INSERT`,
`UPDATE` and `DELETE` privileges (and `USAGE` on the id sequence) works with a table that was prepared in advance,
see [Common Issues](common_issues.md#table-creation-issues).

## Known limitations

- **aiopg pool.** aiopg opens new pool connections while holding a pool-wide lock. If opening a connection hangs
  (for example, right after a PostgreSQL restart behind some proxies), every operation on the pool waits for it,
  up to the pool `timeout` (60 seconds by default). Lower it with `write_kwargs={"timeout": ...}` if needed.
- **psycopg.** The psycopg broker reads notifications only while `listen()` is iterated. A worker with no free
  slots notices a lost connection when the next slot frees up; polling still picks up the messages.
