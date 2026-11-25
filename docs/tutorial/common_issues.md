---
title: Common Issues
---

## Connection Errors

Ensure your connection string is correct:

  ```python
  dsn = "postgresql://postgres:postgres@localhost:5432/postgres"
  ```

Check PostgreSQL is running and accessible:

  ```python
  import asyncpg

  dsn = '...'
  conn = await asyncpg.connect(dsn)
  await conn.close()
  ```

## Table Creation Issues


Ensure user has `CREATE TABLE` permissions or manually create tables using provided schemas.
A broker that does not own its table needs only `SELECT`, `INSERT`, `UPDATE` and `DELETE` on it and `USAGE` on its
id sequence, as long as the table and its indexes below exist:

```sql
-- for broker
CREATE TABLE taskiq_messages (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR NOT NULL,
    task_name VARCHAR NOT NULL,
    message TEXT NOT NULL,
    labels JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    claimed_at TIMESTAMP WITH TIME ZONE,
    attempts INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX taskiq_messages_pending_idx ON taskiq_messages (id) WHERE status = 'pending';
-- only if the broker uses processing_timeout
CREATE INDEX taskiq_messages_processing_idx ON taskiq_messages (claimed_at) WHERE status = 'processing';

-- for result backend
CREATE TABLE taskiq_results (
    task_id UUID PRIMARY KEY,
    result BYTEA,
    is_err BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- for schedule source
CREATE TABLE taskiq_schedules (
    id UUID PRIMARY KEY,
    task_name VARCHAR(100) NOT NULL,
    schedule JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

This is default schemas. So if you changed table names or other parameters, adjust accordingly.

## Driver Import Errors

Install the appropriate driver extra, for example:

```bash
# for asyncpg
pip install taskiq-postgres[asyncpg]
```
