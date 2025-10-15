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


Ensure user has `CREATE TABLE` permissions or manually create tables using provided schemas:

```sql
-- for broker
CREATE TABLE taskiq_queue (
    id SERIAL PRIMARY KEY,
    task_id UUID NOT NULL,
    task_name VARCHAR NOT NULL,
    message BYTEA NOT NULL,
    labels JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

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
