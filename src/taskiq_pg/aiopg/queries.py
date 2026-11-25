CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS {} (
    task_id {} UNIQUE,
    result BYTEA,
    progress BYTEA
)
"""

ADD_PROGRESS_COLUMN_QUERY = """
ALTER TABLE {} ADD COLUMN IF NOT EXISTS progress BYTEA;
"""

CREATE_INDEX_QUERY = """
CREATE INDEX IF NOT EXISTS {}_task_id_idx ON {} USING HASH (task_id)
"""

INSERT_RESULT_QUERY = """
INSERT INTO {} VALUES (%s, %s, NULL)
ON CONFLICT (task_id)
DO UPDATE
SET result = %s
"""

INSERT_PROGRESS_QUERY = """
INSERT INTO {} VALUES (%s, NULL, %s)
ON CONFLICT (task_id)
DO UPDATE
SET progress = %s
"""

SELECT_PROGRESS_QUERY = """
SELECT progress FROM {} WHERE task_id = %s
"""

IS_RESULT_EXISTS_QUERY = """
SELECT EXISTS(
    SELECT 1 FROM {} WHERE task_id = %s and result IS NOT NULL
)
"""

SELECT_RESULT_QUERY = """
SELECT result FROM {} WHERE task_id = %s
"""

DELETE_RESULT_QUERY = """
DELETE FROM {} WHERE task_id = %s
"""

CREATE_SCHEDULES_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS {} (
    id UUID PRIMARY KEY,
    task_name VARCHAR(100) NOT NULL,
    schedule JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

INSERT_SCHEDULE_QUERY = """
INSERT INTO {} (id, task_name, schedule)
VALUES (%s, %s, %s)
ON CONFLICT (id) DO UPDATE
SET task_name = EXCLUDED.task_name,
    schedule = EXCLUDED.schedule,
    updated_at = NOW();
"""

SELECT_SCHEDULES_QUERY = """
SELECT id, task_name, schedule
FROM {};
"""

DELETE_ALL_SCHEDULES_QUERY = """
DELETE FROM {};
"""

DELETE_SCHEDULE_QUERY = """
DELETE FROM {} WHERE id = %s;
"""

CREATE_MESSAGE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS {} (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR NOT NULL,
    task_name VARCHAR NOT NULL,
    message TEXT NOT NULL,
    labels JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

INSERT_MESSAGE_QUERY = """
INSERT INTO {} (task_id, task_name, message, labels)
VALUES (%s, %s, %s, %s)
RETURNING id
"""

CLAIM_MESSAGE_QUERY = """
UPDATE {} SET status = 'processing', claimed_at = now(), attempts = attempts + 1
WHERE id = %s AND status = 'pending'
RETURNING id, message
"""

DELETE_MESSAGE_QUERY = "DELETE FROM {} WHERE id = %s"

CREATE_PENDING_INDEX_QUERY = "CREATE INDEX IF NOT EXISTS {index} ON {table} (id) WHERE status = 'pending'"

CLAIM_NEXT_READY_MESSAGE_QUERY = """
UPDATE {table} SET status = 'processing', claimed_at = now(), attempts = attempts + 1
WHERE id = (
    SELECT id FROM {table}
    WHERE status = 'pending'
        AND created_at + make_interval(secs => CASE
            -- a label that is not a plain number must not break the query for every other row
            WHEN labels->>'delay' ~ '^[0-9]{{1,10}}(\\.[0-9]+)?$' THEN (labels->>'delay')::double precision
            WHEN labels->>'delay' ~ '^[0-9]+(\\.[0-9]+)?$' THEN 1e10  -- over 300 years: never ready
            ELSE 0
        END) <= now()
    ORDER BY id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, message
"""

ADD_DELIVERY_COLUMNS_QUERY = """
ALTER TABLE {table}
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0
"""

CREATE_PROCESSING_INDEX_QUERY = "CREATE INDEX IF NOT EXISTS {index} ON {table} (claimed_at) WHERE status = 'processing'"

RELEASE_STALE_MESSAGES_QUERY = """
UPDATE {table} SET status = 'pending'
WHERE status = 'processing'
    AND COALESCE(claimed_at, created_at) < now() - make_interval(secs => %s)
    AND attempts < %s
"""

FAIL_EXHAUSTED_MESSAGES_QUERY = """
UPDATE {table} SET status = 'failed'
WHERE status = 'processing'
    AND COALESCE(claimed_at, created_at) < now() - make_interval(secs => %s)
    AND attempts >= %s
RETURNING id, task_id, task_name, attempts
"""
