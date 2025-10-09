CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS {} (
    task_id {} UNIQUE,
    result BYTEA
)
"""

CREATE_INDEX_QUERY = """
CREATE INDEX IF NOT EXISTS {}_task_id_idx ON {} USING HASH (task_id)
"""

INSERT_RESULT_QUERY = """
INSERT INTO {} VALUES ($1, $2)
ON CONFLICT (task_id)
DO UPDATE
SET result = $2
"""

IS_RESULT_EXISTS_QUERY = """
SELECT EXISTS(
    SELECT 1 FROM {} WHERE task_id = $1
)
"""

SELECT_RESULT_QUERY = """
SELECT result FROM {} WHERE task_id = $1
"""

DELETE_RESULT_QUERY = """
DELETE FROM {} WHERE task_id = $1
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
VALUES ($1, $2, $3, $4)
RETURNING id
"""

CLAIM_MESSAGE_QUERY = "UPDATE {} SET status = 'processing' WHERE id = $1 AND status = 'pending' RETURNING id, message"

DELETE_MESSAGE_QUERY = "DELETE FROM {} WHERE id = $1"

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
VALUES ($1, $2, $3)
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
