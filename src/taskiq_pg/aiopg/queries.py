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
INSERT INTO {} VALUES (%s, %s)
ON CONFLICT (task_id)
DO UPDATE
SET result = %s
"""

IS_RESULT_EXISTS_QUERY = """
SELECT EXISTS(
    SELECT 1 FROM {} WHERE task_id = %s
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
