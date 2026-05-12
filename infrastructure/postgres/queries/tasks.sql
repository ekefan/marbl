-- name: CreateTask :one
INSERT INTO tasks (
    type,
    value
) VALUES (
    $1, $2
)
RETURNING *;

-- name: GetTaskByID :one
SELECT * FROM tasks
WHERE id = $1
LIMIT 1;

-- name: UpdateTaskState :one
UPDATE tasks
SET
    state = $2::task_state,
    last_update_time = NOW()
WHERE id = $1
AND (
    (state = 'received'   AND $2::task_state = 'processing')
 OR (state = 'processing' AND $2::task_state = 'done')
)
RETURNING *;

-- name: CountByState :many
SELECT state, COUNT(*) AS count
FROM tasks
GROUP BY state;

-- name: CountByType :many
SELECT type, COUNT(*) AS count
FROM tasks
WHERE state = 'done'
GROUP BY type;

-- name: SumValueByType :many
SELECT type, COALESCE(SUM(value), 0)::BIGINT AS total
FROM tasks
WHERE state = 'done'
GROUP BY type;