CREATE TYPE task_state AS ENUM ('received', 'processing', 'done');

CREATE TABLE tasks (
    id                BIGSERIAL       PRIMARY KEY,
    type              SMALLINT        NOT NULL CHECK (type >= 0 AND type <= 9),
    value             SMALLINT        NOT NULL CHECK (value >= 0 AND value <= 99),
    state             task_state      NOT NULL DEFAULT 'received',
    creation_time     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_update_time  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tasks_state ON tasks (state);
CREATE INDEX idx_tasks_type  ON tasks (type);