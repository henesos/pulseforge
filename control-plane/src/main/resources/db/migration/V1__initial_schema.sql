-- Control-plane state only. Raw measurements never touch Postgres; they live in ClickHouse.
-- What is stored here is the small, transactional, mutable part: what to run and what happened.

CREATE TABLE scenarios (
    id           UUID         PRIMARY KEY,
    name         VARCHAR(200) NOT NULL,
    definition   TEXT         NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_scenarios_name UNIQUE (name)
);

COMMENT ON COLUMN scenarios.definition IS
    'Verbatim scenario YAML. Kept as submitted so a run can always be reproduced byte-for-byte, '
    'even after the parser evolves.';

CREATE TABLE test_runs (
    id                UUID         PRIMARY KEY,
    scenario_id       UUID         NOT NULL REFERENCES scenarios (id) ON DELETE RESTRICT,
    status            VARCHAR(20)  NOT NULL,
    arrival_rate      INTEGER      NOT NULL,
    duration_seconds  INTEGER      NOT NULL,
    ramp_up_seconds   INTEGER      NOT NULL DEFAULT 0,
    expected_workers  INTEGER      NOT NULL DEFAULT 0,
    finished_workers  INTEGER      NOT NULL DEFAULT 0,
    status_reason     TEXT,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    CONSTRAINT ck_test_runs_status CHECK (
        status IN ('PENDING', 'RUNNING', 'COMPLETED', 'DEGRADED', 'ABORTED', 'FAILED')
    )
);

COMMENT ON COLUMN test_runs.status_reason IS
    'Why a run ended in DEGRADED or FAILED, e.g. which worker stopped sending heartbeats.';

CREATE INDEX idx_test_runs_scenario ON test_runs (scenario_id, created_at DESC);
CREATE INDEX idx_test_runs_active ON test_runs (status) WHERE status IN ('PENDING', 'RUNNING');

CREATE TABLE run_assertions (
    id          UUID         PRIMARY KEY,
    run_id      UUID         NOT NULL REFERENCES test_runs (id) ON DELETE CASCADE,
    expression  VARCHAR(120) NOT NULL,
    actual      DOUBLE PRECISION,
    passed      BOOLEAN,
    evaluated_at TIMESTAMPTZ
);

CREATE INDEX idx_run_assertions_run ON run_assertions (run_id);
