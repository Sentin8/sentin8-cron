-- SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
-- SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
-- SPDX-FileCopyrightText: Copyright Contributors to the Sentin8:Cron
-- SPDX-License-Identifier: Apache-2.0

-- Sentin8:Cron - standalone scheduling service tables.
-- Stores schedules, job execution records, and dead-letter queue items.
-- Uses PostgreSQL advisory locks for distributed execution safety.

CREATE SCHEMA IF NOT EXISTS cron;
-- Natural key (env_id, domain, schedule_type): one declared schedule per
-- (env, owning service, schedule name) - `vertical enroll` upserts platform
-- schedules ON CONFLICT (env_id, domain, schedule_type), and the uniqueness
-- is the schema-level guarantee a re-enroll converges instead of duplicating
-- rows (cron DESIGN §3 SCHEDULES).
CREATE TABLE IF NOT EXISTS cron.schedules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain          TEXT NOT NULL,                     -- 'ledger', 'payments', etc.
    schedule_type   TEXT NOT NULL,                     -- 'interest_accrual', 'fee_application', etc.
    cron_expression TEXT NOT NULL,                     -- standard 5-field cron
    target_config   JSONB NOT NULL DEFAULT '{}',       -- {baseUrl, authToken, internalPath}
    status          TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'deleted', 'completed')),
    kind            TEXT NOT NULL DEFAULT 'recurring' CHECK (kind IN ('recurring', 'once')),
    -- CRON-6 (cron README §7): IANA zone the cron_expression's wall-clock
    -- fields match against. 'UTC' preserves the legacy behavior.
    timezone        TEXT NOT NULL DEFAULT 'UTC',
    last_run_at     TIMESTAMPTZ,
    next_run_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    env_id  UUID NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (env_id, domain, schedule_type)
);

CREATE INDEX IF NOT EXISTS idx_cron_schedules_env ON cron.schedules (env_id);
CREATE INDEX IF NOT EXISTS idx_cron_schedules_due ON cron.schedules (next_run_at) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_cron_schedules_domain ON cron.schedules (domain);

CREATE TABLE IF NOT EXISTS cron.jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schedule_id     UUID NOT NULL REFERENCES cron.schedules(id),
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    result          JSONB,
    error_message   TEXT,
    next_attempt_at TIMESTAMPTZ,
    env_id  UUID NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cron_jobs_schedule ON cron.jobs (schedule_id);
CREATE INDEX IF NOT EXISTS idx_cron_jobs_env ON cron.jobs (env_id);
CREATE INDEX IF NOT EXISTS idx_cron_jobs_status ON cron.jobs (status);
CREATE INDEX IF NOT EXISTS idx_cron_jobs_retryable ON cron.jobs (next_attempt_at)
    WHERE status = 'pending' AND next_attempt_at IS NOT NULL;

-- One open (pending/running) job per schedule at a time - advisory-lock
-- backstop (design §4.5).
CREATE UNIQUE INDEX IF NOT EXISTS sentin8_cron_jobs_open_per_schedule
    ON cron.jobs (schedule_id)
    WHERE status IN ('pending', 'running');

CREATE TABLE IF NOT EXISTS cron.dlq (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schedule_id     UUID NOT NULL,
    job_id          UUID,
    domain          TEXT NOT NULL,
    schedule_type   TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    error_message   TEXT NOT NULL,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    env_id  UUID NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cron_dlq_env ON cron.dlq (env_id);
CREATE INDEX IF NOT EXISTS idx_cron_dlq_domain ON cron.dlq (domain);

-- Cross-tenant due-schedule read for the ticker (design §4.3). The service
-- role is NOBYPASSRLS, so direct scans see no rows; this SECURITY DEFINER
-- function is the only cross-tenant read surface, scoped to due schedules.
CREATE OR REPLACE FUNCTION cron.due_schedules()
RETURNS TABLE (id UUID, domain TEXT, schedule_type TEXT, cron_expression TEXT,
               target_config JSONB, env_id UUID, retry_count INTEGER,
               max_retries INTEGER, kind TEXT, timezone TEXT)
LANGUAGE sql SECURITY DEFINER STABLE SET search_path = public AS $$
    SELECT s.id, s.domain, s.schedule_type, s.cron_expression, s.target_config,
           s.env_id, s.retry_count, s.max_retries, s.kind, s.timezone
      FROM cron.schedules s
     WHERE s.status = 'active' AND s.next_run_at <= now()
     ORDER BY s.next_run_at LIMIT 50;
$$;

-- Enable RLS on all tables
ALTER TABLE cron.schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE cron.schedules FORCE ROW LEVEL SECURITY;
ALTER TABLE cron.jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE cron.jobs FORCE ROW LEVEL SECURITY;
ALTER TABLE cron.dlq ENABLE ROW LEVEL SECURITY;
ALTER TABLE cron.dlq FORCE ROW LEVEL SECURITY;

-- RLS policies
DROP POLICY IF EXISTS cron_schedules_select ON cron.schedules;
CREATE POLICY cron_schedules_select ON cron.schedules
    FOR SELECT USING (sentin8_current_env_id() = env_id);
DROP POLICY IF EXISTS cron_schedules_insert ON cron.schedules;
CREATE POLICY cron_schedules_insert ON cron.schedules
    FOR INSERT WITH CHECK (sentin8_current_env_id() = env_id);
DROP POLICY IF EXISTS cron_schedules_update ON cron.schedules;
CREATE POLICY cron_schedules_update ON cron.schedules
    FOR UPDATE USING (sentin8_current_env_id() = env_id);
DROP POLICY IF EXISTS cron_schedules_delete ON cron.schedules;
CREATE POLICY cron_schedules_delete ON cron.schedules
    FOR DELETE USING (sentin8_current_env_id() = env_id);

DROP POLICY IF EXISTS cron_jobs_select ON cron.jobs;
CREATE POLICY cron_jobs_select ON cron.jobs
    FOR SELECT USING (sentin8_current_env_id() = env_id);
DROP POLICY IF EXISTS cron_jobs_insert ON cron.jobs;
CREATE POLICY cron_jobs_insert ON cron.jobs
    FOR INSERT WITH CHECK (sentin8_current_env_id() = env_id);
DROP POLICY IF EXISTS cron_jobs_update ON cron.jobs;
CREATE POLICY cron_jobs_update ON cron.jobs
    FOR UPDATE USING (sentin8_current_env_id() = env_id);

DROP POLICY IF EXISTS cron_dlq_select ON cron.dlq;
CREATE POLICY cron_dlq_select ON cron.dlq
    FOR SELECT USING (sentin8_current_env_id() = env_id);
DROP POLICY IF EXISTS cron_dlq_insert ON cron.dlq;
CREATE POLICY cron_dlq_insert ON cron.dlq
    FOR INSERT WITH CHECK (sentin8_current_env_id() = env_id);
DROP POLICY IF EXISTS cron_dlq_update ON cron.dlq;
CREATE POLICY cron_dlq_update ON cron.dlq
    FOR UPDATE USING (sentin8_current_env_id() = env_id);
DROP POLICY IF EXISTS cron_dlq_delete ON cron.dlq;
CREATE POLICY cron_dlq_delete ON cron.dlq
    FOR DELETE USING (sentin8_current_env_id() = env_id);

-- ── Grants ──

-- sentin8_cron_service: DML on all cron tables + EXECUTE on the
-- cross-tenant due-schedule reader.
GRANT SELECT, INSERT, UPDATE, DELETE ON
    cron.schedules, cron.jobs, cron.dlq
    TO sentin8_cron_service;
GRANT EXECUTE ON FUNCTION cron.due_schedules() TO sentin8_cron_service;


-- ── Local outbox (DBX, architecture-principles §6.7) ─────────────────────
-- Producer path: cron.outbox rows commit in the SAME transaction as the
-- domain change; a post-commit relay pushes them to Bus POST /v1/produce.
-- Generated from sentin8-outbox::outbox_table_sql("cron").
CREATE TABLE IF NOT EXISTS cron.outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id          UUID NOT NULL,
    event_type      TEXT NOT NULL,
    stream_family   TEXT NOT NULL,
    payload         JSONB NOT NULL,
    publisher_id_token  TEXT,
    publisher_subject   TEXT,
    relayed         BOOLEAN NOT NULL DEFAULT false,
    relayed_at      TIMESTAMPTZ,
    relay_attempts  INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cron_outbox_unrelayed
    ON cron.outbox (created_at) WHERE relayed = false;
ALTER TABLE cron.outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE cron.outbox FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS outbox_select ON cron.outbox;
CREATE POLICY outbox_select ON cron.outbox FOR SELECT
    USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
DROP POLICY IF EXISTS outbox_insert ON cron.outbox;
CREATE POLICY outbox_insert ON cron.outbox FOR INSERT
    WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
DROP POLICY IF EXISTS outbox_update ON cron.outbox;
CREATE POLICY outbox_update ON cron.outbox FOR UPDATE
    USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
CREATE OR REPLACE FUNCTION cron.cron_notify_outbox() RETURNS TRIGGER
LANGUAGE plpgsql AS $fn$
BEGIN
    PERFORM pg_notify('cron_outbox_changed', '');
    RETURN NEW;
END;
$fn$;
DROP TRIGGER IF EXISTS outbox_notify_trigger ON cron.outbox;
CREATE TRIGGER outbox_notify_trigger
    AFTER INSERT ON cron.outbox
    FOR EACH ROW
    EXECUTE FUNCTION cron.cron_notify_outbox();


-- ── Local audit journal (DBX §6.8) ────────────────────────────────────────
-- Spill journal for the canonical audit push: rows written same-tx as the
-- domain change, pushed to Sys POST /v1/audit (sentin8:sys:audit:write), then
-- marked pushed. A Sys outage never drops audit. Generated from
-- sentin8-audit::audit_journal_sql("cron").
CREATE TABLE IF NOT EXISTS cron.audit_journal (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id          UUID NOT NULL,
    action          TEXT NOT NULL,
    target_resource_type TEXT,
    target_resource_id    TEXT,
    actor_user_id   TEXT,
    actor_subject_type TEXT,
    severity        TEXT NOT NULL DEFAULT 'info',
    correlation_id  TEXT,
    before_state    JSONB,
    after_state     JSONB,
    details         JSONB,
    pushed          BOOLEAN NOT NULL DEFAULT false,
    pushed_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cron_audit_journal_unpushed
    ON cron.audit_journal (created_at) WHERE pushed = false;
ALTER TABLE cron.audit_journal ENABLE ROW LEVEL SECURITY;
ALTER TABLE cron.audit_journal FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS audit_journal_dml ON cron.audit_journal;
CREATE POLICY audit_journal_dml ON cron.audit_journal
    USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())
    WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id());


-- ── Schema grants (service-owned - the 2026-10-03 ownership pass) ─────
-- This service's own schema: USAGE for its role (platform.sql no longer
-- seeds per-service USAGE; the service declares it here).
GRANT USAGE ON SCHEMA cron TO sentin8_cron_service;
GRANT SELECT, INSERT, UPDATE, DELETE ON cron.outbox TO sentin8_cron_service;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA cron TO sentin8_cron_service;
GRANT SELECT, INSERT, UPDATE, DELETE ON cron.audit_journal TO sentin8_cron_service;
