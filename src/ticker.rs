// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Sentin8:Cron
// SPDX-License-Identifier: Apache-2.0

//! Cron ticker loop - query due schedules (cross-tenant, SECURITY DEFINER),
//! acquire connection-scoped advisory locks, execute via proxy-back with an
//! M2M token, track job status with exponential backoff, move failures to
//! the DLQ, drain retryable pending jobs, and emit lifecycle events
//! inbox-first.
//!
//! Concurrency and failure-path details: cron `DESIGN.md` §7.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use crate::config::CronConfig;

/// Ticker heartbeat - `Some` once the first tick has run, refreshed each
/// tick. Read by the health endpoint.
pub static LAST_TICK: std::sync::Mutex<Option<DateTime<Utc>>> = std::sync::Mutex::new(None);

/// Backoff for the retry-th retry: 60s × 2^retry, capped at 1 hour
/// .
fn backoff_delay(retry_count: i32) -> chrono::Duration {
    let secs = 60i64
        .saturating_mul(1i64 << retry_count.clamp(0, 10))
        .min(3600);
    chrono::Duration::seconds(secs)
}

/// Run the ticker loop. Spawns a background task that wakes every
/// `ticker_interval_secs` and processes due schedules.
pub fn spawn(config: Arc<CronConfig>, pool: sqlx::PgPool) {
    let interval = Duration::from_secs(config.ticker_interval_secs);
    tokio::spawn(async move {
        loop {
            match tick(&pool, &config).await {
                Ok(()) => {
                    if let Ok(mut last) = LAST_TICK.lock() {
                        *last = Some(Utc::now());
                    }
                }
                Err(e) => tracing::warn!(error = %e, "cron tick failed"),
            }
            tokio::time::sleep(interval).await;
        }
    });
}

/// One tick - find due schedules, lock each, execute, record job; then
/// drain retryable pending jobs.
async fn tick(pool: &sqlx::PgPool, config: &Arc<CronConfig>) -> Result<(), sqlx::Error> {
    // Cross-tenant due read via the SECURITY DEFINER function - the
    // service role is NOBYPASSRLS and direct-table scans see no rows.
    // DBX : the function lives in cron's OWN schema (`cron.due_schedules`,
    // services/cron/sql/init.sql - search_path = cron, public would also
    // resolve it unqualified, but the reference stays explicit).
    let due = sqlx::query(
        r#"SELECT id, domain, schedule_type, cron_expression, target_config,
                  env_id, retry_count, max_retries, kind, timezone
           FROM cron.due_schedules()"#,
    )
    .fetch_all(pool)
    .await?;

    for row in due {
        let id: Uuid = row.get("id");
        let domain: String = row.get("domain");
        let schedule_type: String = row.get("schedule_type");
        let cron_expression: String = row.get("cron_expression");
        let target_config: serde_json::Value = row.get("target_config");
        let env_id: Uuid = row.get("env_id");
        let retry_count: i32 = row.get("retry_count");
        let max_retries: i32 = row.get("max_retries");
        let kind: String = row.get("kind");
        let timezone: String = row.get("timezone");

        if let Err(e) = process_due(
            pool,
            config,
            id,
            &domain,
            &schedule_type,
            &cron_expression,
            &target_config,
            env_id,
            retry_count,
            max_retries,
            &kind,
            &timezone,
        )
        .await
        {
            tracing::warn!(schedule_id = %id, error = %e, "cron schedule processing failed");
        }
    }

    // Step 3 - drain retryable jobs (schedule-driven + manually retried).
    // Per-job attempt tracking lives on the SCHEDULE (`schedules
    // .retry_count`, design ) - the jobs table is an execution record
    // and has no retry column. The drain reads the schedule's current
    // retry_count/max_retries; process_pending_job re-executes the job and
    // `run_locked` updates the schedule's counter on the outcome.
    let retryable = sqlx::query(
        r#"SELECT j.id, j.schedule_id, s.retry_count, s.domain, s.schedule_type,
                  s.cron_expression, s.target_config, s.max_retries, j.env_id, s.timezone
           FROM cron.jobs j
           JOIN cron.schedules s ON s.id = j.schedule_id
           WHERE j.status = 'pending' AND j.next_attempt_at IS NOT NULL
             AND j.next_attempt_at <= now()
           ORDER BY j.next_attempt_at LIMIT 50"#,
    )
    .fetch_all(pool)
    .await?;

    for row in retryable {
        let job_id: Uuid = row.get("id");
        let schedule_id: Uuid = row.get("schedule_id");
        let retry_count: i32 = row.get("retry_count");
        let domain: String = row.get("domain");
        let schedule_type: String = row.get("schedule_type");
        let cron_expression: String = row.get("cron_expression");
        let target_config: serde_json::Value = row.get("target_config");
        let max_retries: i32 = row.get("max_retries");
        let env_id: Uuid = row.get("env_id");
        let timezone: String = row.get("timezone");

        if let Err(e) = process_pending_job(
            pool,
            config,
            job_id,
            schedule_id,
            &domain,
            &schedule_type,
            &cron_expression,
            &target_config,
            env_id,
            retry_count,
            max_retries,
            &timezone,
        )
        .await
        {
            tracing::warn!(job_id = %job_id, error = %e, "cron job retry failed");
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_due(
    pool: &sqlx::PgPool,
    config: &Arc<CronConfig>,
    id: Uuid,
    domain: &str,
    schedule_type: &str,
    cron_expression: &str,
    target_config: &serde_json::Value,
    env_id: Uuid,
    retry_count: i32,
    max_retries: i32,
    kind: &str,
    timezone: &str,
) -> Result<(), String> {
    // One dedicated connection for the whole lock window - session
    // advisory locks are per-connection.
    let mut conn = pool.acquire().await.map_err(|e| e.to_string())?;
    let lock_key = uuid_to_i64(id);
    let got_lock: bool = sqlx::query("SELECT pg_try_advisory_lock($1)")
        .bind(lock_key)
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| e.to_string())?
        .get::<bool, _>(0);
    if !got_lock {
        return Ok(());
    }

    let result = run_locked(
        &mut conn,
        pool,
        config,
        id,
        domain,
        schedule_type,
        cron_expression,
        target_config,
        env_id,
        retry_count,
        max_retries,
        None,
        kind,
        timezone,
    )
    .await;

    let unlocked: bool = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .fetch_one(&mut *conn)
        .await
        .map(|r| r.get::<bool, _>(0))
        .unwrap_or(false);
    if !unlocked {
        tracing::warn!(schedule_id = %id, "cron advisory lock was not held on unlock");
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn process_pending_job(
    pool: &sqlx::PgPool,
    config: &Arc<CronConfig>,
    job_id: Uuid,
    schedule_id: Uuid,
    domain: &str,
    schedule_type: &str,
    cron_expression: &str,
    target_config: &serde_json::Value,
    env_id: Uuid,
    retry_count: i32,
    max_retries: i32,
    timezone: &str,
) -> Result<(), String> {
    let mut conn = pool.acquire().await.map_err(|e| e.to_string())?;
    let lock_key = uuid_to_i64(schedule_id);
    let got_lock: bool = sqlx::query("SELECT pg_try_advisory_lock($1)")
        .bind(lock_key)
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| e.to_string())?
        .get::<bool, _>(0);
    if !got_lock {
        return Ok(());
    }
    // Claim the job - only if still pending.
    let claimed = sqlx::query(
        "UPDATE cron.jobs SET status = 'running', started_at = now() WHERE id = $1 AND status = 'pending'",
    )
    .bind(job_id)
    .execute(&mut *conn)
    .await
    .map_err(|e| e.to_string())?;
    if claimed.rows_affected() == 0 {
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_key)
            .execute(&mut *conn)
            .await;
        return Ok(());
    }

    let result = run_locked(
        &mut conn,
        pool,
        config,
        schedule_id,
        domain,
        schedule_type,
        cron_expression,
        target_config,
        env_id,
        retry_count,
        max_retries,
        Some(job_id),
        "recurring",
        timezone,
    )
    .await;

    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(&mut *conn)
        .await;
    result
}

/// Next fire time strictly after now, evaluated in the schedule's IANA
/// timezone (CRON-6, cron README §7). The wall-clock fields match local
/// time in `timezone`; the result is a UTC instant. Falls back to one
/// hour ahead when the expression is unparseable - that case is rejected
/// at create time; the fallback only guards legacy rows. An unparseable
/// timezone falls back to UTC evaluation.
fn next_fire_in_tz(cron_expression: &str, timezone: &str) -> DateTime<Utc> {
    crate::cron_expr::CronExpr::parse(cron_expression)
        .ok()
        .and_then(|c| c.next_after_in_tz(Utc::now(), timezone))
        .unwrap_or_else(|| Utc::now() + chrono::Duration::hours(1))
}

/// Shared execution path while holding the advisory lock on `conn`.
/// `existing_job` = Some for the retry-drain path (job already claimed),
/// None for the due-schedule path (create the job here).
#[allow(clippy::too_many_arguments)]
async fn run_locked(
    conn: &mut sqlx::PgConnection,
    pool: &sqlx::PgPool,
    config: &Arc<CronConfig>,
    schedule_id: Uuid,
    domain: &str,
    schedule_type: &str,
    cron_expression: &str,
    target_config: &serde_json::Value,
    env_id: Uuid,
    retry_count: i32,
    max_retries: i32,
    existing_job: Option<Uuid>,
    kind: &str,
    timezone: &str,
) -> Result<(), String> {
    // Job record + tenant context on the same connection (RLS WITH CHECK).
    //
    // Transaction-locality (cron DESIGN §5): both statements run in ONE
    // transaction on the lock connection. Each was previously autocommit -
    // `set_config(..., true)` is transaction-local, so the context
    // evaporated at the context-set statement's own commit and every
    // INSERT tripped the jobs RLS policy (`new row violates row-level
    // security policy for table "jobs"` on every due schedule).
    let job_id = match existing_job {
        Some(j) => j,
        None => {
            // Open-job guard (cron DESIGN §5): a schedule with a
            // pending/running job already has its next attempt scheduled
            // - the retry drain owns that execution. Claiming the same
            // schedule now would collide on
            // `sentin8_cron_jobs_open_per_schedule` and only produce a
            // per-tick warn loop while the retry backoff runs its course.
            // The due read skips the schedule this tick. The check runs in
            // a tenant-context transaction (jobs is FORCE-RLS'd - a bare
            // pool read returns no rows; §4.3d same-connection locality).
            let mut gtx = <sqlx::PgConnection as sqlx::Connection>::begin(&mut *conn)
                .await
                .map_err(|e| e.to_string())?;
            sqlx::query("SELECT sentin8_set_tenant_context($1)")
                .bind(env_id)
                .execute(&mut *gtx)
                .await
                .map_err(|e| e.to_string())?;
            let open: Option<Uuid> = sqlx::query_scalar(
                "SELECT id FROM cron.jobs WHERE schedule_id = $1 AND status IN ('pending','running') LIMIT 1",
            )
            .bind(schedule_id)
            .fetch_optional(&mut *gtx)
            .await
            .map_err(|e| e.to_string())?;
            gtx.commit().await.map_err(|e| e.to_string())?;
            if open.is_some() {
                // Skip WITHOUT unlocking - the caller (process_due /
                // process_pending_job) owns the unlock on this connection;
                // unlocking here double-releases (sqlx logs the
                // "not owned" notice and the caller's own check then
                // warns "lock was not held on unlock").
                return Ok(());
            }
            let mut tx = <sqlx::PgConnection as sqlx::Connection>::begin(&mut *conn)
                .await
                .map_err(|e| e.to_string())?;
            sqlx::query("SELECT sentin8_set_tenant_context($1)")
                .bind(env_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
            let row = sqlx::query(
                r#"INSERT INTO cron.jobs (schedule_id, status, started_at, env_id)
                   VALUES ($1, 'running', now(), $2)
                   RETURNING id"#,
            )
            .bind(schedule_id)
            .bind(env_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
            let j: Uuid = row.get("id");
            tx.commit().await.map_err(|e| e.to_string())?;
            j
        }
    };

    let correlation_id = schedule_id.to_string();
    let idempotency_key = format!("schedule:{schedule_id}:{job_id}");
    let result = execute_proxy_back(
        target_config,
        schedule_type,
        env_id,
        schedule_id,
        job_id,
        &correlation_id,
        &idempotency_key,
        &config.m2m,
    )
    .await;
    match result {
        Ok(result_json) => {
            let next_run = next_fire_in_tz(cron_expression, timezone);
            let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
            sqlx::query("SELECT sentin8_set_tenant_context($1)")
                .bind(env_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
            sqlx::query(
                r#"UPDATE cron.jobs
                   SET status = 'completed', completed_at = now(), result = $2, next_attempt_at = NULL
                   WHERE id = $1"#,
            )
            .bind(job_id)
            .bind(&result_json)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
            sqlx::query(
                r#"UPDATE cron.schedules
                   SET last_run_at = now(), next_run_at = $2, retry_count = 0
                   WHERE id = $1"#,
            )
            .bind(schedule_id)
            .bind(next_run)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
            sentin8::bus::outbox::insert_outbox_row(
                &mut *tx,
                env_id,
                "cron",
                "sentin8:cron:schedule:completed",
                &serde_json::json!({
                    "schedule_id": schedule_id, "job_id": job_id,
                    "domain": domain, "schedule_type": schedule_type,
                    "env_id": env_id, "result": result_json,
                    "correlation_id": correlation_id,
                    "idempotency_key": idempotency_key,
                }),
            )
            .await
            .map_err(|e| e.to_string())?;
            // One-shot schedules (cron README §7): the fire IS the
            // completion - mark `completed` so the due query never returns
            // the row again (recurring rows keep the expression-driven
            // next_run_at reschedule).
            if kind == "once" {
                sqlx::query(
                    r#"UPDATE cron.schedules SET status = 'completed', updated_at = now() WHERE id = $1"#,
                )
                .bind(schedule_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
            }
            tx.commit().await.map_err(|e| e.to_string())?;
            Ok(())
        }
        Err(e) => {
            let new_retry = retry_count + 1;
            let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
            sqlx::query("SELECT sentin8_set_tenant_context($1)")
                .bind(env_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
            if new_retry >= max_retries {
                sqlx::query(
                    r#"INSERT INTO cron.dlq (schedule_id, job_id, domain, schedule_type, payload, error_message, retry_count, env_id)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
                )
                .bind(schedule_id)
                .bind(job_id)
                .bind(domain)
                .bind(schedule_type)
                .bind(target_config)
                .bind(&e)
                .bind(new_retry)
                .bind(env_id)
                .execute(&mut *tx)
                .await
                .map_err(|e2| e2.to_string())?;
                sqlx::query(
                    r#"UPDATE cron.jobs SET status = 'failed', completed_at = now(), error_message = $2, next_attempt_at = NULL
                       WHERE id = $1"#,
                )
                .bind(job_id)
                .bind(&e)
                .execute(&mut *tx)
                .await
                .map_err(|e2| e2.to_string())?;
                // Next attempt per the cron expression - DLQ does not stop the schedule.
                sqlx::query(
                    r#"UPDATE cron.schedules SET retry_count = 0, next_run_at = $2 WHERE id = $1"#,
                )
                .bind(schedule_id)
                .bind(next_fire_in_tz(cron_expression, timezone))
                .execute(&mut *tx)
                .await
                .map_err(|e2| e2.to_string())?;
                sentin8::bus::outbox::insert_outbox_row(
                    &mut *tx,
                    env_id,
                    "cron",
                    "sentin8:cron:schedule:failed",
                    &serde_json::json!({
                        "schedule_id": schedule_id, "job_id": job_id,
                        "domain": domain, "schedule_type": schedule_type,
                        "env_id": env_id, "error": e, "retry_count": new_retry,
                        "correlation_id": correlation_id,
                        "idempotency_key": idempotency_key,
                    }),
                )
                .await
                .map_err(|e2| e2.to_string())?;
            } else {
                let delay = backoff_delay(new_retry).num_seconds();
                sqlx::query(
                    r#"UPDATE cron.jobs
                       SET status = 'pending', completed_at = now(), error_message = $2,
                           next_attempt_at = now() + make_interval(secs => $3)
                       WHERE id = $1"#,
                )
                .bind(job_id)
                .bind(&e)
                .bind(delay)
                .execute(&mut *tx)
                .await
                .map_err(|e2| e2.to_string())?;
                sqlx::query(
                    r#"UPDATE cron.schedules SET retry_count = $2, next_run_at = now() + make_interval(secs => $3) WHERE id = $1"#,
                )
                .bind(schedule_id)
                .bind(new_retry)
                .bind(delay)
                .execute(&mut *tx)
                .await
                .map_err(|e2| e2.to_string())?;
            }
            tx.commit().await.map_err(|e2| e2.to_string())?;
            Err(e)
        }
    }
}

/// Proxy-back execution - call the target service's internal API with a
/// fresh M2M token and a self-contained envelope.
///
/// `pub` (not `pub(crate)`): the test binary includes this module via
/// `#[path]`, and the proxy-back contract is asserted from there.
#[allow(clippy::too_many_arguments)]
pub async fn execute_proxy_back(
    target_config: &serde_json::Value,
    schedule_type: &str,
    env_id: Uuid,
    schedule_id: Uuid,
    _job_id: Uuid,
    correlation_id: &str,
    idempotency_key: &str,
    m2m: &crate::config::M2mSettings,
) -> Result<serde_json::Value, String> {
    let base_url = target_config
        .get("baseUrl")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let internal_path = target_config
        .get("internalPath")
        .and_then(|v| v.as_str())
        .unwrap_or("/v1/internal");
    if base_url.is_empty() {
        return Err("missing baseUrl in target_config".into());
    }
    // the cert-presenting transport (the `sentin8:cron`
    // workload identity rides the TLS handshake - no bearer).
    let client = m2m
        .client()
        .map_err(|e| format!("proxy-back transport: {e}"))?;
    let url = format!("{base_url}{internal_path}/schedules/run");
    // Bound the callback (cron DESIGN §7): a hung
    // callback must not hold its claimed job indefinitely; on timeout the
    // call resolves as a failure and the retry/backoff path releases the
    // claim.
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(crate::config::CronConfig::from_env().job_timeout_secs),
        client
            .post(&url)
            .json(&serde_json::json!({
                "schedule_id": schedule_id,
                "schedule_type": schedule_type,
                "env_id": env_id,
                "correlation_id": correlation_id,
                "idempotency_key": idempotency_key,
            }))
            .send(),
    )
    .await
    .map_err(|_| "proxy-back timed out after job timeout")?
    .map_err(|e| format!("proxy-back HTTP error: {e}"))?;
    if !resp.status().is_success() {
        // Failure-path response capture (cron README §7): retain the first
        // 4KiB of the non-2xx body for debugging - bounded storage.
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        let snippet: String = body.chars().take(4096).collect();
        return Err(format!("proxy-back status {status}: {snippet}"));
    }
    resp.json()
        .await
        .map_err(|e| format!("proxy-back decode: {e}"))
}

/// Derive a stable i64 key from a UUID for advisory locks.
///
/// `pub` for the same reason as `execute_proxy_back` - test-binary access.
pub fn uuid_to_i64(id: Uuid) -> i64 {
    let bytes = id.as_bytes();
    let mut h: i64 = 0;
    for &b in &bytes[..8] {
        h = h.wrapping_shl(8).wrapping_add(b as i64);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        assert_eq!(backoff_delay(0).num_seconds(), 60);
        assert_eq!(backoff_delay(1).num_seconds(), 120);
        assert_eq!(backoff_delay(2).num_seconds(), 240);
        assert_eq!(backoff_delay(8).num_seconds(), 3600);
        assert_eq!(backoff_delay(50).num_seconds(), 3600);
    }
}
