// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Sentin8:Cron
// SPDX-License-Identifier: Apache-2.0

//! Cron HTTP handlers - schedule, job, and DLQ management API.
//!
//! Stories.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::auth;
use crate::config::CronConfig;

#[derive(Clone)]
/// app state.
pub struct AppState {
    /// The config.
    pub config: Arc<CronConfig>,
    /// The pool.
    pub pool: sqlx::PgPool,
    /// Shield engine ( - ): policy is an ADDITIONAL
    /// gate after scope checks; can only deny.
    /// The policy.
    pub policy: Arc<crate::policy::Engine>,
    /// Shared Sentin8:Policy admin surface ( - one
    /// implementation for every service; mounted in server::routes).
    /// The admin.
    pub admin: sentin8::shield::admin::SharedAdminState,
}

fn err_db(e: sqlx::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}"))
}

/// Set the RLS tenant context inside a transaction. `sentin8_set_tenant_context`
/// uses `set_config(..., true)` - transaction-local, so this MUST run inside
/// the same transaction as the domain statements (
/// ).
async fn set_rls_tx(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    env_id: Uuid,
) -> Result<(), (StatusCode, String)> {
    sqlx::query("SELECT sentin8_set_tenant_context($1)")
        .bind(env_id)
        .execute(&mut **tx)
        .await
        .map_err(err_db)?;
    Ok(())
}

/// Begin a transaction with the tenant context set - the RLS-scoped unit
/// for every handler (auth SEC- pattern).
async fn begin_tenant_tx(
    pool: &sqlx::PgPool,
    env_id: Uuid,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, (StatusCode, String)> {
    let mut tx = pool.begin().await.map_err(err_db)?;
    set_rls_tx(&mut tx, env_id).await?;
    Ok(tx)
}

// ── Schedules ────────────────────────────────────────────────────────────

/// Query params accepted by all env-scoped endpoints. `target_env_id` is
/// forwarded by the BFF proxy to identify the tenant env when the caller's
/// token carries the system env (platform operators managing a tenant env).
/// REQUIRED - no silent fallback to the caller's own env.
#[derive(Debug, Deserialize, ToSchema)]
/// The targetenvquery type.
pub struct TargetEnvQuery {
    #[serde(default)]
    /// target env identifier.
    pub target_env_id: Option<String>,
}

/// Resolve the effective env_id: prefer `target_env_id` from query params
/// (BFF proxy), fall back to the token's env_id (the M2M service
/// self-registration path - schedule create rides the service's own
/// token, whose env is the system env).
fn resolve_env_id(a: &auth::AuthContext, q: &TargetEnvQuery) -> Result<Uuid, (StatusCode, String)> {
    if let Some(id) = q
        .target_env_id
        .as_deref()
        .and_then(|s| Uuid::parse_str(s).ok())
    {
        return Ok(id);
    }
    Ok(a.env_id())
}
#[derive(Debug, Deserialize, ToSchema)]
/// Request body for createschedule.
pub struct CreateScheduleRequest {
    /// The domain.
    pub domain: String,
    /// The schedule type.
    pub schedule_type: String,
    /// The cron expression.
    pub cron_expression: String,
    #[serde(default)]
    /// The target config.
    pub target_config: serde_json::Value,
    /// `once` = single fire at `fire_at` (then `completed`); default
    /// recurring = the cron expression drives every fire.
    #[serde(default)]
    pub kind: Option<String>,
    /// Single fire time for `kind: "once"` schedules (RFC 3339).
    #[serde(default)]
    pub fire_at: Option<chrono::DateTime<chrono::Utc>>,
    /// IANA zone the cron_expression's wall-clock fields match against
    /// (CRON-6, cron README §7). Default 'UTC'. Rejected at create time
    /// when unparseable.
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default = "default_max_retries")]
    /// Maximum retries.
    pub max_retries: i32,
}

fn default_max_retries() -> i32 {
    3
}

/// Normalize a client-supplied `target_config` into a JSON OBJECT (cron
/// DESIGN §3): blank strings, JSON-null and missing values become `{}`;
/// a JSON-encoded string (`"{\"baseUrl\":...}"`) is parsed through - the
/// RFW admin form sends TEXT, and without this unwrap the empty-string
/// case persisted a bare string that only surfaced as a per-tick ticker
/// failure. Any other non-object shape is a 400.
fn normalize_target_config(v: serde_json::Value) -> Result<serde_json::Value, String> {
    match v {
        serde_json::Value::Null => Ok(serde_json::json!({})),
        serde_json::Value::String(s) if s.trim().is_empty() => Ok(serde_json::json!({})),
        serde_json::Value::String(s) => serde_json::from_str::<serde_json::Value>(&s)
            .map(normalize_target_config_nested)
            .unwrap_or_else(|e| Err(format!("target_config is not valid JSON: {e}"))),
        obj @ serde_json::Value::Object(_) => Ok(obj),
        other => Err(format!(
            "target_config must be a JSON object, got: {}",
            other
        )),
    }
}

/// Flatten a one-level JSON-encoded string field inside an object
/// (`{"target_config": "{...}"}` style payloads) - not needed today; the
/// identity passthrough keeps the shape stable.
fn normalize_target_config_nested(v: serde_json::Value) -> Result<serde_json::Value, String> {
    Ok(v)
}
#[derive(Debug, Serialize, ToSchema)]
/// Response payload for schedule.
pub struct ScheduleResponse {
    /// Identifier.
    pub id: Uuid,
    /// The domain.
    pub domain: String,
    /// The schedule type.
    pub schedule_type: String,
    /// The cron expression.
    pub cron_expression: String,
    /// The target config.
    pub target_config: serde_json::Value,
    /// The status.
    pub status: String,
    /// The next run at.
    pub next_run_at: chrono::DateTime<chrono::Utc>,
    /// Maximum retries.
    pub max_retries: i32,
}

/// `POST /v1/schedules`
#[utoipa::path(
    post,
    operation_id = "schedule.create",
    path = "/v1/schedules",
    request_body = CreateScheduleRequest,
    responses(
        (status = 200, description = "Schedule created", body = ScheduleResponse),
        (status = 403, description = "Missing cron:write scope"),
    ),
    security(("sentin8:cron:write" = [])),
    tag = "cron",
)]
/// Create schedule.
pub async fn create_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TargetEnvQuery>,
    Json(req): Json<CreateScheduleRequest>,
) -> Result<Json<ScheduleResponse>, (StatusCode, String)> {
    let a = auth::verify_token(&state.config, &headers).await?;
    crate::policy::require_policy(
        &state.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("sentin8:cron:write"),
    )
    .await?;
    let env_id = resolve_env_id(&a, &q)?;
    // `kind: "once"` = one-shot schedule (cron README §7): `next_run_at`
    // is the single fire time and the tick marks the schedule `completed`
    // after one claim; recurring (the default) keeps the expression as the
    // driver. The expression is still validated for `once` rows so a
    // later resume path cannot trip on a garbage expression.
    let once = req.kind.as_deref() == Some("once");
    let expr = crate::cron_expr::CronExpr::parse(&req.cron_expression)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    // CRON-6: validate the IANA zone at create time - a bad zone is a 400,
    // not a runtime surprise on every tick.
    let timezone = req.timezone.clone().unwrap_or_else(|| "UTC".to_string());
    if timezone.parse::<chrono_tz::Tz>().is_err() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid timezone: {timezone}"),
        ));
    }
    let next_run_at = match (once, req.fire_at) {
        (true, Some(at)) => at,
        (true, None) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "kind 'once' requires fire_at".into(),
            ))
        }
        (false, _) => expr
            .next_after_in_tz(chrono::Utc::now(), &timezone)
            .ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    "cron expression has no future fire time".into(),
                )
            })?,
    };
    // Target validation at CREATE time (cron DESIGN §3; WARN-22): a config
    // the ticker can never execute must be rejected HERE - the previous
    // behavior accepted a blank/malformed config and every tick then
    // failed ("missing baseUrl"), retried into backoff, and re-fired due
    // claims against the same schedule (advisory-lock + open-job unique
    // contention) - a per-tick warn loop for a configuration typo.
    let target_config = normalize_target_config(req.target_config.clone())
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    if target_config
        .get("baseUrl")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .is_empty()
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "target_config must carry a non-empty baseUrl (the proxied service's origin)".into(),
        ));
    }
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    // Natural-key conflict resolution (cron DESIGN §3 SCHEDULES): the
    // full unique index on (env_id, domain, schedule_type) backs the
    // `vertical enroll` upsert, so a soft-deleted/completed row still
    // holds the key. A re-created schedule with the same natural key
    // REVIVES the deleted/completed row with the new definition (the
    // name was released for re-use by the delete; the key itself is
    // permanent); a conflict against a LIVE schedule is a real 409,
    // not a 500 on the constraint.
    let existing: Option<(Uuid, String)> = sqlx::query_as(
        r#"SELECT id, status FROM schedules
           WHERE env_id = $1 AND domain = $2 AND schedule_type = $3"#,
    )
    .bind(env_id)
    .bind(&req.domain)
    .bind(&req.schedule_type)
    .fetch_optional(&mut *tx)
    .await
    .map_err(err_db)?;
    let row = match existing {
        Some((_, status)) if status != "deleted" && status != "completed" => {
            return Err((
                StatusCode::CONFLICT,
                format!(
                    "schedule '{}:{}' already exists in this environment",
                    req.domain, req.schedule_type
                ),
            ));
        }
        Some((id, _)) => {
            sqlx::query(
                r#"UPDATE schedules
                     SET cron_expression = $2, target_config = $3,
                         status = 'active', kind = $4, timezone = $5,
                         next_run_at = $6, retry_count = 0,
                         max_retries = $7, updated_at = now()
                   WHERE id = $1
                   RETURNING id, domain, schedule_type, cron_expression, target_config, status, next_run_at, max_retries, kind"#,
            )
            .bind(id)
            .bind(&req.cron_expression)
            .bind(&target_config)
            .bind(if once { "once" } else { "recurring" })
            .bind(&timezone)
            .bind(next_run_at)
            .bind(req.max_retries)
            .fetch_one(&mut *tx)
            .await
            .map_err(err_db)?
        }
        None => {
            sqlx::query(
                r#"INSERT INTO schedules (domain, schedule_type, cron_expression, target_config, max_retries, env_id, next_run_at, kind, timezone)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                   RETURNING id, domain, schedule_type, cron_expression, target_config, status, next_run_at, max_retries, kind"#,
            )
            .bind(&req.domain)
            .bind(&req.schedule_type)
            .bind(&req.cron_expression)
            .bind(&target_config)
            .bind(req.max_retries)
            .bind(env_id)
            .bind(next_run_at)
            .bind(if once { "once" } else { "recurring" })
            .bind(&timezone)
            .fetch_one(&mut *tx)
            .await
            .map_err(err_db)?
        }
    };
    let schedule = row_to_schedule(row);
    emit_event_tx(
        &mut tx,
        env_id,
        "sentin8:cron:schedule:created",
        &serde_json::json!({
            "schedule_id": schedule.id, "domain": schedule.domain,
            "schedule_type": schedule.schedule_type,
            "cron_expression": schedule.cron_expression, "env_id": env_id,
        }),
    )
    .await?;
    tx.commit().await.map_err(err_db)?;
    Ok(Json(schedule))
}

/// `GET /v1/schedules`
#[utoipa::path(
    get,
    operation_id = "schedule.list",
    path = "/v1/schedules",
    responses(
        (status = 200, description = "List of schedules", body = [ScheduleResponse]),
        (status = 403, description = "Missing cron:read scope"),
    ),
    security(("sentin8:cron:read" = [])),
    tag = "cron",
)]
/// List schedules.
pub async fn list_schedules(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TargetEnvQuery>,
) -> Result<Json<Vec<ScheduleResponse>>, (StatusCode, String)> {
    let a = auth::verify_token(&state.config, &headers).await?;
    let env_id = resolve_env_id(&a, &q)?;
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    let rows = sqlx::query(
        r#"SELECT id, domain, schedule_type, cron_expression, target_config, status, next_run_at, max_retries
           FROM schedules WHERE env_id = $1 AND status <> 'deleted'
           ORDER BY next_run_at"#,
    )

    .bind(env_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(err_db)?;
    Ok(Json(rows.into_iter().map(row_to_schedule).collect()))
}

/// `GET /v1/schedules/{id}` - schedule detail.
#[utoipa::path(
    get,
    operation_id = "schedule.get",
    path = "/v1/schedules/{id}",
    params(("id" = Uuid, Path, description = "Schedule ID")),
    responses(
        (status = 200, description = "Schedule detail", body = ScheduleResponse),
        (status = 404, description = "Schedule not found"),
        (status = 403, description = "Missing cron:read scope"),
    ),
    security(("sentin8:cron:read" = [])),
    tag = "cron",
)]
/// Fetch schedule.
pub async fn get_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TargetEnvQuery>,
    Path(id): Path<Uuid>,
) -> Result<Json<ScheduleResponse>, (StatusCode, String)> {
    let a = auth::verify_token(&state.config, &headers).await?;
    let env_id = resolve_env_id(&a, &q)?;
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    let row = sqlx::query(
        r#"SELECT id, domain, schedule_type, cron_expression, target_config, status, next_run_at, max_retries
           FROM schedules WHERE id = $1 AND env_id = $2 AND status <> 'deleted'"#,
    )
    .bind(id)
    .bind(env_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(err_db)?
    .ok_or_else(|| (StatusCode::NOT_FOUND, format!("schedule {id} not found")))?;
    Ok(Json(row_to_schedule(row)))
}

/// `PATCH /v1/schedules/{id}` - pause/resume via status (cron README §7).
/// `{status: "paused" | "active"}`; resume recomputes `next_run_at` from
/// now so a paused schedule does not fire for misses during the pause.
#[derive(Debug, Deserialize, ToSchema)]
/// Request payload for update schedule.
pub struct UpdateScheduleRequest {
    /// Target status (`paused` or `active`).
    pub status: String,
}

/// Update schedule status.
#[utoipa::path(
    patch,
    operation_id = "schedule.update",
    path = "/v1/schedules/{id}",
    params(("id" = Uuid, Path, description = "Schedule ID")),
    request_body = UpdateScheduleRequest,
    responses(
        (status = 204, description = "Status applied"),
        (status = 400, description = "Unknown status"),
        (status = 404, description = "Schedule not found"),
        (status = 403, description = "Missing cron:write scope"),
    ),
    security(("sentin8:cron:write" = [])),
    tag = "cron",
)]
pub async fn update_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TargetEnvQuery>,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateScheduleRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let a = auth::verify_token(&state.config, &headers).await?;
    crate::policy::require_policy(
        &state.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("sentin8:cron:write"),
    )
    .await?;
    let env_id = resolve_env_id(&a, &q)?;
    match req.status.as_str() {
        "paused" => pause_schedule_inner(&state, env_id, id).await,
        "active" => resume_schedule_inner(&state, env_id, id).await,
        _ => Err((
            StatusCode::BAD_REQUEST,
            "status must be 'paused' or 'active'".into(),
        )),
    }
}

/// `DELETE /v1/schedules/{id}`
#[utoipa::path(
    delete,
    operation_id = "schedule.delete",
    path = "/v1/schedules/{id}",
    params(("id" = Uuid, Path, description = "Schedule ID")),
    responses(
        (status = 204, description = "Schedule deleted"),
        (status = 404, description = "Schedule not found"),
        (status = 403, description = "Missing cron:write scope"),
    ),
    security(("sentin8:cron:write" = [])),
    tag = "cron",
)]
/// Delete schedule.
pub async fn delete_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TargetEnvQuery>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    let a = auth::verify_token(&state.config, &headers).await?;
    crate::policy::require_policy(
        &state.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("sentin8:cron:write"),
    )
    .await?;
    let env_id = resolve_env_id(&a, &q)?;
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    let res = sqlx::query(
        "UPDATE schedules SET status = 'deleted', updated_at = now() WHERE id = $1 AND env_id = $2",
    )
    .bind(id)
    .bind(env_id)
    .execute(&mut *tx)
    .await
    .map_err(err_db)?;
    if res.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, "schedule not found".into()));
    }
    emit_event_tx(
        &mut tx,
        env_id,
        "sentin8:cron:schedule:deleted",
        &serde_json::json!({ "schedule_id": id, "env_id": env_id }),
    )
    .await?;
    tx.commit().await.map_err(err_db)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Shared pause path for the POST alias and the PATCH status update.
async fn pause_schedule_inner(
    state: &AppState,
    env_id: Uuid,
    id: Uuid,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    let res = sqlx::query(
        "UPDATE schedules SET status = 'paused', updated_at = now() WHERE id = $1 AND env_id = $2",
    )
    .bind(id)
    .bind(env_id)
    .execute(&mut *tx)
    .await
    .map_err(err_db)?;
    if res.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, "schedule not found".into()));
    }
    emit_event_tx(
        &mut tx,
        env_id,
        "sentin8:cron:schedule:paused",
        &serde_json::json!({ "schedule_id": id, "env_id": env_id }),
    )
    .await?;
    tx.commit().await.map_err(err_db)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Shared resume path for the POST alias and the PATCH status. Recomputes
/// `next_run_at` from now - a paused schedule does not fire for misses
/// during the pause.
async fn resume_schedule_inner(
    state: &AppState,
    env_id: Uuid,
    id: Uuid,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    let row = sqlx::query(
        "SELECT cron_expression, timezone FROM schedules WHERE id = $1 AND env_id = $2",
    )
    .bind(id)
    .bind(env_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(err_db)?
    .ok_or_else(|| (StatusCode::NOT_FOUND, "schedule not found".into()))?;
    let expr: String = row.get("cron_expression");
    // CRON-6: evaluate the resume fire time in the schedule's timezone
    // (a row seeded before the column exists reads as the column default).
    let tz: String = row
        .try_get::<String, _>("timezone")
        .unwrap_or_else(|_| "UTC".into());
    let next_run_at = crate::cron_expr::CronExpr::parse(&expr)
        .ok()
        .and_then(|c| c.next_after_in_tz(chrono::Utc::now(), &tz))
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "cron expression has no future fire time".into(),
            )
        })?;
    let res = sqlx::query(
        "UPDATE schedules SET status = 'active', next_run_at = $3, updated_at = now() WHERE id = $1 AND env_id = $2",
    )
    .bind(id)
    .bind(env_id)
    .bind(next_run_at)
    .execute(&mut *tx)
    .await
    .map_err(err_db)?;
    if res.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, "schedule not found".into()));
    }
    emit_event_tx(
        &mut tx,
        env_id,
        "sentin8:cron:schedule:resumed",
        &serde_json::json!({ "schedule_id": id, "env_id": env_id }),
    )
    .await?;
    tx.commit().await.map_err(err_db)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/schedules/{id}/pause`
#[utoipa::path(
    post,
    operation_id = "schedule.pause",
    path = "/v1/schedules/{id}/pause",
    params(("id" = Uuid, Path, description = "Schedule ID")),
    responses(
        (status = 204, description = "Schedule paused"),
        (status = 404, description = "Schedule not found"),
    ),
    security(("sentin8:cron:write" = [])),
    tag = "cron",
)]
/// Pause schedule.
pub async fn pause_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TargetEnvQuery>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    let a = auth::verify_token(&state.config, &headers).await?;
    let env_id = resolve_env_id(&a, &q)?;
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    let res = sqlx::query(
        "UPDATE schedules SET status = 'paused', updated_at = now() WHERE id = $1 AND env_id = $2",
    )
    .bind(id)
    .bind(env_id)
    .execute(&mut *tx)
    .await
    .map_err(err_db)?;
    if res.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, "schedule not found".into()));
    }
    emit_event_tx(
        &mut tx,
        env_id,
        "sentin8:cron:schedule:paused",
        &serde_json::json!({ "schedule_id": id, "env_id": env_id }),
    )
    .await?;
    tx.commit().await.map_err(err_db)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/schedules/{id}/resume`
#[utoipa::path(
    post,
    operation_id = "schedule.resume",
    path = "/v1/schedules/{id}/resume",
    params(("id" = Uuid, Path, description = "Schedule ID")),
    responses(
        (status = 204, description = "Schedule resumed"),
        (status = 404, description = "Schedule not found"),
    ),
    security(("sentin8:cron:write" = [])),
    tag = "cron",
)]
/// Resume schedule.
pub async fn resume_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TargetEnvQuery>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    let a = auth::verify_token(&state.config, &headers).await?;
    let env_id = resolve_env_id(&a, &q)?;
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    // Recompute the next fire time from now - a paused schedule does not
    // fire for misses during the pause. CRON-6: evaluated in the
    // schedule's timezone.
    let row = sqlx::query(
        "SELECT cron_expression, timezone FROM schedules WHERE id = $1 AND env_id = $2",
    )
    .bind(id)
    .bind(env_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(err_db)?
    .ok_or_else(|| (StatusCode::NOT_FOUND, "schedule not found".into()))?;
    let expr: String = row.get("cron_expression");
    let tz: String = row
        .try_get::<String, _>("timezone")
        .unwrap_or_else(|_| "UTC".into());
    let next_run_at = crate::cron_expr::CronExpr::parse(&expr)
        .ok()
        .and_then(|c| c.next_after_in_tz(chrono::Utc::now(), &tz))
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "cron expression has no future fire time".into(),
            )
        })?;
    let res = sqlx::query(
        "UPDATE schedules SET status = 'active', next_run_at = $3, updated_at = now() WHERE id = $1 AND env_id = $2",
    )
    .bind(id)
    .bind(env_id)
    .bind(next_run_at)
    .execute(&mut *tx)
    .await
    .map_err(err_db)?;
    if res.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, "schedule not found".into()));
    }
    emit_event_tx(
        &mut tx,
        env_id,
        "sentin8:cron:schedule:resumed",
        &serde_json::json!({ "schedule_id": id, "env_id": env_id }),
    )
    .await?;
    tx.commit().await.map_err(err_db)?;
    Ok(StatusCode::NO_CONTENT)
}

fn row_to_schedule(r: sqlx::postgres::PgRow) -> ScheduleResponse {
    ScheduleResponse {
        id: r.get("id"),
        domain: r.get("domain"),
        schedule_type: r.get("schedule_type"),
        cron_expression: r.get("cron_expression"),
        target_config: r.get("target_config"),
        status: r.get("status"),
        next_run_at: r.get("next_run_at"),
        max_retries: r.get("max_retries"),
    }
}

// ── Jobs ────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, ToSchema)]
/// Response payload for job.
pub struct JobResponse {
    /// Identifier.
    pub id: Uuid,
    /// schedule identifier.
    pub schedule_id: Uuid,
    /// The status.
    pub status: String,
    /// started time.
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    /// completed time.
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    /// The error message.
    pub error_message: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
/// The jobquery type.
pub struct JobQuery {
    #[serde(default)]
    /// schedule identifier.
    pub schedule_id: Option<Uuid>,
    #[serde(default)]
    /// The status.
    pub status: Option<String>,
}

/// `GET /v1/jobs`
#[utoipa::path(
    get,
    operation_id = "job.list",
    path = "/v1/jobs",
    params(("schedule_id" = Option<Uuid>, Query, description = "Filter by schedule ID")),
    responses(
        (status = 200, description = "List of jobs", body = [JobResponse]),
        (status = 403, description = "Missing cron:read scope"),
    ),
    security(("sentin8:cron:read" = [])),
    tag = "cron",
)]
/// List jobs.
pub async fn list_jobs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<JobQuery>,
    Query(teq): Query<TargetEnvQuery>,
) -> Result<Json<Vec<JobResponse>>, (StatusCode, String)> {
    let a = auth::verify_token(&state.config, &headers).await?;
    let env_id = resolve_env_id(&a, &teq)?;
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    let rows = sqlx::query(
        r#"SELECT id, schedule_id, status, started_at, completed_at, error_message
           FROM jobs WHERE env_id = $1
             AND ($2::uuid IS NULL OR schedule_id = $2)
             AND ($3::text IS NULL OR status = $3)
           ORDER BY created_at DESC LIMIT 100"#,
    )
    .bind(env_id)
    .bind(q.schedule_id)
    .bind(q.status)
    .fetch_all(&mut *tx)
    .await
    .map_err(err_db)?;
    Ok(Json(
        rows.into_iter()
            .map(|r| JobResponse {
                id: r.get("id"),
                schedule_id: r.get("schedule_id"),
                status: r.get("status"),
                started_at: r.get("started_at"),
                completed_at: r.get("completed_at"),
                error_message: r.get("error_message"),
            })
            .collect(),
    ))
}

/// `GET /v1/jobs/{id}` - job detail.
#[utoipa::path(
    get,
    operation_id = "job.get",
    path = "/v1/jobs/{id}",
    params(("id" = Uuid, Path, description = "Job ID")),
    responses(
        (status = 200, description = "Job detail", body = JobResponse),
        (status = 404, description = "Job not found"),
        (status = 403, description = "Missing cron:read scope"),
    ),
    security(("sentin8:cron:read" = [])),
    tag = "cron",
)]
/// Fetch job.
pub async fn get_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(teq): Query<TargetEnvQuery>,
    Path(id): Path<Uuid>,
) -> Result<Json<JobResponse>, (StatusCode, String)> {
    let a = auth::verify_token(&state.config, &headers).await?;
    let env_id = resolve_env_id(&a, &teq)?;
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    let row = sqlx::query(
        r#"SELECT id, schedule_id, status, started_at, completed_at, error_message
           FROM jobs WHERE id = $1 AND env_id = $2"#,
    )
    .bind(id)
    .bind(env_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(err_db)?
    .ok_or_else(|| (StatusCode::NOT_FOUND, format!("job {id} not found")))?;
    Ok(Json(JobResponse {
        id: row.get("id"),
        schedule_id: row.get("schedule_id"),
        status: row.get("status"),
        started_at: row.get("started_at"),
        completed_at: row.get("completed_at"),
        error_message: row.get("error_message"),
    }))
}

/// `POST /v1/jobs/{id}/retry` - reset a failed job to `pending` with
/// `next_attempt_at = now`; the ticker drains it on the next tick
/// .
#[utoipa::path(
    post,
    operation_id = "job.retry",
    path = "/v1/jobs/{id}/retry",
    params(("id" = Uuid, Path, description = "Job ID")),
    responses(
        (status = 204, description = "Job queued for retry"),
        (status = 404, description = "Failed job not found"),
    ),
    security(("sentin8:cron:write" = [])),
    tag = "cron",
)]
/// Retry job.
pub async fn retry_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(teq): Query<TargetEnvQuery>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    let a = auth::verify_token(&state.config, &headers).await?;
    let env_id = resolve_env_id(&a, &teq)?;
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    let res = sqlx::query(
        r#"UPDATE jobs SET status = 'pending', error_message = NULL,
              next_attempt_at = now()
           WHERE id = $1 AND env_id = $2 AND status = 'failed'"#,
    )
    .bind(id)
    .bind(env_id)
    .execute(&mut *tx)
    .await
    .map_err(err_db)?;
    if res.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, "failed job not found".into()));
    }
    let schedule_id: Uuid = sqlx::query("SELECT schedule_id FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_one(&mut *tx)
        .await
        .map_err(err_db)?
        .get("schedule_id");
    emit_event_tx(
        &mut tx,
        env_id,
        "sentin8:cron:job:retried",
        &serde_json::json!({ "job_id": id, "schedule_id": schedule_id, "env_id": env_id }),
    )
    .await?;
    tx.commit().await.map_err(err_db)?;
    Ok(StatusCode::NO_CONTENT)
}

// ── DLQ ──────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, ToSchema)]
/// Response payload for dlq.
pub struct DlqResponse {
    /// Identifier.
    pub id: Uuid,
    /// schedule identifier.
    pub schedule_id: Uuid,
    /// The domain.
    pub domain: String,
    /// The schedule type.
    pub schedule_type: String,
    /// The error message.
    pub error_message: String,
    /// The retry count.
    pub retry_count: i32,
}

#[derive(Debug, Deserialize, ToSchema)]
/// The dlqquery type.
pub struct DlqQuery {
    #[serde(default)]
    /// The domain.
    pub domain: Option<String>,
}

/// `GET /v1/dlq`
#[utoipa::path(
    get,
    operation_id = "dlq.list",
    path = "/v1/dlq",
    responses(
        (status = 200, description = "List of DLQ items", body = [DlqResponse]),
        (status = 403, description = "Missing cron:read scope"),
    ),
    security(("sentin8:cron:read" = [])),
    tag = "cron",
)]
/// List dlq.
pub async fn list_dlq(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<DlqQuery>,
    Query(teq): Query<TargetEnvQuery>,
) -> Result<Json<Vec<DlqResponse>>, (StatusCode, String)> {
    let a = auth::verify_token(&state.config, &headers).await?;
    let env_id = resolve_env_id(&a, &teq)?;
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    let rows = sqlx::query(
        r#"SELECT id, schedule_id, domain, schedule_type, error_message, retry_count
           FROM dlq WHERE env_id = $1
             AND ($2::text IS NULL OR domain = $2)
           ORDER BY created_at DESC LIMIT 100"#,
    )
    .bind(env_id)
    .bind(q.domain)
    .fetch_all(&mut *tx)
    .await
    .map_err(err_db)?;
    Ok(Json(
        rows.into_iter()
            .map(|r| DlqResponse {
                id: r.get("id"),
                schedule_id: r.get("schedule_id"),
                domain: r.get("domain"),
                schedule_type: r.get("schedule_type"),
                error_message: r.get("error_message"),
                retry_count: r.get("retry_count"),
            })
            .collect(),
    ))
}

/// `GET /v1/dlq/{id}` - DLQ item detail.
#[utoipa::path(
    get,
    operation_id = "dlq.get",
    path = "/v1/dlq/{id}",
    params(("id" = Uuid, Path, description = "DLQ item ID")),
    responses(
        (status = 200, description = "DLQ item detail", body = DlqResponse),
        (status = 404, description = "DLQ item not found"),
    ),
    security(("sentin8:cron:read" = [])),
    tag = "cron",
)]
/// Fetch dlq.
pub async fn get_dlq(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(teq): Query<TargetEnvQuery>,
    Path(id): Path<Uuid>,
) -> Result<Json<DlqResponse>, (StatusCode, String)> {
    let a = auth::verify_token(&state.config, &headers).await?;
    let env_id = resolve_env_id(&a, &teq)?;
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    let row = sqlx::query(
        r#"SELECT id, schedule_id, domain, schedule_type, error_message, retry_count
           FROM dlq WHERE id = $1 AND env_id = $2"#,
    )
    .bind(id)
    .bind(env_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(err_db)?
    .ok_or_else(|| (StatusCode::NOT_FOUND, format!("dlq item {id} not found")))?;
    Ok(Json(DlqResponse {
        id: row.get("id"),
        schedule_id: row.get("schedule_id"),
        domain: row.get("domain"),
        schedule_type: row.get("schedule_type"),
        error_message: row.get("error_message"),
        retry_count: row.get("retry_count"),
    }))
}

/// `POST /v1/dlq/{id}/republish` - republish a DLQ item as a new pending
/// job and delete the DLQ row.
#[utoipa::path(
    post,
    operation_id = "dlq.republish",
    path = "/v1/dlq/{id}/republish",
    params(("id" = Uuid, Path, description = "DLQ item ID")),
    responses(
        (status = 204, description = "DLQ item republished"),
        (status = 404, description = "DLQ item not found"),
    ),
    security(("sentin8:cron:write" = [])),
    tag = "cron",
)]
/// Handle republish dlq.
pub async fn republish_dlq(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(teq): Query<TargetEnvQuery>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    let a = auth::verify_token(&state.config, &headers).await?;
    let env_id = resolve_env_id(&a, &teq)?;
    let mut tx = begin_tenant_tx(&state.pool, env_id).await?;
    let row = sqlx::query("SELECT schedule_id, env_id FROM dlq WHERE id = $1 AND env_id = $2")
        .bind(id)
        .bind(env_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(err_db)?
        .ok_or((StatusCode::NOT_FOUND, "dlq item not found".into()))?;
    let schedule_id: Uuid = row.get("schedule_id");
    let item_env: Uuid = row.get("env_id");
    sqlx::query(
        r#"INSERT INTO jobs (schedule_id, status, env_id, next_attempt_at)
           VALUES ($1, 'pending', $2, now())"#,
    )
    .bind(schedule_id)
    .bind(item_env)
    .execute(&mut *tx)
    .await
    .map_err(err_db)?;
    sqlx::query("DELETE FROM dlq WHERE id = $1 AND env_id = $2")
        .bind(id)
        .bind(item_env)
        .execute(&mut *tx)
        .await
        .map_err(err_db)?;
    emit_event_tx(
        &mut tx,
        item_env,
        "sentin8:cron:dlq:republished",
        &serde_json::json!({
            "dlq_id": id, "job_id": schedule_id,
            "schedule_id": schedule_id, "env_id": item_env,
        }),
    )
    .await?;
    tx.commit().await.map_err(err_db)?;
    Ok(StatusCode::NO_CONTENT)
}

// ── Health ──────────────────────────────────────────────────────────────

/// `GET /v1/health` - status, ticker heartbeat, active schedule count.

/// The rich liveness payload (ticker heartbeat + schedule count).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CronHealth {
    /// Always `ok` on a live instance.
    pub status: String,
    /// Always `cron`.
    pub service: String,
    /// Build identity id.
    pub version: String,
    /// Build hash.
    pub build: String,
    /// Whether the scheduler ticker ticked within 3 intervals.
    pub ticker_running: bool,
    /// Active schedule rows.
    pub active_schedules: i64,
    /// Live socket count (when the traffic counter is mounted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_connections: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/v1/health",
    responses(
        (status = 200, description = "Liveness + ticker heartbeat + schedule count", body = CronHealth),
    ),
    security(),
    tag = "meta",
)]
/// Handle health.
pub async fn health(State(state): State<AppState>) -> Json<CronHealth> {
    let ticker_running = crate::ticker::LAST_TICK
        .lock()
        .ok()
        .and_then(|last| *last)
        .map(|t| {
            (chrono::Utc::now() - t)
                < chrono::Duration::seconds(state.config.ticker_interval_secs as i64 * 3)
        })
        .unwrap_or(false);
    let active_schedules = sqlx::query(
        r#"SELECT count(*)::int8 AS n FROM cron.schedules
           WHERE status = 'active'"#,
    )
    .fetch_one(&state.pool)
    .await
    .ok()
    .and_then(|r| r.try_get::<i64, _>("n").ok())
    .unwrap_or(0);
    Json(CronHealth {
        status: "ok".to_string(),
        service: "cron".to_string(),
        version: sentin8::build_info::id(),
        build: sentin8::build_info::build_hash().to_string(),
        ticker_running,
        active_schedules,
        active_connections: sentin8::middleware::traffic_counter::process_active_connections()
            .map(|n| n as i64),
    })
}

/// Append an event to the shared `inbox` table **inside the domain
/// transaction** (inbox-first, design ). Failure aborts the transaction
/// - the event and the domain change commit together.
async fn emit_event_tx(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    env_id: Uuid,
    event_type: &str,
    payload: &serde_json::Value,
) -> Result<(), (StatusCode, String)> {
    sentin8::bus::outbox::insert_outbox_row(&mut **tx, env_id, "cron", event_type, payload)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, event_type = %event_type, "cron: inbox event insert failed - rolling back domain change");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("event emit failed: {e}"))
        })?;
    Ok(())
}
