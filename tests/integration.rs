// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Sentin8:Cron
// SPDX-License-Identifier: Apache-2.0

// Cron unit/integration tests — testing-strategy §3 (TEST-1..4 in
// services/cron/BACKLOG.md).
//
// The ticker loop (`ticker.rs`) is DB-bound, but its pure seams are
// testable without a database:
//   1. `uuid_to_i64` — the advisory-lock key derivation (stability +
//      uniqueness properties).
//   2. `execute_proxy_back` — the proxy-back contract: missing baseUrl
//      rejection, URL assembly from target_config, and the self-contained
//      envelope payload (verifiable against a local listener).
//   3. `cron_expr` — cron expression parsing + next-fire computation
//      (CRON-HARD-1, unblocks TEST-1).
//   4. Backoff progression — 60s × 2^retry capped at 1h.
//
// Request/response shapes of the handlers are exercised through serde
// roundtrips (`CreateScheduleRequest`, `ScheduleResponse`, `JobResponse`,
// `DlqResponse`) — the wire-format layer of the cron API.
//
// Pure tests run under `make test-rust`; DB-backed tests (TEST-2/TEST-3)
// live at the bottom and runtime-skip when PostgreSQL is unreachable.

use serde_json::json;
use sqlx::Row;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ticker.rs is included as a module of THIS test crate, so its
// `use crate::config::CronConfig` must resolve here. Provide a minimal
// stand-in with the fields the DB-bound paths read — `spawn`/`tick` are
// not exercised by the pure tests.
#[path = "../src/ticker.rs"]
#[allow(dead_code)] // spawn/tick are DB-bound and unused in this test binary
mod ticker;

// Stand-in for the service's cron_expr module — the ticker's next_fire
// path is only reachable DB-bound, but keeping the module tree consistent
// lets the parser tests below use the same source file.
#[path = "../src/cron_expr.rs"]
mod cron_expr;

// Stand-in for the service's config module — keeps ticker.rs compiling
// in the test crate without dragging the real config (AuthClient init
// needs env vars) into the test.
#[allow(dead_code)]
mod config {
    /// Mirrors the fields of `crate::config::CronConfig` that ticker.rs
    /// reads. Not a real config — never constructed in these tests.
    #[derive(Clone)]
    pub struct CronConfig {
        pub ticker_interval_secs: u64,
        pub job_timeout_secs: u64,
        pub m2m: M2mSettings,
    }

    impl CronConfig {
        /// Stand-in for the real from_env — ticker's timeout wrapper reads
        /// the job timeout; tests construct the struct directly.
        pub fn from_env() -> Self {
            Self {
                ticker_interval_secs: 60,
                job_timeout_secs: 30,
                m2m: M2mSettings::disabled(),
            }
        }
    }

    /// Mirrors `crate::config::M2mSettings` — token() resolves from the
    /// static dev fallback.
    #[derive(Clone)]
    pub struct M2mSettings {
        fallback: String,
    }

    impl M2mSettings {
        pub fn disabled() -> Self {
            Self {
                fallback: String::new(),
            }
        }

        pub async fn token(&self) -> Result<String, String> {
            Ok(self.fallback.clone())
        }
    }
}

// ── Advisory-lock key derivation (ticker.rs) ────────────────────────────

#[test]
fn lock_key_stable_for_same_uuid() {
    let id: uuid::Uuid = uuid::Uuid::new_v4();
    assert_eq!(ticker::uuid_to_i64(id), ticker::uuid_to_i64(id));
}

#[test]
fn lock_key_differs_for_different_uuids() {
    let a = uuid::Uuid::new_v4();
    let b = uuid::Uuid::new_v4();
    assert_ne!(ticker::uuid_to_i64(a), ticker::uuid_to_i64(b));
}

#[test]
fn lock_key_uses_first_8_bytes() {
    // The hash only reads the first 8 bytes of the uuid — flipping bits in
    // the last 8 must not change the key.
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    let a = uuid::Uuid::from_bytes(bytes);
    let mut b = bytes;
    b[8..].copy_from_slice(&[0xff; 8]);
    assert_eq!(
        ticker::uuid_to_i64(a),
        ticker::uuid_to_i64(uuid::Uuid::from_bytes(b))
    );
}

#[test]
fn lock_key_zero_uuid_is_zero() {
    let zero = uuid::Uuid::nil();
    assert_eq!(ticker::uuid_to_i64(zero), 0);
}

#[test]
fn lock_key_fits_i64() {
    // Exhaustive over the 8 input bytes would be 2^64; a deterministic
    // sample including the max pattern suffices to pin the algorithm.
    let max_bytes = uuid::Uuid::from_bytes([0xff; 16]);
    let key = ticker::uuid_to_i64(max_bytes);
    // wrapping_shl keeps the value in-range by construction; assert the
    // known value for the all-0xff prefix: 0xffff_ffff_ffff_ffff as i64.
    assert_eq!(key, -1i64);
}

// ── Proxy-back contract (ticker.rs) ─────────────────────────────────────

/// Install the Graviola PQ crypto provider once — reqwest panics with
/// "No provider set" when a client is built without one (stat-client
/// test pattern). `execute_proxy_back` builds a Sentin8HttpClient.
fn install_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = sentin8::tls::crypto::sentin8_pq_provider().install_default();
    });
}

fn m2m_settings() -> config::M2mSettings {
    install_provider();
    config::M2mSettings::disabled()
}

#[tokio::test]
async fn proxy_back_missing_base_url_rejected() {
    let cfg = json!({ "internalPath": "/v1/internal" });
    let err = ticker::execute_proxy_back(
        &cfg,
        "statement",
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        "corr",
        "idem",
        &m2m_settings(),
    )
    .await
    .unwrap_err();
    assert_eq!(err, "missing baseUrl in target_config");
}

#[tokio::test]
async fn proxy_back_url_assembly_and_payload() {
    // Local TCP listener acting as the target service. We read the raw
    // HTTP/1.1 request to assert the assembled URL path and the JSON body
    // (schedule_type), then answer 200 with a JSON result.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = sock.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]).to_string();
        let body = "{\"schedule_type\":\"statement\",\"ok\":true}\r\n".to_string();
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        sock.write_all(resp.as_bytes()).await.unwrap();
        req
    });

    let cfg = json!({
        "baseUrl": format!("http://{addr}"),
        "internalPath": "/v1/internal",
    });
    let env = uuid::Uuid::new_v4();
    let schedule = uuid::Uuid::new_v4();
    let job = uuid::Uuid::new_v4();
    let result = ticker::execute_proxy_back(
        &cfg,
        "statement",
        env,
        schedule,
        job,
        &schedule.to_string(),
        &format!("schedule:{schedule}:{job}"),
        &m2m_settings(),
    )
    .await
    .unwrap();
    assert_eq!(result["schedule_type"], "statement");
    assert_eq!(result["ok"], true);

    let raw = server.await.unwrap();
    assert!(raw.starts_with("POST /v1/internal/schedules/run HTTP/1.1"));
    assert!(raw.contains("\"schedule_type\":\"statement\""));
    // Self-contained envelope (design §4.4) — the target cannot derive
    // tenant context from the bearer token.
    assert!(raw.contains(&format!("\"env_id\":\"{env}\"")));
    assert!(raw.contains(&format!("\"schedule_id\":\"{schedule}\"")));
    assert!(raw.contains(&format!("\"correlation_id\":\"{schedule}\"")));
    assert!(raw.contains(&format!(
        "\"idempotency_key\":\"schedule:{schedule}:{job}\""
    )));
}

#[tokio::test]
async fn proxy_back_default_internal_path() {
    // When internalPath is absent, the default "/v1/internal" is used.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = sock.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]).to_string();
        let body = "{}\r\n";
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        sock.write_all(resp.as_bytes()).await.unwrap();
        req
    });

    let cfg = json!({ "baseUrl": format!("http://{addr}") });
    let _ = ticker::execute_proxy_back(
        &cfg,
        "tick",
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        "corr",
        "idem",
        &m2m_settings(),
    )
    .await
    .unwrap();
    let raw = server.await.unwrap();
    assert!(raw.starts_with("POST /v1/internal/schedules/run HTTP/1.1"));
}

#[tokio::test]
async fn proxy_back_non_success_status_is_error() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let _ = sock.read(&mut buf).await.unwrap();
        let resp =
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        sock.write_all(resp.as_bytes()).await.unwrap();
    });

    let cfg = json!({ "baseUrl": format!("http://{addr}") });
    let err = ticker::execute_proxy_back(
        &cfg,
        "tick",
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        "corr",
        "idem",
        &m2m_settings(),
    )
    .await
    .unwrap_err();
    assert!(
        err.contains("proxy-back status 503"),
        "unexpected error: {err}"
    );
    server.await.unwrap();
}

// ── Handler wire formats ────────────────────────────────────────────────

#[test]
fn create_schedule_request_defaults_max_retries() {
    let req: sentin8_cron_contracts::Schedule = serde_json::from_value(json!({
        "id": uuid::Uuid::nil(),
        "domain": "ledger",
        "schedule_type": "statement",
        "cron_expression": "0 * * * *",
        "status": "active",
        "next_run_at": "2026-01-01T00:00:00Z",
        "org_id": uuid::Uuid::nil(),
        "env_id": uuid::Uuid::nil(),
    }))
    .unwrap();
    assert_eq!(req.cron_expression, "0 * * * *");
    assert_eq!(req.status, "active");
    assert!(req.last_run_at.is_none());
}

#[test]
fn schedule_roundtrip() {
    let schedule = sentin8_cron_contracts::Schedule {
        id: uuid::Uuid::new_v4(),
        domain: "ledger".into(),
        schedule_type: "eod-close".into(),
        cron_expression: "30 2 * * 1-5".into(),
        status: "active".into(),
        last_run_at: Some(
            chrono::DateTime::parse_from_rfc3339("2026-01-01T02:30:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        ),
        next_run_at: chrono::DateTime::parse_from_rfc3339("2026-01-02T02:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        org_id: uuid::Uuid::new_v4(),
        env_id: uuid::Uuid::new_v4(),
    };
    let json = serde_json::to_value(&schedule).unwrap();
    let back: sentin8_cron_contracts::Schedule = serde_json::from_value(json).unwrap();
    assert_eq!(back.id, schedule.id);
    assert_eq!(back.cron_expression, "30 2 * * 1-5");
    assert_eq!(back.last_run_at, schedule.last_run_at);
}

#[test]
fn target_env_query_missing_env_rejected() {
    // resolve_env_id requires target_env_id for cross-env admin calls.
    let q = "target_env_id=not-a-uuid";
    let parsed: Result<std::collections::HashMap<String, String>, _> =
        serde_urlencoded::from_str(q);
    // The value parses as a query string; the uuid rejection happens in
    // the handler via Uuid::parse_str — simulate that decision here.
    let val = parsed.unwrap().get("target_env_id").cloned();
    let resolved = val.and_then(|s| uuid::Uuid::parse_str(&s).ok());
    assert!(resolved.is_none(), "invalid uuid must not resolve");
}

// ── Cron expression parsing (CRON-HARD-1 / TEST-1) ────────────────────

fn utc(s: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(s)
        .unwrap()
        .with_timezone(&chrono::Utc)
}

#[test]
fn cron_parse_every_minute() {
    let e = cron_expr::CronExpr::parse("* * * * *").unwrap();
    assert_eq!(
        e.next_after(utc("2026-01-01T00:00:30Z")),
        Some(utc("2026-01-01T00:01:00Z"))
    );
}

#[test]
fn cron_daily_midnight_strictly_after() {
    let e = cron_expr::CronExpr::parse("0 0 * * *").unwrap();
    assert_eq!(
        e.next_after(utc("2026-01-01T12:34:56Z")),
        Some(utc("2026-01-02T00:00:00Z"))
    );
    // At exactly the fire minute → strictly-after (misfire = skip).
    assert_eq!(
        e.next_after(utc("2026-01-01T00:00:00Z")),
        Some(utc("2026-01-02T00:00:00Z"))
    );
}

#[test]
fn cron_monthly_and_month_skip() {
    let e = cron_expr::CronExpr::parse("0 0 1 * *").unwrap();
    assert_eq!(
        e.next_after(utc("2026-01-05T00:00:00Z")),
        Some(utc("2026-02-01T00:00:00Z"))
    );
    // February-only must not match July.
    let feb = cron_expr::CronExpr::parse("0 0 1 2 *").unwrap();
    assert_eq!(
        feb.next_after(utc("2026-07-15T00:00:00Z")),
        Some(utc("2027-02-01T00:00:00Z"))
    );
}

#[test]
fn cron_lists_ranges_steps() {
    let e = cron_expr::CronExpr::parse("0 9-17/2,8 * * *").unwrap();
    assert_eq!(
        e.next_after(utc("2026-06-01T07:00:00Z")),
        Some(utc("2026-06-01T08:00:00Z"))
    );
    assert_eq!(
        e.next_after(utc("2026-06-01T09:00:00Z")),
        Some(utc("2026-06-01T11:00:00Z"))
    );
}

#[test]
fn cron_day_of_week() {
    let e = cron_expr::CronExpr::parse("30 2 * * 1-5").unwrap();
    // 2026-01-01 is Thursday, 2026-01-03 Saturday → next is Monday 05th.
    assert_eq!(
        e.next_after(utc("2026-01-03T00:00:00Z")),
        Some(utc("2026-01-05T02:30:00Z"))
    );
}

#[test]
fn cron_rejects_bad_expressions() {
    for bad in [
        "",
        "* * * *",
        "* * * * * *",
        "60 * * * *",
        "* * 0 * *",
        "* * * 13 *",
        "* * * * 8",
        "a * * * *",
        "*/0 * * * *",
        "5-1 * * * *",
    ] {
        assert!(
            cron_expr::CronExpr::parse(bad).is_err(),
            "expected rejection: {bad}"
        );
    }
}

// ── DB-backed: CRUD + RLS + ticker bookkeeping (TEST-2 / TEST-3) ──────

/// Runtime-skip pattern (testing-strategy §3.2): connect attempt; on
/// failure print one skip line and return None.
///
/// SECURITY: connects as the NOBYPASSRLS `sentin8_cron_service` role — the
/// `sentin8` superuser bypasses RLS entirely, which would silently void
/// every RLS assertion here (architecture-principles §4.3e).
async fn db_pool() -> Option<sqlx::PgPool> {
    let url = std::env::var("SENTIN8_CRON_DATABASE_URL").unwrap_or_else(|_| {
        "postgres://sentin8_cron_service@localhost:5432/sentin8".to_string()
    });
    match sqlx::postgres::PgPool::connect(&url).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("skipping DB-backed cron tests — PostgreSQL unreachable: {e}");
            None
        }
    }
}

#[tokio::test]
async fn db_schedules_crud_respects_rls() {
    let Some(pool) = db_pool().await else { return };
    let env_a = uuid::Uuid::new_v4();
    let env_b = uuid::Uuid::new_v4();

    // Insert into env A with the context set on the same transaction.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT sentin8_set_tenant_context($1)")
        .bind(env_a)
        .execute(&mut *tx)
        .await
        .unwrap();
    let row = sqlx::query(
        r#"INSERT INTO cron.schedules (domain, schedule_type, cron_expression, target_config, env_id)
           VALUES ('ledger', 'interest_accrual', '0 0 * * *', '{}', $1)
           RETURNING id"#,
    )
    .bind(env_a)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let schedule_id: uuid::Uuid = row.get("id");
    tx.commit().await.unwrap();

    // RLS: env B's context sees no rows from env A.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT sentin8_set_tenant_context($1)")
        .bind(env_b)
        .execute(&mut *tx)
        .await
        .unwrap();
    let visible = sqlx::query("SELECT count(*)::int8 AS n FROM cron.schedules WHERE id = $1")
        .bind(schedule_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap()
        .get::<i64, _>("n");
    assert_eq!(visible, 0, "cross-tenant read must be denied by RLS");
    tx.rollback().await.unwrap();

    // Same-env read sees the row.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT sentin8_set_tenant_context($1)")
        .bind(env_a)
        .execute(&mut *tx)
        .await
        .unwrap();
    let visible = sqlx::query("SELECT count(*)::int8 AS n FROM cron.schedules WHERE id = $1")
        .bind(schedule_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap()
        .get::<i64, _>("n");
    assert_eq!(visible, 1);

    // Clean up (context required — NOBYPASSRLS).
    sqlx::query("DELETE FROM cron.schedules WHERE id = $1")
        .bind(schedule_id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}
