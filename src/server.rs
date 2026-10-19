// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Sentin8:Cron
// SPDX-License-Identifier: Apache-2.0

//! Axum server setup for Cron.

use std::sync::Arc;

use axum::routing::{get, post};

use crate::handlers::{self, AppState};
use crate::ticker;

/// Build state.
pub async fn build_state(
    core: sentin8::shell::ServiceCore,
) -> anyhow::Result<crate::handlers::AppState> {
    let mut config = crate::config::CronConfig::from_env();

    // ── Sentin8:Policy - boot load refuses to start on invalid
    // policy text. Hot-reload via the shared consumer.
    // ── Sentin8:Policy (- shell-owned boot): consume the
    // engine the shell booted in step 8 - no second engine, consumer, or
    // DCR registration here.
    let policy_engine = core.admin.policy.clone();

    // auth DESIGN §4.3f: ensure the `sentin8:cron` workload
    // CERTIFICATE (load-or-mint from Acme - the certificate IS the
    // identity; no DCR, no token) and install the cert-presenting
    // transport for proxy-back calls. The target service's TLS layer
    // authenticates the handshake. The workload grant is declared in
    // registration.rs (`M2M_GRANTS`); `vertical enroll` provisions the
    // scope rows + the registry map.
    let ca_cert_path = sentin8::auth::m2m::platform_ca_path();
    match sentin8::auth::m2m::ensure_workload_certificate(
        &config.sentin8_auth_issuer,
        "sentin8:cron",
        &ca_cert_path,
    )
    .await
    {
        Ok(cert) => {
            let mtls = sentin8::middleware::mtls::MtlsConfig::new(
                &cert.ca_cert_path,
                &cert.cert_path,
                &cert.key_path,
            );
            match sentin8::middleware::mtls::build_mtls_client(&mtls) {
                Ok(client) => {
                    tracing::info!(
                        workload = "sentin8:cron",
                        "cron: cert-presenting proxy-back transport installed"
                    );
                    config.m2m.set_inner(
                        sentin8::middleware::http_client::Sentin8HttpClient::from_client(client),
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "cron: proxy-back transport build failed");
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "cron: workload certificate mint failed (proxy-back disabled until next boot)");
        }
    }

    let state = AppState {
        config: Arc::new(config.clone()),
        pool: core.pool.clone(),
        policy: policy_engine.clone(),
        admin: sentin8::shield::admin::SharedAdminState {
            service: "sentin8:cron".to_string(),
            pool: core.pool.clone(),
            policy: policy_engine.clone(),
            auth_client: Arc::new(config.auth_client.clone()),
        },
    };

    // Start the ticker loop.
    ticker::spawn(Arc::new(config), core.pool);

    Ok(state)
}

/// Add this service's routes onto the shell-provided router. The shell
/// owns the boot sequence, state binding, OpenAPI merge and the standard
/// middleware stack; this adds ONLY routes (including the service's own
/// `/v1/health` - custom payload).
/// Handle routes.
pub fn routes(
    router: axum::Router<crate::handlers::AppState>,
) -> axum::Router<crate::handlers::AppState> {
    router
        .route("/v1/health", get(handlers::health))
        // ── Admin UI (RFW) ─────────────────────────────────────────────────
        .route("/v1/admin/ui/{*page}", get(crate::admin_ui::get_admin_ui))
        .route("/v1/admin/{resource}", get(crate::admin_ui::get_admin_data))
        // Schedules
        .route(
            "/v1/schedules",
            post(handlers::create_schedule).get(handlers::list_schedules),
        )
        .route(
            "/v1/schedules/{id}",
            get(handlers::get_schedule)
                .delete(handlers::delete_schedule)
                .patch(handlers::update_schedule),
        )
        .route("/v1/schedules/{id}/pause", post(handlers::pause_schedule))
        .route("/v1/schedules/{id}/resume", post(handlers::resume_schedule))
        // POST alias for RFW row_action_post (renderer only supports POST)
        .route("/v1/schedules/{id}/delete", post(crate::openapi::rfw_aliases::delete_schedule_alias))
        // Jobs
        .route("/v1/jobs", get(handlers::list_jobs))
        .route("/v1/jobs/{id}", get(handlers::get_job))
        .route("/v1/jobs/{id}/retry", post(handlers::retry_job))
        // DLQ
        .route("/v1/dlq", get(handlers::list_dlq))
        .route("/v1/dlq/{id}", get(handlers::get_dlq))
        .route("/v1/dlq/{id}/republish", post(handlers::republish_dlq))
        // Traffic counter - counts every inbound request (excludes health checks).
        // ── Sentin8:Policy admin API ( - shared implementation,
        // one surface per service; handlers are generic over this AppState).
        .route(
            "/v1/admin/policy-sets",
            get(sentin8::shield::admin::list_policy_sets::<handlers::AppState>),
        )
        .route(
            "/v1/admin/policy-sets/{name}",
            get(sentin8::shield::admin::get_policy_set::<handlers::AppState>)
                .put(sentin8::shield::admin::put_policy_set::<handlers::AppState>)
                .delete(sentin8::shield::admin::delete_policy_set::<handlers::AppState>),
        )
        .route(
            "/v1/admin/policy-sets/{name}/test",
            post(sentin8::shield::admin::test_policy_set::<handlers::AppState>),
        )
        .route(
            "/v1/admin/policy-sets/enforce",
            post(sentin8::shield::admin::admin_enforce_policy_set::<handlers::AppState>),
        )
        .route(
            "/v1/admin/policy-sets/shadow",
            post(sentin8::shield::admin::admin_shadow_policy_set::<handlers::AppState>),
        )
        .route(
            "/v1/admin/policy-sets/disable",
            post(sentin8::shield::admin::admin_disable_policy_set::<handlers::AppState>),
        )
        .route(
            "/v1/admin/relations",
            get(sentin8::shield::admin::list_relations::<handlers::AppState>)
                .put(sentin8::shield::admin::put_relation::<handlers::AppState>)
                .delete(sentin8::shield::admin::delete_relation::<handlers::AppState>),
        )
        .route(
            "/v1/admin/relations/check",
            get(sentin8::shield::admin::check_relation::<handlers::AppState>),
        )
        .route(
            "/v1/admin/attributes",
            get(sentin8::shield::admin::list_attributes::<handlers::AppState>)
                .put(sentin8::shield::admin::put_attribute::<handlers::AppState>)
                .delete(sentin8::shield::admin::delete_attribute::<handlers::AppState>),
        )
}

/// Shared policy-admin surface accessor - the shared
/// handlers extract this service's admin state via [`HasPolicyAdmin`].
impl sentin8::shield::admin::HasPolicyAdmin for AppState {
    fn policy_admin(&self) -> &sentin8::shield::admin::SharedAdminState {
        &self.admin
    }
}
