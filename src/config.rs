// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Sentin8:Cron
// SPDX-License-Identifier: Apache-2.0

//! Cron configuration loaded from environment variables.

use std::env;

use sentin8::auth::client::{Client as AuthClient, ClientOptions};

/// Proxy-back transport for schedule callbacks (, auth DESIGN
/// §4.3f): the `sentin8:cron` CERTIFICATE is the identity - presented
/// in the TLS handshake on every proxy-back call; the target service's
/// TLS layer authenticates it. No bearer token exists on this path.
#[derive(Clone)]
pub struct M2mSettings {
    /// The cert-presenting transport (installed once by `build_state`
    /// after the workload certificate is ensured).
    inner: Option<std::sync::Arc<sentin8::middleware::http_client::Sentin8HttpClient>>,
}

impl std::fmt::Debug for M2mSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("M2mSettings")
            .field("configured", &self.inner.is_some())
            .finish()
    }
}

impl M2mSettings {
    /// Handle disabled.
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    /// Install the cert-presenting transport (cron DESIGN §4.4): called
    /// once by `build_state` after `ensure_workload_certificate` - after
    /// this, `client()` returns the mTLS transport presenting the
    /// `sentin8:cron` workload identity.
    pub fn set_inner(&mut self, client: sentin8::middleware::http_client::Sentin8HttpClient) {
        self.inner = Some(std::sync::Arc::new(client));
    }

    /// The cert-presenting transport for proxy-back calls.
    ///
    /// # Errors
    /// [`String`] when no transport is installed (the workload
    /// certificate was never ensured - a provisioning state).
    pub fn client(&self) -> Result<sentin8::middleware::http_client::Sentin8HttpClient, String> {
        self.inner.as_ref().map(|c| (**c).clone()).ok_or_else(|| {
            "no proxy-back transport configured (workload certificate missing)".into()
        })
    }
}

#[derive(Debug, Clone)]
/// Configuration for cron.
pub struct CronConfig {
    /// The port.
    pub port: u16,
    /// The database URL.
    pub database_url: String,
    /// The sentin8 auth issuer.
    pub sentin8_auth_issuer: String,
    /// The sentin8 auth JWKS URL.
    pub sentin8_auth_jwks_url: String,
    /// Ticker interval in seconds (default 60).
    /// The ticker interval secs.
    pub ticker_interval_secs: u64,
    /// Job timeout in seconds (default 30).
    /// The job timeout secs.
    pub job_timeout_secs: u64,
    /// Shared Auth client - constructed once so the discovery and JWKS
    /// caches are reused across every request (5-minute TTL each).
    /// Cloning is cheap (the caches live behind an `Arc`).
    /// The auth client.
    pub auth_client: AuthClient,
    /// M2M token source for proxy-back execution (cron DESIGN §4.4).
    /// The m2m.
    pub m2m: M2mSettings,
}

impl CronConfig {
    /// Handle from env.
    pub fn from_env() -> Self {
        let cfg = sentin8::config::Sentin8Config::from_env();
        let sentin8_auth_issuer =
            env::var("SENTIN8_AUTH_ISSUER").unwrap_or_else(|_| cfg.sentin8_auth_issuer());
        Self {
            port: env::var("SENTIN8_CRON_PORT")
                .expect("SENTIN8_CRON_PORT must be set by captain")
                .parse()
                .expect("SENTIN8_CRON_PORT must be a valid u16"),
            database_url: env::var("SENTIN8_CRON_DATABASE_URL").unwrap_or_else(|_| {
                env::var("DATABASE_URL").unwrap_or_else(|_| {
                    sentin8::config::Sentin8Config::default_database_url().to_string()
                })
            }),
            sentin8_auth_jwks_url: env::var("SENTIN8_AUTH_JWKS_URL")
                .unwrap_or_else(|_| cfg.sentin8_auth_jwks_url()),
            ticker_interval_secs: env::var("SENTIN8_CRON_TICKER_INTERVAL")
                .unwrap_or("60".into())
                .parse()
                .unwrap_or(60),
            job_timeout_secs: env::var("SENTIN8_CRON_JOB_TIMEOUT")
                .unwrap_or("30".into())
                .parse()
                .unwrap_or(30),
            auth_client: AuthClient::new(ClientOptions {
                base_url: sentin8_auth_issuer.clone(),
                ..Default::default()
            }),
            m2m: M2mSettings::disabled(),
            sentin8_auth_issuer,
        }
    }
}
