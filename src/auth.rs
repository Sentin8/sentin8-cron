// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Sentin8:Cron
// SPDX-License-Identifier: Apache-2.0

//! Auth token verification for Cron.
//!
//! Re-exports the shared types from the sentin8-auth-client SDK and provides
//! thin wrappers matching the original Cron call sites (taking `&CronConfig` + `&HeaderMap`).

use axum::http::{HeaderMap, StatusCode};

use crate::config::CronConfig;

pub use sentin8::auth::client::AuthContext;

/// Verify the Bearer token and return the auth context.
pub async fn verify_token(
    config: &CronConfig,
    headers: &HeaderMap,
) -> Result<AuthContext, (StatusCode, String)> {
    sentin8::auth::client::verify_bearer_token(&config.auth_client, headers).await
}
