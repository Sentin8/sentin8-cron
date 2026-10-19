// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Sentin8 sentin8-cron
// SPDX-License-Identifier: Apache-2.0

//! Router drift guard (OAPEC design §3).
//!
//! Introspects the typed-but-unbound router (`Router<AppState>` before
//! `with_state` - the guard dispatches no request, so no state is needed)
//! and asserts the compiled spec covers every served route and carries no
//! stale spec pairs. A route without an annotation fails CI before the site
//! API pages can trail.

use sentin8::shell::drift_guard::{
    assert_openapi_covers_router, served_routes, spec_pairs,
};

#[tokio::test]
async fn openapi_spec_covers_every_served_route() {
    let router: axum::Router<sentin8_cron_server::handlers::AppState> =
        sentin8_cron_server::server::routes(axum::Router::new());
    let served = served_routes(&router);
    let pairs = spec_pairs(&<sentin8_cron_server::openapi::CronOpenApi as sentin8::openapi::OpenApi>::openapi());
    assert_openapi_covers_router(&served, &pairs, &[]);
}
