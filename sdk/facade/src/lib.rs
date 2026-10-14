//! sentin8-cron-sdk — per-service SDK mini-facade (SDD §3.1).
//!
//! Re-exports the service's client + contracts crates. No logic, no
//! features (SDD-4b). Consumers import as `use sentin8_cron_sdk::{client|contracts}::…`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

/// cron-client — HTTP client (re-export).
pub mod client {
    pub use sentin8_cron_client::*;
}

/// cron-contracts — wire types + OpenAPI spec (re-export).
pub mod contracts {
    pub use sentin8_cron_contracts::*;
}
