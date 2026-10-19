// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Sentin8:Cron
// SPDX-License-Identifier: Apache-2.0

//! # cron-contracts
//!
//! Rust types for the Cron standalone scheduling service.

#![deny(missing_docs)]

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A registered recurring schedule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schedule {
    /// Schedule id.
    pub id: Uuid,
    /// Owning business domain (e.g. `ledger`).
    pub domain: String,
    /// Schedule kind within the domain.
    pub schedule_type: String,
    /// Standard 5-field cron expression.
    pub cron_expression: String,
    /// Lifecycle status (`active`/`paused`/`deleted`).
    pub status: String,
    /// Last successful tick, when the schedule has run.
    pub last_run_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Next scheduled tick.
    pub next_run_at: chrono::DateTime<chrono::Utc>,
    /// Owning organization.
    pub org_id: Uuid,
    /// Owning environment (the tenant key).
    pub env_id: Uuid,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(s: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    fn sample() -> Schedule {
        Schedule {
            id: uuid::Uuid::new_v4(),
            domain: "ledger".into(),
            schedule_type: "eod-close".into(),
            cron_expression: "30 2 * * 1-5".into(),
            status: "active".into(),
            last_run_at: Some(utc("2026-01-01T02:30:00Z")),
            next_run_at: utc("2026-01-02T02:30:00Z"),
            org_id: uuid::Uuid::new_v4(),
            env_id: uuid::Uuid::new_v4(),
        }
    }

    #[test]
    fn schedule_roundtrip() {
        let s = sample();
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["cron_expression"], "30 2 * * 1-5");
        assert_eq!(v["next_run_at"], "2026-01-02T02:30:00Z");
        let back: Schedule = serde_json::from_value(v).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn schedule_last_run_nullable() {
        let mut s = sample();
        s.last_run_at = None;
        let v = serde_json::to_value(&s).unwrap();
        // last_run_at is Option without skip_serializing_if - wire keeps null.
        assert!(v.get("last_run_at").is_some_and(|x| x.is_null()));
        let back: Schedule = serde_json::from_value(v).unwrap();
        assert!(back.last_run_at.is_none());
    }
}
