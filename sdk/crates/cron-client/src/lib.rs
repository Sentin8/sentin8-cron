// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Sentin8:Cron
// SPDX-License-Identifier: Apache-2.0

//! # cron-client
//!
//! Rust HTTP client for the Cron standalone scheduling service.
//!
//! Used by Sentin8 services (e.g. Payments) to register recurring schedules that
//! proxy back into the originating service via an internal HTTP endpoint.
//! The client authenticates using a static bearer token issued to the
//! calling service (M2M scope `sentin8:cron:write`).

#![deny(missing_docs)]

use std::sync::Arc;

use sentin8_middleware::http_client::Sentin8HttpClient;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// Errors returned by the Cron client.
#[derive(Debug, Error)]
pub enum CronError {
    /// The HTTP request failed or returned a non-success status.
    #[error("Cron HTTP error: {0}")]
    Http(String),
    /// The response body could not be decoded.
    #[error("Cron response decode error: {0}")]
    Decode(String),
}

/// A request to create a new schedule (mirrors Cron's
/// `POST /v1/schedules` body).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateScheduleRequest {
    /// Owning business domain (e.g. `ledger`).
    pub domain: String,
    /// Schedule kind within the domain.
    pub schedule_type: String,
    /// Standard 5-field cron expression.
    pub cron_expression: String,
    /// Callback configuration: `{"base_url", "internal_path", "auth_token"}`.
    #[serde(default)]
    pub target_config: serde_json::Value,
    /// Retry budget per tick.
    #[serde(default = "default_max_retries")]
    pub max_retries: i32,
}

fn default_max_retries() -> i32 {
    3
}

impl CreateScheduleRequest {
    /// Build a new schedule request targeting a callback URL.
    ///
    /// `target_config` follows the Cron ticker contract:
    /// `{ "base_url" : "...", "internal_path" : "...", "auth_token" : "..." }`.
    pub fn new(
        domain: impl Into<String>,
        schedule_type: impl Into<String>,
        cron_expression: impl Into<String>,
        target_config: serde_json::Value,
    ) -> Self {
        Self {
            domain: domain.into(),
            schedule_type: schedule_type.into(),
            cron_expression: cron_expression.into(),
            target_config,
            max_retries: default_max_retries(),
        }
    }
}

/// A schedule record returned by Cron (mirrors `cron-contracts::Schedule`
/// with the additional `target_config`/`max_retries` fields exposed by the
/// HTTP API).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleResponse {
    /// Schedule id.
    pub id: Uuid,
    /// Owning business domain (e.g. `ledger`).
    pub domain: String,
    /// Schedule kind within the domain.
    pub schedule_type: String,
    /// Standard 5-field cron expression.
    pub cron_expression: String,
    /// Callback configuration as registered.
    pub target_config: serde_json::Value,
    /// Lifecycle status (`active`/`deleted`).
    pub status: String,
    /// Next scheduled tick.
    pub next_run_at: chrono::DateTime<chrono::Utc>,
    /// Retry budget per tick.
    pub max_retries: i32,
}

/// The Cron client. Cheap to clone (wraps an `Arc`).
#[derive(Clone)]
pub struct CronClient {
    base_url: String,
    auth_token: Arc<String>,
    http: Sentin8HttpClient,
}

impl CronClient {
    /// Construct a new client.
    ///
    /// - `base_url`: Cron server URL, e.g. `http://localhost:3107`.
    /// - `auth_token`: bearer token with `sentin8:cron:write` scope.
    pub fn new(base_url: impl Into<String>, auth_token: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            auth_token: Arc::new(auth_token.into()),
            http: Sentin8HttpClient::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// `POST /v1/schedules` - register a new schedule.
    pub async fn create_schedule(
        &self,
        req: &CreateScheduleRequest,
    ) -> Result<ScheduleResponse, CronError> {
        let resp = self
            .http
            .post(self.url("/v1/schedules"))
            .bearer_auth(self.auth_token.as_str())
            .json(req)
            .send()
            .await
            .map_err(|e| CronError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CronError::Http(format!("status {}", resp.status())));
        }
        resp.json()
            .await
            .map_err(|e| CronError::Decode(e.to_string()))
    }

    /// `GET /v1/schedules` - list schedules.
    pub async fn list_schedules(&self) -> Result<Vec<ScheduleResponse>, CronError> {
        let resp = self
            .http
            .get(self.url("/v1/schedules"))
            .bearer_auth(self.auth_token.as_str())
            .send()
            .await
            .map_err(|e| CronError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CronError::Http(format!("status {}", resp.status())));
        }
        resp.json()
            .await
            .map_err(|e| CronError::Decode(e.to_string()))
    }

    /// `PATCH /v1/schedules/{id}` - pause/resume via status
    /// (`{"status": "paused" | "active"}`; resume recomputes
    /// `next_run_at` from now server-side).
    pub async fn update_schedule_status(&self, id: Uuid, status: &str) -> Result<(), CronError> {
        let resp = self
            .http
            .patch(self.url(&format!("/v1/schedules/{id}")))
            .bearer_auth(self.auth_token.as_str())
            .json(&serde_json::json!({ "status": status }))
            .send()
            .await
            .map_err(|e| CronError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CronError::Http(format!("status {}", resp.status())));
        }
        Ok(())
    }

    /// `DELETE /v1/schedules/{id}` - delete a schedule.
    pub async fn delete_schedule(&self, id: Uuid) -> Result<(), CronError> {
        let resp = self
            .http
            .delete(self.url(&format!("/v1/schedules/{id}")))
            .bearer_auth(self.auth_token.as_str())
            .send()
            .await
            .map_err(|e| CronError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(CronError::Http(format!("status {}", resp.status())));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn install_provider() {
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            let _ = sentin8_tls::crypto::sentin8_pq_provider().install_default();
        });
    }

    async fn serve(
        status_line: &'static str,
        body: &'static str,
    ) -> (tokio::task::JoinHandle<String>, std::net::SocketAddr) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let n = sock.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]).to_string();
            let resp = format!(
                "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            req
        });
        (handle, addr)
    }

    #[test]
    fn create_schedule_request_defaults() {
        let req = CreateScheduleRequest::new(
            "ledger",
            "statement",
            "0 * * * *",
            json!({"baseUrl": "https://ledger.local"}),
        );
        assert_eq!(req.max_retries, 3, "default_max_retries");
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["cron_expression"], "0 * * * *");
        assert_eq!(v["target_config"]["baseUrl"], "https://ledger.local");
    }

    #[tokio::test]
    async fn create_schedule_posts_body() {
        install_provider();
        let body = r#"{"id":"01960000-0000-7000-8000-00000000000a","domain":"ledger","schedule_type":"statement","cron_expression":"0 * * * *","target_config":{},"status":"active","next_run_at":"2026-01-01T00:00:00Z","max_retries":3}"#;
        let (handle, addr) = serve("HTTP/1.1 200 OK", body).await;
        let c = CronClient::new(format!("http://{addr}"), "tok");
        let req = CreateScheduleRequest::new(
            "ledger",
            "statement",
            "0 * * * *",
            json!({"baseUrl": "https://ledger.local"}),
        );
        let s = c.create_schedule(&req).await.unwrap();
        assert_eq!(s.schedule_type, "statement");
        assert_eq!(s.max_retries, 3);
        let raw = handle.await.unwrap();
        assert!(raw.starts_with("POST /v1/schedules HTTP/1.1"), "got: {raw}");
        assert!(
            raw.contains("authorization: Bearer tok") || raw.contains("Authorization: Bearer tok")
        );
    }

    #[tokio::test]
    async fn update_schedule_patch_body() {
        install_provider();
        let (handle, addr) = serve("HTTP/1.1 204 No Content", "").await;
        let c = CronClient::new(format!("http://{addr}"), "tok");
        let id = Uuid::new_v4();
        c.update_schedule_status(id, "paused").await.unwrap();
        let raw = handle.await.unwrap();
        assert!(
            raw.starts_with(&format!("PATCH /v1/schedules/{id} HTTP/1.1")),
            "got: {raw}"
        );
        assert!(raw.contains("paused"), "got: {raw}");
    }

    #[tokio::test]
    async fn delete_schedule_builds_path() {
        install_provider();
        let (handle, addr) = serve("HTTP/1.1 200 OK", "").await;
        let c = CronClient::new(format!("http://{addr}"), "tok");
        let id = Uuid::new_v4();
        c.delete_schedule(id).await.unwrap();
        let raw = handle.await.unwrap();
        assert!(
            raw.starts_with(&format!("DELETE /v1/schedules/{id} HTTP/1.1")),
            "got: {raw}"
        );
    }

    #[tokio::test]
    async fn error_status_maps_to_cron_error() {
        install_provider();
        let (handle, addr) = serve("HTTP/1.1 401 Unauthorized", "{}").await;
        let c = CronClient::new(format!("http://{addr}"), "tok");
        let err = c.list_schedules().await.unwrap_err();
        assert!(
            matches!(err, CronError::Http(ref m) if m.contains("401")),
            "{err:?}"
        );
        handle.await.unwrap();
    }
}
