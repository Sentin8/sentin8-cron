// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Sentin8:Cron
// SPDX-License-Identifier: Apache-2.0

//! Sentin8:Cron Admin UI handlers — RFW definitions.
//! Served from `GET /v1/admin/ui/{page}`, rendered by Sentin8:Dash at runtime.

use crate::handlers::AppState;
use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    Json,
};
use sentin8::rfw::page::*;
use sentin8::rfw::response::AdminUiResponse;
use sentin8::rfw::widget::*;
use serde::Deserialize;
use sqlx::Row;

#[derive(Deserialize)]
/// The targetenvquery type.
pub struct TargetEnvQuery {
    #[serde(default)]
    target_env_id: Option<String>,
}

/// Fetch admin ui.
pub async fn get_admin_ui(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(_q): Query<TargetEnvQuery>,
    Path(page): Path<String>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let a = crate::auth::verify_token(&state.config, &headers).await?;
    a.require_any_scope(&[
        "sentin8:cron:write",
        "sentin8:cron:read",
        "sentin8:sys:admin",
        "sentin8:sys:org:admin",
        "sentin8:sys:env:admin",
    ])?;
    let def = match page.as_str() {
        "overview" => AdminPage::new("Cron Overview")
            .widget(card("Summary", "Active schedules, jobs today, failures."))
            .build(),
        "jobs" => AdminPage::new("Jobs")
            .widget(table(
                vec![
                    col("id", "Job ID"),
                    cols("name", "Name"),
                    cols("status", "Status"),
                    cols("error_message", "Error"),
                ],
                "/jobs",
            ))
            .build(),
        "schedules" => AdminPage::new("Schedules")
            .action(link("Create Schedule", "schedules/new"))
            .widget(table_with_actions(
                vec![
                    col("id", "Schedule ID"),
                    cols("name", "Name"),
                    cols("cron_expression", "Cron"),
                    cols("status", "Status"),
                ],
                "/schedules",
                vec![
                    row_action_post("Pause", "schedules/{id}/pause"),
                    row_action_post("Resume", "schedules/{id}/resume"),
                    row_action_post("Delete", "schedules/{id}/delete"),
                ],
            ))
            .build(),
        "schedules/new" => AdminPage::new("Create Schedule")
            .widget(RemoteWidget::Form(WidgetForm {
                fields: vec![
                    WidgetFormField {
                        key: "domain".into(),
                        label: "Domain".into(),
                        field_type: FormFieldType::Text,
                        required: true,
                        placeholder: Some("e.g. ledger".into()),
                        default_value: None,
                        options: None,
                        validation: None,
                        width: None,
                        help_text: Some(
                            "The domain this schedule belongs to (e.g. ledger, payments).".into(),
                        ),
                        visible_if: None,
                    },
                    WidgetFormField {
                        key: "schedule_type".into(),
                        label: "Schedule Type".into(),
                        field_type: FormFieldType::Select,
                        required: true,
                        placeholder: None,
                        default_value: Some(serde_json::json!("cron")),
                        options: Some(vec![
                            FormFieldOption {
                                value: "cron".into(),
                                label: "Cron (recurring)".into(),
                            },
                            FormFieldOption {
                                value: "one_shot".into(),
                                label: "One-shot".into(),
                            },
                        ]),
                        validation: None,
                        width: None,
                        help_text: None,
                        visible_if: None,
                    },
                    WidgetFormField {
                        key: "cron_expression".into(),
                        label: "Cron Expression".into(),
                        field_type: FormFieldType::Text,
                        required: true,
                        placeholder: Some("*/5 * * * *".into()),
                        default_value: None,
                        options: None,
                        validation: None,
                        width: None,
                        help_text: Some(
                            "Standard cron expression (min hour day month weekday).".into(),
                        ),
                        visible_if: None,
                    },
                    WidgetFormField {
                        key: "target_config".into(),
                        label: "Target Config (JSON)".into(),
                        field_type: FormFieldType::Text,
                        required: true,
                        placeholder: Some(
                            "{\"baseUrl\":\"https://{service}.{env}.{org}.{tld}\",\"internalPath\":\"/v1/internal\"}".into(),
                        ),
                        default_value: None,
                        options: None,
                        validation: None,
                        width: None,
                        help_text: Some(
                            "The proxy-back target: `baseUrl` (the owning \
                            service's origin) + optional `internalPath`. \
                            The create-time validation rejects a config without\
                             a non-empty `baseUrl` — the ticker can never \
                             execute a blank/malformed config (cron DESIGN §3, \
                             WARN-22).".into(),
                        ),
                        visible_if: None,
                    },
                ],
                submit_endpoint: "/v1/schedules".into(),
                submit_label: "Create Schedule".into(),
                cancel_label: "Cancel".into(),
                method: FormMethod::Post,
            }))
            .build(),
        // ── Policy Studio — shared page
        // constructors; the guard guarantees `Some` ( dispatch).
        p if sentin8::rfw::policy_page("sentin8:cron", p).is_some() => {
            sentin8::rfw::policy_page("sentin8:cron", p)
                .expect("policy_page checked in guard")
                .build()
        }
        _ => {
            return Err((
                axum::http::StatusCode::NOT_FOUND,
                format!("Unknown admin UI page: {page}"),
            ))
        }
    };
    Ok(Json(
        serde_json::to_value(AdminUiResponse::page(def)).unwrap(),
    ))
}

/// `GET /v1/admin/{resource}` — returns a JSON array of admin data rows.
/// SECURITY: `target_env_id` is REQUIRED — no fallback to the token's env_id.
/// The caller must explicitly specify which env they are administering.
/// RLS context is set before querying to enforce tenant isolation.
/// Fetch admin data.
pub async fn get_admin_data(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TargetEnvQuery>,
    Path(resource): Path<String>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let a = crate::auth::verify_token(&state.config, &headers).await?;
    a.require_any_scope(&[
        "sentin8:cron:write",
        "sentin8:cron:read",
        "sentin8:sys:admin",
        "sentin8:sys:org:admin",
        "sentin8:sys:env:admin",
    ])?;
    // SECURITY: target_env_id is required — no fallback.
    // Without it, a tenant user could read system env data via their token env_id.
    let env_id = q
        .target_env_id
        .as_deref()
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
        .ok_or_else(|| {
            (
                axum::http::StatusCode::BAD_REQUEST,
                "missing target_env_id — must specify the env to administer".to_string(),
            )
        })?;
    // Set RLS context on the SAME connection used for data queries — inside
    // an explicit transaction (cron DESIGN §7): set_config(…, true) is
    // transaction-local, so on a pool-acquired autocommit connection the
    // context evaporates before the data query and FORCE-RLS hides every row
    // (the UI table rendered "No data." while the row existed).
    let mut conn = state
        .pool
        .begin()
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    sqlx::query("SELECT sentin8_set_tenant_context($1)")
        .bind(env_id)
        .execute(&mut *conn)
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // env_id is the sole tenant key — RLS enforces isolation.
    let rows: Vec<serde_json::Value> = match resource.as_str() {
        "jobs" => sqlx::query("SELECT id::text, schedule_id::text, status, error_message FROM jobs WHERE env_id = $1 ORDER BY created_at DESC LIMIT 100")
            .bind(env_id)
            .fetch_all(&mut *conn).await
            .map(|rows| rows.iter().map(|row| serde_json::json!({
                "id": row.get::<Option<String>, &str>("id").unwrap_or_default(),
                "name": row.get::<Option<String>, &str>("schedule_id").unwrap_or_default(),
                "status": row.get::<Option<String>, &str>("status").unwrap_or_default(),
                "error_message": row.get::<Option<String>, &str>("error_message").unwrap_or_default(),
            })).collect()).unwrap_or_default(),
        "schedules" => sqlx::query("SELECT id::text, domain, cron_expression, status FROM schedules WHERE env_id = $1 ORDER BY created_at DESC LIMIT 100")
            .bind(env_id)
            .fetch_all(&mut *conn).await
            .map(|rows| rows.iter().map(|row| serde_json::json!({
                "id": row.get::<Option<String>, &str>("id").unwrap_or_default(),
                "name": row.get::<Option<String>, &str>("domain").unwrap_or_default(),
                "cron_expression": row.get::<Option<String>, &str>("cron_expression").unwrap_or_default(),
                "status": row.get::<Option<String>, &str>("status").unwrap_or_default(),
            })).collect()).unwrap_or_default(),
        _ => vec![],
    };
    Ok(Json(serde_json::Value::Array(rows)))
}

fn col(key: &str, label: &str) -> WidgetDataTableColumn {
    WidgetDataTableColumn {
        key: key.into(),
        label: label.into(),
        sortable: false,
        width: None,
        format: None,
    }
}
fn cols(key: &str, label: &str) -> WidgetDataTableColumn {
    WidgetDataTableColumn {
        key: key.into(),
        label: label.into(),
        sortable: true,
        width: None,
        format: None,
    }
}
fn table(columns: Vec<WidgetDataTableColumn>, endpoint: &str) -> RemoteWidget {
    RemoteWidget::DataTable(WidgetDataTable {
        columns,
        data_endpoint: endpoint.into(),
        sortable: true,
        filterable: true,
        page_size: 20,
        empty_message: Some("No data.".into()),
        row_link: None,
        row_actions: vec![],
    })
}
fn table_with_actions(
    columns: Vec<WidgetDataTableColumn>,
    endpoint: &str,
    row_actions: Vec<WidgetButton>,
) -> RemoteWidget {
    RemoteWidget::DataTable(WidgetDataTable {
        columns,
        data_endpoint: endpoint.into(),
        sortable: true,
        filterable: true,
        page_size: 20,
        empty_message: Some("No data.".into()),
        row_link: None,
        row_actions,
    })
}
fn _row_action(label: &str, route: &str) -> WidgetButton {
    WidgetButton {
        label: label.into(),
        button_type: ButtonType::Link,
        route: Some(route.into()),
        endpoint: None,
        icon: None,
    }
}
fn row_action_post(label: &str, endpoint: &str) -> WidgetButton {
    WidgetButton {
        label: label.into(),
        button_type: ButtonType::Primary,
        route: None,
        endpoint: Some(endpoint.into()),
        icon: None,
    }
}
fn card(title: &str, body: &str) -> RemoteWidget {
    RemoteWidget::Card(WidgetCard {
        title: Some(title.into()),
        subtitle: None,
        children: vec![RemoteWidget::Text(WidgetText {
            text: body.into(),
            text_type: WidgetTextType::Body,
            align: TextAlign::Left,
            max_lines: None,
        })],
        actions: vec![],
    })
}
fn link(label: &str, route: &str) -> AdminUiAction {
    AdminUiAction {
        label: label.into(),
        action_type: AdminUiActionType::Link,
        route: Some(route.into()),
        endpoint: None,
        icon: Some("add".into()),
    }
}
