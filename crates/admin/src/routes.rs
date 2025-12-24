use axum::{Json, extract::State, http::StatusCode};
use serde::Serialize;
use std::sync::Arc;

use crate::auth::check_basic_auth;
use crate::server::AdminServer;

#[derive(Serialize)]
pub struct OverviewResponse {
    pub broker: fibril_metrics::BrokerStatsSnapshot,
    pub storage: fibril_metrics::StorageStatsSnapshot,
}

pub async fn overview(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<OverviewResponse>, StatusCode> {
    check_basic_auth(&headers, &server.config.auth).await?;

    Ok(Json(OverviewResponse {
        broker: server.metrics.broker().snapshot(),
        storage: server.metrics.storage().snapshot(),
    }))
}

pub async fn connections(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_basic_auth(&headers, &server.config.auth).await?;

    Ok(Json(server.metrics.connections().snapshot()))
}

pub async fn subscriptions(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_basic_auth(&headers, &server.config.auth).await?;

    Ok(Json(server.metrics.connections().snapshot_subs()))
}
