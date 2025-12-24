use askama::Template;
use axum::{
    Router,
    response::Html,
    routing::{get, get_service},
};
use fibril_util::StaticAuthHandler;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::services::ServeDir;

use crate::routes;
use fibril_metrics::Metrics;

pub struct AdminConfig {
    // TODO: better type, parse earlier
    pub bind: String,
    pub auth: Option<StaticAuthHandler>,
}

pub struct AdminServer {
    pub metrics: Metrics,
    pub config: AdminConfig,
}

impl AdminServer {
    pub fn new(metrics: Metrics, config: AdminConfig) -> Self {
        Self { metrics, config }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let state = Arc::new(self);

        let handler404 = move || async move {
            Html(Html(
                NotFound {
                    page: "404",
                    title: "Not Found",
                }
                .render()
                .unwrap(),
            ))
        };

        let app = Router::new()
            .nest_service("/static", get_service(ServeDir::new("admin-ui")))
            .route(
                "/",
                get(move || async move {
                    Html(
                        OverviewPage {
                            page: "dashboard",
                            title: "Overview",
                        }
                        .render()
                        .unwrap(),
                    )
                }),
            )
            .route(
                "/admin/connections",
                get(move || async move {
                    Html(
                        Connections {
                            page: "connections",
                            title: "Connections",
                        }
                        .render()
                        .unwrap(),
                    )
                }),
            )
            .route(
                "/admin/subscriptions",
                get(move || async move {
                    Html(
                        Subscriptions {
                            page: "subscriptions",
                            title: "Subscriptions",
                        }
                        .render()
                        .unwrap(),
                    )
                }),
            )
            .route("/admin/api/overview", get(routes::overview))
            .route("/admin/api/connections", get(routes::connections))
            .route("/admin/api/subscriptions", get(routes::subscriptions))
            .fallback(handler404)
            .with_state(state.clone());

        let listener = TcpListener::bind(&state.config.bind).await?;
        tracing::info!("listening on {}", state.config.bind);
        axum::serve(listener, app).await?;
        Ok(())
    }
}

#[derive(Template)]
#[template(path = "pages/overview.html")]
struct OverviewPage {
    page: &'static str,
    title: &'static str,
}

#[derive(Template)]
#[template(path = "pages/connections.html")]
struct Connections {
    page: &'static str,
    title: &'static str,
}

#[derive(Template)]
#[template(path = "pages/subscriptions.html")]
struct Subscriptions {
    page: &'static str,
    title: &'static str,
}

#[derive(Template)]
#[template(path = "pages/404.html")]
struct NotFound {
    page: &'static str,
    title: &'static str,
}
