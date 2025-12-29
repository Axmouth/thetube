use askama::Template;
use axum::{
    Router,
    extract::Path,
    response::{Html, IntoResponse},
    routing::{get, get_service},
};
use fibril_storage::Storage;
use fibril_util::StaticAuthHandler;
use http::{Response, Uri, header};
use rust_embed::{Embed, RustEmbed};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::util::ServiceExt;
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
    pub storage: Arc<dyn Storage>,
}

fn render<T: Template>(tpl: T) -> Html<String> {
    match tpl.render() {
        Ok(v) => Html(v),
        Err(e) => {
            tracing::error!("template render error: {e}");
            Html("<h1>500 - template error</h1>".into())
        }
    }
}

impl AdminServer {
    pub fn new(metrics: Metrics, config: AdminConfig, storage: Arc<dyn Storage>) -> Self {
        Self { metrics, config, storage }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let state = Arc::new(self);

        let mut app = Router::new();

        #[cfg(debug_assertions)]
        {
            // DEV: serve the whole admin-ui folder from disk
            app = app.nest_service("/static", ServeDir::new("crates/admin/admin-ui"));
        }

        #[cfg(not(debug_assertions))]
        {
            // RELEASE: serve embedded assets
            app = app.route("/static/{*file}", get(admin_static));
        }

        let app = app
            .route("/", get(overview_page))
            .route("/admin/connections", get(connections_page))
            .route("/admin/subscriptions", get(subscriptions_page))
            .route("/admin/api/overview", get(routes::overview))
            .route("/admin/api/connections", get(routes::connections))
            .route("/admin/api/subscriptions", get(routes::subscriptions))
            .fallback(not_found)
            .with_state(state.clone());

        let listener = TcpListener::bind(&state.config.bind).await?;
        print_admin_banner(&state.config.bind, state.config.auth.is_some());
        tracing::info!("listening on {}", state.config.bind);
        axum::serve(listener, app).await?;
        Ok(())
    }
}

#[derive(RustEmbed)]
#[folder = "admin-ui"]
struct AdminAssets;

async fn admin_static(Path(path): Path<String>) -> impl IntoResponse {
    let path = path.trim_start_matches('/');
    if let Some(file) = AdminAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return ([(header::CONTENT_TYPE, mime.as_ref())], file.data).into_response();
    }
    tracing::debug!("admin static not found: {path}");

    not_found().await.into_response()
}

async fn not_found() -> impl IntoResponse {
    render(NotFound {
        page: "404",
        title: "Not Found",
    })
}

async fn overview_page() -> impl IntoResponse {
    render(OverviewPage {
        page: "dashboard",
        title: "Overview",
    })
}

async fn connections_page() -> impl IntoResponse {
    render(Connections {
        page: "connections",
        title: "Connections",
    })
}

async fn subscriptions_page() -> impl IntoResponse {
    render(Subscriptions {
        page: "subscriptions",
        title: "Subscriptions",
    })
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

pub fn print_admin_banner(bind: &str, auth: bool) {
    let auth = if auth { "enabled " } else { "disabled" };

    tracing::info!(
        r#"
                                                
┌──────────────────────────────────────────────┐
│            Fibril Admin Console              │
├──────────────────────────────────────────────┤
│  Web UI        : http://{bind:<20} │
│  Mode          : internal / operator         │
│  Auth          : {auth:<27} │
└──────────────────────────────────────────────┘
                                                
"#,
        bind = bind
    );
}
