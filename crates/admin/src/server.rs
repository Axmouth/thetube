use axum::{
    Router,
    response::Html,
    routing::{get, get_service},
};
use dashmap::DashMap;
use fibril_util::StaticAuthHandler;
use std::{sync::Arc, time::SystemTime};
use tokio::{net::TcpListener, sync::OnceCell};
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
        let cache = Arc::new(PageCache::new());

        let handler404 = {
            let cache = cache.clone();
            move || async move { Html(cache.render("admin-ui/pages/404.html", "Not Found", "/").await) }
        };

        let app = Router::new()
            .nest_service("/static", get_service(ServeDir::new("admin-ui")))
            .route(
                "/",
                get({
                    let cache = cache.clone();
                    move || async move {
                        Html(
                            cache
                                .render("admin-ui/pages/overview.html", "Overview", "/")
                                .await,
                        )
                    }
                }),
            )
            .route(
                "/admin/connections",
                get({
                    let cache = cache.clone();
                    move || async move {
                        Html(
                            cache
                                .render("admin-ui/pages/connections.html", "Connections", "/admin/connections")
                                .await,
                        )
                    }
                }),
            )
            .route(
                "/admin/subscriptions",
                get({
                    let cache = cache.clone();
                    move || async move {
                        Html(
                            cache
                                .render("admin-ui/pages/subscriptions.html", "Subscriptions", "/admin/subscriptions")
                                .await,
                        )
                    }
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

static LAYOUT: OnceCell<String> = OnceCell::const_new();

struct PageCache {
    rendered: DashMap<String, (SystemTime, String)>,
}

impl PageCache {
    pub fn new() -> Self {
        Self {
            rendered: DashMap::new(),
        }
    }

    pub async fn render(&self, path: &str, title: &str, url: &str) -> String {
        let meta = tokio::fs::metadata(path).await.unwrap();
        let mtime = meta.modified().unwrap();

        if let Some(entry) = self.rendered.get(path)
            && entry.value().0 == mtime
        {
            // return entry.value().1.clone();
        }

        let layout = LAYOUT
            .get_or_init(|| async {
                tokio::fs::read_to_string("admin-ui/layout.html")
                    .await
                    .unwrap()
            })
            .await;
        let mut layout = tokio::fs::read_to_string("admin-ui/layout.html")
            .await
            .unwrap();

        layout = layout.replace("{{path}}", path);

        let page = tokio::fs::read_to_string(path).await.unwrap();

        let html = layout
            .replace(
                "{{active_dashboard}}",
                if url == "/" { "class=\"nav-link active\" aria-current=\"page\"" } else { "class=\"nav-link\"" },
            )
            .replace(
                "{{active_connections}}",
                if url == "/admin/connections" { "class=\"nav-link active\" aria-current=\"page\"" } else { "class=\"nav-link\"" },
            )
            .replace(
                "{{active_subscriptions}}",
                if url == "/admin/subscriptions" { "class=\"nav-link active\" aria-current=\"page\"" } else { "class=\"nav-link\"" },
            )
            .replace("{{title}}", title)
            .replace("{{content}}", &page);

        self.rendered.insert(path.into(), (mtime, html.clone()));
        html
    }
}
