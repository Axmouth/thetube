use axum::{http::HeaderMap, http::StatusCode};
use base64::Engine;
use fibril_util::{AuthHandler, StaticAuthHandler};

pub async fn check_basic_auth(
    headers: &HeaderMap,
    auth: &Option<StaticAuthHandler>,
) -> Result<(), StatusCode> {
    let Some(auth) = auth else {
        return Ok(());
    };

    let Some(value) = headers.get("authorization") else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    let value = value.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
    let b64 = value
        .strip_prefix("Basic ")
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let decoded = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    let decoded = String::from_utf8(decoded).map_err(|_| StatusCode::UNAUTHORIZED)?;
    let (user, pass) = decoded.split_once(':').ok_or(StatusCode::UNAUTHORIZED)?;

    if auth.verify(user, pass).await {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}
