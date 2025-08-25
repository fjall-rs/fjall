mod error;

use crate::error::Error;
use axum::{
    extract,
    http::header::{self, HeaderName},
    http::StatusCode,
    routing::{delete, get, post, put},
    Router,
};
use fjall::{Config, Keyspace, PartitionHandle, PersistMode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Serialize)]
struct InsertBody {
    item: Value,
}

#[derive(Deserialize, Serialize)]
struct BulkBody {
    upsert: Option<Vec<(String, Value)>>,
    remove: Option<Vec<String>>,
}

#[derive(Clone)]
struct State {
    keyspace: Keyspace,
    db: PartitionHandle,
}

async fn insert_batch(
    extract::State(state): extract::State<State>,
    extract::Json(body): extract::Json<BulkBody>,
) -> Result<(StatusCode, [(HeaderName, String); 1], &'static str), Error> {
    log::debug!("BATCH");

    let before = std::time::Instant::now();

    tokio::task::spawn_blocking(move || {
        let mut batch = state.keyspace.batch();

        if let Some(remove) = &body.remove {
            for key in remove {
                batch.remove(&state.db, key.clone());
            }
        }

        if let Some(upsert) = &body.upsert {
            for item in upsert {
                batch.insert(
                    &state.db,
                    item.0.clone(),
                    serde_json::to_string(&item.1).unwrap(),
                );
            }
        }

        batch.commit()?;
        state.keyspace.persist(PersistMode::SyncAll)
    })
    .await
    .unwrap()?;

    Ok((
        StatusCode::OK,
        [(
            HeaderName::from_bytes(b"x-took-ms").unwrap(),
            before.elapsed().as_millis().to_string(),
        )],
        "OK",
    ))
}

async fn delete_item(
    extract::Path(key): extract::Path<String>,
    extract::State(state): extract::State<State>,
) -> Result<(StatusCode, [(HeaderName, String); 1], &'static str), Error> {
    log::debug!("DEL {key}");

    let before = std::time::Instant::now();

    tokio::task::spawn_blocking(move || {
        state.db.remove(key)?;
        state.keyspace.persist(PersistMode::SyncAll)
    })
    .await
    .unwrap()?;

    Ok((
        StatusCode::OK,
        [(
            HeaderName::from_bytes(b"x-took-ms").unwrap(),
            before.elapsed().as_millis().to_string(),
        )],
        "OK",
    ))
}

async fn insert_item(
    extract::Path(key): extract::Path<String>,
    extract::State(state): extract::State<State>,
    extract::Json(body): extract::Json<InsertBody>,
) -> Result<(StatusCode, [(HeaderName, String); 1], &'static str), Error> {
    log::debug!(
        "SET {key} {:?}",
        serde_json::to_string_pretty(&body.item).unwrap()
    );

    let before = std::time::Instant::now();

    tokio::task::spawn_blocking(move || {
        state
            .db
            .insert(key, serde_json::to_string(&body.item).unwrap())?;
        state.keyspace.persist(PersistMode::SyncAll)
    })
    .await
    .unwrap()?;

    Ok((
        StatusCode::CREATED,
        [(
            HeaderName::from_bytes(b"x-took-ms").unwrap(),
            before.elapsed().as_millis().to_string(),
        )],
        "Created",
    ))
}

async fn get_item(
    extract::Path(key): extract::Path<String>,
    extract::State(state): extract::State<State>,
) -> Result<(StatusCode, [(HeaderName, String); 2], Vec<u8>), Error> {
    log::debug!("GET {key}");

    let before = std::time::Instant::now();

    let item = tokio::task::spawn_blocking(move || state.db.get(key))
        .await
        .unwrap()?;

    Ok(match item {
        Some(item) => (
            StatusCode::OK,
            [
                (
                    HeaderName::from_bytes(b"x-took-ms").unwrap(),
                    before.elapsed().as_millis().to_string(),
                ),
                (header::CONTENT_TYPE, mime::APPLICATION_JSON.to_string()),
            ],
            item.to_vec(),
        ),
        None => (
            StatusCode::NOT_FOUND,
            [
                (
                    HeaderName::from_bytes(b"x-took-ms").unwrap(),
                    before.elapsed().as_millis().to_string(),
                ),
                (header::CONTENT_TYPE, mime::APPLICATION_JSON.to_string()),
            ],
            b"null".to_vec(),
        ),
    })
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    let port = std::env::var("PORT").unwrap_or("8000".into());
    let port = port.parse::<u16>().expect("invalid port");

    log::info!("Opening database");

    let keyspace = Config::new(".fjall_data").open()?;
    let db = keyspace.open_partition("data", Default::default())?;

    let state = State { keyspace, db };

    log::info!("Starting on port {port}");

    let app = Router::new()
        .route("/:key", get(get_item))
        .route("/:key", put(insert_item))
        .route("/:key", delete(delete_item))
        .route("/batch", post(insert_batch))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&format!("0.0.0.0:{port}")).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::{body::Body, extract::Request, http::Method};
    use http_body_util::BodyExt as _;
    use serde_json::json;
    use tower::ServiceExt as _;

    #[tokio::test]
    async fn test_write_read() -> Result<(), Error> {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Trace)
            .init();

        let data_folder = tempfile::tempdir()?;

        let keyspace = Config::new(data_folder).open()?;
        let db = keyspace.open_partition("data", Default::default())?;

        let state = State { keyspace, db };

        let app = Router::new()
            .route("/:key", get(get_item))
            .route("/:key", put(insert_item))
            .route("/:key", delete(delete_item))
            .route("/batch", post(insert_batch))
            .with_state(state);

        // Insert

        let item = json!({
            "name": "Peter",
        });

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/asd")
                    .header(header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                    .body(Body::from(
                        serde_json::to_vec(&InsertBody { item: item.clone() }).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        // Get

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/asd")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(item, body);

        // Delete

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/asd")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Get deleted

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/asd")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        Ok(())
    }
}
