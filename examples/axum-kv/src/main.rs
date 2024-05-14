use axum::{
    body::Body,
    extract::{Path, State},
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post, put},
    Json, Router,
};
use fjall::{Config, FlushMode, Keyspace, PartitionHandle};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

type SharedState = Arc<AppState>;

struct AppState {
    keyspace: Keyspace,
    db: PartitionHandle,
}

#[derive(Serialize, Deserialize)]
struct InsertBody {
    item: Value,
}

#[derive(Serialize, Deserialize)]
struct BulkBody {
    upsert: Option<Vec<(String, Value)>>,
    remove: Option<Vec<String>>,
}

async fn kv_get(Path(key): Path<String>, State(state): State<SharedState>) -> impl IntoResponse {
    let db = &state.db;

    let Ok(kv) = db.get(key) else {
        return Response::builder()
            .status(500)
            .body(Body::from("Internal server error"))
            .unwrap();
    };

    if let Some(value) = kv {
        Response::builder()
            .status(200)
            .body(Body::from(value.to_vec()))
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Not found"))
            .unwrap()
    }
}

async fn kv_set(
    Path(key): Path<String>,
    State(state): State<SharedState>,
    Json(payload): Json<InsertBody>,
) -> impl IntoResponse {
    let serialized = serde_json::to_string(&payload.item).unwrap();

    if state.db.insert(key, serialized).is_err() {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::CREATED
    }
}

async fn kv_batch(
    State(state): State<SharedState>,
    Json(payload): Json<BulkBody>,
) -> impl IntoResponse {
    let mut batch = state.keyspace.batch();

    if let Some(remove) = &payload.remove {
        for key in remove {
            batch.remove(&state.db, key.clone());
        }
    }

    if let Some(upsert) = &payload.upsert {
        for item in upsert {
            batch.insert(
                &state.db,
                item.0.clone(),
                serde_json::to_string(&item.1).unwrap(),
            );
        }
    }

    if batch.commit().is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    if state.keyspace.persist(FlushMode::SyncAll).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::OK
}

async fn kv_del(Path(key): Path<String>, State(state): State<SharedState>) -> impl IntoResponse {
    if state.db.remove(key).is_err() {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::OK
    }
}

#[tokio::main]
async fn main() -> fjall::Result<()> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    let port = std::env::var("PORT").unwrap_or("8000".into());
    let port = port.parse::<u16>().expect("invalid port");

    log::info!("Opening database");

    let keyspace = Config::default().open()?;
    let db = keyspace.open_partition("data", Default::default())?;

    let state: SharedState = Arc::new(AppState {
        keyspace: keyspace.clone(),
        db: db.clone(),
    });

    log::info!("Starting on port {port}");

    // build our application with a single route
    let app = Router::new()
        .route("/batch", post(kv_batch))
        .route("/:key", get(kv_get))
        .route("/:key", put(kv_set))
        .route("/:key", delete(kv_del))
        .with_state(Arc::clone(&state));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
