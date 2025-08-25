mod error;

use actix_web::{
    delete, get,
    middleware::Logger,
    post, put,
    web::{self},
    App, HttpResponse, HttpServer,
};
use error::RouteResult;
use fjall::{Config, Keyspace, PartitionHandle, PersistMode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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

#[post("/batch")]
async fn insert_batch(
    state: web::Data<AppState>,
    body: web::Json<BulkBody>,
) -> RouteResult<HttpResponse> {
    log::debug!("BATCH");

    let before = std::time::Instant::now();

    web::block(move || {
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

    Ok(HttpResponse::Ok()
        .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
        .body("OK"))
}

#[delete("/{key}")]
async fn delete_item(
    path: web::Path<String>,
    state: web::Data<AppState>,
) -> RouteResult<HttpResponse> {
    let key = path.into_inner();
    log::debug!("DEL {key}");

    let before = std::time::Instant::now();

    web::block(move || {
        state.db.remove(key)?;
        state.keyspace.persist(PersistMode::SyncAll)
    })
    .await
    .unwrap()?;

    Ok(HttpResponse::Ok()
        .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
        .body("OK"))
}

#[put("/{key}")]
async fn insert_item(
    path: web::Path<String>,
    state: web::Data<AppState>,
    body: web::Json<InsertBody>,
) -> RouteResult<HttpResponse> {
    let key = path.into_inner();
    log::debug!(
        "SET {key} {:?}",
        serde_json::to_string_pretty(&body.item).unwrap()
    );

    let before = std::time::Instant::now();

    web::block(move || {
        state
            .db
            .insert(key, serde_json::to_string(&body.item).unwrap())?;
        state.keyspace.persist(PersistMode::SyncAll)
    })
    .await
    .unwrap()?;

    Ok(HttpResponse::Created()
        .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
        .body("Created"))
}

#[get("/{key}")]
async fn get_item(
    path: web::Path<String>,
    state: web::Data<AppState>,
) -> RouteResult<HttpResponse> {
    let key = path.into_inner();
    log::debug!("GET {key}");

    let before = std::time::Instant::now();

    if key.is_empty() {
        return Ok(HttpResponse::BadRequest().body("Bad Request"));
    }

    let item = web::block(move || state.db.get(key)).await.unwrap()?;

    match item {
        Some(item) => {
            // TODO: Not sure if this can be more elegant
            let body = actix_web::body::BoxBody::new(actix_web::web::Bytes::from(item));

            Ok(HttpResponse::Ok()
                .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
                .content_type("application/json; utf-8")
                .body(body))
        }
        None => {
            let body = json!(null);

            Ok(HttpResponse::NotFound()
                .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
                .content_type("application/json; utf-8")
                .body(serde_json::to_string(&body).unwrap()))
        }
    }
}

#[actix_web::main]
async fn main() -> fjall::Result<()> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    let port = std::env::var("PORT").unwrap_or("8000".into());
    let port = port.parse::<u16>().expect("invalid port");

    log::info!("Opening database");

    let keyspace = Config::new(".fjall_data").open()?;
    let db = keyspace.open_partition("data", Default::default())?;

    log::info!("Starting on port {port}");

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::new("%r %s - %{User-Agent}i"))
            .app_data(web::Data::new(AppState {
                keyspace: keyspace.clone(),
                db: db.clone(),
            }))
            .service(insert_item)
            .service(insert_batch)
            .service(get_item)
            .service(delete_item)
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{
        http::{header::ContentType, Method},
        test, App,
    };

    #[actix_web::test]
    async fn test_write_read() -> fjall::Result<()> {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Trace)
            .init();

        let data_folder = tempfile::tempdir()?;

        let keyspace = Config::new(data_folder).open()?;
        let db = keyspace.open_partition("data", Default::default())?;

        let app = App::new()
            .app_data(web::Data::new(AppState {
                keyspace: keyspace.clone(),
                db: db.clone(),
            }))
            .service(insert_item)
            .service(get_item)
            .service(delete_item)
            .service(insert_batch);
        let app = test::init_service(app).await;

        // Insert

        let item = json!({
            "name": "Peter"
        });

        let req = test::TestRequest::default()
            .method(Method::PUT)
            .uri("/asd")
            .insert_header(ContentType::json())
            .set_json(InsertBody { item: item.clone() })
            .to_request();

        let res = test::call_service(&app, req).await;
        assert!(res.status().is_success());

        // Get

        let req = test::TestRequest::default().uri("/asd").to_request();
        let res: serde_json::Value = test::call_and_read_body_json(&app, req).await;

        assert_eq!(item, res);

        // Delete

        let req = test::TestRequest::default()
            .method(Method::DELETE)
            .uri("/asd")
            .to_request();

        let res = test::call_service(&app, req).await;
        assert!(res.status().is_success());

        // Get deleted

        let req = test::TestRequest::default().uri("/asd").to_request();
        let res = test::call_service(&app, req).await;
        assert_eq!(404, res.status());

        Ok(())
    }
}
