mod error;

use actix_web::{
    delete, get,
    middleware::Logger,
    post, put,
    web::{self},
    App, HttpResponse, HttpServer,
};
use error::MyResult;
use fjall::{Config, Keyspace, PartitionHandle};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// This struct represents state
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
    data: web::Data<AppState>,
    body: web::Json<BulkBody>,
) -> MyResult<HttpResponse> {
    log::debug!("BATCH");

    let before = std::time::Instant::now();

    let mut batch = data.db.batch();

    if let Some(remove) = &body.remove {
        for key in remove {
            batch.remove(key.clone());
        }
    }

    if let Some(upsert) = &body.upsert {
        for item in upsert {
            batch.insert(item.0.clone(), serde_json::to_string(&item.1).unwrap());
        }
    }

    batch.commit()?;
    data.keyspace.persist()?;

    Ok(HttpResponse::Ok()
        .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
        .body("OK"))
}

#[delete("/{key}")]
async fn delete_item(path: web::Path<String>, data: web::Data<AppState>) -> MyResult<HttpResponse> {
    let key = path.into_inner();
    log::debug!("DEL {key}");

    let before = std::time::Instant::now();

    data.db.remove(key)?;
    data.keyspace.persist()?;

    Ok(HttpResponse::Ok()
        .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
        .body("OK"))
}

#[put("/{key}")]
async fn insert_item(
    path: web::Path<String>,
    data: web::Data<AppState>,
    body: web::Json<InsertBody>,
) -> MyResult<HttpResponse> {
    let key = path.into_inner();
    log::debug!(
        "SET {key} {:?}",
        serde_json::to_string_pretty(&body.item).unwrap()
    );

    let before = std::time::Instant::now();

    data.db
        .insert(key, serde_json::to_string(&body.item).unwrap())?;
    data.keyspace.persist()?;

    Ok(HttpResponse::Created()
        .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
        .body("Created"))
}

#[get("/{key}")]
async fn get_item(path: web::Path<String>, data: web::Data<AppState>) -> MyResult<HttpResponse> {
    let key = path.into_inner();
    log::debug!("GET {key}");

    let before = std::time::Instant::now();

    if key.is_empty() {
        return Ok(HttpResponse::BadRequest().body("Bad Request"));
    }

    let item = data.db.get(key)?;

    match item {
        Some(item) => Ok(HttpResponse::Ok()
            .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
            .content_type("application/json; utf-8")
            .body(item.to_vec())),
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

    let data_folder = std::env::var("DATA_FOLDER").unwrap_or(".data".into());
    log::info!("Opening database at {data_folder}");

    let keyspace = Config::default().open()?;
    let db = keyspace.open_partition("data" /* PartitionConfig {} */)?;

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
        let db = keyspace.open_partition("data" /* PartitionConfig {} */)?;

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
