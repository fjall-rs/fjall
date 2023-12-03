mod error;

use actix_web::{
    get,
    middleware::Logger,
    post,
    web::{self},
    App, HttpResponse, HttpServer,
};
use error::MyResult;
use lsm_tree::{Config, Tree};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize)]
struct PrefixQueryParams {
    limit: Option<u32>,
}

// This struct represents state
struct AppState {
    db: Tree,
}

#[derive(Serialize, Deserialize)]
struct InsertBody {
    item: Value,
}

#[derive(Serialize, Deserialize)]
struct BulkBody {
    upsert: Option<Vec<(String, String, String, Value)>>,
    remove: Option<Vec<(String, String, String)>>,
}

#[post("/{subject}")]
async fn insert_subject(
    data: web::Data<AppState>,
    body: web::Json<InsertBody>,
    path: web::Path<String>,
) -> MyResult<HttpResponse> {
    eprintln!("INSERT SUBJECT");

    let before = std::time::Instant::now();

    let subject_key = path.into_inner();

    data.db.insert(
        format!("s:{subject_key}"),
        serde_json::to_string(&body.item).unwrap(),
    )?;

    data.db.flush()?;

    Ok(HttpResponse::Ok()
        .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
        .body("OK"))
}

#[post("/{subject}/{verb}/{object}")]
async fn insert_relation(
    data: web::Data<AppState>,
    body: web::Json<InsertBody>,
    path: web::Path<(String, String, String)>,
) -> MyResult<HttpResponse> {
    eprintln!("INSERT RELATION");

    let before = std::time::Instant::now();

    let (subject_key, verb_key, object_key) = path.into_inner();

    data.db.insert(
        format!("v:s:{subject_key}:v:{verb_key}:o:{object_key}"),
        serde_json::to_string(&body.item).unwrap(),
    )?;

    data.db.flush()?;

    Ok(HttpResponse::Ok()
        .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
        .body("OK"))
}

#[get("/{subject}")]
async fn get_subject(path: web::Path<String>, data: web::Data<AppState>) -> MyResult<HttpResponse> {
    let before = std::time::Instant::now();

    let subject_key = path.into_inner();
    let key = format!("s:{subject_key}");

    match data.db.get(key)? {
        Some(item) => Ok(HttpResponse::Ok()
            .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
            .content_type("application/json; utf-8")
            .body(item)),
        None => Ok(HttpResponse::NotFound()
            .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
            .content_type("text:html; utf-8")
            .body("Not found")),
    }
}

#[get("/{subject}/{verb}")]
async fn list_by_verb(
    path: web::Path<(String, String)>,
    data: web::Data<AppState>,
    query: web::Query<PrefixQueryParams>,
) -> MyResult<HttpResponse> {
    let (subject, verb) = path.into_inner();

    let before = std::time::Instant::now();

    let all = data
        .db
        .prefix(format!("v:s:{subject}:v:{verb}:"))?
        .into_iter()
        .take(query.limit.unwrap_or(10_000) as usize)
        .collect::<Vec<_>>();

    let count = all.len();

    let mut edges = vec![];

    for item in all {
        let item = item?;

        let key = std::str::from_utf8(&item).unwrap();
        let verb_key = key.split(':').nth(4).unwrap();
        let object_key = key.split(':').nth(6).unwrap();
        let relation_data =
            serde_json::from_str::<serde_json::Value>(std::str::from_utf8(&item).unwrap()).unwrap();

        let object_data = data
            .db
            .get(format!("s:{object_key}"))?
            .map(|x| {
                serde_json::from_str::<serde_json::Value>(std::str::from_utf8(&x).unwrap()).unwrap()
            })
            .unwrap_or(serde_json::Value::Null);

        edges.push(serde_json::json!({
            "key": verb_key,
            "data": relation_data,
            "node": {
                "key": object_key,
                "data": object_data,
            },
        }));
    }

    let body = serde_json::json!({
        "edges": edges,
    });

    Ok(HttpResponse::Ok()
        .append_header(("x-count", count.to_string()))
        .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
        .content_type("application/json; utf-8")
        .body(serde_json::to_string(&body).unwrap()))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    let port = std::env::var("PORT").unwrap_or("8000".into());
    let port = port.parse::<u16>().expect("invalid port");

    let data_folder = std::env::var("DATA_FOLDER").unwrap_or(".data".into());
    log::info!("Opening database at {data_folder}");
    let db = Config::new(&data_folder)
        .block_cache_capacity(25_600) // 100 MB
        .open()
        .expect("failed to open db");

    log::info!("Starting on port {port}");

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::new("%r %s - %{User-Agent}i"))
            .app_data(web::Data::new(AppState { db: db.clone() }))
            .service(insert_subject)
            .service(insert_relation)
            .service(get_subject)
            .service(list_by_verb)
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await
}
