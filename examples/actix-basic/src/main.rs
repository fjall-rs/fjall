mod error;

use actix_web::{
    get,
    middleware::Logger,
    web::{self},
    App, HttpResponse, HttpServer,
};
use error::RouteResult;
use fjall::{Config, Keyspace, PartitionHandle};
use serde_json::json;

struct AppState {
    keyspace: Keyspace,
    db: PartitionHandle,
}

#[get("/")]
async fn hello_world(state: web::Data<AppState>) -> RouteResult<HttpResponse> {
    let len = state.db.len()?;
    let msg = format!("Hello, I have {len} items");

    Ok(HttpResponse::Ok().json(json!({
        "message": msg,
    })))
}

#[actix_web::main]
async fn main() -> fjall::Result<()> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    let port = std::env::var("PORT").unwrap_or("8000".into());
    let port = port.parse::<u16>().expect("invalid port");

    log::info!("Opening database");

    let keyspace = Config::default().open()?;
    let db = keyspace.open_partition("data", Default::default())?;

    log::info!("Starting on port {port}");

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::new("%r %s - %{User-Agent}i"))
            .app_data(web::Data::new(AppState {
                keyspace: keyspace.clone(),
                db: db.clone(),
            }))
            .service(hello_world)
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await?;

    Ok(())
}
