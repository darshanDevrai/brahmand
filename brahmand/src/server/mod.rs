use std::{env, sync::Arc};

use axum::{Router, routing::post};
use clickhouse::Client;
use handlers::query_handler;

use dotenv::dotenv;
use tokio::sync::{OnceCell, RwLock};

use crate::graph_catalog::graph_schema::GraphSchema;

mod clickhouse_client;
mod graph_catalog;
mod handlers;
mod models;

// #[derive(Clone)]
struct AppState {
    clickhouse_client: Client,
}

pub static GLOBAL_GRAPH_SCHEMA: OnceCell<RwLock<GraphSchema>> = OnceCell::const_new();

pub async fn run() {
    dotenv().ok();
    // Create and configure the ClickHouse client.
    let client = clickhouse_client::get_client();

    let app_state = AppState {
        clickhouse_client: client.clone(),
    };

    graph_catalog::initialize_global_schema(client.clone()).await;

    println!("GLOBAL_GRAPH_SCHEMA {:?}", GLOBAL_GRAPH_SCHEMA.get());

    // Spawn the background task to monitor schema updates.
    tokio::spawn(async move {
        if let Err(e) = graph_catalog::monitor_schema_updates(client).await {
            eprintln!("Error in schema monitor: {}", e);
        }
    });

    // Build the Axum router, injecting the ClickHouse client as shared state.
    let app = Router::new()
        .route("/query", post(query_handler))
        // .route("/ddl", post(ddl_handler))s
        .with_state(Arc::new(app_state));

    let app_host = env::var("BRAHMAND_HOST").unwrap_or("0.0.0.0".to_string());
    let app_port = env::var("BRAHMAND_PORT").unwrap_or("8080".to_string());

    let bind_address = app_host + ":" + &app_port;
    println!(" Server running on - {}", bind_address);
    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind(bind_address).await.unwrap();
    axum::serve(listener, app)
        // .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}
