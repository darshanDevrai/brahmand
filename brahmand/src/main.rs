mod open_cypher_parser;
// mod query_engine;
pub mod clickhouse_query_generator;
mod graph_catalog;
mod query_planner;
pub mod render_plan;
mod server;

#[tokio::main]
async fn main() {
    println!("\nbrahmandDB v{}\n", env!("CARGO_PKG_VERSION"));
    server::run().await;
}
