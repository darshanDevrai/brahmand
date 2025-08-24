mod open_cypher_parser;
// mod query_engine;
mod query_planner;
mod server;
mod graph_schema;
pub mod render_plan;
pub mod clickhouse_query_generator;

#[tokio::main]
async fn main() {
    println!("\nbrahmandDB v{}\n", env!("CARGO_PKG_VERSION"));
    server::run().await;
}
