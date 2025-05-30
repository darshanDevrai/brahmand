mod open_cypher_parser;
mod query_engine;
mod server;

#[tokio::main]
async fn main() {
    println!("\nbrahmandDB v{}\n", env!("CARGO_PKG_VERSION"));
    server::run().await;
}
