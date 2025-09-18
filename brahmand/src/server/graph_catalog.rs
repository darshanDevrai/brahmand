use std::time::Duration;

use clickhouse::Client;
use tokio::{sync::RwLock, time::interval};

use crate::graph_catalog::graph_schema::{GraphSchema, GraphSchemaElement};

use super::{GLOBAL_GRAPH_SCHEMA, models::GraphCatalog};

pub async fn initialize_global_schema(clickhouse_client: Client) {
    let schema = get_graph_catalog(clickhouse_client).await.unwrap();
    // Set the global schema wrapped in an RwLock.
    GLOBAL_GRAPH_SCHEMA.set(RwLock::new(schema)).ok();
}

pub async fn refresh_global_schema(clickhouse_client: Client) -> Result<(), String> {
    let new_schema = get_graph_catalog(clickhouse_client).await?;
    // Acquire a write lock asynchronously.
    let global_schema_lock = GLOBAL_GRAPH_SCHEMA
        .get()
        .ok_or("Global schema not initialized")?;
    let mut schema_guard = global_schema_lock.write().await;
    *schema_guard = new_schema;
    println!("Global schema refreshed");
    Ok(())
}

pub async fn get_graph_schema() -> GraphSchema {
    let schema_guard = GLOBAL_GRAPH_SCHEMA
        .get()
        .expect("Global schema not initialized")
        .read()
        .await;
    schema_guard.clone()
}

pub async fn get_graph_catalog(clickhouse_client: Client) -> Result<GraphSchema, String> {
    let graph_catalog_query = "SELECT id, schema_json FROM graph_catalog FINAL";
    let graph_catalog_result = clickhouse_client
        .query(graph_catalog_query)
        .fetch_one::<GraphCatalog>()
        .await;

    match graph_catalog_result {
        Ok(graph_catalog) => {
            let graph_schema: GraphSchema = serde_json::from_str(&graph_catalog.schema_json)
                .map_err(|e| format!("Schema parsing error: {}", e))?;

            Ok(graph_schema)
        }
        Err(err) => {
            // if it is a connection error then send error to the client from server
            // if the graph meta table is not present then create a one.
            let err_msg = err.to_string();
            // println!("err_msg -> {:?}", err_msg);

            if err_msg.contains("UNKNOWN_TABLE") {
                println!("Creating the graph_catalog table");
                let create_graph_catalog_query = "
                CREATE TABLE graph_catalog (
                    id UInt64,
                    schema_json String 
                ) ENGINE = ReplacingMergeTree()
                ORDER BY id";

                let _ = clickhouse_client
                    .clone()
                    .with_option("wait_end_of_query", "1")
                    .query(create_graph_catalog_query)
                    .execute()
                    .await
                    .map_err(|e| format!("Clickhouse Error: {}", e));

                let graph_catalog = GraphCatalog {
                    id: 1,
                    schema_json: r#"{"version": 1,"nodes": {},"relationships": {}, "relationships_indexes": {}}"#.to_string(),
                };
                let mut insert = clickhouse_client
                    .insert("graph_catalog")
                    .map_err(|e| format!("Clickhouse Error: {}", e))?;
                insert
                    .write(&graph_catalog)
                    .await
                    .map_err(|e| format!("Clickhouse Error: {}", e))?;
                insert
                    .end()
                    .await
                    .map_err(|e| format!("Clickhouse Error: {}", e))?;

                let graph_schema: GraphSchema = serde_json::from_str(&graph_catalog.schema_json)
                    .map_err(|e| format!("Schema parsing error: {}", e))?;

                Ok(graph_schema)
            } else {
                Err(format!("Clickhouse Error: {}", err_msg))
            }
        }
    }
}

pub async fn validate_schema(graph_schema_element: &Vec<GraphSchemaElement>) -> Result<(), String> {
    for element in graph_schema_element {
        if let GraphSchemaElement::Rel(relationship_schema) = element {
            // here check if both from_node and to_node tables are present or not in the schema

                let graph_schema_lock = GLOBAL_GRAPH_SCHEMA
                    .get()
                    .expect("Schema not initialized")
                    .read()
                    .await;

                if !graph_schema_lock
                    .get_nodes_schemas()
                    .contains_key(&relationship_schema.from_node)
                    || !graph_schema_lock
                        .get_nodes_schemas()
                        .contains_key(&relationship_schema.to_node)
                {
                    return Err("From and To node tables must be present before creating a relationship between them".to_string());
                }
        }
    }

    Ok(())
}

pub async fn add_to_schema(
    clickhouse_client: Client,
    graph_schema_elements: Vec<GraphSchemaElement>,
) -> Result<(), String> {
    let mut graph_schema = GLOBAL_GRAPH_SCHEMA.get().unwrap().write().await;

    for element in graph_schema_elements {
        match element {
            GraphSchemaElement::Node(node_schema) => {
                graph_schema.insert_node_schema(node_schema.table_name.to_string(), node_schema);
                graph_schema.increment_version();
            }
            GraphSchemaElement::Rel(relationship_schema) => {
                graph_schema.insert_rel_schema(
                    relationship_schema.table_name.to_string(),
                    relationship_schema,
                );
                graph_schema.increment_version();
            }
            GraphSchemaElement::RelIndex(relationship_index_schema) => {
                graph_schema.insert_rel_index_schema(
                    relationship_index_schema.table_name.to_string(),
                    relationship_index_schema,
                );
            }
        }
    }

    let schema_json = serde_json::to_string(&*graph_schema)
        .map_err(|e| format!("Schema serialization error: {}", e))?;

    let graph_catalog = GraphCatalog { id: 1, schema_json };

    let mut insert = clickhouse_client
        .insert("graph_catalog")
        .map_err(|e| format!("Clickhouse Error: {}", e))?;
    insert
        .write(&graph_catalog)
        .await
        .map_err(|e| format!("Clickhouse Error: {}", e))?;
    insert
        .end()
        .await
        .map_err(|e| format!("Clickhouse Error: {}", e))?;

    Ok(())
}

// This function periodically checks for schema updates.
// This will be helpful in distributed environment where schema has changed.
// In distributed environment, I think Keeper Map engine makes sense.
pub async fn monitor_schema_updates(ch_client: Client) -> Result<(), String> {
    // TODO Currently checking after every min. Make it an option to set by user.
    let mut ticker = interval(Duration::from_secs(60));

    loop {
        ticker.tick().await;

        // get in memory data for the graph schema
        let in_mem_schema_guard = GLOBAL_GRAPH_SCHEMA
            .get()
            .expect("Global schema not initialized")
            .read()
            .await;

        let mem_version = in_mem_schema_guard.get_version();

        // Fetch the schema from ClickHouse.
        let remote_schema = match get_graph_catalog(ch_client.clone()).await {
            Ok(schema) => schema,
            Err(err) => {
                eprintln!("Error fetching remote schema: {}", err);
                continue;
            }
        };

        // Compare versions. If they differ, update the global schema.
        if remote_schema.get_version() != mem_version {
            let mut schema_guard = GLOBAL_GRAPH_SCHEMA
                .get()
                .expect("Global schema not initialized")
                .write()
                .await;
            *schema_guard = remote_schema.clone();

            println!(
                "Global schema updated from version {} to {}",
                mem_version,
                remote_schema.get_version()
            );
        }
    }
}
