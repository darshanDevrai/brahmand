use std::{sync::Arc, time::Instant};

use axum::{
    Json,
    extract::State,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use clickhouse::Client;
use serde_json::Value;
use tokio::io::AsyncBufReadExt;
use uuid::Uuid;

use crate::{
    open_cypher_parser::{self},
    query_engine::{
        // self,
        self, types::{GraphSchemaElement, QueryType, TraversalMode}
    }, query_planner,
};


use super::{
    AppState, graph_meta,
    models::{OutputFormat, QueryRequest},
};

pub async fn query_handler(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<QueryRequest>,
) -> Result<Response, (StatusCode, String)> {
    let instant = Instant::now();

    let graph_schema = graph_meta::get_graph_schema().await;

    let output_format = payload.format.unwrap_or(OutputFormat::JSONEachRow);

    // parse cypher query
    let cypher_ast = open_cypher_parser::parse_query(&payload.query).map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Brahmand Error: {}", e),
        )
    })?;

    let mut traversal_mode = TraversalMode::Cte;

    if let Some(mode) = payload.mode {
        traversal_mode = mode
    }

    // TODO convert this error to axum error with proper message. Expose the module name in traces but not to users
    let (query_type, ch_sql_queries, graph_schema_element_opt) =
        query_planner::evaluate_query(cypher_ast, &traversal_mode, &graph_schema).map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Brahmand Error: {}", e),
            )
        })?;
        // query_engine::evaluate_query(cypher_ast, &traversal_mode, &graph_schema).map_err(|e| {
        //     (
        //         axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        //         format!("Brahmand Error: {}", e),
        //     )
        // })?;
    if query_type == QueryType::Ddl {
        return ddl_handler(
            app_state.clickhouse_client.clone(),
            ch_sql_queries,
            graph_schema_element_opt,
        )
        .await;
    }

    if traversal_mode == TraversalMode::Cte {
        execute_cte_queries(app_state, ch_sql_queries, output_format, instant).await
    } else {
        execute_temp_table_queries(app_state, ch_sql_queries, output_format, instant).await
    }
}

async fn execute_cte_queries(
    app_state: Arc<AppState>,
    ch_sql_queries: Vec<String>,
    output_format: OutputFormat,
    instant: Instant,
) -> Result<Response, (StatusCode, String)> {
    let ch_query_string = ch_sql_queries.join(" ");

    if output_format == OutputFormat::Pretty
        || output_format == OutputFormat::PrettyCompact
        || output_format == OutputFormat::Csv
        || output_format == OutputFormat::CSVWithNames
    {
        let mut lines = app_state
            .clickhouse_client
            .clone()
            .query(&ch_query_string)
            .fetch_bytes(output_format)
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Clickhouse Error: {}", e),
                )
            })?
            .lines();

        let mut rows: Vec<String> = vec![];
        while let Some(line) = lines.next_line().await.map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Clickhouse Error: {}", e),
            )
        })? {
            // let value: serde_json::Value = serde_json::de::from_str(&line).unwrap();
            rows.push(line);
        }

        let now = Instant::now();
        let elapsed = now.duration_since(instant).as_secs_f64();
        let elapsed_rounded = (elapsed * 1000.0).round() / 1000.0;
        rows.push(format!("\nElapsed: {} sec", elapsed_rounded));

        let text = rows.join("\n");

        let mut response = (StatusCode::OK, text).into_response();
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        Ok(response)
    } else {
        let mut lines = app_state
            .clickhouse_client
            .clone()
            .query(&ch_query_string)
            .fetch_bytes("JSONEachRow")
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Clickhouse Error: {}", e),
                )
            })?
            .lines();

        let mut rows: Vec<Value> = vec![];
        while let Some(line) = lines.next_line().await.map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Clickhouse Error: {}", e),
            )
        })? {
            let value: serde_json::Value = serde_json::de::from_str(&line).unwrap();
            rows.push(value);
        }

        Ok(Json(rows).into_response())
    }
}

async fn execute_temp_table_queries(
    app_state: Arc<AppState>,
    mut ch_sql_queries: Vec<String>,
    output_format: OutputFormat,
    instant: Instant,
) -> Result<Response, (StatusCode, String)> {
    let session_id = Uuid::new_v4();
    let ch_client = app_state
        .clickhouse_client
        .clone()
        .with_option("session_id", session_id);
    let last_query = ch_sql_queries.pop().unwrap();

    for ch_query in ch_sql_queries {
        println!("\n ch_query -> {:?}", ch_query);
        let ch_client = app_state
            .clickhouse_client
            .clone()
            .with_option("session_id", session_id);
        ch_client
            .query(&ch_query)
            .execute()
            .await
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    if output_format == OutputFormat::Pretty
        || output_format == OutputFormat::PrettyCompact
        || output_format == OutputFormat::Csv
        || output_format == OutputFormat::CSVWithNames
    {
        let mut lines = ch_client
            .query(&last_query)
            .fetch_bytes(output_format)
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Clickhouse Error: {}", e),
                )
            })?
            .lines();

        let mut rows: Vec<String> = vec![];
        while let Some(line) = lines.next_line().await.map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Clickhouse Error: {}", e),
            )
        })? {
            // let value: serde_json::Value = serde_json::de::from_str(&line).unwrap();
            rows.push(line);
        }

        let now = Instant::now();
        let elapsed = now.duration_since(instant).as_secs_f64();
        let elapsed_rounded = (elapsed * 1000.0).round() / 1000.0;
        rows.push(format!("\nElapsed: {} sec", elapsed_rounded));

        let text = rows.join("\n");

        let mut response = (StatusCode::OK, text).into_response();
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        Ok(response)
    } else {
        // Execute the arbitrary ClickHouse query.
        let mut lines = ch_client
            .query(&last_query)
            .fetch_bytes("JSONEachRow")
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .lines();

        let mut rows: Vec<Value> = vec![];
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        {
            let value: serde_json::Value = serde_json::de::from_str(&line).unwrap();
            rows.push(value);
        }

        Ok(Json(rows).into_response())
    }
}

pub async fn ddl_handler(
    clickhouse_client: Client,
    ch_sql_queries: Vec<String>,
    graph_schema_element_opt: Option<GraphSchemaElement>,
) -> Result<Response, (StatusCode, String)> {
    // // parse cypher query
    // let cypher_ast = open_cypher_parser::parse_query(&payload.query).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // let (query_type,ch_sql_queries) = query_engine::evaluate_ddl_query(cypher_ast).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let graph_schema_element = graph_schema_element_opt.unwrap();

    graph_meta::validate_schema(&graph_schema_element)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Brahmand Error: {}", e),
            )
        })?;

    for ch_query in ch_sql_queries {
        println!("\n ch_query -> {:?}", ch_query);
        let ch_client = clickhouse_client
            .clone()
            .with_option("wait_end_of_query", "1");

        ch_client.query(&ch_query).execute().await.map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Clickhouse Error: {}", e),
            )
        })?;
    }

    // Now that DDL is applied successfully, add graph schema element into the schema and update the graph meta table here

    graph_meta::add_to_schema(clickhouse_client.clone(), graph_schema_element)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Clickhouse Error: {}", e),
            )
        })?;

    graph_meta::refresh_global_schema(clickhouse_client)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Clickhouse Error: {}", e),
            )
        })?;

    let mut response = (StatusCode::OK, "DDL applied successfully".to_string()).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));

    // println!("IN DDL HANDLER GLOBAL_GRAPH_SCHEMA {:?}",GLOBAL_GRAPH_SCHEMA.get());
    Ok(response)
}
