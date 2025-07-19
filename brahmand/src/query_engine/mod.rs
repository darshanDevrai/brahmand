use errors::QueryEngineError;
use types::{GraphSchema, GraphSchemaElement, QueryIR, QueryType, TraversalMode};

use crate::open_cypher_parser::ast::OpenCypherQueryAst;

mod ch_query_generator;
mod errors;
mod optimizer;
mod planner;
pub mod types;

pub fn get_query_type(query_ast: &OpenCypherQueryAst) -> QueryType {
    if query_ast.create_node_table_clause.is_some() || query_ast.create_rel_table_clause.is_some() {
        QueryType::Ddl
    } else if query_ast.delete_clause.is_some() {
        QueryType::Delete
    } else if query_ast.set_clause.is_some() || query_ast.remove_clause.is_some() {
        QueryType::Update
    } else {
        QueryType::Read
    }
}

// For generation step, we will start generating table joins.
// While traversing if there are any conditions and return clauses then we will apply them before hand
// Use the last node in the final Select query.
// Do joins in reverse order

// pub fn generate_query(logical_plan: LogicalPlan, physical_plan: PhysicalPlan){

// }

pub fn evaluate_query(
    query_ast: OpenCypherQueryAst,
    traversal_mode: &TraversalMode,
    current_graph_schema: &GraphSchema,
) -> Result<(QueryType, Vec<String>, Option<GraphSchemaElement>), QueryEngineError> {
    let query_type = get_query_type(&query_ast);

    if query_type == QueryType::Read {
        let logical_plan = planner::evaluate_query(query_ast)?;

        let physical_plan =
            optimizer::generate_physical_plan(logical_plan.clone(), current_graph_schema)?;

        let query_ir = QueryIR {
            query_type: query_type.clone(),
            logical_plan,
            physical_plan,
        };

        let sql_queries = ch_query_generator::generate_read_query(query_ir, traversal_mode)?;
        println!("\nsql_queries {:}", sql_queries.join("\n"));

        Ok((query_type, sql_queries, None))
    } else if query_type == QueryType::Ddl {
        let (ddl_queries, graph_schema_element) =
            ch_query_generator::generate_ddl_query(query_ast, current_graph_schema)?;

        Ok((query_type, ddl_queries, Some(graph_schema_element)))
    } else {
        Err(QueryEngineError::UnsupportedQueryType)
    }
}
