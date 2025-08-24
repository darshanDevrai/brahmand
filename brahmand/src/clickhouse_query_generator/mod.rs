use errors::ClickhouseQueryGeneratorError;

use crate::{graph_schema::graph_schema::{GraphSchema, GraphSchemaElement}, open_cypher_parser::ast::OpenCypherQueryAst, render_plan::{render_plan::RenderPlan, ToSql}};


mod common;
mod ddl_query;
pub mod errors;
mod to_sql_query;

// pub fn generate_sql(plan: RenderPlan) -> String{
//     let mut sql = String::new();
//     sql.push_str(&plan.ctes.to_sql());
//     sql.push_str(&plan.select.to_sql());
//     sql.push_str(&plan.from.to_sql());
//     sql.push_str(&plan.joins.to_sql());
//     sql.push_str(&plan.filters.to_sql());
//     sql.push_str(&plan.group_by.to_sql());
//     sql.push_str(&plan.order_by.to_sql());
//     sql.push_str(&plan.limit.to_sql());
//     sql.push_str(&plan.skip.to_sql());
//     println!("\n\n sql - \n{}", sql);
//     return sql
// }

pub fn generate_sql(plan: RenderPlan) -> String {
    plan.to_sql()
}

pub fn generate_ddl_query(
    query_ast: OpenCypherQueryAst,
    current_graph_schema: &GraphSchema,
) -> Result<(Vec<String>, GraphSchemaElement), ClickhouseQueryGeneratorError> {
    ddl_query::generate_query(query_ast, current_graph_schema)
}
