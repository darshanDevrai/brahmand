

use std::sync::Arc;

use errors::QueryPlannerError;
use types::QueryType;

use crate::{graph_schema::graph_schema::GraphSchema, open_cypher_parser::ast::OpenCypherQueryAst, query_planner::{logical_plan::logical_plan::LogicalPlan}};


pub mod logical_expr;
pub mod logical_plan;
pub mod optimizer;
pub mod transformed;
pub mod analyzer;
// pub mod render_plan;
pub mod plan_ctx;
pub mod types;
mod errors;

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

pub fn evaluate_read_query(query_ast: OpenCypherQueryAst, current_graph_schema: &GraphSchema) -> Result<LogicalPlan, QueryPlannerError> {
        let (logical_plan, mut plan_ctx) = logical_plan::evaluate_query(query_ast)?;

        // println!("\n\n PLAN Before  {} \n\n", logical_plan);
        // println!("\n plan_ctx {}",plan_ctx);
        let logical_plan = analyzer::initial_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;
        // println!("\n\n PLAN after initial analyzing - \n{} \n\n", logical_plan);
        // println!("\n plan_ctx after initial analyzing - \n{}",plan_ctx);
        // logical_plan.print_graph_rels();
        let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx)?;
        // logical_plan.print_graph_rels();
        let logical_plan = analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;
        // println!("\n\n plan_ctx after intermediate analyzing \n {}",plan_ctx);
        // println!("\n plan after intermediate analyzing{}", logical_plan);
        let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx)?;

        let logical_plan = analyzer::final_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;
        
        // println!("\n\n plan_ctx after \n {}",plan_ctx);
        println!("\n plan after{}", logical_plan);



        // let render_plan = logical_plan.to_render_plan()?;

        // println!("\n\n render_plan {}", render_plan);
        let logical_plan = Arc::into_inner(logical_plan).ok_or(QueryPlannerError::LogicalPlanExtractor)?;
        Ok(logical_plan)

}