

use errors::QueryPlannerError;

use crate::{open_cypher_parser::ast::OpenCypherQueryAst, query_engine::types::{GraphSchema, GraphSchemaElement, QueryType, TraversalMode}, query_planner::render_plan::plan_builder::RenderPlanBuilder};


pub mod logical_expr;
pub mod logical_plan;
pub mod optimizer;
pub mod transformed;
pub mod analyzer;
pub mod render_plan;
pub mod clickhouse_query_generator;
pub mod plan_ctx;
// pub mod types;
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


pub fn evaluate_query(
    query_ast: OpenCypherQueryAst,
    traversal_mode: &TraversalMode,
    current_graph_schema: &GraphSchema,
) -> Result<(QueryType, Vec<String>, Option<GraphSchemaElement>), QueryPlannerError> {
    let query_type = get_query_type(&query_ast);

    // println!("query_ast {:#}", query_ast);

    // if query_type == QueryType::Read {
        let (logical_plan, mut plan_ctx) = logical_plan::evaluate_query(query_ast)?;

        // println!("\n\n PLAN Before  {} \n\n", logical_plan);
        // println!("\n plan_ctx {}",plan_ctx);
        let logical_plan = analyzer::initial_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;
        // logical_plan.print_graph_rels();
        let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx)?;
        // logical_plan.print_graph_rels();
        let logical_plan = analyzer::final_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;
        let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx)?;
        
        // println!("\n\n plan_ctx after \n {}",plan_ctx);
        println!("\n plan after{}", logical_plan);

        let render_plan = logical_plan.to_render_plan();

        // println!("\n render_planr{}", render_plan);

        let sql_query = clickhouse_query_generator::generate_sql(render_plan);

        Ok((query_type, vec![sql_query], None))

        // match logical_plan_res {
        //     Ok((logical_plan, mut plan_ctx)) => {
        //         // println!("\n\n PLAN Before  {} \n\n", logical_plan);
        //         // println!("\n plan_ctx {}",plan_ctx);
        //         let logical_plan = analyzer::initial_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;
        //         // logical_plan.print_graph_rels();
        //         let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx)?;
        //         // logical_plan.print_graph_rels();
        //         let logical_plan = analyzer::final_analyzing(logical_plan, &mut plan_ctx, current_graph_schema)?;
        //         let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx)?;
                
        //         // println!("\n\n plan_ctx after \n {}",plan_ctx);
        //         println!("\n plan after{}", logical_plan);

        //         let render_plan = logical_plan.to_render_plan();

        //         // println!("\n render_planr{}", render_plan);

        //         let sql_query = clickhouse_query_generator::generate_sql(render_plan);

        //         Ok((query_type, vec![sql_query], None))

        //     },
        //     Err(e) => {
        //         println!("Error - {:?}", e);
        //         Err(QueryPlannerError::UnsupportedQueryType)
        //     }
        // }

    //     let physical_plan =
    //         optimizer::generate_physical_plan(logical_plan.clone(), current_graph_schema)?;

    //     let query_ir = QueryIR {
    //         query_type: query_type.clone(),
    //         logical_plan,
    //         physical_plan,
    //     };
    //     println!("query_ir {:#}\n\n",query_ir);
    //     let sql_queries = ch_query_generator::generate_read_query(query_ir, traversal_mode)?;
    //     println!("\n\n\n sql_queries {:#}", sql_queries.join("\n\n"));

    //     Ok((query_type, sql_queries, None))
    // } else if query_type == QueryType::Ddl {
    //     let (ddl_queries, graph_schema_element) =
    //         ch_query_generator::generate_ddl_query(query_ast, current_graph_schema)?;

    //     Ok((query_type, ddl_queries, Some(graph_schema_element)))
    // } else {
        // Err("www".to_string())
    // }
}