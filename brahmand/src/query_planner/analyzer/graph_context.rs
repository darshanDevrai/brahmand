use crate::{graph_catalog::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema}, query_planner::{analyzer::{analyzer_pass::AnalyzerResult, errors::{AnalyzerError, Pass}}, logical_expr::logical_expr::Direction, logical_plan::logical_plan::GraphRel, plan_ctx::plan_ctx::{PlanCtx, TableCtx}}};



#[derive(Debug, Clone)]
pub struct GraphContext<'a> {
    pub left: GraphNodeContext<'a>,
    pub rel: GraphRelContext<'a>,
    pub right: GraphNodeContext<'a>
}

#[derive(Debug, Clone)]
pub struct GraphNodeContext<'a> {
    pub alias: &'a String,
    pub table_ctx: &'a TableCtx,
    pub label: String,
    pub schema: &'a NodeSchema,
    pub id_column: String,
    pub cte_name: String
}

#[derive(Debug, Clone)]
pub struct GraphRelContext<'a> {
    pub alias: &'a String,
    pub table_ctx: &'a TableCtx,
    pub label: String,
    pub schema: &'a RelationshipSchema,
    pub cte_name: String
    // id_column: String,
}


pub fn get_graph_context<'a>(graph_rel: &'a GraphRel, plan_ctx: &'a mut PlanCtx, graph_schema: &'a GraphSchema, pass: Pass) -> AnalyzerResult<GraphContext<'a>> {
    // get required information 
    let left_alias = &graph_rel.left_connection;
    let rel_alias = &graph_rel.alias;
    let right_alias = &graph_rel.right_connection;


    let left_ctx = plan_ctx.get_node_table_ctx(left_alias).map_err(|e| AnalyzerError::PlanCtx { pass: pass.clone(), source: e})?;
    let rel_ctx = plan_ctx.get_rel_table_ctx(rel_alias).map_err(|e| AnalyzerError::PlanCtx { pass: pass.clone(), source: e})?;
    let right_ctx = plan_ctx.get_node_table_ctx(right_alias).map_err(|e| AnalyzerError::PlanCtx { pass: pass.clone(), source: e})?;

    let left_label = left_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: pass.clone(), source: e})?;
    let rel_label = rel_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: pass.clone(), source: e})?;
    let original_rel_label = rel_label.replace(format!("_{}", Direction::Incoming).as_str(), "").replace(format!("_{}", Direction::Outgoing).as_str(), "").replace(format!("_{}", Direction::Either).as_str(), "");
    let right_label = right_ctx.get_label_str().map_err(|e| AnalyzerError::PlanCtx { pass: pass.clone(), source: e})?;

    let left_schema = graph_schema.get_node_schema(&left_label).map_err(|e| AnalyzerError::GraphSchema { pass: pass.clone(), source: e})?;
    let rel_schema = graph_schema.get_rel_schema(&original_rel_label).map_err(|e| AnalyzerError::GraphSchema { pass: pass.clone(), source: e})?;
    let right_schema = graph_schema.get_node_schema(&right_label).map_err(|e| AnalyzerError::GraphSchema { pass: pass, source: e})?;

    let left_node_id_column = left_schema.node_id.column.clone();
    let right_node_id_column = right_schema.node_id.column.clone();

    let left_cte_name = format!("{}_{}",left_label, left_alias);
    let rel_cte_name = format!("{}_{}", rel_label, rel_alias);
    let right_cte_name = format!("{}_{}", right_label, right_alias);


    let graph_context = GraphContext {
        left: GraphNodeContext { alias: left_alias, table_ctx: left_ctx, label: left_label, schema: left_schema, id_column: left_node_id_column, cte_name: left_cte_name },
        rel: GraphRelContext { alias: rel_alias, table_ctx: rel_ctx, label: rel_label, schema: rel_schema, cte_name: rel_cte_name},
        right: GraphNodeContext{ alias: right_alias, table_ctx: right_ctx, label: right_label, schema: right_schema, id_column: right_node_id_column, cte_name: right_cte_name },
    };

    Ok(graph_context)

}