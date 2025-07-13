use std::sync::Arc;

use crate::query_engine_v2::{analyzer::analyzer_pass::AnalyzerPass, expr::plan_expr::{Column, PlanExpr, PropertyAccess, TableAlias}, logical_plan::logical_plan::{LogicalPlan, PlanCtx, Projection, ProjectionItem}, transformed::Transformed};




pub struct ProjectionTagging;




impl AnalyzerPass for ProjectionTagging {

    // Check if the projection item is only * then check for explicitly mentioned aliases and add * as their projection.
    // in the final projection, put all explicit alias.* 

    // If there is any projection on relationship then use edgelist of that relation.

    fn analyze(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Transformed<Arc<LogicalPlan>> {
        match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                // handler select all. e.g. -
                // 
                // MATCH (u:User)-[c:Created]->(p:Post) 
                //      RETURN *;
                // 
                // We will treat it as - 
                // 
                // MATCH (u:User)-[c:Created]->(p:Post) 
                // RETURN u, c, p;
                // 
                // To achieve this we will convert `RETURN *` into `RETURN u, c, p`   
                // let mut proj_items_to_mutate:Vec<ProjectionItem> = if projection.items.len() == 1 && projection.items.first().unwrap().expression == PlanExpr::Star {
                let mut proj_items_to_mutate:Vec<ProjectionItem> = if self.select_all_present(&projection.items) {
                    // we will create projection items with only table alias as return item. tag_projection will handle the proper tagging and overall projection manupulation.
                    let explicit_aliases = self.get_explicit_aliases(plan_ctx);
                    explicit_aliases.iter().map(|exp_alias| {
                        let table_alias = TableAlias(exp_alias.clone());
                        ProjectionItem{
                            expression: PlanExpr::TableAlias(table_alias.clone()),
                            col_alias: None,
                            // belongs_to_table: Some(table_alias),
                        }
                    }).collect()
                } else {
                    projection.items.clone()
                };

                for item in &mut proj_items_to_mutate  {
                    self.tag_projection(item, plan_ctx);
                }

                Transformed::Yes(Arc::new(LogicalPlan::Projection(Projection{
                    input: projection.input.clone(),
                    items: proj_items_to_mutate,
                })))
            },
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.analyze(graph_node.input.clone(), plan_ctx);
                let self_tf = self.analyze(graph_node.self_plan.clone(), plan_ctx);
                graph_node.rebuild_or_clone(child_tf, self_tf, logical_plan.clone())
            },
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.analyze(graph_rel.left.clone(), plan_ctx);
                let center_tf = self.analyze(graph_rel.center.clone(), plan_ctx);
                let right_tf = self.analyze(graph_rel.right.clone(), plan_ctx);
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            },
            LogicalPlan::Scan(scan) => {
                Transformed::No(logical_plan.clone())
            },
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::ConnectedTraversal(connected_traversal) => {
                let start_tf = self.analyze(connected_traversal.start_node.clone(), plan_ctx);
                let rel_tf = self.analyze(connected_traversal.relationship.clone(), plan_ctx);
                let end_tf = self.analyze(connected_traversal.end_node.clone(), plan_ctx);
                connected_traversal.rebuild_or_clone(start_tf, rel_tf, end_tf, logical_plan.clone())
            },
            LogicalPlan::Filter(filter) => {
                let child_tf = self.analyze(filter.input.clone(), plan_ctx);
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GroupBy(group_by   ) => {
                let child_tf = self.analyze(group_by.input.clone(), plan_ctx);
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.analyze(order_by.input.clone(), plan_ctx);
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Skip(skip) => {
                let child_tf = self.analyze(skip.input.clone(), plan_ctx);
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Limit(limit) => {
                let child_tf = self.analyze(limit.input.clone(), plan_ctx);
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            },
        }
        
    }


    
    
}

impl ProjectionTagging {
    pub fn new() -> Self { 
        ProjectionTagging 
    }

    fn select_all_present(&self, projection_items: &Vec<ProjectionItem>) -> bool {
        projection_items.iter().any(|item| item.expression == PlanExpr::Star)
    }

    fn get_explicit_aliases(&self, plan_ctx: &mut PlanCtx) -> Vec<String> {
        plan_ctx.alias_table_ctx_map
            .iter()
            .filter_map(|(alias, table_ctx)| {
                if table_ctx.explicit_alias {
                    Some(alias.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn tag_projection(&self, item: &mut ProjectionItem, plan_ctx: &mut PlanCtx) {
        match item.expression.clone() {
            PlanExpr::TableAlias(table_alias) => {
                // if just table alias i.e MATCH (p:Post) Return p; then For final overall projection keep p.* and for p's projection keep *. 

                let table_ctx = plan_ctx.alias_table_ctx_map.get_mut(&table_alias.0).unwrap();
                let tagged_proj = ProjectionItem {
                    expression: PlanExpr::Star,
                    col_alias: None,
                    // belongs_to_table: Some(table_alias.clone()),
                };
                table_ctx.projection_items = vec![tagged_proj];
                // if table_ctx is of relation then mark use_edge_list = true
                if table_ctx.is_rel {
                    table_ctx.use_edge_list = true;
                }

                // update the overall projection
                item.expression = PlanExpr::PropertyAccessExp(PropertyAccess{
                    table_alias: table_alias.clone(),
                    column: Column("*".to_string()),
                });
                // item.belongs_to_table = Some(table_alias.clone())
            },
            PlanExpr::PropertyAccessExp(property_access) => {
                let table_ctx = plan_ctx.alias_table_ctx_map.get_mut(&property_access.table_alias.0).unwrap();
                // item.belongs_to_table = Some(TableAlias(property_access.table_alias.0.clone()));
                table_ctx.projection_items.push(item.clone());
            }
            PlanExpr::OperatorApplicationExp(operator_application) => {
                for operand in &operator_application.operands {
                    let mut operand_return_item = ProjectionItem {
                        expression: operand.clone(),
                        col_alias: None,
                        // belongs_to_table: None,
                    };
                    self.tag_projection(&mut operand_return_item, plan_ctx);
                }
            },
            PlanExpr::ScalarFnCall(scalar_fn_call) => {
                for arg in &scalar_fn_call.args {
                    let mut arg_return_item = ProjectionItem {
                        expression: arg.clone(),
                        col_alias: None,
                        // belongs_to_table: None,
                    };
                    self.tag_projection(&mut arg_return_item, plan_ctx);
                }
            },
            // For now I am not tagging Aggregate fns, but I will tag later for aggregate pushdown when I implement the aggregate push down optimization
            // PlanExpr::AggregateFnCall(aggregate_fn_call) => {
            //     for arg in &aggregate_fn_call.args {
            //         let mut arg_return_item = ProjectionItem {
            //             expression: arg.clone(),
            //             col_alias: None,
            //             belongs_to_table: None,
            //         };
            //         self.tag_projection(&mut arg_return_item, plan_ctx);
            //     }
            // },
            _ => ()
            
            // PlanExpr::Literal(literal) => todo!(),
            // PlanExpr::Variable(_) => todo!(),
            // PlanExpr::Star => todo!(),
            // PlanExpr::ColumnAlias(column_alias) => todo!(),
            // PlanExpr::Column(column) => todo!(),
            // PlanExpr::Parameter(_) => todo!(),
            // PlanExpr::List(plan_exprs) => todo!(),
            // PlanExpr::PathPattern(path_pattern) => todo!(),
        }
        
    }

     

}
