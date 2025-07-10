use std::sync::Arc;

use crate::query_engine_v2::{expr::plan_expr::{PlanExpr, TableAlias}, logical_plan::logical_plan::{LogicalPlan, PlanCtx, ReturnItem}, optimizer::optimizer_pass::OptimizerPass, transformed::Transformed};




pub struct ProjectionTagging;




impl OptimizerPass for ProjectionTagging {

    fn optimize(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Transformed<Arc<LogicalPlan>> {
        match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                for item in &projection.items {
                    self.tag_projection(item.clone(), plan_ctx);
                }
            },
            LogicalPlan::Filter(filter) => {
                self.optimize(filter.input.clone(), plan_ctx);
            },
            LogicalPlan::OrderBy(order_by) => {
                self.optimize(order_by.input.clone(), plan_ctx);
            },
            LogicalPlan::Skip(skip) => {
                self.optimize(skip.input.clone(), plan_ctx);
            },
            LogicalPlan::Limit(limit) => {
                self.optimize(limit.input.clone(), plan_ctx);
            },
            _ => ()
            // LogicalPlan::ConnectedTraversal(connected_traversal) => {},
            // LogicalPlan::Empty => return,
            // LogicalPlan::Scan(_) => return,
        }

        Transformed::No(logical_plan)
    }
    
    
}

impl ProjectionTagging {
    pub fn new() -> Self { 
        ProjectionTagging 
    }

    fn tag_projection(&self, mut item: ReturnItem, plan_ctx: &mut PlanCtx) {
        match &item.expression {
            PlanExpr::TableAlias(table_alias) => {
                let table_ctx = plan_ctx.alias_table_ctx_map.get_mut(&table_alias.0).unwrap();
                item.belongs_to_table = Some(TableAlias(table_alias.0.clone()));
                table_ctx.return_items.push(item);
            },
            PlanExpr::PropertyAccessExp(property_access) => {
                let table_ctx = plan_ctx.alias_table_ctx_map.get_mut(&property_access.table_alias.0).unwrap();
                item.belongs_to_table = Some(TableAlias(property_access.table_alias.0.clone()));
                table_ctx.return_items.push(item);
            }
            PlanExpr::OperatorApplicationExp(operator_application) => {
                for operand in &operator_application.operands {
                    let operand_return_item = ReturnItem {
                        expression: operand.clone(),
                        col_alias: None,
                        belongs_to_table: None,
                    };
                    self.tag_projection(operand_return_item, plan_ctx);
                }
            },
            PlanExpr::ScalarFnCall(scalar_fn_call) => {
                for arg in &scalar_fn_call.args {
                    let arg_return_item = ReturnItem {
                        expression: arg.clone(),
                        col_alias: None,
                        belongs_to_table: None,
                    };
                    self.tag_projection(arg_return_item, plan_ctx);
                }
            },
            PlanExpr::AggregateFnCall(aggregate_fn_call) => {
                for arg in &aggregate_fn_call.args {
                    let arg_return_item = ReturnItem {
                        expression: arg.clone(),
                        col_alias: None,
                        belongs_to_table: None,
                    };
                    self.tag_projection(arg_return_item, plan_ctx);
                }
            },
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
