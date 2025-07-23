use std::{collections::HashSet, sync::Arc};

use crate::query_planner::{analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult}, logical_expr::logical_expr::{AggregateFnCall, LogicalExpr, Operator, OperatorApplication, PropertyAccess, ScalarFnCall}, logical_plan::logical_plan::{Filter, LogicalPlan, ProjectionItem}, plan_ctx::plan_ctx::PlanCtx, transformed::Transformed};




pub struct FilterTagging;

impl AnalyzerPass for FilterTagging {

    


    fn analyze(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        
        match logical_plan.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.analyze(graph_node.input.clone(), plan_ctx)?;
                Ok(graph_node.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.analyze(graph_rel.left.clone(), plan_ctx)?;
                let center_tf = self.analyze(graph_rel.center.clone(), plan_ctx)?;
                let right_tf = self.analyze(graph_rel.right.clone(), plan_ctx)?;
                Ok(graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone()))
            },
            LogicalPlan::Cte(cte   ) => {
                let child_tf = self.analyze( cte.input.clone(), plan_ctx)?;
                Ok(cte.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Empty => Ok(Transformed::No(logical_plan.clone())),
            LogicalPlan::Scan(_) => Ok(Transformed::No(logical_plan.clone())),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.analyze(graph_joins.input.clone(), plan_ctx)?;
                Ok(graph_joins.rebuild_or_clone(child_tf, logical_plan.clone()))
            },
            LogicalPlan::Filter(filter) => {
                        let child_tf = self.analyze(filter.input.clone(), plan_ctx)?;
                        // call filter tagging and get new filter
                        let final_filter_opt = self.extract_filters(filter.predicate.clone(), plan_ctx);
                        // if final filter has some predicate left then create new filter else remove the filter node and return the child input
                        if let Some(final_filter) = final_filter_opt {
                            Ok(Transformed::Yes(Arc::new(LogicalPlan::Filter(Filter {
                                input: child_tf.get_plan(),
                                predicate: final_filter,
                            }))))
                        } else {
                            Ok(Transformed::Yes(child_tf.get_plan()))
                        }
                    },
            LogicalPlan::Projection(projection) => {
                        let child_tf = self.analyze(projection.input.clone(), plan_ctx)?;
                        Ok(projection.rebuild_or_clone(child_tf, logical_plan.clone()))
                    },
            LogicalPlan::GroupBy(group_by   ) => {
                        let child_tf = self.analyze(group_by.input.clone(), plan_ctx)?;
                        Ok(group_by.rebuild_or_clone(child_tf, logical_plan.clone()))
                    },
            LogicalPlan::OrderBy(order_by) => {
                        let child_tf = self.analyze(order_by.input.clone(), plan_ctx)?;
                        Ok(order_by.rebuild_or_clone(child_tf, logical_plan.clone()))
                    },
            LogicalPlan::Skip(skip) => {
                        let child_tf = self.analyze(skip.input.clone(), plan_ctx)?;
                        Ok(skip.rebuild_or_clone(child_tf, logical_plan.clone()))
                    },
            LogicalPlan::Limit(limit) => {
                        let child_tf = self.analyze(limit.input.clone(), plan_ctx)?;
                        Ok(limit.rebuild_or_clone(child_tf,logical_plan.clone()))
                    },
            
        }
        
    }
}


impl FilterTagging {
    pub fn new() -> Self {
        FilterTagging
    }

    // If there is any filter on relationship then use edgelist of that relation.
    pub fn extract_filters(&self, filter_predicate: LogicalExpr, plan_ctx: &mut PlanCtx) -> Option<LogicalExpr> {
        let mut extracted_filters: Vec<OperatorApplication> = vec![];
        let mut extracted_projections: Vec<PropertyAccess> = vec![];

        let remaining = self.process_expr(filter_predicate,
            &mut extracted_filters,
            &mut extracted_projections,
            false,
        );

        // tag extracted filters to respective table data
        for extracted_filter in extracted_filters {
            let mut table_name = "";
            for operand in &extracted_filter.operands {
                match operand {
                    LogicalExpr::PropertyAccessExp(property_access) => {
                        table_name = &property_access.table_alias.0;
                    },
                    // in case of fn, we check for any argument is of type prop access
                    LogicalExpr::ScalarFnCall(scalar_fn_call) => {
                        for arg in &scalar_fn_call.args {
                            if let LogicalExpr::PropertyAccessExp(property_access) = arg {
                                table_name = &property_access.table_alias.0;
                            }
                        }
                    },
                    // in case of fn, we check for any argument is of type prop access
                    LogicalExpr::AggregateFnCall(aggregate_fn_call) => {
                        for arg in &aggregate_fn_call.args {
                            if let LogicalExpr::PropertyAccessExp(property_access) = arg {
                                table_name = &property_access.table_alias.0;
                            }
                        }
                    },
                    _ => ()
                }
            }

            if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(table_name) {
                let converted_filters = self.convert_prop_acc_to_column(LogicalExpr::OperatorApplicationExp(extracted_filter));
                table_ctx.insert_filter(converted_filters);

                if table_ctx.is_relation() {
                    table_ctx.set_use_edge_list(true);
                }
            }

        }

        // add extracted_projections to their respective nodes.
        for prop_acc in extracted_projections {
            let table_alias = prop_acc.table_alias.clone();
            if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&table_alias.0){
                table_ctx.insert_projection(ProjectionItem {
                    expression: LogicalExpr::PropertyAccessExp(prop_acc),
                    col_alias: None,
                });
                
                // If there is any filter on relationship then use edgelist of that relation.
                if table_ctx.is_relation() {
                    table_ctx.set_use_edge_list(true);
                }
            }
            // else TODO throw error

        }

        remaining


    }

    fn convert_prop_acc_to_column(&self, expr: LogicalExpr) -> LogicalExpr {
        match expr {
            LogicalExpr::PropertyAccessExp(property_access) => {
                LogicalExpr::Column(property_access.column) 
            },
            LogicalExpr::OperatorApplicationExp(op_app) => {
                let mut new_operands: Vec<LogicalExpr> = vec![];
                for operand in op_app.operands {
                    let new_operand = self.convert_prop_acc_to_column(operand);
                    new_operands.push(new_operand);
                }
                LogicalExpr::OperatorApplicationExp(OperatorApplication { operator: op_app.operator, operands: new_operands })
            },
            LogicalExpr::List(exprs) => {
                let mut new_exprs = Vec::new();
                for sub_expr in exprs {

                    let new_expr = self.convert_prop_acc_to_column(sub_expr);
                    new_exprs.push(new_expr);

                }
                LogicalExpr::List(new_exprs)
            },
            LogicalExpr::ScalarFnCall(fc) => {
                let mut new_args = Vec::new();
                for arg in fc.args {
                    let new_arg =  self.convert_prop_acc_to_column(arg);
                    new_args.push(new_arg);

                }
                LogicalExpr::ScalarFnCall(ScalarFnCall {
                    name: fc.name,
                    args: new_args,
                })
            }

            LogicalExpr::AggregateFnCall(fc) =>{
                let mut new_args = Vec::new();
                for arg in fc.args {
                    let new_arg =  self.convert_prop_acc_to_column(arg);
                    new_args.push(new_arg);
                }
                LogicalExpr::AggregateFnCall(AggregateFnCall {
                    name: fc.name,
                    args: new_args,
                })
            }
            other => other,
        }
    }

    fn process_expr(
        &self,
        expr: LogicalExpr,
        extracted_filters: &mut Vec<OperatorApplication>,
        extracted_projections: &mut Vec<PropertyAccess>,
        in_or: bool,
    ) -> Option<LogicalExpr> {
        match expr {
            // When we have an operator application, process it separately.
            LogicalExpr::OperatorApplicationExp(mut op_app) => {
                // Check if the current operator is an Or.
                let current_is_or = op_app.operator == Operator::Or;
                // Update our flag: once inside an Or, we stay inside.
                let new_in_or = in_or || current_is_or;
    
                // Process each operand recursively, passing the flag.
                let mut new_operands = Vec::new();
                for operand in op_app.operands {
                    if let Some(new_operand) =
                        self.process_expr(operand, extracted_filters, extracted_projections, new_in_or)
                    {
                        new_operands.push(new_operand);
                    }
                }
                // Update the operator application with the processed operands.
                op_app.operands = new_operands;
    
    
                // TODO ALl aggregated functions will be evaluated in final where clause. We have to check what kind of fns we can put here.
                // because if we put aggregated fns like count() then it will mess up the final result because we want the count of all joined entries in the set,
                // in case of anchor node this could lead incorrect answers.
                if !new_in_or {
                    let mut should_extract: bool = false;
                    let mut temp_prop_acc: Vec<PropertyAccess> = vec![];
                    let mut condition_belongs_to: HashSet<&str> = HashSet::new();
                    let mut agg_operand_found = false;
    
                    for operand in &op_app.operands {
                        // if any of the fn argument belongs to one table then extract it.
                        if let LogicalExpr::ScalarFnCall(fc) = operand {
                            for arg in &fc.args {
                                if let LogicalExpr::PropertyAccessExp(prop_acc) = arg {
                                    condition_belongs_to.insert(&prop_acc.table_alias.0);
                                    temp_prop_acc.push(prop_acc.clone());
                                    should_extract = true;
                                }
                            }
                        } if let LogicalExpr::AggregateFnCall(fc) = operand {
                            for arg in &fc.args {
                                if let LogicalExpr::PropertyAccessExp(prop_acc) = arg {
                                    condition_belongs_to.insert(&prop_acc.table_alias.0);
                                    temp_prop_acc.push(prop_acc.clone());
                                    should_extract = false;
                                    agg_operand_found = true; 
                                }
                            }
                        }else if let LogicalExpr::PropertyAccessExp(prop_acc) = operand {
                            condition_belongs_to.insert(&prop_acc.table_alias.0);
                            temp_prop_acc.push(prop_acc.clone());
                            should_extract = true;
                        }
                    }
    
                    // if it is a multinode condition then we are not extracting. It will be kept at overall conditions
                    // and applied at the end in the final query.
                    if should_extract && !agg_operand_found && condition_belongs_to.len() == 1 {
                        extracted_filters.push(op_app);
                        return None;
                    } else if condition_belongs_to.len() > 1 {
                        extracted_projections.append(&mut temp_prop_acc);
                    }
                }
    
                // If after processing there is only one operand left and it is not unary then collapse the operator application.
                if op_app.operands.len() == 1 && op_app.operator != Operator::Not {
                    return Some(op_app.operands.into_iter().next().unwrap()); // unwrap is safe we are checking the len in condition
                }
    
                // if both operands has been extracted then remove the parent op
                if op_app.operands.is_empty() {
                    return None;
                }
    
                // Otherwise, return the rebuilt operator application.
                Some(LogicalExpr::OperatorApplicationExp(op_app))
            }
            
            // If we have a function call, process each argument.
            LogicalExpr::ScalarFnCall(fc) => {
                let mut new_args = Vec::new();
                for arg in fc.args {
                    if let Some(new_arg) = self.process_expr(arg, extracted_filters, extracted_projections, in_or) {
                        new_args.push(new_arg);
                    }
                }
                Some(LogicalExpr::ScalarFnCall(ScalarFnCall {
                    name: fc.name,
                    args: new_args,
                }))
            }

            LogicalExpr::AggregateFnCall(fc) =>{
                let mut new_args = Vec::new();
                for arg in fc.args {
                    if let Some(new_arg) = self.process_expr(arg, extracted_filters, extracted_projections, in_or) {
                        new_args.push(new_arg);
                    }
                }
                Some(LogicalExpr::AggregateFnCall(AggregateFnCall {
                    name: fc.name,
                    args: new_args,
                }))
            }
    
            // For a list, process each element.
            LogicalExpr::List(exprs) => {
                let mut new_exprs = Vec::new();
                for sub_expr in exprs {
                    if let Some(new_expr) =
                        self.process_expr(sub_expr, extracted_filters, extracted_projections, in_or)
                    {
                        new_exprs.push(new_expr);
                    }
                }
                Some(LogicalExpr::List(new_exprs))
            }
    
            // Base cases – literals, variables, and property accesses remain unchanged.
            other => Some(other),
        }
    }

    
    
}
