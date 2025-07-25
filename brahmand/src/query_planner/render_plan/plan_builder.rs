
use crate::query_planner::{logical_plan::{self, logical_plan::LogicalPlan}, render_plan::{errors::RenderBuildError, render_expr::{AggregateFnCall, Operator, OperatorApplication, RenderExpr, ScalarFnCall}, render_plan::{Cte, CteItems, FilterItems, FromTable, GroupByExpressions, Join, JoinItems, LimitItem, OrderByItem, OrderByItems, RenderPlan, SelectItem, SelectItems, SkipItem}}};





pub type RenderPlanBuilderResult<T> = Result<T, RenderBuildError>;

pub(crate) trait RenderPlanBuilder {

    fn extract_last_node_cte(&self) -> Option<Cte>;

    fn extract_final_filters(&self) -> Option<RenderExpr>;

    fn extract_ctes(&self, last_node_alias: &str) -> RenderPlanBuilderResult<Vec<Cte>>;

    fn extract_select_items(&self) -> Vec<SelectItem>;

    fn extract_from(&self) -> Option<FromTable>;

    fn extract_filters(&self) -> Option<RenderExpr>;

    fn extract_joins(&self) -> Vec<Join>;

    fn extract_group_by(&self) -> Vec<RenderExpr>;

    fn extract_order_by(&self) -> Vec<OrderByItem>;

    fn extract_limit(&self) -> Option<i64>;

    fn extract_skip(&self) -> Option<i64>;

    fn to_render_plan(&self) -> RenderPlanBuilderResult<RenderPlan>;

}


impl RenderPlanBuilder for LogicalPlan {

    fn extract_last_node_cte(&self) -> Option<Cte> {

        match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(_) => None,
            LogicalPlan::GraphNode(graph_node) => {
                graph_node.input.extract_last_node_cte()
            },
            LogicalPlan::GraphRel(graph_rel) => {
                // process left node first. 
                let left_node_cte_opt = graph_rel.left.extract_last_node_cte();

                // If last node is still not found then check at the right tree
                if left_node_cte_opt.is_none() {

                    graph_rel.right.extract_last_node_cte()
                } else {
                    left_node_cte_opt
                }
            },
            LogicalPlan::Filter(filter) => filter.input.extract_last_node_cte(),
            LogicalPlan::Projection(projection) => projection.input.extract_last_node_cte(),
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_last_node_cte(),
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_last_node_cte(),
            LogicalPlan::Skip(skip) => skip.input.extract_last_node_cte(),
            LogicalPlan::Limit(limit) => limit.input.extract_last_node_cte(),
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_last_node_cte(),
            LogicalPlan::Cte(logical_cte) => {
                let filters = logical_cte.input.extract_filters();
                // TODO check if it is empty then throw error
                let select_items = logical_cte.input.extract_select_items();
                if select_items.is_empty() {
                    return  None;
                }
                if let Some(from_table) = logical_cte.input.extract_from() {
                    let render_cte = Cte{ 
                        cte_name: logical_cte.name.clone(), 
                        select: SelectItems(select_items),
                        from: from_table, 
                        filters: FilterItems(filters)
                    };
                    return Some(render_cte);
                }
                None                
            }
        }
    }

    fn extract_ctes(&self, last_node_alias: &str) -> RenderPlanBuilderResult<Vec<Cte>> {
        
        match &self {
            LogicalPlan::Empty => Ok(vec![]),
            LogicalPlan::Scan(_) => Ok(vec![]),
            LogicalPlan::GraphNode(graph_node) => {
                graph_node.input.extract_ctes(last_node_alias)
            },
            LogicalPlan::GraphRel(graph_rel) => {
                
                // first extract the bottom one
                let mut right_cte = graph_rel.right.extract_ctes(last_node_alias)?;
                // then process the center
                let mut center_cte = graph_rel.center.extract_ctes(last_node_alias)?;
                right_cte.append(&mut center_cte);
                // then left 
                let left_alias = &graph_rel.left_connection;
                if left_alias != &last_node_alias{
                    let mut left_cte = graph_rel.left.extract_ctes(last_node_alias)?;
                    right_cte.append(&mut left_cte);
                }
                
                Ok(right_cte)
            },
            LogicalPlan::Filter(filter) => filter.input.extract_ctes(last_node_alias),
            LogicalPlan::Projection(projection) => projection.input.extract_ctes(last_node_alias),
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_ctes(last_node_alias),
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_ctes(last_node_alias),
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_ctes(last_node_alias),
            LogicalPlan::Skip(skip) => skip.input.extract_ctes(last_node_alias),
            LogicalPlan::Limit(limit) => limit.input.extract_ctes(last_node_alias),
            LogicalPlan::Cte(logical_cte) => {
                let mut select_items = logical_cte.input.extract_select_items();

                if select_items.is_empty() {
                    return  Err(RenderBuildError::MissingSelectItems);
                }

                for select_item in select_items.iter_mut() {
                    if let RenderExpr::PropertyAccessExp(pro_acc) = &select_item.expression {
                        *select_item = SelectItem {
                            expression: RenderExpr::Column(pro_acc.column.clone()),
                            col_alias: None,
                        };
                    }
                }

                let mut from_table = logical_cte.input.extract_from().ok_or(RenderBuildError::MissingFromTable)?;
                from_table.table_alias = None;
                let filters = logical_cte.input.extract_filters();
                Ok(vec![Cte{ 
                    cte_name: logical_cte.name.clone(), 
                    select: SelectItems(select_items),
                    from: from_table, 
                    filters: FilterItems(filters) 
                }])
            }
        }
    }

    fn extract_select_items(&self) -> Vec<SelectItem> {
        match &self {
            LogicalPlan::Empty => vec![],
            LogicalPlan::Scan(_) => vec![],
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_select_items(),
            LogicalPlan::GraphRel(graph_rel) => {
                        let mut left_select_items = graph_rel.left.extract_select_items();
                        let mut center_select_items = graph_rel.center.extract_select_items();
                        let mut right_select_items = graph_rel.right.extract_select_items();
                        left_select_items.append(&mut center_select_items);
                        left_select_items.append(&mut right_select_items);
                
                        left_select_items
                    },
            LogicalPlan::Filter(filter) => filter.input.extract_select_items(),
            LogicalPlan::Projection(projection) => {
                        projection.items.iter().map(|item| SelectItem {
                            expression: item.expression.clone().into(),
                            col_alias: item.col_alias.clone().map(Into::into)
                        }).collect()
                    },
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_select_items(),
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_select_items(),
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_select_items(),
            LogicalPlan::Skip(skip) => skip.input.extract_select_items(),
            LogicalPlan::Limit(limit) => limit.input.extract_select_items(),
            LogicalPlan::Cte(cte) => cte.input.extract_select_items(),
        }
    }

    fn extract_from(&self) -> Option<FromTable> {
        match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(scan) => {
                        Some(FromTable {
                            table_name: scan.table_name.clone().unwrap_or_else(|| scan.table_alias.clone()),
                            table_alias: Some(scan.table_alias.clone()),
                        })
                    },
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_from(),
            LogicalPlan::GraphRel(graph_rel) => {
                        let left_from_opt = graph_rel.left.extract_from();
                        let center_from_opt = graph_rel.center.extract_from();
                        let right_from_opt = graph_rel.right.extract_from();

                        if left_from_opt.is_some() {
                            left_from_opt
                        } else if center_from_opt.is_some() {
                            center_from_opt
                        } else if right_from_opt.is_some() {
                            right_from_opt
                        } else {
                            None
                        }

                    },
            LogicalPlan::Filter(filter) => filter.input.extract_from(),
            LogicalPlan::Projection(projection) => projection.input.extract_from(),
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_from(),
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_from(),
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_from(),
            LogicalPlan::Skip(skip) => skip.input.extract_from(),
            LogicalPlan::Limit(limit) => limit.input.extract_from(),
            LogicalPlan::Cte(cte) => cte.input.extract_from(),
            
        }
    }

    fn extract_filters(&self) -> Option<RenderExpr> {
        match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(_) => None,
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_filters(),
            LogicalPlan::GraphRel(graph_rel) => {
                let left_filter_opt = graph_rel.left.extract_filters();
                let center_filter_opt = graph_rel.center.extract_filters();
                let right_filter_opt = graph_rel.right.extract_filters();

                if left_filter_opt.is_some() {
                    left_filter_opt
                } else if center_filter_opt.is_some() {
                    center_filter_opt
                } else if right_filter_opt.is_some() {
                    right_filter_opt
                } else {
                    None
                }
            },
            LogicalPlan::Filter(filter) => Some(filter.predicate.clone().into()),
            LogicalPlan::Projection(projection) => projection.input.extract_filters(),
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_filters(),
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_filters(),
            LogicalPlan::Skip(skip) => skip.input.extract_filters(),
            LogicalPlan::Limit(limit) => limit.input.extract_filters(),
            LogicalPlan::Cte(cte) => cte.input.extract_filters(),
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_filters(),
        }
    }

    fn extract_final_filters(&self) -> Option<RenderExpr> {
        match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(_) => None,
            LogicalPlan::GraphNode(_) => None,
            LogicalPlan::GraphRel(_) => None,
            LogicalPlan::Filter(filter) => Some(filter.predicate.clone().into()),
            LogicalPlan::Projection(projection) => projection.input.extract_final_filters(),
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_final_filters(),
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_final_filters(),
            LogicalPlan::Skip(skip) => skip.input.extract_final_filters(),
            LogicalPlan::Limit(limit) => limit.input.extract_final_filters(),
            LogicalPlan::Cte(_) => None,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_final_filters(),
        }
    } 

    fn extract_joins(&self) -> Vec<Join> {
        match &self {
            LogicalPlan::Empty => vec![],
            LogicalPlan::Scan(_) => vec![],
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_joins(),
            LogicalPlan::GraphRel(graph_rel) => {
                let mut left_join_items = graph_rel.left.extract_joins();
                let mut center_join_items = graph_rel.center.extract_joins();
                let mut right_join_items = graph_rel.right.extract_joins();
                left_join_items.append(&mut center_join_items);
                left_join_items.append(&mut right_join_items);
        
                left_join_items
            },
            LogicalPlan::Filter(filter) => filter.input.extract_joins(),
            LogicalPlan::Projection(projection) => projection.input.extract_joins(),
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_joins(),
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_joins(),
            LogicalPlan::Skip(skip) => skip.input.extract_joins(),
            LogicalPlan::Limit(limit) => limit.input.extract_joins(),
            LogicalPlan::Cte(cte) => cte.input.extract_joins(),
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.joins.iter().cloned().map(Into::into).collect(),
        }
    }

    fn extract_group_by(&self) -> Vec<RenderExpr> {
        match &self {
            LogicalPlan::Empty => vec![],
            LogicalPlan::Scan(_) => vec![],
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_group_by(),
            LogicalPlan::GraphRel(graph_rel) => {
                let mut left_group_by_items = graph_rel.left.extract_group_by();
                let mut center_group_by_items = graph_rel.center.extract_group_by();
                let mut right_group_by_items = graph_rel.right.extract_group_by();
                left_group_by_items.append(&mut center_group_by_items);
                left_group_by_items.append(&mut right_group_by_items);
        
                left_group_by_items
            },
            LogicalPlan::Filter(filter) => filter.input.extract_group_by(),
            LogicalPlan::Projection(projection) => projection.input.extract_group_by(),
            LogicalPlan::GroupBy(group_by) => group_by.expressions.iter().cloned().map(Into::into).collect(),//.collect::<Vec<RenderExpr>>(),
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_group_by(),
            LogicalPlan::Skip(skip) => skip.input.extract_group_by(),
            LogicalPlan::Limit(limit) => limit.input.extract_group_by(),
            LogicalPlan::Cte(cte) => cte.input.extract_group_by(),
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_group_by(),
        }
    }

    fn extract_order_by(&self) -> Vec<OrderByItem> {
        match &self {
            LogicalPlan::Empty => vec![],
            LogicalPlan::Scan(_) => vec![],
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_order_by(),
            LogicalPlan::GraphRel(graph_rel) => {
                let mut left_order_by_items = graph_rel.left.extract_order_by();
                let mut center_order_by_items = graph_rel.center.extract_order_by();
                let mut right_order_by_items = graph_rel.right.extract_order_by();
                left_order_by_items.append(&mut center_order_by_items);
                left_order_by_items.append(&mut right_order_by_items);
        
                left_order_by_items
            },
            LogicalPlan::Filter(filter) => filter.input.extract_order_by(),
            LogicalPlan::Projection(projection) => projection.input.extract_order_by(),
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_order_by(),
            LogicalPlan::OrderBy(order_by) => order_by.items.iter().cloned().map(Into::into).collect(),
            LogicalPlan::Skip(skip) => skip.input.extract_order_by(),
            LogicalPlan::Limit(limit) => limit.input.extract_order_by(),
            LogicalPlan::Cte(cte) => cte.input.extract_order_by(),
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_order_by(),
        }
    }

    fn extract_limit(&self) -> Option<i64> {
        match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(_) => None,
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_limit(),
            LogicalPlan::GraphRel(graph_rel) => {
                let left_limit_opt = graph_rel.left.extract_limit();
                let center_limit_opt = graph_rel.center.extract_limit();
                let right_limit_opt = graph_rel.right.extract_limit();

                if left_limit_opt.is_some() {
                    left_limit_opt
                } else if center_limit_opt.is_some() {
                    center_limit_opt
                } else if right_limit_opt.is_some() {
                    right_limit_opt
                } else {
                    None
                }
            },
            LogicalPlan::Filter(filter) => filter.input.extract_limit(),
            LogicalPlan::Projection(projection) => projection.input.extract_limit(),
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_limit(),
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_limit(),
            LogicalPlan::Skip(skip) => skip.input.extract_limit(),
            LogicalPlan::Limit(limit) => Some(limit.count),
            LogicalPlan::Cte(cte) => cte.input.extract_limit(),
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_limit(),
        }
    }

    fn extract_skip(&self) -> Option<i64> {
        match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(_) => None,
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_skip(),
            LogicalPlan::GraphRel(graph_rel) => {
                let left_skip_opt = graph_rel.left.extract_skip();
                let center_skip_opt = graph_rel.center.extract_skip();
                let right_skip_opt = graph_rel.right.extract_skip();

                if left_skip_opt.is_some() {
                    left_skip_opt
                } else if center_skip_opt.is_some() {
                    center_skip_opt
                } else if right_skip_opt.is_some() {
                    right_skip_opt
                } else {
                    None
                }
            },
            LogicalPlan::Filter(filter) => filter.input.extract_skip(),
            LogicalPlan::Projection(projection) => projection.input.extract_skip(),
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_skip(),
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_skip(),
            LogicalPlan::Skip(skip) => Some(skip.count),
            LogicalPlan::Limit(limit) => limit.input.extract_skip(),
            LogicalPlan::Cte(cte) => cte.input.extract_skip(),
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_skip(),
        }
    }

    

    fn to_render_plan(&self) -> RenderPlanBuilderResult<RenderPlan> {

        let mut extracted_ctes: Vec<Cte> = vec![];
        let final_from: FromTable;
        let mut last_node_filters_opt: Option<RenderExpr> = None;
        let final_filters: Option<RenderExpr>;

        // TODO remove unwrap with error
        let last_node_cte = self.extract_last_node_cte();

        if let Some(last_node_cte) = self.extract_last_node_cte() {

            let last_node_alias = last_node_cte.cte_name.split('_').nth(1).ok_or(RenderBuildError::MalformedCTEName)?;

            extracted_ctes = self.extract_ctes(last_node_alias)?;
            final_from = last_node_cte.from;

            last_node_filters_opt = clean_last_node_filters(last_node_cte.filters.0.unwrap());

            let final_filters_opt = self.extract_final_filters();

            let final_combined_filters = if last_node_filters_opt.is_some() && final_filters_opt.is_some() {
                Some(RenderExpr::OperatorApplicationExp(OperatorApplication { 
                    operator: Operator::And, 
                    operands: vec![final_filters_opt.unwrap(), last_node_filters_opt.unwrap()] 
                }))
            } else if final_filters_opt.is_some() {
                final_filters_opt
            } else if last_node_filters_opt.is_some() {
                last_node_filters_opt
            } else {
                None
            };

            final_filters = final_combined_filters;

        } else {
            final_from = self.extract_from().ok_or(RenderBuildError::MissingFromTable)?;
            final_filters = self.extract_filters();
        }

        
        let final_select_items = self.extract_select_items();

        if final_select_items.is_empty() {
            return  Err(RenderBuildError::MissingSelectItems);
        }

        let mut extracted_joins = self.extract_joins();
        extracted_joins.sort_by_key(|join| join.joining_on.len());


        let extracted_group_by_exprs = self.extract_group_by();

        let extracted_order_by = self.extract_order_by();

        let extracted_limit_item = self.extract_limit();

        let extracted_skip_item = self.extract_skip();
 
        Ok(RenderPlan {
            ctes: CteItems(extracted_ctes),
            select: SelectItems(final_select_items),
            from: final_from,
            joins: JoinItems(extracted_joins),
            filters: FilterItems(final_filters),
            group_by: GroupByExpressions(extracted_group_by_exprs),
            order_by: OrderByItems(extracted_order_by),
            limit: LimitItem(extracted_limit_item),
            skip: SkipItem(extracted_skip_item),
        })


    }
    
}



fn clean_last_node_filters(filter_expr: RenderExpr) -> Option<RenderExpr> {
    match filter_expr {
        // remove InSubqeuery as we have added it in graph_traversal_planning phase. Since this is for last node, we are going to select that node directly
        // we do not need this InSubquery
        RenderExpr::InSubquery(_sq) => None,
        RenderExpr::OperatorApplicationExp(op) => {
            let mut stripped = Vec::new();
            for operand in op.operands {
                if let Some(e) = clean_last_node_filters(operand) {
                    stripped.push(e);
                }
            }
            match stripped.len() {
                0 => None,
                1 => Some(stripped.into_iter().next().unwrap()),
                _ => Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: op.operator,
                    operands: stripped,
                })),
            }
        }
        RenderExpr::List(list) => {
            let mut stripped = Vec::new();
            for inner in list {
                if let Some(e) = clean_last_node_filters(inner) {
                    stripped.push(e);
                }
            }
            match stripped.len() {
                0 => None,
                1 => Some(stripped.into_iter().next().unwrap()),
                _ => Some(RenderExpr::List(stripped)),
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            let mut stripped_args = Vec::new();
            for arg in agg.args {
                if let Some(e) = clean_last_node_filters(arg) {
                    stripped_args.push(e);
                }
            }
            if stripped_args.is_empty() {
                None
            } else {
                Some(RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: agg.name,
                    args: stripped_args,
                }))
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            let mut stripped_args = Vec::new();
            for arg in func.args {
                if let Some(e) = clean_last_node_filters(arg) {
                    stripped_args.push(e);
                }
            }
            if stripped_args.is_empty() {
                None
            } else {
                Some(RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: func.name,
                    args: stripped_args,
                }))
            }
        }
        other => Some(other),
        // RenderExpr::PropertyAccessExp(pa) => Some(RenderExpr::PropertyAccessExp(pa)),
        // RenderExpr::Literal(l) => Some(RenderExpr::Literal(l)),
        // RenderExpr::Variable(v) => Some(RenderExpr::Variable(v)),
        // RenderExpr::Star => Some(RenderExpr::Star),
        // RenderExpr::TableAlias(ta) => Some(RenderExpr::TableAlias(ta)),
        // RenderExpr::ColumnAlias(ca) => Some(RenderExpr::ColumnAlias(ca)),
        // RenderExpr::Column(c) => Some(RenderExpr::Column(c)),
        // RenderExpr::Parameter(p) => Some(RenderExpr::Parameter(p)),
    }
}