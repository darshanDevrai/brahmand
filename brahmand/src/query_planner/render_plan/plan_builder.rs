
use crate::query_planner::{logical_plan::{self, logical_plan::LogicalPlan}, render_plan::{errors::RenderBuildError, render_expr::{AggregateFnCall, ColumnAlias, Operator, OperatorApplication, RenderExpr, ScalarFnCall}, render_plan::{Cte, CteItems, FilterItems, FromTable, GroupByExpressions, Join, JoinItems, LimitItem, OrderByItem, OrderByItems, RenderPlan, SelectItem, SelectItems, SkipItem}}};





pub type RenderPlanBuilderResult<T> = Result<T, RenderBuildError>;

pub(crate) trait RenderPlanBuilder {

    fn extract_last_node_cte(&self) -> RenderPlanBuilderResult<Option<Cte>>;

    fn extract_final_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_ctes(&self, last_node_alias: &str) -> RenderPlanBuilderResult<Vec<Cte>>;

    fn extract_select_items(&self) -> RenderPlanBuilderResult<Vec<SelectItem>>;

    fn extract_from(&self) -> RenderPlanBuilderResult<FromTable>;

    fn extract_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_joins(&self) -> RenderPlanBuilderResult<Vec<Join>>;

    fn extract_group_by(&self) -> RenderPlanBuilderResult<Vec<RenderExpr>>;

    fn extract_order_by(&self) -> RenderPlanBuilderResult<Vec<OrderByItem>>;

    fn extract_limit(&self) -> Option<i64>;

    fn extract_skip(&self) -> Option<i64>;

    fn to_render_plan(&self) -> RenderPlanBuilderResult<RenderPlan>;

}


impl RenderPlanBuilder for LogicalPlan {

    fn extract_last_node_cte(&self) -> RenderPlanBuilderResult<Option<Cte>> {

        let last_node_cte = match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(_) => None,
            LogicalPlan::GraphNode(graph_node) => {
                graph_node.input.extract_last_node_cte()?
            },
            LogicalPlan::GraphRel(graph_rel) => {
                // Last node is at the top of the tree. 
                // process left node first. 
                let left_node_cte_opt = graph_rel.left.extract_last_node_cte()?;

                // If last node is still not found then check at the right tree
                if left_node_cte_opt.is_none() {

                    graph_rel.right.extract_last_node_cte()?
                } else {
                    left_node_cte_opt
                }
            },
            LogicalPlan::Filter(filter) => filter.input.extract_last_node_cte()?,
            LogicalPlan::Projection(projection) => projection.input.extract_last_node_cte()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_last_node_cte()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_last_node_cte()?,
            LogicalPlan::Skip(skip) => skip.input.extract_last_node_cte()?,
            LogicalPlan::Limit(limit) => limit.input.extract_last_node_cte()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_last_node_cte()?,
            LogicalPlan::Cte(logical_cte) => {
                let filters = logical_cte.input.extract_filters()?;
                let select_items = logical_cte.input.extract_select_items()?;
                let from_table = logical_cte.input.extract_from()?;
                let render_cte = Cte{ 
                    cte_name: logical_cte.name.clone(), 
                    select: SelectItems(select_items),
                    from: from_table, 
                    filters: FilterItems(filters)
                };
                Some(render_cte)
            }
        };
        Ok(last_node_cte)
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
                let mut select_items = logical_cte.input.extract_select_items()?;

                for select_item in select_items.iter_mut() {
                    if let RenderExpr::PropertyAccessExp(pro_acc) = &select_item.expression {
                        *select_item = SelectItem {
                            expression: RenderExpr::Column(pro_acc.column.clone()),
                            col_alias: None,
                        };
                    }
                }

                let mut from_table = logical_cte.input.extract_from()?;
                from_table.table_alias = None;
                let filters = logical_cte.input.extract_filters()?;
                Ok(vec![Cte{ 
                    cte_name: logical_cte.name.clone(), 
                    select: SelectItems(select_items),
                    from: from_table, 
                    filters: FilterItems(filters) 
                }])
            }
        }
    }

    fn extract_select_items(&self) -> RenderPlanBuilderResult<Vec<SelectItem>> {
        let select_items = match &self {
            LogicalPlan::Empty => Err(RenderBuildError::MissingSelectItems)?,
            LogicalPlan::Scan(_) => Err(RenderBuildError::MissingSelectItems)?,
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_select_items()?,
            LogicalPlan::GraphRel(_) => Err(RenderBuildError::MissingSelectItems)?,
            LogicalPlan::Filter(filter) => filter.input.extract_select_items()?,
            LogicalPlan::Projection(projection) => {
                let items = projection.items.iter().map(|item| {
                    let expr = item.expression.clone().try_into()?;
                    let alias = item.col_alias.clone().map(ColumnAlias::try_from).transpose()?;
                    Ok( SelectItem { expression: expr, col_alias: alias } )
                });
            
                let vec = items.collect::<Result<Vec<SelectItem>, RenderBuildError>>()?;
                vec
            },
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_select_items()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_select_items()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_select_items()?,
            LogicalPlan::Skip(skip) => skip.input.extract_select_items()?,
            LogicalPlan::Limit(limit) => limit.input.extract_select_items()?,
            LogicalPlan::Cte(cte) => cte.input.extract_select_items()?,
        };

        Ok(select_items)
    }

    fn extract_from(&self) -> RenderPlanBuilderResult<FromTable> {
        let from_table = match &self {
            LogicalPlan::Empty => Err(RenderBuildError::MissingFromTable)?,
            LogicalPlan::Scan(scan) => {
                        FromTable {
                            table_name: scan.table_name.clone().ok_or(RenderBuildError::MissingFromTable)?,
                            table_alias: Some(scan.table_alias.clone()),
                        }
                    },
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_from()?,
            LogicalPlan::GraphRel(_) => Err(RenderBuildError::MissingFromTable)?,
            LogicalPlan::Filter(filter) => filter.input.extract_from()?,
            LogicalPlan::Projection(projection) => projection.input.extract_from()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_from()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_from()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_from()?,
            LogicalPlan::Skip(skip) => skip.input.extract_from()?,
            LogicalPlan::Limit(limit) => limit.input.extract_from()?,
            LogicalPlan::Cte(cte) => cte.input.extract_from()?,
            
        };
        Ok(from_table)
    }

    fn extract_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        let filters = match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(_) => None,
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_filters()?,
            LogicalPlan::GraphRel(_) => None,
            LogicalPlan::Filter(filter) => Some(filter.predicate.clone().try_into()?),
            LogicalPlan::Projection(projection) => projection.input.extract_filters()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_filters()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_filters()?,
            LogicalPlan::Skip(skip) => skip.input.extract_filters()?,
            LogicalPlan::Limit(limit) => limit.input.extract_filters()?,
            LogicalPlan::Cte(cte) => cte.input.extract_filters()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_filters()?,
        };
        Ok(filters)
    }

    fn extract_final_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        let final_filters = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_final_filters()?,
            LogicalPlan::Skip(skip) => skip.input.extract_final_filters()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_final_filters()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_final_filters()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_final_filters()?,
            LogicalPlan::Projection(projection) => projection.input.extract_final_filters()?,
            LogicalPlan::Filter(filter) => Some(filter.predicate.clone().try_into()?),
            _ => None            
        };
        Ok(final_filters)
    } 

    fn extract_joins(&self) -> RenderPlanBuilderResult<Vec<Join>> {
        let joins = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_joins()?,
            LogicalPlan::Skip(skip) => skip.input.extract_joins()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_joins()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_joins()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.joins.iter().cloned().map(Join::try_from).collect::<Result<Vec<Join>, RenderBuildError>>()?,
            _ => vec![],
        };
        Ok(joins)
    }

    fn extract_group_by(&self) -> RenderPlanBuilderResult<Vec<RenderExpr>> {
        let group_by = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_group_by()?,
            LogicalPlan::Skip(skip) => skip.input.extract_group_by()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_group_by()?,
            LogicalPlan::GroupBy(group_by) => group_by.expressions.iter().cloned().map(RenderExpr::try_from).collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?,//.collect::<Vec<RenderExpr>>(),
            _ => vec![],
        };
        Ok(group_by)
    }

    fn extract_order_by(&self) -> RenderPlanBuilderResult<Vec<OrderByItem>> {
        let order_by = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_order_by()?,
            LogicalPlan::Skip(skip) => skip.input.extract_order_by()?,
            LogicalPlan::OrderBy(order_by) => order_by.items.iter().cloned().map(OrderByItem::try_from).collect::<Result<Vec<OrderByItem>, RenderBuildError>>()?,
            _ => vec![],
        };
        Ok(order_by)
    }

    fn extract_skip(&self) -> Option<i64> {
        match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_skip(),
            LogicalPlan::Skip(skip) => Some(skip.count),
            _ => None
        }
    }

    fn extract_limit(&self) -> Option<i64> {
        match &self {
            LogicalPlan::Limit(limit) => Some(limit.count),
            _ => None
        }
    }



    

    fn to_render_plan(&self) -> RenderPlanBuilderResult<RenderPlan> {

        let mut extracted_ctes: Vec<Cte> = vec![];
        let final_from: FromTable;
        let final_filters: Option<RenderExpr>;

        if let Some(last_node_cte) = self.extract_last_node_cte()? {

            let last_node_alias = last_node_cte.cte_name.split('_').nth(1).ok_or(RenderBuildError::MalformedCTEName)?;

            extracted_ctes = self.extract_ctes(last_node_alias)?;
            final_from = last_node_cte.from;

            let last_node_filters_opt = clean_last_node_filters(last_node_cte.filters.0);

            let final_filters_opt = self.extract_final_filters()?;

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
            final_from = self.extract_from()?;
            final_filters = self.extract_filters()?;
        }

        
        let final_select_items = self.extract_select_items()?;


        let mut extracted_joins = self.extract_joins()?;
        extracted_joins.sort_by_key(|join| join.joining_on.len());


        let extracted_group_by_exprs = self.extract_group_by()?;

        let extracted_order_by = self.extract_order_by()?;

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
            skip: SkipItem(extracted_skip_item),
            limit: LimitItem(extracted_limit_item),
        })


    }
    
}



fn clean_last_node_filters(filter_opt: Option<RenderExpr>) -> Option<RenderExpr> {
    if let Some(filter_expr) = filter_opt {
        match filter_expr {
            // remove InSubqeuery as we have added it in graph_traversal_planning phase. Since this is for last node, we are going to select that node directly
            // we do not need this InSubquery
            RenderExpr::InSubquery(_sq) => None,
            RenderExpr::OperatorApplicationExp(op) => {
                let mut stripped = Vec::new();
                for operand in op.operands {
                    if let Some(e) = clean_last_node_filters(Some(operand)) {
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
                    if let Some(e) = clean_last_node_filters(Some(inner)) {
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
                    if let Some(e) = clean_last_node_filters(Some(arg)) {
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
                    if let Some(e) = clean_last_node_filters(Some(arg)) {
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
    } else {
        None
    }
    
}