
use std::{sync::Arc, fmt};

use crate::query_planner::{logical_expr::logical_expr::{ColumnAlias, Direction, Literal, LogicalExpr, Operator, OperatorApplication, Property, TableAlias}, transformed::Transformed};

#[derive(Debug, PartialEq, Clone)]
pub enum LogicalPlan {
    Empty,
    
    Scan (Scan),

    GraphNode(GraphNode),

    GraphRel(GraphRel),

    Filter (Filter),

    Projection (Projection),

    GroupBy (GroupBy),

    OrderBy (OrderBy),

    Skip (Skip),

    Limit (Limit),

    Cte(Cte),

    GraphJoins(GraphJoins),

    Union(Union)

}



#[derive(Debug, PartialEq, Clone)]
pub struct Scan {
    pub table_alias: Option<String>,
    pub table_name: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct GraphNode {
    pub input: Arc<LogicalPlan>,
    pub alias: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct GraphRel {
    pub left: Arc<LogicalPlan>,
    pub center: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub alias: String,
    pub direction: Direction,
    pub left_connection: String,
    pub right_connection: String,
    pub is_rel_anchor: bool
}

#[derive(Debug, PartialEq, Clone)]
pub struct Cte {
    pub input: Arc<LogicalPlan>,
    pub name: String
}

#[derive(Debug, PartialEq, Clone)]
pub struct Union {
    pub inputs: Vec<Arc<LogicalPlan>>,
    pub union_type: UnionType
}

#[derive(Debug, PartialEq, Clone)]
pub enum UnionType {
    Distinct,
    All,
}

#[derive(Debug, PartialEq, Clone)]
pub struct GraphJoins {
    pub input: Arc<LogicalPlan>,
    pub joins: Vec<Join>,
}

#[derive(Debug, PartialEq, Clone,)]
pub struct Join {
    pub table_name: String,
    pub table_alias: String,
    pub joining_on: Vec<OperatorApplication>,
    pub join_type: JoinType
}

#[derive(Debug, PartialEq, Clone)]
pub enum JoinType {
    Join, 
    Inner,
    Left,
    Right
}


#[derive(Debug, PartialEq, Clone)]
pub struct ConnectedTraversal {
    pub start_node: Arc<LogicalPlan>,
    pub relationship: Arc<LogicalPlan>,
    pub end_node: Arc<LogicalPlan>,
    pub rel_alias: String,
    pub rel_direction: Direction,
    pub nested_node_alias: Option<String>
}

#[derive(Debug, PartialEq, Clone)]
pub struct Filter {
    pub input: Arc<LogicalPlan>,
    pub predicate: LogicalExpr,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Projection {
    pub input: Arc<LogicalPlan>,
    pub items: Vec<ProjectionItem>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct GroupBy {
    pub input: Arc<LogicalPlan>,
    pub expressions: Vec<LogicalExpr>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ProjectionItem {
    pub expression: LogicalExpr,
    pub col_alias: Option<ColumnAlias>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderBy {
    pub input: Arc<LogicalPlan>,
    pub items: Vec<OrderByItem>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Skip {
    pub input: Arc<LogicalPlan>,
    pub count: i64,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Limit {
    pub input: Arc<LogicalPlan>,
    pub count: i64,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByItem {
    pub expression: LogicalExpr,
    pub order: OrderByOrder,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderByOrder {
    Asc,
    Desc,
}

impl Filter {
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::Filter(Filter {
                    input: new_input.clone(),
                    predicate: self.predicate.clone(),
                    ..self.clone()
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(_) => {
                Transformed::No(old_plan.clone())
            }
        }
    }
}

impl Projection {
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::Projection(Projection {
                    input: new_input.clone(),
                    items: self.items.clone(),
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(_) => {
                Transformed::No(old_plan.clone())
            }
        }
    }
}

impl GroupBy {
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::GroupBy(GroupBy {
                    input: new_input.clone(),
                    expressions: self.expressions.clone(),
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(_) => {
                Transformed::No(old_plan.clone())
            }
        }
    }
}

impl OrderBy {
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::OrderBy(OrderBy {
                    input: new_input.clone(),
                    items: self.items.clone(),
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(_) => {
                Transformed::No(old_plan.clone())
            }
        }
    }
}

impl Skip {
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::Skip(Skip {
                    input: new_input.clone(),
                    count: self.count,
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(_) => {
                Transformed::No(old_plan.clone())
            }
        }
    }
}

impl Limit {
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::Limit(Limit {
                    input: new_input.clone(),
                    count: self.count,
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(_) => {
                Transformed::No(old_plan.clone())
            }
        }
    }
}

impl GraphNode {
    // pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>, self_tf: Transformed<Arc<LogicalPlan>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_graph_node = LogicalPlan::GraphNode(GraphNode { 
                    input: new_input.clone(), 
                    // self_plan: self_tf.get_plan(), 
                    alias: self.alias.clone(), 
                });
                Transformed::Yes(Arc::new(new_graph_node))
            }
            Transformed::No(_) => {
                Transformed::No(old_plan.clone())
            }
        }
    }
}

impl GraphRel {
    pub fn rebuild_or_clone(&self, left_tf: Transformed<Arc<LogicalPlan>>, center_tf: Transformed<Arc<LogicalPlan>>, right_tf: Transformed<Arc<LogicalPlan>>,  old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {
        let left_changed = left_tf.is_yes();
        let right_changed = right_tf.is_yes();
        let center_changed =  center_tf.is_yes();

        if left_changed | right_changed | center_changed {
            let new_graph_rel = LogicalPlan::GraphRel(GraphRel { 
                left: left_tf.get_plan(), 
                center: center_tf.get_plan(), 
                right: right_tf.get_plan(),
                alias: self.alias.clone(), 
                left_connection: self.left_connection.clone(), 
                right_connection: self.right_connection.clone(),
                direction: self.direction.clone(),
                // is_anchor_graph_rel: self.is_anchor_graph_rel,
                is_rel_anchor: self.is_rel_anchor
            });
            Transformed::Yes(Arc::new(new_graph_rel))
        }else{
            Transformed::No(old_plan.clone())
        }
    }
}


impl Cte {
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                // if new input is empty then remove the CTE 
                if matches!(new_input.as_ref(), LogicalPlan::Empty) {
                    Transformed::Yes(new_input.clone())
                }else{
                    let new_node = LogicalPlan::Cte(Cte {
                        input: new_input.clone(),
                        name: self.name.clone(),
                    });
                    Transformed::Yes(Arc::new(new_node))
                }
            }
            Transformed::No(_) => {
                Transformed::No(old_plan.clone())
            }
        }
    }
}

impl GraphJoins {
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_graph_joins = LogicalPlan::GraphJoins(GraphJoins { 
                    input: new_input.clone(), 
                    joins: self.joins.clone()
                });
                Transformed::Yes(Arc::new(new_graph_joins))
            }
            Transformed::No(_) => {
                Transformed::No(old_plan.clone())
            }
        }
    }
}

impl Union {

    pub fn rebuild_or_clone(&self, inputs_tf: Vec<Transformed<Arc<LogicalPlan>>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {

        // iterate over inputs_tf vec and check if any one of them is transformed. 
        // If yes then break the iteration and club all inputs irrespective of transformation status. 
        // If no then return the old plan. 
        let mut is_transformed = false;
        for input_tf in &inputs_tf {
            if input_tf.is_yes() {
                is_transformed = true;
                break;
            }
        }
        if is_transformed {
            let new_inputs: Vec<Arc<LogicalPlan>> = inputs_tf.into_iter().map(|tf| tf.get_plan()).collect();
            let new_union = LogicalPlan::Union(Union { 
                inputs: new_inputs,
                union_type: self.union_type.clone() 
            });
            Transformed::Yes(Arc::new(new_union))
        } else {    
            Transformed::No(old_plan.clone())
        }
    }
}

impl<'a> From<crate::open_cypher_parser::ast::ReturnItem<'a>> for ProjectionItem {
    fn from(value: crate::open_cypher_parser::ast::ReturnItem<'a>) -> Self {
        ProjectionItem {
            expression: value.expression.into(),
            col_alias: value.alias.map(|alias| ColumnAlias(alias.to_string())),
            // belongs_to_table: None, // This will be set during planning phase
        }
    }
}

impl<'a> From<crate::open_cypher_parser::ast::OrderByItem<'a>> for OrderByItem {
    fn from(value: crate::open_cypher_parser::ast::OrderByItem<'a>) -> Self {
        OrderByItem {
            expression: if let crate::open_cypher_parser::ast::Expression::Variable(var) = value.expression {
                LogicalExpr::ColumnAlias(ColumnAlias(var.to_string()))
            } else{
                value.expression.into()
            },
            order: match value.order {
                crate::open_cypher_parser::ast::OrerByOrder::Asc => OrderByOrder::Asc,
                crate::open_cypher_parser::ast::OrerByOrder::Desc => OrderByOrder::Desc,
            },
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_tree(f, "", true, true)
    }
}

impl LogicalPlan {
    pub fn get_empty_match_plan() -> Self {
        LogicalPlan::Projection(Projection {
            input: Arc::new(LogicalPlan::Filter(Filter {
                input: Arc::new(LogicalPlan::Empty),
                predicate: LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        LogicalExpr::Literal(Literal::Integer(1)),
                        LogicalExpr::Literal(Literal::Integer(0)),
                    ]
                })
            })),
            items: vec![ProjectionItem {
                expression: LogicalExpr::Literal(Literal::Integer(1)),
                col_alias: None
            }]
        })
    }
}

impl LogicalPlan {
    fn fmt_with_tree(&self, f: &mut fmt::Formatter<'_>, prefix: &str, is_last: bool, is_root: bool) -> fmt::Result {
        let (branch, next_prefix) = if is_last {
            ("└── ", "    ")
        } else {
            ("├── ", "│   ")
        };

        if is_root {
            writeln!(f, "\n{}", self.variant_name())?;
        } else {
            writeln!(f, "{}{}{}", prefix, branch, self.variant_name())?;
        }

        let mut children: Vec<&LogicalPlan> = vec![];
        match self {
            LogicalPlan::GraphNode(graph_node) => {
                children.push(&graph_node.input);
                // children.push(&graph_node.self_plan);
            }
            LogicalPlan::GraphRel(graph_rel) =>  {
                children.push(&graph_rel.left);
                children.push(&graph_rel.center);
                children.push(&graph_rel.right);
            },
            LogicalPlan::Filter(filter) => {
                        children.push(&filter.input);
                    }
            LogicalPlan::Projection(proj) => {
                        children.push(&proj.input);
                    }
            LogicalPlan::GraphJoins(graph_join) => {
                    children.push(&graph_join.input);
                }
            LogicalPlan::OrderBy(order_by) => {
                        children.push(&order_by.input);
                    }
            LogicalPlan::Skip(skip) => {
                        children.push(&skip.input);
                    }
            LogicalPlan::Limit(limit) => {
                        children.push(&limit.input);
                    },
            LogicalPlan::GroupBy(group_by) => {
                        children.push(&group_by.input);
                    }
            LogicalPlan::Cte(cte) => {
                children.push(&cte.input);
            }
            LogicalPlan::Union(union) => {
                for input in &union.inputs {
                    children.push(input);
                }
            }
            _ => {}
        }

        let n = children.len();
        for (i, child) in children.into_iter().enumerate() {
            child.fmt_with_tree(f, &format!("{}{}", prefix, next_prefix), i + 1 == n, false)?;
        }
        Ok(())
    }

    fn variant_name(&self) -> String {
        match self {
            LogicalPlan::GraphNode(graph_node) => format!("Node({})", graph_node.alias),
            LogicalPlan::GraphRel(graph_rel) => format!("GraphRel({:?})(is_rel_anchor: {:?})", graph_rel.direction, graph_rel.is_rel_anchor),
            LogicalPlan::Scan(scan) => format!("scan({:?})", scan.table_alias),
            LogicalPlan::Empty => "".to_string(),
            LogicalPlan::Filter(_) => "Filter".to_string(),
            LogicalPlan::Projection(_) => "Projection".to_string(),
            LogicalPlan::OrderBy(_) => "OrderBy".to_string(),
            LogicalPlan::Skip(_) => "Skip".to_string(),
            LogicalPlan::Limit(_) => "Limit".to_string(),
            LogicalPlan::GroupBy(_) => "GroupBy".to_string(),
            LogicalPlan::Cte(cte) => format!("Cte({})", cte.name),
            LogicalPlan::GraphJoins(_) => "GraphJoins".to_string(),
            LogicalPlan::Union(_) => "Union".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::logical_expr::{
        Column, Literal, LogicalExpr, Operator, OperatorApplication, PropertyAccess, TableAlias
    };
    use crate::open_cypher_parser::ast;

    #[test]
    fn test_filter_rebuild_or_clone_with_transformation() {
        let original_input = Arc::new(LogicalPlan::Empty);
        let new_input = Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some("employees".to_string()),
            table_name: Some("employee_table".to_string()),
        }));
        
        let filter = Filter {
            input: original_input.clone(),
            predicate: LogicalExpr::Literal(Literal::Boolean(true)),
        };
        
        let old_plan = Arc::new(LogicalPlan::Filter(filter.clone()));
        let input_transformed = Transformed::Yes(new_input.clone());
        
        let result = filter.rebuild_or_clone(input_transformed, old_plan.clone());
        
        match result {
            Transformed::Yes(new_plan) => {
                match new_plan.as_ref() {
                    LogicalPlan::Filter(new_filter) => {
                        assert_eq!(new_filter.input, new_input);
                        assert_eq!(new_filter.predicate, LogicalExpr::Literal(Literal::Boolean(true)));
                    },
                    _ => panic!("Expected Filter plan"),
                }
            },
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_filter_rebuild_or_clone_without_transformation() {
        let input = Arc::new(LogicalPlan::Empty);
        let filter = Filter {
            input: input.clone(),
            predicate: LogicalExpr::Literal(Literal::Boolean(true)),
        };
        
        let old_plan = Arc::new(LogicalPlan::Filter(filter.clone()));
        let input_not_transformed = Transformed::No(input.clone());
        
        let result = filter.rebuild_or_clone(input_not_transformed, old_plan.clone());
        
        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, old_plan);
            },
            _ => panic!("Expected no transformation"),
        }
    }

    #[test]
    fn test_projection_rebuild_or_clone_with_transformation() {
        let original_input = Arc::new(LogicalPlan::Empty);
        let new_input = Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some("customers".to_string()),
            table_name: Some("customer_table".to_string()),
        }));
        
        let projection_items = vec![
            ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("customer".to_string()),
                    column: Column("name".to_string()),
                }),
                col_alias: None,
            },
        ];
        
        let projection = Projection {
            input: original_input.clone(),
            items: projection_items.clone(),
        };
        
        let old_plan = Arc::new(LogicalPlan::Projection(projection.clone()));
        let input_transformed = Transformed::Yes(new_input.clone());
        
        let result = projection.rebuild_or_clone(input_transformed, old_plan.clone());
        
        match result {
            Transformed::Yes(new_plan) => {
                match new_plan.as_ref() {
                    LogicalPlan::Projection(new_projection) => {
                        assert_eq!(new_projection.input, new_input);
                        assert_eq!(new_projection.items.len(), 1);
                    },
                    _ => panic!("Expected Projection plan"),
                }
            },
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_graph_node_rebuild_or_clone() {
        let original_input = Arc::new(LogicalPlan::Empty);
        let new_input = Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some("users".to_string()),
            table_name: Some("user_table".to_string()),
        }));
        
        let graph_node = GraphNode {
            input: original_input.clone(),
            alias: "person".to_string(),
        };
        
        let old_plan = Arc::new(LogicalPlan::GraphNode(graph_node.clone()));
        let input_transformed = Transformed::Yes(new_input.clone());
        
        let result = graph_node.rebuild_or_clone(input_transformed, old_plan.clone());
        
        match result {
            Transformed::Yes(new_plan) => {
                match new_plan.as_ref() {
                    LogicalPlan::GraphNode(new_graph_node) => {
                        assert_eq!(new_graph_node.input, new_input);
                        assert_eq!(new_graph_node.alias, "person");
                    },
                    _ => panic!("Expected GraphNode plan"),
                }
            },
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_graph_rel_rebuild_or_clone() {
        let left_plan = Arc::new(LogicalPlan::Empty);
        let center_plan = Arc::new(LogicalPlan::Empty);
        let right_plan = Arc::new(LogicalPlan::Empty);
        let new_left_plan = Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some("users".to_string()),
            table_name: Some("user_table".to_string()),
        }));
        
        let graph_rel = GraphRel {
            left: left_plan.clone(),
            center: center_plan.clone(),
            right: right_plan.clone(),
            alias: "works_for".to_string(),
            direction: Direction::Outgoing,
            left_connection: "employee_id".to_string(),
            right_connection: "company_id".to_string(),
            is_rel_anchor: false,
        };
        
        let old_plan = Arc::new(LogicalPlan::GraphRel(graph_rel.clone()));
        let left_transformed = Transformed::Yes(new_left_plan.clone());
        let center_not_transformed = Transformed::No(center_plan.clone());
        let right_not_transformed = Transformed::No(right_plan.clone());
        
        let result = graph_rel.rebuild_or_clone(
            left_transformed,
            center_not_transformed,
            right_not_transformed,
            old_plan.clone()
        );
        
        match result {
            Transformed::Yes(new_plan) => {
                match new_plan.as_ref() {
                    LogicalPlan::GraphRel(new_graph_rel) => {
                        assert_eq!(new_graph_rel.left, new_left_plan);
                        assert_eq!(new_graph_rel.center, center_plan);
                        assert_eq!(new_graph_rel.right, right_plan);
                        assert_eq!(new_graph_rel.alias, "works_for");
                    },
                    _ => panic!("Expected GraphRel plan"),
                }
            },
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_cte_rebuild_or_clone_with_empty_input() {
        let original_input = Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some("temp".to_string()),
            table_name: Some("temp_table".to_string()),
        }));
        let empty_input = Arc::new(LogicalPlan::Empty);
        
        let cte = Cte {
            input: original_input.clone(),
            name: "temp_results".to_string(),
        };
        
        let old_plan = Arc::new(LogicalPlan::Cte(cte.clone()));
        let input_transformed = Transformed::Yes(empty_input.clone());
        
        let result = cte.rebuild_or_clone(input_transformed, old_plan.clone());
        
        match result {
            Transformed::Yes(new_plan) => {
                // When input is empty, CTE should be removed and return the empty plan
                assert_eq!(new_plan, empty_input);
            },
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_projection_item_from_ast() {
        let ast_return_item = ast::ReturnItem {
            expression: ast::Expression::Variable("customer_name"),
            alias: Some("full_name"),
        };
        
        let projection_item = ProjectionItem::from(ast_return_item);
        
        match projection_item.expression {
            LogicalExpr::TableAlias(alias) => assert_eq!(alias.0, "customer_name"),
            _ => panic!("Expected TableAlias"),
        }
        assert_eq!(projection_item.col_alias, Some(ColumnAlias("full_name".to_string())));
    }

    #[test]
    fn test_order_by_item_from_ast() {
        let ast_order_item = ast::OrderByItem {
            expression: ast::Expression::Variable("price"),
            order: ast::OrerByOrder::Desc,
        };
        
        let order_by_item = OrderByItem::from(ast_order_item);
        
        match order_by_item.expression {
            LogicalExpr::ColumnAlias(alias) => assert_eq!(alias.0, "price"),
            _ => panic!("Expected ColumnAlias"),
        }
        assert_eq!(order_by_item.order, OrderByOrder::Desc);
    }

    #[test]
    fn test_logical_plan_variant_names() {
        let scan = LogicalPlan::Scan(Scan {
            table_alias: Some("products".to_string()),
            table_name: Some("product_catalog".to_string()),
        });
        assert_eq!(scan.variant_name(), "scan(products)");
        
        let graph_node = LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::Empty),
            alias: "customer".to_string(),
        });
        assert_eq!(graph_node.variant_name(), "Node(customer)");
        
        let graph_rel = LogicalPlan::GraphRel(GraphRel {
            left: Arc::new(LogicalPlan::Empty),
            center: Arc::new(LogicalPlan::Empty),
            right: Arc::new(LogicalPlan::Empty),
            alias: "purchased".to_string(),
            direction: Direction::Outgoing,
            left_connection: "customer_id".to_string(),
            right_connection: "order_id".to_string(),
            is_rel_anchor: true,
        });
        assert_eq!(graph_rel.variant_name(), "GraphRel(Outgoing)(is_rel_anchor: true)");
        
        let filter = LogicalPlan::Filter(Filter {
            input: Arc::new(LogicalPlan::Empty),
            predicate: LogicalExpr::Literal(Literal::Boolean(true)),
        });
        assert_eq!(filter.variant_name(), "Filter");
        
        let projection = LogicalPlan::Projection(Projection {
            input: Arc::new(LogicalPlan::Empty),
            items: vec![],
        });
        assert_eq!(projection.variant_name(), "Projection");
        
        let empty = LogicalPlan::Empty;
        assert_eq!(empty.variant_name(), "");
        
        let cte = LogicalPlan::Cte(Cte {
            input: Arc::new(LogicalPlan::Empty),
            name: "active_users".to_string(),
        });
        assert_eq!(cte.variant_name(), "Cte(active_users)");
    }

    #[test]
    fn test_complex_logical_plan_structure() {
        // Create a complex plan: Projection -> Filter -> GraphNode -> Scan
        let scan = LogicalPlan::Scan(Scan {
            table_alias: Some("users".to_string()),
            table_name: Some("user_accounts".to_string()),
        });
        
        let graph_node = LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(scan),
            alias: "user".to_string(),
        });
        
        let filter = LogicalPlan::Filter(Filter {
            input: Arc::new(graph_node),
            predicate: LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::GreaterThan,
                operands: vec![
                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias("user".to_string()),
                        column: Column("age".to_string()),
                    }),
                    LogicalExpr::Literal(Literal::Integer(18)),
                ],
            }),
        });
        
        let projection = LogicalPlan::Projection(Projection {
            input: Arc::new(filter),
            items: vec![
                ProjectionItem {
                    expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias("user".to_string()),
                        column: Column("email".to_string()),
                    }),
                    col_alias: Some(ColumnAlias("email_address".to_string())),
                },
                ProjectionItem {
                    expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias("user".to_string()),
                        column: Column("first_name".to_string()),
                    }),
                    col_alias: None,
                },
            ],
        });
        
        // Verify the structure
        match projection {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.items.len(), 2);
                match proj.input.as_ref() {
                    LogicalPlan::Filter(filter_node) => {
                        match filter_node.input.as_ref() {
                            LogicalPlan::GraphNode(graph_node) => {
                                assert_eq!(graph_node.alias, "user");
                                match graph_node.input.as_ref() {
                                    LogicalPlan::Scan(scan_node) => {
                                        assert_eq!(scan_node.table_alias, Some("users".to_string()));
                                        assert_eq!(scan_node.table_name, Some("user_accounts".to_string()));
                                    },
                                    _ => panic!("Expected Scan at bottom"),
                                }
                            },
                            _ => panic!("Expected GraphNode"),
                        }
                    },
                    _ => panic!("Expected Filter"),
                }
            },
            _ => panic!("Expected Projection at top"),
        }
    }


}



