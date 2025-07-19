
use std::{collections::HashMap, sync::Arc, fmt};

use crate::query_engine_v2::{expr::plan_expr::{ColumnAlias, Direction, OperatorApplication, PlanExpr, Property, TableAlias}, transformed::Transformed};

#[derive(Debug, PartialEq, Clone)]
pub enum LogicalPlan {
    Empty,
    /// Scans nodes of a given label (node type). Used as a leaf for MATCH patterns.
    Scan (Scan),

    GraphNode(GraphNode),

    GraphRel(GraphRel),

    /// Traverses relationships from an input node set.
    ConnectedTraversal (ConnectedTraversal),

    /// Filters rows based on a predicate.
    Filter (Filter),

    /// Projection (returns) a set of expressions/columns.
    Projection (Projection),

    /// Groupby a set of expressions/columns.
    GroupBy (GroupBy),

    /// Orders rows by expressions.
    OrderBy (OrderBy),

    /// Skips a number of rows (Cypher SKIP).
    Skip (Skip),

    /// Limits the result set (Cypher LIMIT).
    Limit (Limit),

    Cte(Cte),

    GraphJoins(GraphJoins)

    // /// (Optional) Supports WITH or subquery blocks
    // With (With),

    // /// (Optional) Union of two plans (for Cypher UNION).
    // Union (Union),

    // /// (Optional) Subquery block
    // Subquery (Subquery),
}



#[derive(Debug, PartialEq, Clone)]
pub struct Scan {
    pub table_alias: String,
    pub table_name: Option<String>,
    // pub properties: Option<Vec<Property>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct GraphNode {
    pub input: Arc<LogicalPlan>,
    // pub self_plan: Arc<LogicalPlan>,
    pub alias: String,
    pub down_connection: Option<String>
}

#[derive(Debug, PartialEq, Clone)]
pub struct GraphRel {
    pub left: Arc<LogicalPlan>,
    pub center: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub alias: String,
    pub direction: Direction,
    pub left_connection: Option<String>,
    pub right_connection: Option<String>,
    // pub is_anchor_graph_rel: bool,
    pub is_rel_anchor: bool
}

#[derive(Debug, PartialEq, Clone)]
pub struct Cte {
    pub input: Arc<LogicalPlan>,
    pub name: String
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
    pub joining_on: Vec<OperatorApplication>
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
    pub predicate: PlanExpr,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Projection {
    pub input: Arc<LogicalPlan>,
    pub items: Vec<ProjectionItem>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct GroupBy {
    pub input: Arc<LogicalPlan>,
    pub expressions: Vec<PlanExpr>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ProjectionItem {
    pub expression: PlanExpr,
    pub col_alias: Option<ColumnAlias>,
    // pub belongs_to_table: Option<TableAlias>,
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
    pub expression: PlanExpr,
    pub order: OrderByOrder,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderByOrder {
    Asc,
    Desc,
}

// #[derive(Debug, PartialEq, Clone)]
// pub struct With {
//     pub input: Arc<LogicalPlan>,
//     pub items: Vec<ReturnItem>,
// }

// #[derive(Debug, PartialEq, Clone)]
// pub struct Union {
//     pub left: Arc<LogicalPlan>,
//     pub right: Arc<LogicalPlan>,
//     pub all: bool,
// }

// #[derive(Debug, PartialEq, Clone)]
// pub struct Subquery {
//     pub input: Arc<LogicalPlan>,
//     pub items: Vec<ReturnItem>,
// }

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


impl ConnectedTraversal {
    pub fn rebuild_or_clone(
        &self,
        start_tf: Transformed<Arc<LogicalPlan>>,
        rel_tf: Transformed<Arc<LogicalPlan>>,
        end_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>
    ) -> Transformed<Arc<LogicalPlan>> {

        let start_changed = start_tf.is_yes();
        let rel_changed =   rel_tf.is_yes();
        let end_changed = end_tf.is_yes();

        if start_changed | rel_changed | end_changed {
            let new_node = LogicalPlan::ConnectedTraversal(ConnectedTraversal {
                start_node: start_tf.get_plan(),
                relationship: rel_tf.get_plan(),
                end_node: end_tf.get_plan(),
                rel_alias: self.rel_alias.clone(),
                rel_direction: self.rel_direction.clone(),
                nested_node_alias: self.nested_node_alias.clone(),
            });
            Transformed::Yes(Arc::new(new_node))
        }else{
            Transformed::No(old_plan.clone())
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
                    down_connection: self.down_connection.clone()
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
                PlanExpr::ColumnAlias(ColumnAlias(var.to_string()))
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
            LogicalPlan::ConnectedTraversal(ct) => {
                        children.push(&ct.start_node);
                        children.push(&ct.relationship);
                        children.push(&ct.end_node);
                    }
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
            LogicalPlan::Scan(scan) => format!("scan({})", scan.table_alias),
            LogicalPlan::ConnectedTraversal(ct) => format!("ConnectedTraversal({:?})", ct.rel_direction),
            LogicalPlan::Empty => "".to_string(),
            LogicalPlan::Filter(_) => "Filter".to_string(),
            LogicalPlan::Projection(_) => "Projection".to_string(),
            LogicalPlan::OrderBy(_) => "OrderBy".to_string(),
            LogicalPlan::Skip(_) => "Skip".to_string(),
            LogicalPlan::Limit(_) => "Limit".to_string(),
            LogicalPlan::GroupBy(_) => "GroupBy".to_string(),
            LogicalPlan::Cte(cte) => format!("Cte({})", cte.name),
            LogicalPlan::GraphJoins(graph_joins) => "GraphJoins".to_string(),
        }
    }

    pub fn print_graph_rels(&self) {
        match self {
            LogicalPlan::GraphRel(graph_rel) => {
                        println!(
                            "GraphRel(alias: {}, left_connection: {:?}, right_connection: {:?})",
                            graph_rel.alias,
                            graph_rel.left_connection,
                            graph_rel.right_connection
                        );
                        // Recursively print children
                        graph_rel.left.print_graph_rels();
                        graph_rel.center.print_graph_rels();
                        graph_rel.right.print_graph_rels();
                    }
            LogicalPlan::GraphNode(graph_node) => {
                        graph_node.input.print_graph_rels();
                        // graph_node.self_plan.print_graph_rels();
                    }
            LogicalPlan::ConnectedTraversal(ct) => {
                        ct.start_node.print_graph_rels();
                        ct.relationship.print_graph_rels();
                        ct.end_node.print_graph_rels();
                    }
            LogicalPlan::Filter(filter) => filter.input.print_graph_rels(),
            LogicalPlan::Projection(proj) => proj.input.print_graph_rels(),
            LogicalPlan::OrderBy(order_by) => order_by.input.print_graph_rels(),
            LogicalPlan::Skip(skip) => skip.input.print_graph_rels(),
            LogicalPlan::Limit(limit) => limit.input.print_graph_rels(),
            LogicalPlan::Empty => { /* do nothing */ }
            LogicalPlan::Scan(_) => { /* do nothing */ }
            LogicalPlan::GroupBy(group_by) => group_by.input.print_graph_rels(),
            LogicalPlan::Cte(cte) => cte.input.print_graph_rels(),
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.print_graph_rels(),
        }
    }
}



