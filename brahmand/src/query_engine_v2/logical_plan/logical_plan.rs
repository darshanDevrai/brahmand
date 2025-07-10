
use std::{collections::HashMap, sync::Arc, fmt};

use crate::query_engine_v2::{expr::plan_expr::{ColumnAlias, Direction, PlanExpr, Property, TableAlias}, transformed::Transformed};

#[derive(Debug, PartialEq, Clone)]
pub enum LogicalPlan {
    Empty,
    /// Scans nodes of a given label (node type). Used as a leaf for MATCH patterns.
    Scan (Scan),

    /// Traverses relationships from an input node set.
    ConnectedTraversal (ConnectedTraversal),

    /// Filters rows based on a predicate.
    Filter (Filter),

    /// Projection (returns) a set of expressions/columns.
    Projection (Projection),

    /// Orders rows by expressions.
    OrderBy (OrderBy),

    /// Skips a number of rows (Cypher SKIP).
    Skip (Skip),

    /// Limits the result set (Cypher LIMIT).
    Limit (Limit),

    // /// (Optional) Supports WITH or subquery blocks
    // With (With),

    // /// (Optional) Union of two plans (for Cypher UNION).
    // Union (Union),

    // /// (Optional) Subquery block
    // Subquery (Subquery),
}

#[derive(Debug, PartialEq, Clone)]
pub struct TableCtx {
    pub label: Option<String>,
    pub properties: Vec<Property>,
    pub extracted_filters: Option<PlanExpr>,
    pub return_items: Vec<ReturnItem>
}

#[derive(Debug, PartialEq, Clone,)]
pub struct PlanCtx {
    pub alias_table_ctx_map: HashMap<String, TableCtx>,
}


impl PlanCtx {
    pub fn default() -> Self {
        PlanCtx {
            alias_table_ctx_map: HashMap::new()
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Scan {
    pub table_alias: String,
    pub table_name: Option<String>,
    // pub properties: Option<Vec<Property>>,
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
    pub items: Vec<ReturnItem>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReturnItem {
    pub expression: PlanExpr,
    pub col_alias: Option<ColumnAlias>,
    pub belongs_to_table: Option<TableAlias>,
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
    pub order: OrerByOrder,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrerByOrder {
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
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::Filter(Filter {
                    input: new_input.clone(),
                    predicate: self.predicate.clone(),
                    ..self.clone()
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(old_input) => {
                Transformed::No(old_input.clone())
            }
        }
    }
}

impl Scan {
    pub fn rebuild_or_clone(&self, _input_tf: Transformed<Arc<LogicalPlan>>) -> Transformed<Arc<LogicalPlan>> {
        // Scan has no input, so just return No with the original scan
        Transformed::No(Arc::new(LogicalPlan::Scan(self.clone())))
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
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::Projection(Projection {
                    input: new_input.clone(),
                    items: self.items.clone(),
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(old_input) => {
                Transformed::No(old_input.clone())
            }
        }
    }
}

impl OrderBy {
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::OrderBy(OrderBy {
                    input: new_input.clone(),
                    items: self.items.clone(),
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(old_input) => {
                Transformed::No(old_input.clone())
            }
        }
    }
}

impl Skip {
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::Skip(Skip {
                    input: new_input.clone(),
                    count: self.count,
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(old_input) => {
                Transformed::No(old_input.clone())
            }
        }
    }
}

impl Limit {
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::Limit(Limit {
                    input: new_input.clone(),
                    count: self.count,
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(old_input) => {
                Transformed::No(old_input.clone())
            }
        }
    }
}

impl<'a> From<crate::open_cypher_parser::ast::ReturnItem<'a>> for ReturnItem {
    fn from(value: crate::open_cypher_parser::ast::ReturnItem<'a>) -> Self {
        ReturnItem {
            expression: value.expression.into(),
            col_alias: value.alias.map(|alias| ColumnAlias(alias.to_string())),
            belongs_to_table: None, // This will be set during planning phase
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
                crate::open_cypher_parser::ast::OrerByOrder::Asc => OrerByOrder::Asc,
                crate::open_cypher_parser::ast::OrerByOrder::Desc => OrerByOrder::Desc,
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
            writeln!(f, "{}", self.variant_name_with_scan())?;
        } else {
            writeln!(f, "{}{}{}", prefix, branch, self.variant_name_with_scan())?;
        }

        let mut children: Vec<&LogicalPlan> = vec![];
        match self {
            LogicalPlan::ConnectedTraversal(ct) => {
                children.push(&ct.start_node);
                children.push(&ct.relationship);
                children.push(&ct.end_node);
            }
            LogicalPlan::Filter(filter) => {
                children.push(&filter.input);
            }
            LogicalPlan::Projection(proj) => {
                children.push(&proj.input);
            }
            LogicalPlan::OrderBy(order_by) => {
                children.push(&order_by.input);
            }
            LogicalPlan::Skip(skip) => {
                children.push(&skip.input);
            }
            LogicalPlan::Limit(limit) => {
                children.push(&limit.input);
            }
            _ => {}
        }

        let n = children.len();
        for (i, child) in children.into_iter().enumerate() {
            child.fmt_with_tree(f, &format!("{}{}", prefix, next_prefix), i + 1 == n, false)?;
        }
        Ok(())
    }

    fn variant_name_with_scan(&self) -> String {
        match self {
            LogicalPlan::Scan(scan) => format!("Scan({})", scan.table_alias),
            LogicalPlan::ConnectedTraversal(ct) => format!("ConnectedTraversal({:?})", ct.rel_direction),
            LogicalPlan::Empty => "Empty".to_string(),
            LogicalPlan::Filter(_) => "Filter".to_string(),
            LogicalPlan::Projection(_) => "Projection".to_string(),
            LogicalPlan::OrderBy(_) => "OrderBy".to_string(),
            LogicalPlan::Skip(_) => "Skip".to_string(),
            LogicalPlan::Limit(_) => "Limit".to_string(),
        }
    }
}


