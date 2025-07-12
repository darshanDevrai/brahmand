
use std::{collections::HashMap, sync::Arc, fmt};

use crate::query_engine_v2::{expr::plan_expr::{ColumnAlias, Direction, PlanExpr, Property, TableAlias}, transformed::Transformed};

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
    pub filter_predicates: Vec<PlanExpr>,
    pub projection_items: Vec<ProjectionItem>
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
pub struct GraphNode {
    pub input: Arc<LogicalPlan>,
    pub self_plan: Arc<LogicalPlan>,
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
    pub is_rel_anchor: bool
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
pub struct ProjectionItem {
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
    pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>, self_tf: Transformed<Arc<LogicalPlan>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {

        let input_changed = input_tf.is_yes();
        let self_changed =   self_tf.is_yes();

        if input_changed | self_changed {
            let new_graph_node = LogicalPlan::GraphNode(GraphNode { 
                input: input_tf.get_plan(), 
                self_plan: self_tf.get_plan(), 
                alias: self.alias.clone(), 
                down_connection: self.down_connection.clone()
            });
            Transformed::Yes(Arc::new(new_graph_node))
        }else{
            Transformed::No(old_plan.clone())
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
                is_rel_anchor: self.is_rel_anchor
            });
            Transformed::Yes(Arc::new(new_graph_rel))
        }else{
            Transformed::No(old_plan.clone())
        }
    }
}

impl<'a> From<crate::open_cypher_parser::ast::ReturnItem<'a>> for ProjectionItem {
    fn from(value: crate::open_cypher_parser::ast::ReturnItem<'a>) -> Self {
        ProjectionItem {
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
                children.push(&graph_node.self_plan);
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

    fn variant_name(&self) -> String {
        match self {
            LogicalPlan::GraphNode(graph_node) => format!("Node({})", graph_node.alias),
            // LogicalPlan::GraphRel(_) => "GraphRel".to_string(),
            LogicalPlan::GraphRel(graph_rel) => format!("GraphRel({:?})(is_rel_anchor: {:?})", graph_rel.direction, graph_rel.is_rel_anchor),
            // LogicalPlan::Scan(_) => "Scan".to_string(),
            LogicalPlan::Scan(scan) => format!("scan({})", scan.table_alias),
            LogicalPlan::ConnectedTraversal(ct) => format!("ConnectedTraversal({:?})", ct.rel_direction),
            LogicalPlan::Empty => "".to_string(),
            LogicalPlan::Filter(_) => "Filter".to_string(),
            LogicalPlan::Projection(_) => "Projection".to_string(),
            LogicalPlan::OrderBy(_) => "OrderBy".to_string(),
            LogicalPlan::Skip(_) => "Skip".to_string(),
            LogicalPlan::Limit(_) => "Limit".to_string(),
        }
    }
}


impl fmt::Display for PlanCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\n---- PlanCtx Starts Here ----")?;
        for (alias, table_ctx) in &self.alias_table_ctx_map {
            writeln!(f, "\n [{}]:", alias)?;
            table_ctx.fmt_with_indent(f, 2)?;
        }
        writeln!(f, "\n---- PlanCtx Ends Here ----")?;
        Ok(())
    }
}

impl TableCtx {
    fn fmt_with_indent(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let pad = " ".repeat(indent);
        writeln!(f, "{}         label: {:?}", pad, self.label)?;
        writeln!(f, "{}         properties: {:?}", pad, self.properties)?;
        writeln!(f, "{}         filter_predicates: {:?}", pad, self.filter_predicates)?;
        writeln!(f, "{}         projection_items: {:?}", pad, self.projection_items)?;
        Ok(())
    }
}


