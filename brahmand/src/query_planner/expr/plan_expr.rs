use crate::{open_cypher_parser::{self, ast::Expression}, query_planner::logical_plan::logical_plan::LogicalPlan};
use std::{fmt, sync::Arc};




#[derive(Debug, PartialEq, Clone)]
pub enum PlanExpr {
    /// A literal, such as a number, string, boolean, or null.
    Literal(Literal),

    Star,

    /// Table Alias (e.g. (p)-[f:Follow]-(u), p, f and u are table alias expr).
    TableAlias(TableAlias),

    ColumnAlias(ColumnAlias),

    /// Columns to use in projection.
    Column(Column),

    /// A parameter, such as `$param` or `$0`.
    Parameter(String),

    /// A list literal: a vector of expressions.
    List(Vec<PlanExpr>),

    AggregateFnCall(AggregateFnCall),

    /// A function call, e.g. length(p) or nodes(p).
    ScalarFnCall(ScalarFnCall),

    /// Property access.
    PropertyAccessExp(PropertyAccess),

    /// An operator application, e.g. 1 + 2 or 3 < 4.
    OperatorApplicationExp(OperatorApplication),

    /// A path-pattern, for instance: (a)-[]->()<-[]-(b)
    PathPattern(PathPattern),

    InSubquery(InSubquery) 
}

#[derive(Debug, PartialEq, Clone)]
pub struct InSubquery {
    pub expr:   Box<PlanExpr>,
    pub subplan: Arc<LogicalPlan>,
}


#[derive(Debug, PartialEq, Clone)]
pub enum Direction {
    Outgoing,
    Incoming,
    Either,
}

impl Direction {
    pub fn reverse(self) -> Self {
        if self == Direction::Incoming {
            Direction::Outgoing
        } else if self == Direction::Outgoing {
            Direction::Incoming
        } else {
            self
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(String),
    Null,
}

#[derive(Debug, PartialEq, Clone)]
pub struct TableAlias(pub String);

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnAlias(pub String);

#[derive(Debug, PartialEq, Clone)]
pub struct Column(pub String);


#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Operator {
    Addition,
    Subtraction,
    Multiplication,
    Division,
    ModuloDivision,
    Exponentiation,
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanEqual,
    GreaterThanEqual,
    And,
    Or,
    In,
    NotIn,
    Not,
    Distinct,
    IsNull,
    IsNotNull,
}



#[derive(Debug, PartialEq, Clone)]
pub struct OperatorApplication {
    pub operator: Operator,
    pub operands: Vec<PlanExpr>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct PropertyAccess {
    pub table_alias: TableAlias,
    pub column: Column,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ScalarFnCall {
    pub name: String,
    pub args: Vec<PlanExpr>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct AggregateFnCall {
    pub name: String,
    pub args: Vec<PlanExpr>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum PathPattern {
    Node(NodePattern),
    ConnectedPattern(Vec<ConnectedPattern>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct NodePattern {
    pub name: Option<String>,
    pub label: Option<String>,
    pub properties: Option<Vec<Property>>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Property {
    PropertyKV(PropertyKVPair),
    Param(String),
}

#[derive(Debug, PartialEq, Clone)]
pub struct PropertyKVPair {
    pub key: String,
    pub value: Literal,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ConnectedPattern {
    pub start_node: std::rc::Rc<std::cell::RefCell<NodePattern>>,
    pub relationship: RelationshipPattern,
    pub end_node: std::rc::Rc<std::cell::RefCell<NodePattern>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct RelationshipPattern {
    pub name: Option<String>,
    pub direction: Direction,
    pub label: Option<String>,
    pub properties: Option<Vec<Property>>,
}


impl<'a> From<open_cypher_parser::ast::Literal<'a>> for Literal {
    fn from(value: open_cypher_parser::ast::Literal) -> Self {
        match value {
            open_cypher_parser::ast::Literal::Integer(val) => Literal::Integer(val),
            open_cypher_parser::ast::Literal::Float(val) => Literal::Float(val),
            open_cypher_parser::ast::Literal::Boolean(val) => Literal::Boolean(val),
            open_cypher_parser::ast::Literal::String(val) => Literal::String(val.to_string()),
            open_cypher_parser::ast::Literal::Null => Literal::Null,
        }
    }
}


impl From<open_cypher_parser::ast::Operator> for Operator {
    fn from(value: open_cypher_parser::ast::Operator) -> Self {
        match value {
            open_cypher_parser::ast::Operator::Addition => Operator::Addition,
            open_cypher_parser::ast::Operator::Subtraction => Operator::Subtraction,
            open_cypher_parser::ast::Operator::Multiplication => Operator::Multiplication,
            open_cypher_parser::ast::Operator::Division => Operator::Division,
            open_cypher_parser::ast::Operator::ModuloDivision => Operator::ModuloDivision,
            open_cypher_parser::ast::Operator::Exponentiation => Operator::Exponentiation,
            open_cypher_parser::ast::Operator::Equal => Operator::Equal,
            open_cypher_parser::ast::Operator::NotEqual => Operator::NotEqual,
            open_cypher_parser::ast::Operator::LessThan => Operator::LessThan,
            open_cypher_parser::ast::Operator::GreaterThan => Operator::GreaterThan,
            open_cypher_parser::ast::Operator::LessThanEqual => Operator::LessThanEqual,
            open_cypher_parser::ast::Operator::GreaterThanEqual => Operator::GreaterThanEqual,
            open_cypher_parser::ast::Operator::And => Operator::And,
            open_cypher_parser::ast::Operator::Or => Operator::Or,
            open_cypher_parser::ast::Operator::In => Operator::In,
            open_cypher_parser::ast::Operator::NotIn => Operator::NotIn,
            open_cypher_parser::ast::Operator::Not => Operator::Not,
            open_cypher_parser::ast::Operator::Distinct => Operator::Distinct,
            open_cypher_parser::ast::Operator::IsNull => Operator::IsNull,
            open_cypher_parser::ast::Operator::IsNotNull => Operator::IsNotNull,
        }
    }
}


impl<'a> From<open_cypher_parser::ast::PropertyAccess<'a>> for PropertyAccess {
    fn from(value: open_cypher_parser::ast::PropertyAccess<'a>) -> Self {
        PropertyAccess {
            table_alias: TableAlias(value.base.to_string()),
            column: Column(value.key.to_string())
        }
    }
}

impl<'a> From<open_cypher_parser::ast::Direction> for Direction {
    fn from(value: open_cypher_parser::ast::Direction) -> Self {
        match value {
            open_cypher_parser::ast::Direction::Outgoing => Direction::Outgoing,
            open_cypher_parser::ast::Direction::Incoming => Direction::Incoming,
            open_cypher_parser::ast::Direction::Either => Direction::Either,
        }
    }
}

impl<'a> From<open_cypher_parser::ast::OperatorApplication<'a>> for OperatorApplication {
    fn from(value: open_cypher_parser::ast::OperatorApplication<'a>) -> Self {
        OperatorApplication {
            operator: Operator::from(value.operator),
            operands: value.operands.into_iter().map(|expr| PlanExpr::from(expr)).collect(),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::FunctionCall<'a>> for PlanExpr {
    fn from(value: open_cypher_parser::ast::FunctionCall<'a>) -> Self {
        let agg_fns = ["count", "min", "max", "avg", "sum"];
        let name_lower = value.name.to_lowercase();
        if agg_fns.contains(&name_lower.as_str()) {
            PlanExpr::AggregateFnCall(AggregateFnCall {
                name: value.name,
                args: value.args.into_iter().map(PlanExpr::from).collect(),
            })
        } else {
            PlanExpr::ScalarFnCall(ScalarFnCall {
                name: value.name,
                args: value.args.into_iter().map(PlanExpr::from).collect(),
            })
        }
    }
}

impl<'a> From<open_cypher_parser::ast::PathPattern<'a>> for PathPattern {
    fn from(value: open_cypher_parser::ast::PathPattern<'a>) -> Self {
        match value {
            open_cypher_parser::ast::PathPattern::Node(node) => PathPattern::Node(NodePattern::from(node)),
            open_cypher_parser::ast::PathPattern::ConnectedPattern(vec_conn) => PathPattern::ConnectedPattern(vec_conn.into_iter().map(ConnectedPattern::from).collect()),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::NodePattern<'a>> for NodePattern {
    fn from(value: open_cypher_parser::ast::NodePattern<'a>) -> Self {
        NodePattern {
            name: value.name.map(|s| s.to_string()),
            label: value.label.map(|s| s.to_string()),
            properties: value.properties.map(|props| props.into_iter().map(Property::from).collect()),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::Property<'a>> for Property {
    fn from(value: open_cypher_parser::ast::Property<'a>) -> Self {
        match value {
            open_cypher_parser::ast::Property::PropertyKV(kv) => Property::PropertyKV(PropertyKVPair::from(kv)),
            open_cypher_parser::ast::Property::Param(s) => Property::Param(s.to_string()),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::PropertyKVPair<'a>> for PropertyKVPair {
    fn from(value: open_cypher_parser::ast::PropertyKVPair<'a>) -> Self {
        PropertyKVPair {
            key: value.key.to_string(),
            value: match value.value {
                open_cypher_parser::ast::Expression::Literal(lit) => Literal::from(lit),
                _ => panic!("Property value must be a literal"),
            },
        }
    }
}

impl<'a> From<open_cypher_parser::ast::ConnectedPattern<'a>> for ConnectedPattern {
    fn from(value: open_cypher_parser::ast::ConnectedPattern<'a>) -> Self {
        ConnectedPattern {
            start_node: std::rc::Rc::new(std::cell::RefCell::new(NodePattern::from(value.start_node.borrow().clone()))),
            relationship: RelationshipPattern::from(value.relationship),
            end_node: std::rc::Rc::new(std::cell::RefCell::new(NodePattern::from(value.end_node.borrow().clone()))),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::RelationshipPattern<'a>> for RelationshipPattern {
    fn from(value: open_cypher_parser::ast::RelationshipPattern<'a>) -> Self {
        RelationshipPattern {
            name: value.name.map(|s| s.to_string()),
            direction: Direction::from(value.direction),
            label: value.label.map(|s| s.to_string()),
            properties: value.properties.map(|props| props.into_iter().map(Property::from).collect()),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::Expression<'a>> for PlanExpr {
    fn from(expr: open_cypher_parser::ast::Expression<'a>) -> Self {
        use open_cypher_parser::ast::Expression;
        match expr {
            Expression::Literal(lit) => PlanExpr::Literal(Literal::from(lit)),
            Expression::Variable(s) => {
                if s == "*" {
                    PlanExpr::Star
                }else{
                    // TODO revisit this 
                    // PlanExpr::Variable(s.to_string())
                    PlanExpr::TableAlias(TableAlias(s.to_string()))
                }
            },
            Expression::Parameter(s) => PlanExpr::Parameter(s.to_string()),
            Expression::List(exprs) => PlanExpr::List(exprs.into_iter().map(PlanExpr::from).collect()),
            Expression::FunctionCallExp(fc) => PlanExpr::from(fc),
            Expression::PropertyAccessExp(pa) => PlanExpr::PropertyAccessExp(PropertyAccess::from(pa)),
            Expression::OperatorApplicationExp(oa) => PlanExpr::OperatorApplicationExp(OperatorApplication::from(oa)),
            Expression::PathPattern(pp) => PlanExpr::PathPattern(PathPattern::from(pp)),
        }
    }
}

impl fmt::Display for TableAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for ColumnAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Integer(i) => write!(f, "{}", i),
            Literal::Float(fl) => write!(f, "{}", fl),
            Literal::Boolean(b) => write!(f, "{}", b),
            Literal::String(s) => write!(f, "{}", s),
            Literal::Null => write!(f, "null"),
        }
    }
}