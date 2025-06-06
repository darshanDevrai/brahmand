#![allow(dead_code)]

use std::{cell::RefCell, fmt, rc::Rc};

#[derive(Debug, PartialEq, Clone)]
pub struct OpenCypherQueryAst<'a> {
    pub match_clause: Option<MatchClause<'a>>,
    pub with_clause: Option<WithClause<'a>>,
    pub where_clause: Option<WhereClause<'a>>,
    pub create_clause: Option<CreateClause<'a>>,
    pub create_node_table_clause: Option<CreateNodeTableClause<'a>>,
    pub create_rel_table_clause: Option<CreateRelTableClause<'a>>,
    pub set_clause: Option<SetClause<'a>>,
    pub remove_clause: Option<RemoveClause<'a>>,
    pub delete_clause: Option<DeleteClause<'a>>,
    pub return_clause: Option<ReturnClause<'a>>,
    pub order_by_clause: Option<OrderByClause<'a>>,
    pub skip_clause: Option<SkipClause>,
    pub limit_clause: Option<LimitClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct MatchClause<'a> {
    pub path_patterns: Vec<PathPattern<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateClause<'a> {
    pub path_patterns: Vec<PathPattern<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateNodeTableClause<'a> {
    pub table_name: &'a str,
    pub table_schema: Vec<ColumnSchema<'a>>,
    pub table_properties: Vec<Expression<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnSchema<'a> {
    pub column_name: &'a str,
    pub column_dtype: &'a str,
    pub default_value: Option<Expression<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateRelTableClause<'a> {
    pub table_name: &'a str,
    pub from: &'a str,
    pub to: &'a str,
    pub table_schema: Vec<ColumnSchema<'a>>,
    pub table_properties: Vec<Expression<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SetClause<'a> {
    pub set_items: Vec<OperatorApplication<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct RemoveClause<'a> {
    pub remove_items: Vec<PropertyAccess<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DeleteClause<'a> {
    pub is_detach: bool,
    pub delete_items: Vec<Expression<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WhereClause<'a> {
    pub conditions: Expression<'a>, //OperatorApplication<'a>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReturnClause<'a> {
    pub return_items: Vec<ReturnItem<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReturnItem<'a> {
    pub expression: Expression<'a>,
    pub alias: Option<&'a str>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WithClause<'a> {
    pub with_items: Vec<WithItem<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WithItem<'a> {
    pub expression: Expression<'a>,
    pub alias: Option<&'a str>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByClause<'a> {
    pub order_by_items: Vec<OrderByItem<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByItem<'a> {
    pub expression: Expression<'a>,
    pub order: OrerByOrder,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrerByOrder {
    Asc,
    Desc,
}

impl From<OrerByOrder> for String {
    fn from(value: OrerByOrder) -> String {
        match value {
            OrerByOrder::Asc => "ASC".to_string(),
            OrerByOrder::Desc => "DESC".to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SkipClause {
    pub skip_item: i64,
}

#[derive(Debug, PartialEq, Clone)]
pub struct LimitClause {
    pub limit_item: i64,
}

#[derive(Debug, PartialEq, Clone)]
pub enum PathPattern<'a> {
    Node(NodePattern<'a>),                       //  Standalone nodes `(a)`
    ConnectedPattern(Vec<ConnectedPattern<'a>>), // Nodes with relationships `(a)-[:REL]->(b)`
}

#[derive(Debug, PartialEq, Clone)]
pub struct NodePattern<'a> {
    pub name: Option<&'a str>,                 // `a` in `(a:Person)`
    pub label: Option<&'a str>,                // `Person` in `(a:Person)`
    pub properties: Option<Vec<Property<'a>>>, // `{name: "Charlie Sheen"}`
}

#[derive(Debug, PartialEq, Clone)]
pub enum Property<'a> {
    PropertyKV(PropertyKVPair<'a>),
    Param(&'a str),
}

#[derive(Debug, PartialEq, Clone)]
pub struct PropertyKVPair<'a> {
    pub key: &'a str,
    pub value: Expression<'a>,
}

// #[derive(Debug, PartialEq, Clone)]
// pub struct ConnectedPattern<'a> {
//     pub start_node: &'a NodePattern<'a>,           // `(a)`
//     pub relationship: RelationshipPattern<'a>, // `-[:REL]->`
//     pub end_node: &'a NodePattern<'a>,             // `(b)`
// }

#[derive(Debug, PartialEq, Clone)]
pub struct ConnectedPattern<'a> {
    pub start_node: Rc<RefCell<NodePattern<'a>>>,
    pub relationship: RelationshipPattern<'a>,
    pub end_node: Rc<RefCell<NodePattern<'a>>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct RelationshipPattern<'a> {
    pub name: Option<&'a str>,
    pub direction: Direction,
    pub label: Option<&'a str>,
    pub properties: Option<Vec<Property<'a>>>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Direction {
    Incoming, // `<-`
    Outgoing, // `->`
    Either,   // `-`
}

impl From<Direction> for String {
    fn from(value: Direction) -> Self {
        match value {
            Direction::Incoming => "incoming".to_string(),
            Direction::Outgoing | Direction::Either => "outgoing".to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Literal<'a> {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(&'a str),
    Null,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Operator {
    // binary
    Addition,         // +
    Subtraction,      // -
    Multiplication,   // +
    Division,         // /
    ModuloDivision,   // %
    Exponentiation,   // ^
    Equal,            // =
    NotEqual,         // <>
    LessThan,         // <
    GreaterThan,      // >
    LessThanEqual,    // <=
    GreaterThanEqual, // >=
    And,
    Or,
    In, // IN [...]
    NotIn,
    // unary
    Not,
    Distinct, // e.g distinct name
    // post fix
    IsNull,    // e.g. city IS NULL
    IsNotNull, // e.g. city IS NOT NULL
}

impl From<Operator> for String {
    fn from(value: Operator) -> Self {
        match value {
            Operator::Addition => "+".to_string(),
            Operator::Subtraction => "-".to_string(),
            Operator::Multiplication => "*".to_string(),
            Operator::Division => "/".to_string(),
            Operator::ModuloDivision => "%".to_string(),
            Operator::Exponentiation => "^".to_string(),
            Operator::Equal => "=".to_string(),
            Operator::NotEqual => "!=".to_string(),
            Operator::LessThan => "<".to_string(),
            Operator::GreaterThan => ">".to_string(),
            Operator::LessThanEqual => "<=".to_string(),
            Operator::GreaterThanEqual => ">=".to_string(),
            Operator::And => "AND".to_string(),
            Operator::Or => "OR".to_string(),
            Operator::In => "IN".to_string(),
            Operator::NotIn => "NOT IN".to_string(),
            Operator::Not => "NOT".to_string(),
            Operator::Distinct => "DISTINCT".to_string(),
            Operator::IsNull => "IS NULL".to_string(),
            Operator::IsNotNull => "IS NOT NULL".to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct OperatorApplication<'a> {
    pub operator: Operator,
    pub operands: Vec<Expression<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct PropertyAccess<'a> {
    pub base: &'a str,
    pub key: &'a str,
}

#[derive(Debug, PartialEq, Clone)]
pub struct FunctionCall<'a> {
    // pub name: &'a str,
    pub name: String,
    pub args: Vec<Expression<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expression<'a> {
    /// A literal, such as a number, string, boolean, or null.
    Literal(Literal<'a>),

    /// A variable (e.g. n, x, or even backtick-quoted names).
    Variable(&'a str),

    /// A parameter, such as `$param` or `$0`.
    Parameter(&'a str),

    // A list literal: a vector of expressions.
    List(Vec<Expression<'a>>),

    // A function call, e.g. length(p) or nodes(p).
    FunctionCallExp(FunctionCall<'a>),

    // Property access. In Cypher you have both static and dynamic property accesses.
    // This variant uses a boxed base expression and a boxed key expression.
    PropertyAccessExp(PropertyAccess<'a>),

    // An operator application, e.g. 1 + 2 or 3 < 4.
    // The operator itself could be another enum.
    OperatorApplicationExp(OperatorApplication<'a>),

    // A path-pattern, for instance: (a)-[]->()<-[]-(b)
    PathPattern(PathPattern<'a>),
    // /// A CASE expression.
    // /// `expr` is used for the simple CASE (e.g. CASE x WHEN ...), and if absent, it's the searched CASE.
    // Case {
    //     expr: Option<Box<Expression>>,
    //     when_then: Vec<(Expression, Expression)>,
    //     else_expr: Option<Box<Expression>>,
    // },
}

impl fmt::Display for Expression<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for OpenCypherQueryAst<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "OpenCypherQueryAst")?;
        if let Some(ref m) = self.match_clause {
            writeln!(f, "├── MatchClause: {:#?}", m)?;
        }
        if let Some(ref w) = self.with_clause {
            writeln!(f, "├── WithClause: {:#?}", w)?;
        }
        if let Some(ref w) = self.where_clause {
            writeln!(f, "├── WhereClause: {:#?}", w)?;
        }
        if let Some(ref c) = self.create_clause {
            writeln!(f, "├── CreateClause: {:#?}", c)?;
        }
        if let Some(ref s) = self.set_clause {
            writeln!(f, "├── SetClause: {:#?}", s)?;
        }
        if let Some(ref r) = self.remove_clause {
            writeln!(f, "├── RemoveClause: {:#?}", r)?;
        }
        if let Some(ref d) = self.delete_clause {
            writeln!(f, "├── DeleteClause: {:#?}", d)?;
        }
        if let Some(ref r) = self.return_clause {
            writeln!(f, "├── ReturnClause: {:#?}", r)?;
        }
        if let Some(ref o) = self.order_by_clause {
            writeln!(f, "├── OrderByClause: {:#?}", o)?;
        }
        if let Some(ref s) = self.skip_clause {
            writeln!(f, "├── SkipClause: {:#?}", s)?;
        }
        if let Some(ref l) = self.limit_clause {
            writeln!(f, "└── LimitClause: {:#?}", l)?;
        }
        Ok(())
    }
}
