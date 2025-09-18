use crate::{
    open_cypher_parser::{self},
    query_planner::logical_plan::logical_plan::LogicalPlan,
};
use std::{fmt, sync::Arc};

#[derive(Debug, PartialEq, Clone)]
pub enum LogicalExpr {
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
    List(Vec<LogicalExpr>),

    AggregateFnCall(AggregateFnCall),

    /// A function call, e.g. length(p) or nodes(p).
    ScalarFnCall(ScalarFnCall),

    /// Property access.
    PropertyAccessExp(PropertyAccess),

    /// An operator application, e.g. 1 + 2 or 3 < 4.
    OperatorApplicationExp(OperatorApplication),

    /// A path-pattern, for instance: (a)-[]->()<-[]-(b)
    PathPattern(PathPattern),

    InSubquery(InSubquery),
}

#[derive(Debug, PartialEq, Clone)]
pub struct InSubquery {
    pub expr: Box<LogicalExpr>,
    pub subplan: Arc<LogicalPlan>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Direction {
    Outgoing,
    Incoming,
    Either,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Direction::Incoming => f.write_str("incoming"),
            Direction::Outgoing => f.write_str("outgoing"),
            Direction::Either => f.write_str("either"),
        }
    }
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
    pub operands: Vec<LogicalExpr>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct PropertyAccess {
    pub table_alias: TableAlias,
    pub column: Column,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ScalarFnCall {
    pub name: String,
    pub args: Vec<LogicalExpr>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct AggregateFnCall {
    pub name: String,
    pub args: Vec<LogicalExpr>,
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
    pub start_node: Arc<NodePattern>,
    pub relationship: RelationshipPattern,
    pub end_node: Arc<NodePattern>,
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
            column: Column(value.key.to_string()),
        }
    }
}

impl From<open_cypher_parser::ast::Direction> for Direction {
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
            operands: value.operands.into_iter().map(LogicalExpr::from).collect(),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::FunctionCall<'a>> for LogicalExpr {
    fn from(value: open_cypher_parser::ast::FunctionCall<'a>) -> Self {
        let agg_fns = ["count", "min", "max", "avg", "sum"];
        let name_lower = value.name.to_lowercase();
        if agg_fns.contains(&name_lower.as_str()) {
            LogicalExpr::AggregateFnCall(AggregateFnCall {
                name: value.name,
                args: value.args.into_iter().map(LogicalExpr::from).collect(),
            })
        } else {
            LogicalExpr::ScalarFnCall(ScalarFnCall {
                name: value.name,
                args: value.args.into_iter().map(LogicalExpr::from).collect(),
            })
        }
    }
}

impl<'a> From<open_cypher_parser::ast::PathPattern<'a>> for PathPattern {
    fn from(value: open_cypher_parser::ast::PathPattern<'a>) -> Self {
        match value {
            open_cypher_parser::ast::PathPattern::Node(node) => {
                PathPattern::Node(NodePattern::from(node))
            }
            open_cypher_parser::ast::PathPattern::ConnectedPattern(vec_conn) => {
                PathPattern::ConnectedPattern(
                    vec_conn.into_iter().map(ConnectedPattern::from).collect(),
                )
            }
        }
    }
}

impl<'a> From<open_cypher_parser::ast::NodePattern<'a>> for NodePattern {
    fn from(value: open_cypher_parser::ast::NodePattern<'a>) -> Self {
        NodePattern {
            name: value.name.map(|s| s.to_string()),
            label: value.label.map(|s| s.to_string()),
            properties: value
                .properties
                .map(|props| props.into_iter().map(Property::from).collect()),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::Property<'a>> for Property {
    fn from(value: open_cypher_parser::ast::Property<'a>) -> Self {
        match value {
            open_cypher_parser::ast::Property::PropertyKV(kv) => {
                Property::PropertyKV(PropertyKVPair::from(kv))
            }
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
            start_node: Arc::new(NodePattern::from(
                value.start_node.borrow().clone(),
            )),
            relationship: RelationshipPattern::from(value.relationship),
            end_node: Arc::new(NodePattern::from(
                value.end_node.borrow().clone(),
            )),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::RelationshipPattern<'a>> for RelationshipPattern {
    fn from(value: open_cypher_parser::ast::RelationshipPattern<'a>) -> Self {
        RelationshipPattern {
            name: value.name.map(|s| s.to_string()),
            direction: Direction::from(value.direction),
            label: value.label.map(|s| s.to_string()),
            properties: value
                .properties
                .map(|props| props.into_iter().map(Property::from).collect()),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::Expression<'a>> for LogicalExpr {
    fn from(expr: open_cypher_parser::ast::Expression<'a>) -> Self {
        use open_cypher_parser::ast::Expression;
        match expr {
            Expression::Literal(lit) => LogicalExpr::Literal(Literal::from(lit)),
            Expression::Variable(s) => {
                if s == "*" {
                    LogicalExpr::Star
                } else {
                    // TODO revisit this
                    // LogicalExpr::Variable(s.to_string())
                    LogicalExpr::TableAlias(TableAlias(s.to_string()))
                }
            }
            Expression::Parameter(s) => LogicalExpr::Parameter(s.to_string()),
            Expression::List(exprs) => {
                LogicalExpr::List(exprs.into_iter().map(LogicalExpr::from).collect())
            }
            Expression::FunctionCallExp(fc) => LogicalExpr::from(fc),
            Expression::PropertyAccessExp(pa) => {
                LogicalExpr::PropertyAccessExp(PropertyAccess::from(pa))
            }
            Expression::OperatorApplicationExp(oa) => {
                LogicalExpr::OperatorApplicationExp(OperatorApplication::from(oa))
            }
            Expression::PathPattern(pp) => LogicalExpr::PathPattern(PathPattern::from(pp)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_cypher_parser::ast;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn test_literal_from_ast() {
        // Test integer conversion
        let ast_int = ast::Literal::Integer(42);
        let logical_int = Literal::from(ast_int);
        assert_eq!(logical_int, Literal::Integer(42));

        // Test float conversion
        let ast_float = ast::Literal::Float(3.14);
        let logical_float = Literal::from(ast_float);
        assert_eq!(logical_float, Literal::Float(3.14));

        // Test boolean conversion
        let ast_bool = ast::Literal::Boolean(true);
        let logical_bool = Literal::from(ast_bool);
        assert_eq!(logical_bool, Literal::Boolean(true));

        // Test string conversion
        let ast_string = ast::Literal::String("New York");
        let logical_string = Literal::from(ast_string);
        assert_eq!(logical_string, Literal::String("New York".to_string()));

        // Test null conversion
        let ast_null = ast::Literal::Null;
        let logical_null = Literal::from(ast_null);
        assert_eq!(logical_null, Literal::Null);
    }

    #[test]
    fn test_direction_reverse() {
        assert_eq!(Direction::Outgoing.reverse(), Direction::Incoming);
        assert_eq!(Direction::Incoming.reverse(), Direction::Outgoing);
        assert_eq!(Direction::Either.reverse(), Direction::Either);
    }

    #[test]
    fn test_operator_application_from_ast() {
        let ast_operator_app = ast::OperatorApplication {
            operator: ast::Operator::Equal,
            operands: vec![
                ast::Expression::Variable("city"),
                ast::Expression::Literal(ast::Literal::String("San Francisco")),
            ],
        };
        let logical_operator_app = OperatorApplication::from(ast_operator_app);

        assert_eq!(logical_operator_app.operator, Operator::Equal);
        assert_eq!(logical_operator_app.operands.len(), 2);

        match &logical_operator_app.operands[0] {
            LogicalExpr::TableAlias(alias) => assert_eq!(alias.0, "city"),
            _ => panic!("Expected TableAlias"),
        }

        match &logical_operator_app.operands[1] {
            LogicalExpr::Literal(Literal::String(s)) => assert_eq!(s, "San Francisco"),
            _ => panic!("Expected String literal"),
        }
    }

    #[test]
    fn test_function_call_conversion_aggregate() {
        let ast_function_call = ast::FunctionCall {
            name: "count".to_string(),
            args: vec![ast::Expression::Variable("person")],
        };
        let logical_expr = LogicalExpr::from(ast_function_call);

        match logical_expr {
            LogicalExpr::AggregateFnCall(agg_fn) => {
                assert_eq!(agg_fn.name, "count");
                assert_eq!(agg_fn.args.len(), 1);
                match &agg_fn.args[0] {
                    LogicalExpr::TableAlias(alias) => assert_eq!(alias.0, "person"),
                    _ => panic!("Expected TableAlias"),
                }
            }
            _ => panic!("Expected AggregateFnCall"),
        }
    }

    #[test]
    fn test_function_call_conversion_scalar() {
        let ast_function_call = ast::FunctionCall {
            name: "length".to_string(),
            args: vec![ast::Expression::Variable("username")],
        };
        let logical_expr = LogicalExpr::from(ast_function_call);

        match logical_expr {
            LogicalExpr::ScalarFnCall(scalar_fn) => {
                assert_eq!(scalar_fn.name, "length");
                assert_eq!(scalar_fn.args.len(), 1);
                match &scalar_fn.args[0] {
                    LogicalExpr::TableAlias(alias) => assert_eq!(alias.0, "username"),
                    _ => panic!("Expected TableAlias"),
                }
            }
            _ => panic!("Expected ScalarFnCall"),
        }
    }

    #[test]
    fn test_node_pattern_from_ast() {
        let ast_node_pattern = ast::NodePattern {
            name: Some("employee"),
            label: Some("Person"),
            properties: Some(vec![ast::Property::PropertyKV(ast::PropertyKVPair {
                key: "department",
                value: ast::Expression::Literal(ast::Literal::String("Engineering")),
            })]),
        };
        let logical_node_pattern = NodePattern::from(ast_node_pattern);

        assert_eq!(logical_node_pattern.name, Some("employee".to_string()));
        assert_eq!(logical_node_pattern.label, Some("Person".to_string()));
        assert!(logical_node_pattern.properties.is_some());

        let properties = logical_node_pattern.properties.unwrap();
        assert_eq!(properties.len(), 1);

        match &properties[0] {
            Property::PropertyKV(kv) => {
                assert_eq!(kv.key, "department");
                assert_eq!(kv.value, Literal::String("Engineering".to_string()));
            }
            _ => panic!("Expected PropertyKV"),
        }
    }

    #[test]
    fn test_relationship_pattern_from_ast() {
        let ast_relationship_pattern = ast::RelationshipPattern {
            name: Some("follows"),
            direction: ast::Direction::Outgoing,
            label: Some("FOLLOWS"),
            properties: Some(vec![ast::Property::PropertyKV(ast::PropertyKVPair {
                key: "since",
                value: ast::Expression::Literal(ast::Literal::Integer(2020)),
            })]),
        };
        let logical_relationship_pattern = RelationshipPattern::from(ast_relationship_pattern);

        assert_eq!(
            logical_relationship_pattern.name,
            Some("follows".to_string())
        );
        assert_eq!(logical_relationship_pattern.direction, Direction::Outgoing);
        assert_eq!(
            logical_relationship_pattern.label,
            Some("FOLLOWS".to_string())
        );
        assert!(logical_relationship_pattern.properties.is_some());

        let properties = logical_relationship_pattern.properties.unwrap();
        assert_eq!(properties.len(), 1);

        match &properties[0] {
            Property::PropertyKV(kv) => {
                assert_eq!(kv.key, "since");
                assert_eq!(kv.value, Literal::Integer(2020));
            }
            _ => panic!("Expected PropertyKV"),
        }
    }

    #[test]
    fn test_connected_pattern_from_ast() {
        let start_node = ast::NodePattern {
            name: Some("user"),
            label: Some("User"),
            properties: None,
        };
        let end_node = ast::NodePattern {
            name: Some("company"),
            label: Some("Company"),
            properties: None,
        };
        let relationship = ast::RelationshipPattern {
            name: Some("works_at"),
            direction: ast::Direction::Outgoing,
            label: Some("WORKS_AT"),
            properties: None,
        };

        let ast_connected_pattern = ast::ConnectedPattern {
            start_node: Rc::new(RefCell::new(start_node)),
            relationship,
            end_node: Rc::new(RefCell::new(end_node)),
        };
        let logical_connected_pattern = ConnectedPattern::from(ast_connected_pattern);

        assert_eq!(
            logical_connected_pattern.start_node.name,
            Some("user".to_string())
        );
        assert_eq!(
            logical_connected_pattern.start_node.label,
            Some("User".to_string())
        );
        assert_eq!(
            logical_connected_pattern.end_node.name,
            Some("company".to_string())
        );
        assert_eq!(
            logical_connected_pattern.end_node.label,
            Some("Company".to_string())
        );
        assert_eq!(
            logical_connected_pattern.relationship.name,
            Some("works_at".to_string())
        );
        assert_eq!(
            logical_connected_pattern.relationship.label,
            Some("WORKS_AT".to_string())
        );
        assert_eq!(
            logical_connected_pattern.relationship.direction,
            Direction::Outgoing
        );
    }

    #[test]
    fn test_path_pattern_node_from_ast() {
        let ast_node = ast::NodePattern {
            name: Some("customer"),
            label: Some("Customer"),
            properties: None,
        };
        let ast_path_pattern = ast::PathPattern::Node(ast_node);
        let logical_path_pattern = PathPattern::from(ast_path_pattern);

        match logical_path_pattern {
            PathPattern::Node(node) => {
                assert_eq!(node.name, Some("customer".to_string()));
                assert_eq!(node.label, Some("Customer".to_string()));
            }
            _ => panic!("Expected Node pattern"),
        }
    }

    #[test]
    fn test_logical_expr_from_expression_variable() {
        // Test star variable
        let ast_star = ast::Expression::Variable("*");
        let logical_star = LogicalExpr::from(ast_star);
        assert_eq!(logical_star, LogicalExpr::Star);

        // Test regular variable
        let ast_var = ast::Expression::Variable("product");
        let logical_var = LogicalExpr::from(ast_var);
        match logical_var {
            LogicalExpr::TableAlias(alias) => assert_eq!(alias.0, "product"),
            _ => panic!("Expected TableAlias"),
        }
    }

    #[test]
    fn test_logical_expr_from_expression_list() {
        let ast_list = ast::Expression::List(vec![
            ast::Expression::Literal(ast::Literal::String("admin")),
            ast::Expression::Literal(ast::Literal::String("user")),
            ast::Expression::Literal(ast::Literal::String("guest")),
        ]);
        let logical_list = LogicalExpr::from(ast_list);

        match logical_list {
            LogicalExpr::List(items) => {
                assert_eq!(items.len(), 3);

                match &items[0] {
                    LogicalExpr::Literal(Literal::String(s)) => assert_eq!(s, "admin"),
                    _ => panic!("Expected string literal"),
                }
                match &items[1] {
                    LogicalExpr::Literal(Literal::String(s)) => assert_eq!(s, "user"),
                    _ => panic!("Expected string literal"),
                }
                match &items[2] {
                    LogicalExpr::Literal(Literal::String(s)) => assert_eq!(s, "guest"),
                    _ => panic!("Expected string literal"),
                }
            }
            _ => panic!("Expected List"),
        }
    }

    #[test]
    fn test_display_implementations() {
        // Test TableAlias display
        let table_alias = TableAlias("customer".to_string());
        assert_eq!(format!("{}", table_alias), "customer");

        // Test ColumnAlias display
        let column_alias = ColumnAlias("full_name".to_string());
        assert_eq!(format!("{}", column_alias), "full_name");

        // Test Column display
        let column = Column("email_address".to_string());
        assert_eq!(format!("{}", column), "email_address");

        // Test Literal display implementations
        assert_eq!(format!("{}", Literal::Integer(12345)), "12345");
        assert_eq!(format!("{}", Literal::Float(99.99)), "99.99");
        assert_eq!(format!("{}", Literal::Boolean(true)), "true");
        assert_eq!(format!("{}", Literal::Boolean(false)), "false");
        assert_eq!(
            format!("{}", Literal::String("Hello World".to_string())),
            "Hello World"
        );
        assert_eq!(format!("{}", Literal::Null), "null");
    }

    #[test]
    fn test_aggregate_function_classification() {
        let agg_functions = ["count", "min", "max", "avg", "sum"];

        for func_name in &agg_functions {
            let ast_function_call = ast::FunctionCall {
                name: func_name.to_string(),
                args: vec![ast::Expression::Variable("revenue")],
            };
            let logical_expr = LogicalExpr::from(ast_function_call);

            match logical_expr {
                LogicalExpr::AggregateFnCall(agg_fn) => {
                    assert_eq!(agg_fn.name, *func_name);
                }
                _ => panic!("Expected aggregate function for {}", func_name),
            }
        }

        // Test non-aggregate function
        let ast_scalar_function = ast::FunctionCall {
            name: "substring".to_string(),
            args: vec![
                ast::Expression::Variable("description"),
                ast::Expression::Literal(ast::Literal::Integer(1)),
                ast::Expression::Literal(ast::Literal::Integer(10)),
            ],
        };
        let logical_expr = LogicalExpr::from(ast_scalar_function);

        match logical_expr {
            LogicalExpr::ScalarFnCall(scalar_fn) => {
                assert_eq!(scalar_fn.name, "substring");
                assert_eq!(scalar_fn.args.len(), 3);
            }
            _ => panic!("Expected scalar function"),
        }
    }
}
