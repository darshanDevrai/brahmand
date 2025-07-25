use crate::query_planner::render_plan::plan_builder::RenderPlanBuilder;
use crate::query_planner::{logical_expr::logical_expr::LogicalExpr, render_plan::render_plan::RenderPlan};

use crate::query_planner::logical_expr::logical_expr::{
    Literal as LogicalLiteral, TableAlias as LogicalTableAlias, ColumnAlias as LogicalColumnAlias,
    Column as LogicalColumn, OperatorApplication as LogicalOperatorApplication, Operator as LogicalOperator,
    PropertyAccess as LogicalPropertyAccess, ScalarFnCall as LogicalScalarFnCall,
    AggregateFnCall as LogicalAggregateFnCall, InSubquery as LogicalInSubquery
};







#[derive(Debug, PartialEq, Clone)]
pub enum RenderExpr {
    Literal(Literal),

    Star,

    TableAlias(TableAlias),

    ColumnAlias(ColumnAlias),

    Column(Column),

    Parameter(String),

    List(Vec<RenderExpr>),

    AggregateFnCall(AggregateFnCall),

    ScalarFnCall(ScalarFnCall),

    PropertyAccessExp(PropertyAccess),

    OperatorApplicationExp(OperatorApplication),

    InSubquery(InSubquery) 
}

#[derive(Debug, PartialEq, Clone)]
pub struct InSubquery {
    pub expr:   Box<RenderExpr>,
    pub subplan: Box<RenderPlan>,
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
    pub operands: Vec<RenderExpr>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct PropertyAccess {
    pub table_alias: TableAlias,
    pub column: Column,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ScalarFnCall {
    pub name: String,
    pub args: Vec<RenderExpr>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct AggregateFnCall {
    pub name: String,
    pub args: Vec<RenderExpr>,
}




impl From<LogicalExpr> for RenderExpr {
    fn from(expr: LogicalExpr) -> Self {
        match expr {
            LogicalExpr::Literal(lit) => RenderExpr::Literal(lit.into()),
            LogicalExpr::Star => RenderExpr::Star,
            LogicalExpr::TableAlias(alias) => RenderExpr::TableAlias(alias.into()),
            LogicalExpr::ColumnAlias(alias) => RenderExpr::ColumnAlias(alias.into()),
            LogicalExpr::Column(col) => RenderExpr::Column(col.into()),
            LogicalExpr::Parameter(s) => RenderExpr::Parameter(s),
            LogicalExpr::List(exprs) => RenderExpr::List(exprs.into_iter().map(RenderExpr::from).collect()),
            LogicalExpr::AggregateFnCall(agg) => RenderExpr::AggregateFnCall(agg.into()),
            LogicalExpr::ScalarFnCall(fn_call) => RenderExpr::ScalarFnCall(fn_call.into()),
            LogicalExpr::PropertyAccessExp(pa) => RenderExpr::PropertyAccessExp(pa.into()),
            LogicalExpr::OperatorApplicationExp(op) => RenderExpr::OperatorApplicationExp(op.into()),
            LogicalExpr::InSubquery(subq) => RenderExpr::InSubquery(subq.into()),
            // PathPattern is not present in RenderExpr
            _ => unimplemented!("Conversion for this LogicalExpr variant is not implemented"),
        }
    }
}

// impl TryFrom<LogicalInSubquery> for InSubquery {
//     type Error = RenderBuildError;

//     fn try_from(value: LogicalInSubquery) -> Result<Self, Self::Error> {
//         let sub_plan = value.subplan.clone().to_render_plan()?;
//         Ok(InSubquery {
//             expr: Box::new((value.expr.as_ref().clone()).into()),
//             subplan: Box::new(sub_plan),
//         })
//     }
// }

impl From<LogicalInSubquery> for InSubquery {
    fn from(value: LogicalInSubquery) -> Self {
        InSubquery {
            expr: Box::new((value.expr.as_ref().clone()).into()),
            // TODO Remove this Unwrap.
            subplan: Box::new(value.subplan.clone().to_render_plan().unwrap()),
        }
    }
}

impl From<LogicalLiteral> for Literal {
    fn from(lit: LogicalLiteral) -> Self {
        match lit {
            LogicalLiteral::Integer(i) => Literal::Integer(i),
            LogicalLiteral::Float(f) => Literal::Float(f),
            LogicalLiteral::Boolean(b) => Literal::Boolean(b),
            LogicalLiteral::String(s) => Literal::String(s),
            LogicalLiteral::Null => Literal::Null,
        }
    }
}

impl From<LogicalTableAlias> for TableAlias {
    fn from(alias: LogicalTableAlias) -> Self {
        TableAlias(alias.0)
    }
}

impl From<LogicalColumnAlias> for ColumnAlias {
    fn from(alias: LogicalColumnAlias) -> Self {
        ColumnAlias(alias.0)
    }
}

impl From<LogicalColumn> for Column {
    fn from(col: LogicalColumn) -> Self {
        Column(col.0)
    }
}

impl From<LogicalPropertyAccess> for PropertyAccess {
    fn from(pa: LogicalPropertyAccess) -> Self {
        PropertyAccess {
            table_alias: pa.table_alias.into(),
            column: pa.column.into(),
        }
    }
}


impl From<LogicalOperatorApplication> for OperatorApplication {
    fn from(op: LogicalOperatorApplication) -> Self {
        OperatorApplication {
            operator: op.operator.into(), 
            operands: op.operands.into_iter().map(RenderExpr::from).collect(),
        }
    }
}

impl From<LogicalOperator> for Operator {
    fn from(value: LogicalOperator) -> Self {
        match value {
            LogicalOperator::Addition => Operator::Addition,
            LogicalOperator::Subtraction => Operator::Subtraction,
            LogicalOperator::Multiplication => Operator::Multiplication,
            LogicalOperator::Division => Operator::Division,
            LogicalOperator::ModuloDivision => Operator::ModuloDivision,
            LogicalOperator::Exponentiation => Operator::Exponentiation,
            LogicalOperator::Equal => Operator::Equal,
            LogicalOperator::NotEqual => Operator::NotEqual,
            LogicalOperator::LessThan => Operator::LessThan,
            LogicalOperator::GreaterThan => Operator::GreaterThan,
            LogicalOperator::LessThanEqual => Operator::LessThanEqual,
            LogicalOperator::GreaterThanEqual => Operator::GreaterThanEqual,
            LogicalOperator::And => Operator::And,
            LogicalOperator::Or => Operator::Or,
            LogicalOperator::In => Operator::In,
            LogicalOperator::NotIn => Operator::NotIn,
            LogicalOperator::Not => Operator::Not,
            LogicalOperator::Distinct => Operator::Distinct,
            LogicalOperator::IsNull => Operator::IsNull,
            LogicalOperator::IsNotNull => Operator::IsNotNull,
        }
    }
}

impl From<LogicalScalarFnCall> for ScalarFnCall {
    fn from(fc: LogicalScalarFnCall) -> Self {
        ScalarFnCall {
            name: fc.name,
            args: fc.args.into_iter().map(RenderExpr::from).collect(),
        }
    }
}

impl From<LogicalAggregateFnCall> for AggregateFnCall {
    fn from(agg: LogicalAggregateFnCall) -> Self {
        AggregateFnCall {
            name: agg.name,
            args: agg.args.into_iter().map(RenderExpr::from).collect(),
        }
    }
}
