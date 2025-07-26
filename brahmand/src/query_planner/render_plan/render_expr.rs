use crate::query_planner::render_plan::plan_builder::RenderPlanBuilder;
use crate::query_planner::{logical_expr::logical_expr::LogicalExpr, render_plan::render_plan::RenderPlan};

use crate::query_planner::logical_expr::logical_expr::{
    Literal as LogicalLiteral, TableAlias as LogicalTableAlias, ColumnAlias as LogicalColumnAlias,
    Column as LogicalColumn, OperatorApplication as LogicalOperatorApplication, Operator as LogicalOperator,
    PropertyAccess as LogicalPropertyAccess, ScalarFnCall as LogicalScalarFnCall,
    AggregateFnCall as LogicalAggregateFnCall, InSubquery as LogicalInSubquery
};

use super::errors::RenderBuildError;







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





impl TryFrom<LogicalExpr> for RenderExpr {
    type Error = RenderBuildError;

    fn try_from(expr: LogicalExpr) -> Result<Self, Self::Error> {
        let expression = match expr {
            LogicalExpr::Literal(lit) => RenderExpr::Literal(lit.try_into()?),
            LogicalExpr::Star => RenderExpr::Star,
            LogicalExpr::TableAlias(alias) => RenderExpr::TableAlias(alias.try_into()?),
            LogicalExpr::ColumnAlias(alias) => RenderExpr::ColumnAlias(alias.try_into()?),
            LogicalExpr::Column(col) => RenderExpr::Column(col.try_into()?),
            LogicalExpr::Parameter(s) => RenderExpr::Parameter(s),
            LogicalExpr::List(exprs) => RenderExpr::List(exprs.into_iter().map(RenderExpr::try_from).collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?),
            LogicalExpr::AggregateFnCall(agg) => RenderExpr::AggregateFnCall(agg.try_into()?),
            LogicalExpr::ScalarFnCall(fn_call) => RenderExpr::ScalarFnCall(fn_call.try_into()?),
            LogicalExpr::PropertyAccessExp(pa) => RenderExpr::PropertyAccessExp(pa.try_into()?),
            LogicalExpr::OperatorApplicationExp(op) => RenderExpr::OperatorApplicationExp(op.try_into()?),
            LogicalExpr::InSubquery(subq) => RenderExpr::InSubquery(subq.try_into()?),
            // PathPattern is not present in RenderExpr
            _ => unimplemented!("Conversion for this LogicalExpr variant is not implemented"),
        };
        Ok(expression)
    }
}

impl TryFrom<LogicalInSubquery> for InSubquery {
    type Error = RenderBuildError;

    fn try_from(value: LogicalInSubquery) -> Result<Self, Self::Error> {
        let sub_plan = value.subplan.clone().to_render_plan()?;
        let in_sub_query = InSubquery {
            expr: Box::new((value.expr.as_ref().clone()).try_into()?),
            subplan: Box::new(sub_plan),
        };
        Ok(in_sub_query)
    }
}

impl TryFrom<LogicalLiteral> for Literal{
    type Error = RenderBuildError;

    fn try_from(lit: LogicalLiteral) -> Result<Self, Self::Error> {
        let literal = match lit {
            LogicalLiteral::Integer(i) => Literal::Integer(i),
            LogicalLiteral::Float(f) => Literal::Float(f),
            LogicalLiteral::Boolean(b) => Literal::Boolean(b),
            LogicalLiteral::String(s) => Literal::String(s),
            LogicalLiteral::Null => Literal::Null,
        };
        Ok(literal)
    }
}


impl TryFrom<LogicalTableAlias> for TableAlias {
    type Error = RenderBuildError;

    fn try_from(alias: LogicalTableAlias) -> Result<Self, Self::Error> {
        Ok(TableAlias(alias.0))
    }
}


impl TryFrom<LogicalColumnAlias> for ColumnAlias {
    type Error = RenderBuildError;

    fn try_from(alias: LogicalColumnAlias) -> Result<Self, Self::Error> {
        Ok(ColumnAlias(alias.0))
    }
}



impl TryFrom<LogicalColumn> for Column {
    type Error = RenderBuildError;

    fn try_from(col: LogicalColumn) -> Result<Self, Self::Error> {
        Ok(Column(col.0))
    }
}



impl TryFrom<LogicalPropertyAccess> for PropertyAccess {
    type Error = RenderBuildError;

    fn try_from(pa: LogicalPropertyAccess) -> Result<Self, Self::Error> {
        let prop_acc = PropertyAccess {
            table_alias: pa.table_alias.try_into()?,
            column: pa.column.try_into()?,
        };
        Ok(prop_acc)
    }
}



impl TryFrom<LogicalOperatorApplication> for OperatorApplication {
    type Error = RenderBuildError;

    fn try_from(op: LogicalOperatorApplication) -> Result<Self, Self::Error> {
        let op_app = OperatorApplication {
            operator: op.operator.try_into()?, 
            operands: op.operands.into_iter().map(RenderExpr::try_from).collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?,
        };
        Ok(op_app)
    }
}

impl TryFrom<LogicalOperator> for Operator {
    type Error = RenderBuildError;

    fn try_from(value: LogicalOperator) -> Result<Self, Self::Error> {
        let operator = match value {
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
        };
        Ok(operator)
    }
}


impl TryFrom<LogicalScalarFnCall> for ScalarFnCall {
    type Error = RenderBuildError;

    fn try_from(fc: LogicalScalarFnCall) -> Result<Self, Self::Error> {
        let scalar_fn = ScalarFnCall {
            name: fc.name,
            args: fc.args.into_iter().map(RenderExpr::try_from).collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?,
        };
        Ok(scalar_fn)
    }
}

impl TryFrom<LogicalAggregateFnCall> for AggregateFnCall {
    type Error = RenderBuildError;

    fn try_from(agg: LogicalAggregateFnCall) -> Result<Self, Self::Error> {
        let agg_fn = AggregateFnCall {
            name: agg.name,
            args: agg.args.into_iter().map(RenderExpr::try_from).collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?,
        };
        Ok(agg_fn)
    }
}