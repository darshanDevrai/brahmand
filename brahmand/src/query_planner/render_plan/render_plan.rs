use crate::query_planner::render_plan::render_expr::{ColumnAlias, OperatorApplication, RenderExpr};

use crate::query_planner::logical_plan::logical_plan::{
    OrderByOrder as LogicalOrderByOrder, OrderByItem as LogicalOrderByItem, Join as LogicalJoin
};

use std::fmt;

#[derive(Debug, PartialEq, Clone)]
pub struct RenderPlan {
    pub ctes: CteItems,
    pub select: SelectItems,
    pub from: FromTable,
    pub joins: JoinItems,
    pub filters: FilterItems,
    pub group_by: GroupByExpressions,
    pub order_by: OrderByItems,
    pub limit: LimitItem,
    pub skip: SkipItem,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectItems(pub Vec<SelectItem>);

#[derive(Debug, PartialEq, Clone)]
pub struct SelectItem {
    pub expression: RenderExpr,
    pub col_alias: Option<ColumnAlias>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct FromTable {
    pub table_name: String,
    pub table_alias: Option<String>
}

#[derive(Debug, PartialEq, Clone)]
pub struct FilterItems(pub Option<RenderExpr>);

#[derive(Debug, PartialEq, Clone)]
pub struct GroupByExpressions(pub Vec<RenderExpr>);


#[derive(Debug, PartialEq, Clone)]
pub struct JoinItems(pub Vec<Join>);

#[derive(Debug, PartialEq, Clone,)]
pub struct Join {
    pub table_name: String,
    pub table_alias: String,
    pub joining_on: Vec<OperatorApplication>
}

impl From<LogicalJoin> for Join {
    fn from(value: LogicalJoin) -> Self {
        Join {
            table_alias: value.table_alias,
            table_name: value.table_name,
            joining_on: value.joining_on.clone().into_iter().map(Into::into).collect()
        }
    }
}


#[derive(Debug, PartialEq, Clone)]
pub struct CteItems(pub Vec<Cte>);

#[derive(Debug, PartialEq, Clone)]
pub struct Cte {
    pub cte_name: String,
    pub select: SelectItems,
    pub from: FromTable,
    pub filters: FilterItems
}

#[derive(Debug, PartialEq, Clone)]
pub struct InSubquery {
    pub expr: RenderExpr,
    pub subplan: SubquerySubPlan,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SubquerySubPlan {
    pub select: SelectItems,
    pub from: FromTable,
}

#[derive(Debug, PartialEq, Clone)]
pub struct LimitItem(pub Option<i64>);

#[derive(Debug, PartialEq, Clone)]
pub struct SkipItem(pub Option<i64>);

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByItems(pub Vec<OrderByItem>);

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByItem {
    pub expression: RenderExpr,
    pub order: OrderByOrder,
}

impl From<LogicalOrderByItem> for OrderByItem {
    fn from(value: LogicalOrderByItem) -> Self {
        OrderByItem {
            expression: value.expression.into(),
            order: value.order.into()
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderByOrder {
    Asc,
    Desc,
}

impl From<LogicalOrderByOrder> for OrderByOrder {
    fn from(value: LogicalOrderByOrder) -> Self {
        match value {
            LogicalOrderByOrder::Asc => OrderByOrder::Asc,
            LogicalOrderByOrder::Desc => OrderByOrder::Desc,
        }
    }
}



impl fmt::Display for RenderPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "---- RenderPlan ----")?;
        writeln!(f, "\nCTEs: {:?}", self.ctes)?;
        writeln!(f, "\nSELECT: {:?}", self.select)?;
        writeln!(f, "\nFROM: {:?}", self.from)?;
        writeln!(f, "\nJOINS: {:?}", self.joins)?;
        writeln!(f, "\nFILTERS: {:?}", self.filters)?;
        writeln!(f, "\nGROUP BY: {:?}", self.group_by)?;
        writeln!(f, "\nORDER BY: {:?}", self.order_by)?;
        writeln!(f, "\nLIMIT: {:?}", self.limit)?;
        writeln!(f, "\nSKIP: {:?}", self.skip)?;
        writeln!(f, "-------------------")
    }
}

