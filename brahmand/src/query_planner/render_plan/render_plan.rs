use crate::query_planner::render_plan::errors::RenderBuildError;
use crate::query_planner::render_plan::render_expr::{ColumnAlias, OperatorApplication, RenderExpr};

use crate::query_planner::logical_plan::logical_plan::{
    OrderByOrder as LogicalOrderByOrder, OrderByItem as LogicalOrderByItem, Join as LogicalJoin
};

use std::fmt;

#[derive(Debug, PartialEq, Clone)]
pub struct RenderPlan {
    pub ctes: CteItems,
    pub select: SelectItems,
    pub from: FromTableItem,
    pub joins: JoinItems,
    pub filters: FilterItems,
    pub group_by: GroupByExpressions,
    pub order_by: OrderByItems,
    pub skip: SkipItem,
    pub limit: LimitItem,
    pub union: UnionItems,
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
pub struct FromTableItem(pub Option<FromTable>);

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


impl TryFrom<LogicalJoin> for Join {
    type Error = RenderBuildError;

    fn try_from(value: LogicalJoin) -> Result<Self, Self::Error> {
        let join = Join {
            table_alias: value.table_alias,
            table_name: value.table_name,
            joining_on: value.joining_on.clone().into_iter().map(OperatorApplication::try_from).collect::<Result<Vec<OperatorApplication>, RenderBuildError>>()?,
        };
        Ok(join)
    }
}


#[derive(Debug, PartialEq, Clone)]
pub struct CteItems(pub Vec<Cte>);

#[derive(Debug, PartialEq, Clone)]
pub struct Cte {
    pub cte_name: String,
    pub cte_plan: RenderPlan
    // pub select: SelectItems,
    // pub from: FromTable,
    // pub filters: FilterItems
}

#[derive(Debug, PartialEq, Clone)]
pub struct UnionItems(pub Vec<RenderPlan>);

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


impl TryFrom<LogicalOrderByItem> for OrderByItem {
    type Error = RenderBuildError;

    fn try_from(value: LogicalOrderByItem) -> Result<Self, Self::Error> {
        let order_by_item = OrderByItem {
            expression: value.expression.try_into()?,
            order: value.order.try_into()?
        };
        Ok(order_by_item)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderByOrder {
    Asc,
    Desc,
}


impl TryFrom<LogicalOrderByOrder> for OrderByOrder {
    type Error = RenderBuildError;

    fn try_from(value: LogicalOrderByOrder) -> Result<Self, Self::Error> {
        let order_by = match value {
            LogicalOrderByOrder::Asc => OrderByOrder::Asc,
            LogicalOrderByOrder::Desc => OrderByOrder::Desc,
        };
        Ok(order_by)
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

