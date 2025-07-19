use crate::query_engine_v2::{clickhouse_query_generator::to_sql::ToSql, render_plan::render_plan::RenderPlan};

pub mod to_sql;

pub fn generate_sql(plan: RenderPlan) -> String{
    let mut sql = String::new();
    sql.push_str(&plan.ctes.to_sql());
    sql.push_str(&plan.select.to_sql());
    sql.push_str(&plan.from.to_sql());
    sql.push_str(&plan.joins.to_sql());
    sql.push_str(&plan.filters.to_sql());
    sql.push_str(&plan.group_by.to_sql());
    sql.push_str(&plan.order_by.to_sql());
    sql.push_str(&plan.limit.to_sql());
    sql.push_str(&plan.skip.to_sql());
    println!("\n\n sql - \n{}", sql);
    return sql
}