use crate::query_engine_v2::render_plan::{render_expr::{Column, ColumnAlias, InSubquery, Literal, Operator, PropertyAccess, RenderExpr, TableAlias}, render_plan::{Cte, CteItems, FilterItems, FromTable, GroupByExpressions, Join, JoinItems, LimitItem, OrderByItems, RenderPlan, SelectItems, SkipItem, OrderByOrder}};
use crate::query_engine_v2::render_plan::render_expr::OperatorApplication;


pub trait ToSql {
    fn to_sql(&self) -> String;
}


impl ToSql for SelectItems {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();

        if self.0.is_empty() {
            return sql;
        }

        sql.push_str("SELECT \n");

        for (i, item) in self.0.iter().enumerate() {
            sql.push_str("      ");
            sql.push_str(&item.expression.to_sql());
            if let Some(alias) = &item.col_alias {
                sql.push_str(" AS ");
                sql.push_str(&alias.0);
            }
            if i + 1 < self.0.len() {
                sql.push_str(", ");
            }
            sql.push('\n');
        }
        sql
    }
}

impl ToSql for FromTable {
    fn to_sql(&self) -> String {
        let mut sql = "FROM ".to_string();
    
        sql.push_str(&self.table_name);
        if let Some(alias) = &self.table_alias {
            if !alias.is_empty() {
                sql.push_str(" AS ");
                sql.push_str(&alias);
            }
        }
        sql.push('\n');
        sql
    }
}

impl ToSql for FilterItems {
    fn to_sql(&self) -> String {
        if let Some(expr) = &self.0 {
            format!("WHERE {}\n", expr.to_sql())
        } else {
            "".into()
        }
    }
}

impl ToSql for GroupByExpressions {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();
        if self.0.is_empty() { 
            return sql;
        }
        sql.push_str("GROUP BY ");
        for (i, e) in self.0.iter().enumerate() {
            sql.push_str(&e.to_sql());
            if i + 1 < self.0.len() {
                sql.push_str(", ");
            }
        }
        sql.push('\n');
        sql
    }
}

impl ToSql for OrderByItems {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();
        if self.0.is_empty() { 
            return sql;
        }
        sql.push_str("ORDER BY ");
        for (i, item) in self.0.iter().enumerate() {
            sql.push_str(&item.expression.to_sql());
            sql.push(' ');
            sql.push_str(&item.order.to_sql());
            if i + 1 < self.0.len() {
                sql.push_str(", ");
            }
        }
        sql.push('\n');
        sql
    }
}

impl ToSql for LimitItem {
    fn to_sql(&self) -> String {
        if let Some(n) = self.0 {
            format!("LIMIT {}\n", n)
        } else {
            "".into()
        }
    }
}

impl ToSql for SkipItem {
    fn to_sql(&self) -> String {
        if let Some(n) = self.0 {
            format!("SKIP {}\n", n)
        } else {
            "".into()
        }
    }
}


impl ToSql for CteItems {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();
        if self.0.is_empty() {
            return sql;
        }
        
        sql.push_str( "WITH ");

        for (i, cte) in self.0.iter().enumerate() {
            sql.push_str(&cte.to_sql());
            // s.push_str(&format!("{} AS (", cte.cte_name));
            // s.push_str(&cte.to_sql_inner());
            // s.push(')');
            if i + 1 < self.0.len() { 
                sql.push_str(", "); 
            }
            sql.push('\n');
        }
        sql
    }
}

impl ToSql for Cte {
    fn to_sql(&self) -> String {
        let mut cte_body = String::new();
        // SELECT
        cte_body.push_str("\n    ");
        cte_body.push_str(&self.select.to_sql());
        // FROM
        cte_body.push_str("    ");
        cte_body.push_str(&self.from.to_sql());
        // cte_body.push_str("  ");
        // inner.push_str("\nFROM ");
        // inner.push_str(&format!(
        //     "{}{}",
        //     self.from.table_name,
        //     self.from
        //         .table_alias
        //         .as_ref()
        //         .map(|a| format!(" {}", a))
        //         .unwrap_or_default()
        // ));
        // WHERE
        let where_str = &self.filters.to_sql();
        if !where_str.is_empty() {
            cte_body.push_str(&format!("    {}", where_str));
        }
        // if let Some(f) = &self.filters.0 {
        //     inner.push_str("\nWHERE ");
        //     inner.push_str(&f.to_sql());
        // }

        let sql = format!("{} AS ({})", self.cte_name, cte_body);
        sql
    }
}

impl ToSql for JoinItems {
    fn to_sql(&self) -> String {
        let mut sql = String::new();
        for join in &self.0 {
            sql.push_str(&join.to_sql());
            // s.push('\n');
        }
        sql
    }
}

impl ToSql for Join {
    fn to_sql(&self) -> String {
        let mut sql = format!("JOIN {} AS {}", self.table_name, self.table_alias);
        if !self.joining_on.is_empty() {
            sql.push_str(" ON ");
            for (i, cond) in self.joining_on.iter().enumerate() {
                sql.push_str(&cond.to_sql());
                if i + 1 < self.joining_on.len() {
                    sql.push_str(" AND ");
                }
            }
        }
        sql.push('\n');
        sql
    }
}




// Finally, hook it all up:
impl RenderPlan {
    pub fn to_sql(&self) -> String {
        let mut sql = String::new();
        sql.push_str(&self.ctes.to_sql());
        // sql.push_str("SELECT ");
        sql.push_str(&self.select.to_sql());
        // sql.push('\n');
        // sql.push_str("FROM ");
        sql.push_str(&self.from.to_sql());
        // sql.push('\n');
        sql.push_str(&self.joins.to_sql());
        sql.push_str(&self.filters.to_sql());
        sql.push_str(&self.group_by.to_sql());
        sql.push_str(&self.order_by.to_sql());
        sql.push_str(&self.limit.to_sql());
        sql.push_str(&self.skip.to_sql());
        sql
    }
}


impl RenderExpr {
    /// Render this expression (including any subqueries) to a SQL string.
    pub fn to_sql(&self) -> String {
        match self {
            RenderExpr::Literal(lit) => match lit {
                Literal::Integer(i) => i.to_string(),
                Literal::Float(f)   => f.to_string(),
                Literal::Boolean(b) => if *b { "true".into() } else { "FfalseALSE".into() },
                Literal::String(s)  => format!("'{}'", s), //format!("'{}'", s.replace('\'', "''")),
                Literal::Null       => "NULL".into(),
            },
            RenderExpr::Variable(name) | RenderExpr::Parameter(name) => name.clone(),
            RenderExpr::Star => "*".into(),
            RenderExpr::TableAlias(TableAlias(a))| RenderExpr::ColumnAlias(ColumnAlias(a)) | RenderExpr::Column(Column(a)) => a.clone(),
            RenderExpr::List(items) => {
                let inner = items.iter()
                                 .map(|e| e.to_sql())
                                 .collect::<Vec<_>>()
                                 .join(", ");
                format!("({})", inner)
            }
            RenderExpr::ScalarFnCall(fn_call) => {
                let args = fn_call.args.iter()
                                       .map(|e| e.to_sql())
                                       .collect::<Vec<_>>()
                                       .join(", ");
                format!("{}({})", fn_call.name, args)
            }
            RenderExpr::AggregateFnCall(agg) => {
                let args = agg.args.iter()
                                   .map(|e| e.to_sql())
                                   .collect::<Vec<_>>()
                                   .join(", ");
                format!("{}({})", agg.name, args)
            }
            RenderExpr::PropertyAccessExp(PropertyAccess { table_alias, column }) => {
                format!("{}.{}", table_alias.0, column.0)
            }
            RenderExpr::OperatorApplicationExp(op) => {
                fn op_str(o: Operator) -> &'static str {
                    match o {
                        Operator::Addition          => "+",
                        Operator::Subtraction       => "-",
                        Operator::Multiplication    => "*",
                        Operator::Division          => "/",
                        Operator::ModuloDivision    => "%",
                        Operator::Exponentiation    => "^",
                        Operator::Equal             => "=",
                        Operator::NotEqual          => "<>",
                        Operator::LessThan          => "<",
                        Operator::GreaterThan       => ">",
                        Operator::LessThanEqual     => "<=",
                        Operator::GreaterThanEqual  => ">=",
                        Operator::And               => "AND",
                        Operator::Or                => "OR",
                        Operator::In                => "IN",
                        Operator::NotIn             => "NOT IN",
                        Operator::Not               => "NOT",
                        Operator::Distinct          => "DISTINCT",
                        Operator::IsNull            => "IS NULL",
                        Operator::IsNotNull         => "IS NOT NULL",
                    }
                }

                let sql_op = op_str(op.operator);
                let rendered: Vec<String> = op
                    .operands
                    .iter()
                    .map(|e| e.to_sql())
                    .collect();

                match rendered.len() {
                    0 => "".into(), // should not happen
                    1 => format!("{} {}", sql_op, &rendered[0]),       // unary
                    2 => format!("{} {} {}", &rendered[0], sql_op, &rendered[1]),
                    _ => {
                        // n-ary: join with the operator
                        rendered.join(&format!(" {} ", sql_op))
                    }
                }
            }
            RenderExpr::InSubquery(InSubquery { expr, subplan }) => {
                let left  = expr.to_sql();
                let body  = subplan.to_sql();  
                let body = body.split_whitespace().collect::<Vec<&str>>().join(" "); 
                
                format!("{} IN ({})", left, body)
            }
        }
    }
}

impl ToSql for OperatorApplication {
    fn to_sql(&self) -> String {
        // Map your enum to SQL tokens
        fn op_str(o: Operator) -> &'static str {
            match o {
                Operator::Addition          => "+",
                Operator::Subtraction       => "-",
                Operator::Multiplication    => "*",
                Operator::Division          => "/",
                Operator::ModuloDivision    => "%",
                Operator::Exponentiation    => "^",
                Operator::Equal             => "=",
                Operator::NotEqual          => "<>",
                Operator::LessThan          => "<",
                Operator::GreaterThan       => ">",
                Operator::LessThanEqual     => "<=",
                Operator::GreaterThanEqual  => ">=",
                Operator::And               => "AND",
                Operator::Or                => "OR",
                Operator::In                => "IN",
                Operator::NotIn             => "NOT IN",
                Operator::Not               => "NOT",
                Operator::Distinct          => "DISTINCT",
                Operator::IsNull            => "IS NULL",
                Operator::IsNotNull         => "IS NOT NULL",
            }
        }

        let sql_op = op_str(self.operator);
        let rendered: Vec<String> = self.operands.iter().map(|e| e.to_sql()).collect();

        match rendered.len() {
            0 => "".into(), // should not happen
            1 => format!("{} {}", sql_op, &rendered[0]),       // unary
            2 => format!("{} {} {}", &rendered[0], sql_op, &rendered[1]),
            _ => {
                // n-ary: join with the operator
                rendered.join(&format!(" {} ", sql_op))
            }
        }
    }
}

impl ToSql for OrderByOrder {
    fn to_sql(&self) -> String {
        match self {
            OrderByOrder::Asc => "ASC".to_string(),
            OrderByOrder::Desc => "DESC".to_string(),
        }
    }
}