use std::{collections::HashMap, fmt};

use crate::query_planner::{logical_expr::logical_expr::{LogicalExpr, Property}, logical_plan::logical_plan::ProjectionItem, plan_ctx::errors::PlanCtxError};


#[derive(Debug, PartialEq, Clone)]
pub struct TableCtx {
    pub label: Option<String>,
    pub properties: Vec<Property>,
    filter_predicates: Vec<LogicalExpr>,
    projection_items: Vec<ProjectionItem>,
    pub is_rel: bool,
    pub use_edge_list: bool,
    pub explicit_alias: bool,
}

impl TableCtx {
    pub fn build(label: Option<String>, properties: Vec<Property>, is_rel: bool, explicit_alias: bool) -> Self {
        TableCtx { 
            label: label, 
            properties: properties, 
            filter_predicates: vec![], 
            projection_items: vec![], 
            is_rel: is_rel, 
            use_edge_list: false, 
            explicit_alias: explicit_alias, 
        }       
    }

    pub fn get_projections(&self) -> &Vec<ProjectionItem> {
        &self.projection_items
    }

    pub fn set_projections(&mut self, proj_items: Vec<ProjectionItem>) {
        self.projection_items = proj_items;
    }

    pub fn insert_projection(&mut self, proj_item: ProjectionItem) {
        if !self.projection_items.contains(&proj_item) {
            self.projection_items.push(proj_item);
        }
    }

    pub fn append_projection(&mut self, proj_items: &mut Vec<ProjectionItem>) {
        self.projection_items.append(proj_items);
        // for proj_item in proj_items {
        //     if !self.projection_items.contains(&proj_item) {
        //         self.projection_items.push(proj_item);
        //     }
        // }
    }

    pub fn get_filters(&self) -> &Vec<LogicalExpr> {
        &self.filter_predicates
    }

    pub fn insert_filter(&mut self, filter_pred: LogicalExpr) {
        if !self.filter_predicates.contains(&filter_pred) {
            self.filter_predicates.push(filter_pred);
        }
    }

    pub fn append_filters(&mut self, filter_preds: &mut Vec<LogicalExpr>) {
        self.filter_predicates.append(filter_preds);
        // for filter_pred in filter_preds {
        //     if !self.filter_predicates.contains(&filter_pred) {
        //         self.filter_predicates.push(filter_pred);
        //     }
        // }
    }
}

#[derive(Debug, PartialEq, Clone,)]
pub struct PlanCtx {
    pub alias_table_ctx_map: HashMap<String, TableCtx>
}

impl PlanCtx {
    pub fn set_table_ctx(&mut self, alias: String, table_ctx: TableCtx) {
        self.alias_table_ctx_map.insert(alias, table_ctx);
    }

    pub fn get_table_ctx(&self, alias: &str) -> Result<TableCtx, PlanCtxError> {
        self.alias_table_ctx_map.get(alias).ok_or(PlanCtxError::MissingTableCtx).cloned()
    } 

    pub fn get_node_table_ctx(&self, node_alias: &str) -> Result<TableCtx, PlanCtxError> {
        self.alias_table_ctx_map.get(node_alias).ok_or(PlanCtxError::MissingNodeTableCtx).cloned()
    }

    pub fn get_rel_table_ctx(&self, rel_alias: &str) -> Result<TableCtx, PlanCtxError> {
        self.alias_table_ctx_map.get(rel_alias).ok_or(PlanCtxError::MissingRelTableCtx).cloned()
    }

    pub fn get_mut_table_ctx(&mut self, alias: &str) -> Result<TableCtx, PlanCtxError> {
        self.alias_table_ctx_map.get_mut(alias).ok_or(PlanCtxError::MissingTableCtx).cloned()
    } 

    pub fn get_mut_node_table_ctx(&mut self, node_alias: &str) -> Result<TableCtx, PlanCtxError> {
        self.alias_table_ctx_map.get_mut(node_alias).ok_or(PlanCtxError::MissingNodeTableCtx).cloned()
    }

    pub fn get_mut_rel_table_ctx(&mut self, rel_alias: &str) -> Result<TableCtx, PlanCtxError> {
        self.alias_table_ctx_map.get_mut(rel_alias).ok_or(PlanCtxError::MissingRelTableCtx).cloned()
    }

    pub fn get_table_ctx_opt(&self, alias: &str) -> Option<&TableCtx> {
        self.alias_table_ctx_map.get(alias)
    } 

    pub fn get_node_table_ctx_opt(&self, node_alias: &str) -> Option<&TableCtx> {
        self.alias_table_ctx_map.get(node_alias)
    }

    pub fn get_rel_table_ctx_opt(&self, rel_alias: &str) -> Option<&TableCtx> {
        self.alias_table_ctx_map.get(rel_alias)
    }

    pub fn get_mut_table_ctx_opt(&mut self, alias: &str) -> Option<&mut TableCtx> {
        self.alias_table_ctx_map.get_mut(alias)
    } 

    pub fn get_mut_node_table_ctx_opt(&mut self, node_alias: &str) -> Option<&mut TableCtx> {
        self.alias_table_ctx_map.get_mut(node_alias)
    }

    pub fn get_mut_rel_table_ctx_opt(&mut self, rel_alias: &str) -> Option<&mut TableCtx> {
        self.alias_table_ctx_map.get_mut(rel_alias)
    }

}




impl PlanCtx {
    pub fn default() -> Self {
        PlanCtx {
            alias_table_ctx_map: HashMap::new()
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
        writeln!(f, "{}         is_rel: {:?}", pad, self.is_rel)?;
        writeln!(f, "{}         use_edge_list: {:?}", pad, self.use_edge_list)?;
        writeln!(f, "{}         explicit_alias: {:?}", pad, self.explicit_alias)?;
        Ok(())
    }
}

