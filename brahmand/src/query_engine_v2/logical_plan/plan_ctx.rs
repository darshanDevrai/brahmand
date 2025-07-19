use std::{collections::HashMap, fmt};

use crate::query_engine_v2::{expr::plan_expr::{PlanExpr, Property}, logical_plan::logical_plan::ProjectionItem};


#[derive(Debug, PartialEq, Clone)]
pub struct TableCtx {
    pub label: Option<String>,
    pub properties: Vec<Property>,
    pub filter_predicates: Vec<PlanExpr>,
    pub projection_items: Vec<ProjectionItem>,
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

    pub fn insert_projection(&mut self, proj_item: ProjectionItem) {
        if !self.projection_items.contains(&proj_item) {
            self.projection_items.push(proj_item);
        }
    }

    pub fn append_projection(&mut self, proj_items: Vec<ProjectionItem>) {
        for proj_item in proj_items {
            if !self.projection_items.contains(&proj_item) {
                self.projection_items.push(proj_item);
            }
        }
    }

    pub fn insert_filter(&mut self, filter_pred: PlanExpr) {
        if !self.filter_predicates.contains(&filter_pred) {
            self.filter_predicates.push(filter_pred);
        }
    }
}

#[derive(Debug, PartialEq, Clone,)]
pub struct PlanCtx {
    pub alias_table_ctx_map: HashMap<String, TableCtx>,
    pub last_node: String
}





impl PlanCtx {
    pub fn default() -> Self {
        PlanCtx {
            alias_table_ctx_map: HashMap::new(),
            last_node: "".to_string()
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
        writeln!(f, "\n-- Last Node = ({})", self.last_node)?;
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

