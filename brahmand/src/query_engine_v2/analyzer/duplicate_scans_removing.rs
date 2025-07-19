use std::{collections::HashSet, sync::Arc};

use crate::query_engine_v2::{analyzer::analyzer_pass::AnalyzerPass, logical_plan::{logical_plan::LogicalPlan, plan_ctx::PlanCtx}, transformed::Transformed};




pub struct DuplicateScansRemoving;



impl AnalyzerPass for DuplicateScansRemoving {
    fn analyze(&self, logical_plan: Arc<LogicalPlan>, _: &mut PlanCtx) -> Transformed<Arc<LogicalPlan>> {
        
        let mut traversed: HashSet<String> = HashSet::new();
        self.remove_duplicate_scans(logical_plan, &mut traversed)
    }
}

impl DuplicateScansRemoving {
    pub fn new() -> Self {
        DuplicateScansRemoving
    }

    fn remove_duplicate_scans(&self, logical_plan: Arc<LogicalPlan>, traversed: &mut HashSet<String>) ->  Transformed<Arc<LogicalPlan>> {

        match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                        let child_tf = self.remove_duplicate_scans(projection.input.clone(), traversed);
                        projection.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::GraphNode(graph_node) => {
                        traversed.insert(graph_node.alias.clone());

                        let child_tf = self.remove_duplicate_scans(graph_node.input.clone(), traversed);
                        // let self_tf = self.remove_duplicate_scans(graph_node.self_plan.clone(), traversed);
                        graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::GraphRel(graph_rel) => {
                        let right_tf = self.remove_duplicate_scans(graph_rel.right.clone(), traversed);
                        let center_tf = self.remove_duplicate_scans(graph_rel.center.clone(), traversed);

                        let left_alias = graph_rel.left_connection.clone().unwrap();

                        let left_tf = if traversed.contains(&left_alias) {
                            Transformed::Yes(Arc::new(LogicalPlan::Empty))
                        } else {
                            self.remove_duplicate_scans(graph_rel.left.clone(), traversed)
                        };
                
                
                
                        // let left_tf = self.remove_duplicate_scans(graph_rel.left.clone(), traversed);
                
                

                        graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
                    },
            LogicalPlan::Cte(cte   ) => {
                        let child_tf = self.remove_duplicate_scans( cte.input.clone(), traversed);
                        cte.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::Scan(_) => {
                        Transformed::No(logical_plan.clone())
                    },
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::ConnectedTraversal(connected_traversal) => {
                        let left_tf = self.remove_duplicate_scans(connected_traversal.start_node.clone(), traversed);
                        let rel_tf = self.remove_duplicate_scans(connected_traversal.relationship.clone(), traversed);
                        let right_tf = self.remove_duplicate_scans(connected_traversal.end_node.clone(), traversed);
                        connected_traversal.rebuild_or_clone(left_tf, rel_tf, right_tf, logical_plan.clone())
                    },
            LogicalPlan::GraphJoins(graph_joins) => {
                        let child_tf = self.remove_duplicate_scans(graph_joins.input.clone(), traversed);
                        graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::Filter(filter) => {
                        let child_tf = self.remove_duplicate_scans(filter.input.clone(), traversed);
                        filter.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::GroupBy(group_by   ) => {
                        let child_tf = self.remove_duplicate_scans(group_by.input.clone(), traversed);
                        group_by.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::OrderBy(order_by) => {
                        let child_tf = self.remove_duplicate_scans(order_by.input.clone(), traversed);
                        order_by.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::Skip(skip) => {
                        let child_tf = self.remove_duplicate_scans(skip.input.clone(), traversed);
                        skip.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
            LogicalPlan::Limit(limit) => {
                        let child_tf = self.remove_duplicate_scans(limit.input.clone(), traversed);
                        limit.rebuild_or_clone(child_tf, logical_plan.clone())
                    },
        }
    }
}