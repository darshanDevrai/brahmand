use std::sync::Arc;


use crate::query_engine_v2::{logical_plan::logical_plan::{LogicalPlan, PlanCtx, Projection}, optimizer::optimizer_pass::OptimizerPass, transformed::Transformed};

pub struct ProjectionPushDown;

impl OptimizerPass for ProjectionPushDown {
    fn optimize(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Transformed<Arc<LogicalPlan>> {
        match logical_plan.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.optimize(graph_node.input.clone(), plan_ctx);
                let self_tf = self.optimize(graph_node.self_plan.clone(), plan_ctx);
                graph_node.rebuild_or_clone(child_tf, self_tf, logical_plan.clone())
            },
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = self.optimize(graph_rel.left.clone(), plan_ctx);
                let center_tf = self.optimize(graph_rel.center.clone(), plan_ctx);
                let right_tf = self.optimize(graph_rel.right.clone(), plan_ctx);
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            },
            LogicalPlan::Scan(scan) => {
                if let Some(table_ctx) = plan_ctx.alias_table_ctx_map.get_mut(&scan.table_alias) {
                    if !table_ctx.projection_items.is_empty() {
                        let new_proj = Arc::new(LogicalPlan::Projection(Projection {
                            input: logical_plan.clone(),
                            items: table_ctx.projection_items.clone(),
                        }));
                        table_ctx.projection_items.clear();
                        return Transformed::Yes(new_proj)
                    }
                }
                Transformed::No(logical_plan.clone())
            },
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::ConnectedTraversal(connected_traversal) => {
                let start_tf = self.optimize(connected_traversal.start_node.clone(), plan_ctx);
                let rel_tf = self.optimize(connected_traversal.relationship.clone(), plan_ctx);
                let end_tf = self.optimize(connected_traversal.end_node.clone(), plan_ctx);
                connected_traversal.rebuild_or_clone(start_tf, rel_tf, end_tf, logical_plan.clone())
            },
            LogicalPlan::Filter(filter) => {
                let child_tf = self.optimize(filter.input.clone(), plan_ctx);
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Projection(projection) => {
                let child_tf = self.optimize(projection.input.clone(), plan_ctx);
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GroupBy(group_by   ) => {
                let child_tf = self.optimize(group_by.input.clone(), plan_ctx);
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.optimize(order_by.input.clone(), plan_ctx);
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Skip(skip) => {
                let child_tf = self.optimize(skip.input.clone(), plan_ctx);
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Limit(limit) => {
                let child_tf = self.optimize(limit.input.clone(), plan_ctx);
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            },
        }
        
    }
}


impl ProjectionPushDown {

    pub fn new() -> Self { 
        ProjectionPushDown 
    }


    // fn push_down_projection(&self, logical_plan: Arc<LogicalPlan>, return_items: Vec<ReturnItem>, table_alias: String) -> Transformed<Arc<LogicalPlan>> {
    //     match logical_plan.as_ref() {
    //         LogicalPlan::Scan(scan) => {
    //             if scan.table_alias == table_alias {
    //                 Transformed::Yes(Arc::new(LogicalPlan::Projection(Projection{
    //                     input: logical_plan.clone(),
    //                     items: return_items
    //                 })))
    //             } else{
    //                 Transformed::No(logical_plan.clone())
    //             }
    //         },
    //         // _ => logical_plan.clone()
    //         LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
    //         LogicalPlan::ConnectedTraversal(connected_traversal) => {
    //             let start_tf = self.push_down_projection(connected_traversal.start_node.clone(), return_items.clone(), table_alias.clone());
    //             let rel_tf = self.push_down_projection(connected_traversal.relationship.clone(), return_items.clone(), table_alias.clone());
    //             let end_tf = self.push_down_projection(connected_traversal.end_node.clone(), return_items, table_alias);
    //             connected_traversal.rebuild_or_clone(start_tf, rel_tf, end_tf, logical_plan.clone())
    //             // Transformed::Yes(Arc::new(LogicalPlan::ConnectedTraversal(ConnectedTraversal {
    //             //     start_node: self.push_down_projection(connected_traversal.start_node.clone(), return_items.clone(), table_alias.clone()).get_plan(),
    //             //     relationship: self.push_down_projection(connected_traversal.relationship.clone(), return_items.clone(), table_alias.clone()).get_plan(),
    //             //     end_node: self.push_down_projection(connected_traversal.end_node.clone(), return_items, table_alias).get_plan(),
    //             //     ..connected_traversal.clone()
    //             // })))
    //         },
    //         LogicalPlan::Filter(filter) => {
    //             let child_tf = self.push_down_projection(filter.input.clone(), return_items.clone(), table_alias.clone());
    //             filter.rebuild_or_clone(child_tf)
    //             // Arc::new(LogicalPlan::Filter(Filter {
    //             //     input: self.push_down_projection(filter.input.clone(), return_items.clone(), table_alias.clone()),
    //             //     predicate: filter.predicate.clone(),
    //             // }))
    //         },
    //         LogicalPlan::Projection(projection) => {
    //             let child_tf = self.push_down_projection(projection.input.clone(), return_items.clone(), table_alias.clone());
    //             projection.rebuild_or_clone(child_tf)
    //             // Arc::new(LogicalPlan::Projection(Projection { 
    //             //     input: self.push_down_projection(projection.input.clone(), return_items.clone(), table_alias.clone()),
    //             //     items: projection.items.clone()
    //             //  }))
    //         },
    //         LogicalPlan::OrderBy(order_by) => {
    //             let child_tf = self.push_down_projection(order_by.input.clone(), return_items.clone(), table_alias.clone());
    //             order_by.rebuild_or_clone(child_tf)
    //             // Arc::new(LogicalPlan::OrderBy(OrderBy {
    //             //     input: self.push_down_projection(order_by.input.clone(), return_items, table_alias),
    //             //     items: order_by.items.clone(),
    //             // }))
    //         },
    //         LogicalPlan::Skip(skip) => {
    //             let child_tf = self.push_down_projection(skip.input.clone(), return_items.clone(), table_alias.clone());
    //             skip.rebuild_or_clone(child_tf)
    //             // Arc::new(LogicalPlan::Skip(Skip {
    //             //     input: self.push_down_projection(skip.input.clone(), return_items, table_alias),
    //             //     count: skip.count,
    //             // }))
    //         },
    //         LogicalPlan::Limit(limit) => {
    //             let child_tf = self.push_down_projection(limit.input.clone(), return_items.clone(), table_alias.clone());
    //             limit.rebuild_or_clone(child_tf)
    //             // Arc::new(LogicalPlan::Limit(Limit {
    //             //     input: self.push_down_projection(limit.input.clone(), return_items, table_alias),
    //             //     count: limit.count,
    //             // }))
    //         },
    //     }
    // }

}

