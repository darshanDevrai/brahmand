use std::sync::Arc;

use crate::query_engine_v2::{logical_plan::{logical_plan::{GraphRel, LogicalPlan}, plan_ctx::PlanCtx}, optimizer::optimizer_pass::OptimizerPass, transformed::Transformed};





pub struct AnchorNodeSelection;

impl OptimizerPass for AnchorNodeSelection {

    fn optimize(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Transformed<Arc<LogicalPlan>> {
        if let Some(anchor_node_alias) = self.find_anchor_node(plan_ctx) {
            return self.anchor_traversal(anchor_node_alias, logical_plan, plan_ctx);
        }

        Transformed::No(logical_plan)
    }
}

impl AnchorNodeSelection {

    pub fn new() -> Self {
        AnchorNodeSelection
    }

    fn find_anchor_node(&self, plan_ctx: &PlanCtx) -> Option<String> {
        let (alias, ctx) = plan_ctx.alias_table_ctx_map
            .iter()
            .max_by_key(|(_, ctx)| ctx.filter_predicates.len())?;
        if ctx.filter_predicates.is_empty() {
            None
        } else {
            Some(alias.clone())
        }
    }


    fn anchor_traversal(&self, anchor_node_alias: String, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Transformed<Arc<LogicalPlan>> {
        match logical_plan.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.anchor_traversal(anchor_node_alias.clone(), graph_node.input.clone(), plan_ctx);
                // let self_tf = self.anchor_traversal(anchor_node_alias, graph_node.self_plan.clone(), plan_ctx);
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GraphRel(graph_rel) => {
                // if anchor node found at right side then it means we have found it at the end of the graph traversal. It is already a start node.
                
                // If found at left then we need to create a new plan and rotate the right side.
                if graph_rel.left_connection == Some(anchor_node_alias.clone()) {
                    let new_anchor_plan = Arc::new(LogicalPlan::GraphRel(GraphRel {
                        left: Arc::new(LogicalPlan::Empty),
                        center: graph_rel.center.clone(),
                        right: graph_rel.left.clone(),
                        alias: graph_rel.alias.clone(),
                        direction: graph_rel.direction.clone().reverse(),
                        // as we are rotating the nodes, we will rotate the connections as well
                        left_connection: graph_rel.right_connection.clone(),
                        right_connection: graph_rel.left_connection.clone(),
                        is_rel_anchor: false
                    }));
                    let rotated_plan = self.rotate_plan(new_anchor_plan, graph_rel.right.clone());
                    
                    return Transformed::Yes(rotated_plan);
                } 

                // similarly check for anchor node at relation i.e. at center
                if graph_rel.alias == anchor_node_alias {
                    let new_anchor_plan = Arc::new(LogicalPlan::GraphRel(GraphRel {
                        left: Arc::new(LogicalPlan::Empty),
                        center: graph_rel.left.clone(),
                        right: graph_rel.center.clone(),
                        alias: graph_rel.alias.clone(),
                        direction: graph_rel.direction.clone().reverse(),
                        // as we are rotating the nodes, we will rotate the connections as well
                        left_connection: graph_rel.right_connection.clone(),
                        right_connection: graph_rel.left_connection.clone(),
                        is_rel_anchor: true
                    }));
                    let rotated_plan = self.rotate_plan(new_anchor_plan, graph_rel.right.clone());
                    
                    return Transformed::Yes(rotated_plan);
                }

                else{
                   
    
                    let left_tf = self.anchor_traversal(anchor_node_alias.clone(), graph_rel.left.clone(), plan_ctx);
                    let center_tf = self.anchor_traversal(anchor_node_alias.clone(), graph_rel.center.clone(), plan_ctx);
                    let right_tf = self.anchor_traversal(anchor_node_alias, graph_rel.right.clone(), plan_ctx);
                    graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
                }
            
            },
            LogicalPlan::Cte(cte   ) => {
                let child_tf = self.anchor_traversal(anchor_node_alias, cte.input.clone(), plan_ctx);
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Scan(_) => {
                    Transformed::No(logical_plan.clone())
            },
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.anchor_traversal(anchor_node_alias, graph_joins.input.clone(), plan_ctx);
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Filter(filter) => {
                        let child_tf = self.anchor_traversal(anchor_node_alias, filter.input.clone(), plan_ctx);
                        filter.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Projection(projection) => {
                        let child_tf = self.anchor_traversal(anchor_node_alias, projection.input.clone(), plan_ctx);
                        projection.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::GroupBy(group_by   ) => {
                let child_tf = self.anchor_traversal(anchor_node_alias, group_by.input.clone(), plan_ctx);
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::OrderBy(order_by) => {
                        let child_tf = self.anchor_traversal(anchor_node_alias, order_by.input.clone(), plan_ctx);
                        order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Skip(skip) => {
                        let child_tf = self.anchor_traversal(anchor_node_alias, skip.input.clone(), plan_ctx);
                        skip.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            LogicalPlan::Limit(limit) => {
                        let child_tf = self.anchor_traversal(anchor_node_alias, limit.input.clone(), plan_ctx);
                        limit.rebuild_or_clone(child_tf, logical_plan.clone())
            },
            
        }
    }

    fn rotate_plan(&self, new_plan: Arc<LogicalPlan>, remaining_plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        match remaining_plan.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                if let LogicalPlan::GraphRel(prev_graph_rel) = new_plan.as_ref() { 
                    let new_constructed_plan = Arc::new(LogicalPlan::GraphRel(GraphRel { 
                        left: Arc::new(LogicalPlan::GraphNode(graph_node.clone())), 
                        center: prev_graph_rel.center.clone(), 
                        right: prev_graph_rel.right.clone(), 
                        alias: prev_graph_rel.alias.clone(),  
                        direction: prev_graph_rel.direction.clone(),
                        left_connection: Some(graph_node.alias.clone()), 
                        right_connection: prev_graph_rel.right_connection.clone(),
                        is_rel_anchor: prev_graph_rel.is_rel_anchor
                    }));
                    return new_constructed_plan;
                }
                // TODO return error in this case
                unreachable!()
            },
            LogicalPlan::GraphRel(graph_rel) => {

                if let LogicalPlan::GraphRel(prev_graph_rel) = new_plan.as_ref() {
                    // check how the prev graph is connected to this current one
                    // We can do that by checking prev graph's left connected to current graph's left or right
                    let (prev_left, new_remaining) = if prev_graph_rel.left_connection == graph_rel.left_connection {
                        (graph_rel.left.clone(), graph_rel.right.clone())
                    } else {
                        (graph_rel.right.clone(), graph_rel.left.clone())
                    };


                    let new_constructed_plan = Arc::new(LogicalPlan::GraphRel(GraphRel { 
                        left: Arc::new(LogicalPlan::Empty), 
                        center: graph_rel.center.clone(), 
                        right: Arc::new(LogicalPlan::GraphRel(GraphRel { 
                            left: prev_left,
                            center: prev_graph_rel.center.clone(), 
                            right: prev_graph_rel.right.clone(), 
                            alias: prev_graph_rel.alias.clone(), 
                            direction: prev_graph_rel.direction.clone(), 
                            left_connection: prev_graph_rel.left_connection.clone(), 
                            right_connection: prev_graph_rel.right_connection.clone(),
                            is_rel_anchor: prev_graph_rel.is_rel_anchor 
                        })), 
                        alias: graph_rel.alias.clone(), 
                        direction: graph_rel.direction.clone().reverse(), 
                        // We don't need to rotate the left_conn and right_conn as we have done it at the anchor node.
                        // Here we are respecting the connection pattern
                        left_connection: graph_rel.left_connection.clone(), 
                        right_connection: graph_rel.right_connection.clone(),
                        is_rel_anchor: false
                    }));

                    return self.rotate_plan(new_constructed_plan, new_remaining);
                }

                // TODO return error in this case
                unreachable!()
                
            },
            _ => new_plan.clone()
        }
    }

    

}