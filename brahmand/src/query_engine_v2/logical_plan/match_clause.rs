use std::sync::Arc;

use uuid::Uuid;

use crate::{open_cypher_parser::ast::{ConnectedPattern, MatchClause, NodePattern, PathPattern}, query_engine_v2::{expr::plan_expr::{Column, Operator, OperatorApplication, PlanExpr, Property}, logical_plan::{logical_plan::{GraphNode, GraphRel, LogicalPlan, Scan}, plan_ctx::{PlanCtx, TableCtx}}}};
use super::errors::PlannerError;


fn generate_scan(alias: String, label: Option<String>) -> Arc<LogicalPlan> {
    Arc::new(LogicalPlan::Scan(Scan{
        table_alias: alias,
        table_name: label,
    }))
}

fn convert_properties(props: Vec<Property>) -> Vec<PlanExpr> {
    let mut extracted_props: Vec<PlanExpr> = vec![];

    for prop in props {

        match prop {
            Property::PropertyKV(property_kvpair) => {
                // println!("\n property_kvpair.value {:?} \n",property_kvpair.value);
                let op_app = PlanExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        PlanExpr::Column(Column(property_kvpair.key)),
                        PlanExpr::Literal(property_kvpair.value)
                    ]
                });
                extracted_props.push(op_app);
            },
            Property::Param(_) => todo!(),
        }
        
    }

    extracted_props

}

fn convert_properties_to_operator_application(plan_ctx: &mut PlanCtx) {

    for (_,table_ctx) in plan_ctx.alias_table_ctx_map.iter_mut() {
        let mut extracted_props = convert_properties(std::mem::take(&mut table_ctx.properties));
        if !extracted_props.is_empty() {
            table_ctx.use_edge_list = true;
        }
        table_ctx.filter_predicates.append(&mut extracted_props); 
    }

}


fn generate_id()-> String {
    format!("a{}",Uuid::new_v4().to_string()[..10].to_string().replace("-", ""))
}

fn traverse_connected_pattern<'a>(connected_patterns: &Vec<ConnectedPattern<'a>>, mut plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx, path_pattern_idx: usize) -> Result<Arc<LogicalPlan>, PlannerError> {
    

    for connected_pattern in connected_patterns {
        

        let start_node_ref = connected_pattern.start_node.borrow();
        let start_node_label = start_node_ref.label.map(|val| val.to_string());
        let start_node_alias = if let Some(alias) = start_node_ref.name { alias.to_string()} else {generate_id()};
        let mut start_node_props = start_node_ref.properties.clone().map(|props| props.into_iter().map(Property::from).collect()).unwrap_or_else(Vec::new);
        
        let rel = &connected_pattern.relationship;
        let rel_alias = if let Some(alias) = rel.name { alias.to_string()} else {generate_id()};
        let rel_label = rel.label.map(|val| val.to_string());
        let rel_properties = rel.properties.clone().map(|props| props.into_iter().map(Property::from).collect()).unwrap_or_else(Vec::new);

        let end_node_ref = connected_pattern.end_node.borrow();
        let end_node_alias = if let Some(alias) = end_node_ref.name { alias.to_string()} else {generate_id()};
        let end_node_label = end_node_ref.label.map(|val| val.to_string());
        let mut end_node_props = end_node_ref.properties.clone().map(|props| props.into_iter().map(Property::from).collect()).unwrap_or_else(Vec::new);
        

        // if start alias already present in ctx map, it means the current nested connected pattern's start node will be connecting at right side plan and end node will be at the left
        if let Some(table_ctx) = plan_ctx.alias_table_ctx_map.get_mut(&start_node_alias.clone()){
            if start_node_label.is_some() {
                table_ctx.label = start_node_label;
            }
            if !start_node_props.is_empty() {
                table_ctx.properties.append(&mut start_node_props);
            }

            let end_graph_node = GraphNode {
                input: generate_scan(end_node_alias.clone(), None),
                // input: Arc::new(LogicalPlan::Empty),
                // self_plan: generate_scan(end_node_alias.clone(), None),
                alias: end_node_alias.clone(),
                down_connection: Some(rel_alias.clone()),
            };
            // plan_ctx.alias_table_ctx_map.insert(end_node_alias.clone(), TableCtx { label: end_node_label, properties: end_node_props, filter_predicates: vec![], projection_items: vec![], is_rel: false, use_edge_list: false, explicit_alias: end_node_ref.name.is_some() });
            plan_ctx.alias_table_ctx_map.insert(end_node_alias.clone(),TableCtx::build(end_node_label, end_node_props, false, end_node_ref.name.is_some()));

            let graph_rel_node = GraphRel{
                left: Arc::new(LogicalPlan::GraphNode(end_graph_node)),
                center: generate_scan(rel_alias.clone(), None),
                right: plan.clone(),
                alias: rel_alias.clone(),
                direction: rel.direction.clone().into(),
                left_connection: Some(end_node_alias),
                right_connection: Some(start_node_alias),
                // is_anchor_graph_rel: false,
                is_rel_anchor: false
            };
            // plan_ctx.alias_table_ctx_map.insert(rel_alias, TableCtx { label: rel_label, properties: rel_properties, filter_predicates: vec![], projection_items: vec![], is_rel: true, use_edge_list: false, explicit_alias: rel.name.is_some()  });
            plan_ctx.alias_table_ctx_map.insert(rel_alias,TableCtx::build(rel_label, rel_properties, true, rel.name.is_some()));

            
            plan = Arc::new(LogicalPlan::GraphRel(graph_rel_node));
        }
        // if end alias already present in ctx map, it means the current nested connected pattern's end node will be connecting at right side plan and start node will be at the left
        else if let Some(table_ctx) = plan_ctx.alias_table_ctx_map.get_mut(&end_node_alias) {
            if end_node_label.is_some() {
                table_ctx.label = end_node_label;
            }
            if !end_node_props.is_empty() {
                table_ctx.properties.append(&mut end_node_props);
            }

            let start_graph_node = GraphNode {
                input: generate_scan(start_node_alias.clone(), None),
                // input: Arc::new(LogicalPlan::Empty),
                // self_plan: generate_scan(start_node_alias.clone(), None),
                alias: start_node_alias.clone(),
                down_connection: Some(rel_alias.clone()),
            };
            // plan_ctx.alias_table_ctx_map.insert(start_node_alias.clone(), TableCtx { label: start_node_label, properties: start_node_props, filter_predicates: vec![], projection_items: vec![], is_rel: false, use_edge_list: false, explicit_alias: start_node_ref.name.is_some() });
            plan_ctx.alias_table_ctx_map.insert(start_node_alias.clone(),TableCtx::build(start_node_label, start_node_props, false, start_node_ref.name.is_some()));

            let graph_rel_node = GraphRel{
                left: Arc::new(LogicalPlan::GraphNode(start_graph_node)),
                center: generate_scan(rel_alias.clone(), None),
                right: plan.clone(),
                alias: rel_alias.clone(),
                direction: rel.direction.clone().into(),
                left_connection: Some(start_node_alias),
                right_connection: Some(end_node_alias),
                // is_anchor_graph_rel: false,
                is_rel_anchor: false
            };
            // plan_ctx.alias_table_ctx_map.insert(rel_alias, TableCtx { label: rel_label, properties: rel_properties, filter_predicates: vec![], projection_items: vec![], is_rel: true, use_edge_list: false, explicit_alias: rel.name.is_some() });
            plan_ctx.alias_table_ctx_map.insert(rel_alias,TableCtx::build(rel_label, rel_properties, true, rel.name.is_some()));
           
            plan = Arc::new(LogicalPlan::GraphRel(graph_rel_node));

        }
        // not connected with existing nodes
        else {

            // if two comma separated patterns found and they are not connected to each other i.e. there is no common node alias between them then throw error.
            if path_pattern_idx > 0 {
                // throw error
                return Err(PlannerError::DisconnectedPatternFound);
            }

            // we will keep start graph node at the right side and end at the left side
            let start_graph_node = GraphNode {
                input: generate_scan(start_node_alias.clone(), None),
                // input: plan.clone(), // or Arc::new(LogicalPlan::Empty)
                // self_plan: generate_scan(start_node_alias.clone(), None),
                alias: start_node_alias.clone(),
                down_connection: None,
            };
            // plan_ctx.alias_table_ctx_map.insert(start_node_alias.clone(), TableCtx { label: start_node_label, properties: start_node_props, filter_predicates: vec![], projection_items: vec![], is_rel: false, use_edge_list: false, explicit_alias: start_node_ref.name.is_some()  });
            plan_ctx.alias_table_ctx_map.insert(start_node_alias.clone(),TableCtx::build(start_node_label, start_node_props, false, start_node_ref.name.is_some()));

            let end_graph_node = GraphNode {
                input: generate_scan(end_node_alias.clone(), None),
                // input: plan.clone(), // or Arc::new(LogicalPlan::Empty)
                // self_plan: generate_scan(end_node_alias.clone(), None),
                alias: end_node_alias.clone(),
                down_connection: Some(rel_alias.clone()),
            };
            // plan_ctx.alias_table_ctx_map.insert(end_node_alias.clone(), TableCtx { label: end_node_label, properties: end_node_props, filter_predicates: vec![], projection_items: vec![], is_rel: false, use_edge_list: false, explicit_alias: end_node_ref.name.is_some()  });
            plan_ctx.alias_table_ctx_map.insert(end_node_alias.clone(),TableCtx::build(end_node_label, end_node_props, false, end_node_ref.name.is_some()));


            let graph_rel_node = GraphRel{
                left: Arc::new(LogicalPlan::GraphNode(end_graph_node)),
                center: generate_scan(rel_alias.clone(), None),
                right: Arc::new(LogicalPlan::GraphNode(start_graph_node)),
                alias: rel_alias.clone(),
                direction: rel.direction.clone().into(),
                left_connection: Some(end_node_alias),
                right_connection: Some(start_node_alias),
                // is_anchor_graph_rel: false,
                is_rel_anchor: false
            };
            // plan_ctx.alias_table_ctx_map.insert(rel_alias, TableCtx { label: rel_label, properties: rel_properties, filter_predicates: vec![], projection_items: vec![], is_rel: true, use_edge_list: false, explicit_alias: rel.name.is_some()  });
            plan_ctx.alias_table_ctx_map.insert(rel_alias,TableCtx::build(rel_label, rel_properties, true, rel.name.is_some()));

            
            plan =  Arc::new(LogicalPlan::GraphRel(graph_rel_node));
        }

    }

    Ok(plan)
}

// fn traverse_connected_pattern<'a>(connected_patterns: &Vec<ConnectedPattern<'a>>, mut plan: LogicalPlan, plan_ctx: &mut PlanCtx) -> LogicalPlan {
    
//     for connected_pattern in connected_patterns {
//         let start_node_plan: LogicalPlan;
//         let rel_plan: LogicalPlan;
//         let end_node_plan: LogicalPlan;

//         let mut nested_node_alias:Option<String> = None;

//         let start_node_ref = connected_pattern.start_node.borrow();
//         let start_node_label = start_node_ref.label.map(|val| val.to_string());
//         let start_node_alias = if let Some(alias) = start_node_ref.name { alias.to_string()} else {Uuid::new_v4().to_string()};
//         let mut start_node_props = start_node_ref.properties.clone().map(|props| props.into_iter().map(Property::from).collect()).unwrap_or_else(Vec::new);
//         // if alias already present in ctx map, it means the nested connected pattern is connecting at start position
//         if let Some(table_ctx) = plan_ctx.alias_table_ctx_map.get_mut(&start_node_alias){
//             if start_node_label.is_some() {
//                 table_ctx.label = start_node_label;
//             }
//             if !start_node_props.is_empty() {
//                 table_ctx.properties.append(&mut start_node_props);
//             }
//             start_node_plan = plan.clone();
//             nested_node_alias = Some(start_node_alias);
//         }else{
//             plan_ctx.alias_table_ctx_map.insert(start_node_alias.clone(), TableCtx { label: start_node_label, properties: start_node_props, filter_predicates: vec![], projection_items: vec![] });
//             // initially we will pass None for label and props. In later pass, we will get this info from table ctx after schema inference
//             start_node_plan = generate_scan(start_node_alias, None);
//         }

//         let rel = &connected_pattern.relationship;
//         let rel_alias = if let Some(alias) = rel.name { alias.to_string()} else {Uuid::new_v4().to_string()};
//         let rel_label = rel.label.map(|val| val.to_string());
//         let mut rel_properties = rel.properties.clone().map(|props| props.into_iter().map(Property::from).collect()).unwrap_or_else(Vec::new);
//         if let Some(table_ctx) = plan_ctx.alias_table_ctx_map.get_mut(&rel_alias) {
//             if rel_label.is_some() {
//                 table_ctx.label = rel_label;
//             }
//             if !rel_properties.is_empty() {
//                 table_ctx.properties.append(&mut rel_properties);
//             }

//             rel_plan = plan.clone();
//             nested_node_alias = Some(rel_alias.clone());
//         }else{
//             plan_ctx.alias_table_ctx_map.insert(rel_alias.clone(), TableCtx { label: rel_label, properties: rel_properties, filter_predicates: vec![], projection_items: vec![] });
//             rel_plan = generate_scan(rel_alias.clone(), None);
//         }


//         let end_node_ref = connected_pattern.end_node.borrow();
//         let end_node_alias = if let Some(alias) = end_node_ref.name { alias.to_string()} else {Uuid::new_v4().to_string()};
//         let end_node_label = end_node_ref.label.map(|val| val.to_string());
//         let mut end_node_props = end_node_ref.properties.clone().map(|props| props.into_iter().map(Property::from).collect()).unwrap_or_else(Vec::new);
//         if let Some(table_ctx) = plan_ctx.alias_table_ctx_map.get_mut(&end_node_alias) {
//             if end_node_label.is_some() {
//                 table_ctx.label = end_node_label;
//             }
//             if !end_node_props.is_empty() {
//                 table_ctx.properties.append(&mut end_node_props);
//             }
//             end_node_plan = plan.clone();
//             nested_node_alias = Some(end_node_alias);
//         }else{
//             plan_ctx.alias_table_ctx_map.insert(end_node_alias.clone(), TableCtx { label: end_node_label, properties: end_node_props, filter_predicates: vec![], projection_items: vec![] });
//             end_node_plan = generate_scan(end_node_alias, None);
//         }


//         let connected_traversal_plan = LogicalPlan::ConnectedTraversal(ConnectedTraversal {
//             start_node: start_node_plan.into(),
//             relationship: rel_plan.into(),
//             end_node: end_node_plan.into(),
//             rel_alias: rel_alias,
//             rel_direction: rel.direction.clone().into(),
//             nested_node_alias
//         });
//         // println!("connected_traversal_plan {:?}", connected_traversal_plan);
//         plan = connected_traversal_plan;
//     }

//     // println!("\n\n plan {:?}", plan);
//     plan

//     // Err(PlannerError::EmptyNode)
// }

fn traverse_node_pattern(node_pattern: &NodePattern, plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Result<Arc<LogicalPlan>, PlannerError> {
    

    // For now we are not supporting empty node. standalone node with name is supported.
    let node_alias = node_pattern.name.ok_or(PlannerError::EmptyNode)?.to_string();
    let node_label = node_pattern.label.map(|val| val.to_string());
    let mut node_props = node_pattern.properties.clone().map(|props| props.into_iter().map(Property::from).collect()).unwrap_or_else(Vec::new);
    
    // if alias already present in ctx map then just add its conditions and do not add it in the logical plan
    if let Some(table_ctx) = plan_ctx.alias_table_ctx_map.get_mut(&node_alias){
        if node_label.is_some() {
            table_ctx.label = node_label;
        }
        if !node_props.is_empty() {
            table_ctx.properties.append(&mut node_props);
        }
        return Ok(plan);
    }else{
        // plan_ctx.alias_table_ctx_map.insert(node_alias.clone(), TableCtx { label: node_label, properties: node_props, filter_predicates: vec![], projection_items: vec![], is_rel: false, use_edge_list: false, explicit_alias: node_pattern.name.is_some() });
        plan_ctx.alias_table_ctx_map.insert(node_alias.clone(),TableCtx::build(node_label, node_props, false, node_pattern.name.is_some()));

        let graph_node = GraphNode {
            input: generate_scan(node_alias.clone(), None),
            // input: LogicalPlan::Empty.into(),
            // self_plan: generate_scan(node_alias.clone(), None),
            alias: node_alias,
            down_connection: None,
        };
        return Ok(Arc::new(LogicalPlan::GraphNode(graph_node)));
    }
}



// fn traverse_node_pattern(node_pattern: &NodePattern, plan: LogicalPlan, plan_ctx: &mut PlanCtx) -> Result<LogicalPlan, PlannerError> {
    

//     // For now we are not supporting empty node. standalone node with name is supported.
//     let node_alias = node_pattern.name.ok_or(PlannerError::EmptyNode)?.to_string();
//     let node_label = node_pattern.label.map(|val| val.to_string());
//     let mut node_props = node_pattern.properties.clone().map(|props| props.into_iter().map(Property::from).collect()).unwrap_or_else(Vec::new);
    
//     // if alias already present in ctx map then just add its conditions and do not add it in the logical plan
//     if let Some(table_ctx) = plan_ctx.alias_table_ctx_map.get_mut(&node_alias){
//         if node_label.is_some() {
//             table_ctx.label = node_label;
//         }
//         if !node_props.is_empty() {
//             table_ctx.properties.append(&mut node_props);
//         }
//         return Ok(plan);
//     }else{
//         plan_ctx.alias_table_ctx_map.insert(node_alias.clone(), TableCtx { label: node_label, properties: node_props, filter_predicates: vec![], projection_items: vec![] });
//         // initially we will pass None for label and props. In later pass, we will get this info from table ctx after schema inference
//         let node_plan = generate_scan(node_alias, None);

//         let connected_traversal_plan = LogicalPlan::ConnectedTraversal(ConnectedTraversal {
//             start_node: node_plan.into(),
//             relationship: LogicalPlan::Empty.into(),
//             end_node: LogicalPlan::Empty.into(),
//             rel_alias: "".to_string(),
//             rel_direction: Direction::Either,
//             nested_node_alias: None
//         });
//         return Ok(connected_traversal_plan);
//     }
// }


pub fn evaluate_match_clause<'a>(
    match_clause: &MatchClause<'a>,
    mut plan: Arc<LogicalPlan>,
    mut plan_ctx: &mut PlanCtx
) -> Result<Arc<LogicalPlan>, PlannerError> {
    // let mut logical_plan: LogicalPlan;
    // let mut plan:LogicalPlan = LogicalPlan::Empty;
    // let mut plan_ctx = PlanCtx::default();
    for (idx, path_pattern) in match_clause.path_patterns.iter().enumerate() {
        match path_pattern {
            PathPattern::Node(node_pattern) => {
                plan = traverse_node_pattern(node_pattern, plan, &mut plan_ctx)?;
            }
            PathPattern::ConnectedPattern(connected_patterns) => {
                plan = traverse_connected_pattern(connected_patterns, plan, &mut plan_ctx, idx)?;
            }
        }
    }

    convert_properties_to_operator_application(plan_ctx);
    Ok(plan)

}


