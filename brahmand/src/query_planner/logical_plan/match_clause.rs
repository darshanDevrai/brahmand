use std::sync::Arc;

use uuid::Uuid;

use crate::{open_cypher_parser::ast, query_planner::{logical_expr::logical_expr::{Column, LogicalExpr, Operator, OperatorApplication, Property}, logical_plan::{errors::LogicalPlanError, logical_plan::{GraphNode, GraphRel, LogicalPlan, Scan}, plan_builder::LogicalPlanResult}, plan_ctx::plan_ctx::{PlanCtx, TableCtx}}};


fn generate_scan(alias: String, label: Option<String>) -> Arc<LogicalPlan> {
    Arc::new(LogicalPlan::Scan(Scan{
        table_alias: alias,
        table_name: label,
    }))
}

fn convert_properties(props: Vec<Property>) -> LogicalPlanResult<Vec<LogicalExpr>> {
    let mut extracted_props: Vec<LogicalExpr> = vec![];

    for prop in props {

        match prop {
            Property::PropertyKV(property_kvpair) => {
                let op_app = LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        LogicalExpr::Column(Column(property_kvpair.key)),
                        LogicalExpr::Literal(property_kvpair.value)
                    ]
                });
                extracted_props.push(op_app);
            },
            Property::Param(_) => {
                return Err(LogicalPlanError::FoundParamInProperties)
            },
        }
        
    }

    Ok(extracted_props)

}

fn convert_properties_to_operator_application(plan_ctx: &mut PlanCtx) -> LogicalPlanResult<()> {

    for (_,table_ctx) in plan_ctx.get_mut_alias_table_ctx_map().iter_mut() {
        let mut extracted_props = convert_properties(table_ctx.get_and_clear_properties())?;
        if !extracted_props.is_empty() {
            table_ctx.set_use_edge_list(true);
        }
        table_ctx.append_filters(&mut extracted_props); 
    }
    Ok(())
}


fn generate_id()-> String {
    format!("a{}",Uuid::new_v4().to_string()[..10].to_string().replace("-", ""))
}

fn traverse_connected_pattern<'a>(connected_patterns: &Vec<ast::ConnectedPattern<'a>>, mut plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx, path_pattern_idx: usize) -> LogicalPlanResult<Arc<LogicalPlan>> {
    
    for connected_pattern in connected_patterns {    

        let start_node_ref = connected_pattern.start_node.borrow();
        let start_node_label = start_node_ref.label.map(|val| val.to_string());
        let start_node_alias = if let Some(alias) = start_node_ref.name { alias.to_string()} else {generate_id()};
        let start_node_props = start_node_ref.properties.clone().map(|props| props.into_iter().map(Property::from).collect()).unwrap_or_else(Vec::new);
        
        let rel = &connected_pattern.relationship;
        let rel_alias = if let Some(alias) = rel.name { alias.to_string()} else {generate_id()};
        let rel_label = rel.label.map(|val| val.to_string());
        let rel_properties = rel.properties.clone().map(|props| props.into_iter().map(Property::from).collect()).unwrap_or_else(Vec::new);

        let end_node_ref = connected_pattern.end_node.borrow();
        let end_node_alias = if let Some(alias) = end_node_ref.name { alias.to_string()} else {generate_id()};
        let end_node_label = end_node_ref.label.map(|val| val.to_string());
        let end_node_props = end_node_ref.properties.clone().map(|props| props.into_iter().map(Property::from).collect()).unwrap_or_else(Vec::new);
        

        // if start alias already present in ctx map, it means the current nested connected pattern's start node will be connecting at right side plan and end node will be at the left
        if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&start_node_alias){
            if start_node_label.is_some() {
                table_ctx.set_label(start_node_label);
            }
            if !start_node_props.is_empty() {
                table_ctx.append_properties(start_node_props);
            }

            let end_graph_node = GraphNode {
                input: generate_scan(end_node_alias.clone(), None),
                alias: end_node_alias.clone(),
            };
            plan_ctx.insert_table_ctx(end_node_alias.clone(), TableCtx::build(end_node_alias.clone(), end_node_label, end_node_props, false, end_node_ref.name.is_some()));

            let graph_rel_node = GraphRel{
                left: Arc::new(LogicalPlan::GraphNode(end_graph_node)),
                center: generate_scan(rel_alias.clone(), None),
                right: plan.clone(),
                alias: rel_alias.clone(),
                direction: rel.direction.clone().into(),
                left_connection: end_node_alias,
                right_connection: start_node_alias,
                is_rel_anchor: false
            };
            plan_ctx.insert_table_ctx(rel_alias.clone(), TableCtx::build(rel_alias, rel_label, rel_properties, true, rel.name.is_some()));

            
            plan = Arc::new(LogicalPlan::GraphRel(graph_rel_node));
        }
        // if end alias already present in ctx map, it means the current nested connected pattern's end node will be connecting at right side plan and start node will be at the left
        else if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&end_node_alias) {
            if end_node_label.is_some() {
                table_ctx.set_label(end_node_label);
            }
            if !end_node_props.is_empty() {
                table_ctx.append_properties(end_node_props);
            }

            let start_graph_node = GraphNode {
                input: generate_scan(start_node_alias.clone(), None),
                alias: start_node_alias.clone(),
            };
            plan_ctx.insert_table_ctx(start_node_alias.clone(), TableCtx::build(start_node_alias.clone(), start_node_label, start_node_props, false, start_node_ref.name.is_some()));

            let graph_rel_node = GraphRel{
                left: Arc::new(LogicalPlan::GraphNode(start_graph_node)),
                center: generate_scan(rel_alias.clone(), None),
                right: plan.clone(),
                alias: rel_alias.clone(),
                direction: rel.direction.clone().into(),
                left_connection: start_node_alias,
                right_connection: end_node_alias,
                is_rel_anchor: false
            };
            plan_ctx.insert_table_ctx(rel_alias.clone(), TableCtx::build(rel_alias, rel_label, rel_properties, true, rel.name.is_some()));
           
            plan = Arc::new(LogicalPlan::GraphRel(graph_rel_node));

        }
        // not connected with existing nodes
        else {

            // if two comma separated patterns found and they are not connected to each other i.e. there is no common node alias between them then throw error.
            if path_pattern_idx > 0 {
                // throw error
                return Err(LogicalPlanError::DisconnectedPatternFound);
            }

            // we will keep start graph node at the right side and end at the left side
            let start_graph_node = GraphNode {
                input: generate_scan(start_node_alias.clone(), None),
                alias: start_node_alias.clone(),
            };
            plan_ctx.insert_table_ctx(start_node_alias.clone(), TableCtx::build(start_node_alias.clone(), start_node_label, start_node_props, false, start_node_ref.name.is_some()));

            let end_graph_node = GraphNode {
                input: generate_scan(end_node_alias.clone(), None),
                alias: end_node_alias.clone(),
            };
            plan_ctx.insert_table_ctx(end_node_alias.clone(), TableCtx::build(end_node_alias.clone(), end_node_label, end_node_props, false, end_node_ref.name.is_some()));


            let graph_rel_node = GraphRel{
                left: Arc::new(LogicalPlan::GraphNode(end_graph_node)),
                center: generate_scan(rel_alias.clone(), None),
                right: Arc::new(LogicalPlan::GraphNode(start_graph_node)),
                alias: rel_alias.clone(),
                direction: rel.direction.clone().into(),
                left_connection: end_node_alias,
                right_connection: start_node_alias,
                is_rel_anchor: false
            };
            plan_ctx.insert_table_ctx(rel_alias.clone(), TableCtx::build(rel_alias, rel_label, rel_properties, true, rel.name.is_some()));

            
            plan =  Arc::new(LogicalPlan::GraphRel(graph_rel_node));
        }

    }

    Ok(plan)
}

fn traverse_node_pattern(node_pattern: &ast::NodePattern, plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> LogicalPlanResult<Arc<LogicalPlan>> {
    

    // For now we are not supporting empty node. standalone node with name is supported.
    let node_alias = node_pattern.name.ok_or(LogicalPlanError::EmptyNode)?.to_string();
    let node_label = node_pattern.label.map(|val| val.to_string());
    let node_props = node_pattern.properties.clone().map(|props| props.into_iter().map(Property::from).collect()).unwrap_or_else(Vec::new);
    
    // if alias already present in ctx map then just add its conditions and do not add it in the logical plan
    if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&node_alias){
        if node_label.is_some() {
            table_ctx.set_label(node_label);
        }
        if !node_props.is_empty() {
            table_ctx.append_properties(node_props);
        }
        return Ok(plan);
    }else{
        // plan_ctx.alias_table_ctx_map.insert(node_alias.clone(), TableCtx { label: node_label, properties: node_props, filter_predicates: vec![], projection_items: vec![], is_rel: false, use_edge_list: false, explicit_alias: node_pattern.name.is_some() });
        plan_ctx.insert_table_ctx(node_alias.clone(), TableCtx::build(node_alias.clone(), node_label, node_props, false, node_pattern.name.is_some()));

        let graph_node = GraphNode {
            input: generate_scan(node_alias.clone(), None),
            alias: node_alias,
        };
        return Ok(Arc::new(LogicalPlan::GraphNode(graph_node)));
    }
}


pub fn evaluate_match_clause<'a>(
    match_clause: &ast::MatchClause<'a>,
    mut plan: Arc<LogicalPlan>,
    mut plan_ctx: &mut PlanCtx
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    for (idx, path_pattern) in match_clause.path_patterns.iter().enumerate() {
        match path_pattern {
            ast::PathPattern::Node(node_pattern) => {
                plan = traverse_node_pattern(node_pattern, plan, &mut plan_ctx)?;
            }
            ast::PathPattern::ConnectedPattern(connected_patterns) => {
                plan = traverse_connected_pattern(connected_patterns, plan, &mut plan_ctx, idx)?;
            }
        }
    }

    convert_properties_to_operator_application(plan_ctx)?;
    Ok(plan)

}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::logical_expr::{Direction, Literal, PropertyKVPair};
    use crate::open_cypher_parser::ast;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn test_convert_properties_with_kv_pairs() {
        let properties = vec![
            Property::PropertyKV(PropertyKVPair {
                key: "name".to_string(),
                value: Literal::String("John".to_string()),
            }),
            Property::PropertyKV(PropertyKVPair {
                key: "age".to_string(),
                value: Literal::Integer(30),
            }),
        ];

        let result = convert_properties(properties).unwrap();
        assert_eq!(result.len(), 2);

        // Check first property conversion
        match &result[0] {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                assert_eq!(op_app.operator, Operator::Equal);
                assert_eq!(op_app.operands.len(), 2);
                match &op_app.operands[0] {
                    LogicalExpr::Column(col) => assert_eq!(col.0, "name"),
                    _ => panic!("Expected Column"),
                }
                match &op_app.operands[1] {
                    LogicalExpr::Literal(Literal::String(s)) => assert_eq!(s, "John"),
                    _ => panic!("Expected String literal"),
                }
            },
            _ => panic!("Expected OperatorApplication"),
        }

        // Check second property conversion
        match &result[1] {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                assert_eq!(op_app.operator, Operator::Equal);
                match &op_app.operands[1] {
                    LogicalExpr::Literal(Literal::Integer(age)) => assert_eq!(*age, 30),
                    _ => panic!("Expected Integer literal"),
                }
            },
            _ => panic!("Expected OperatorApplication"),
        }
    }

    #[test]
    fn test_convert_properties_with_param_returns_error() {
        let properties = vec![
            Property::PropertyKV(PropertyKVPair {
                key: "name".to_string(),
                value: Literal::String("Alice".to_string()),
            }),
            Property::Param("param1".to_string()),
        ];

        let result = convert_properties(properties);
        assert!(result.is_err());
        match result.unwrap_err() {
            LogicalPlanError::FoundParamInProperties => (), // Expected error
            _ => panic!("Expected FoundParamInProperties error"),
        }
    }

    #[test]
    fn test_convert_properties_empty_list() {
        let properties = vec![];
        let result = convert_properties(properties).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_generate_id_uniqueness() {
        let id1 = generate_id();
        let id2 = generate_id();
        
        // IDs should be unique
        assert_ne!(id1, id2);
        
        // IDs should start with 'a'
        assert!(id1.starts_with('a'));
        assert!(id2.starts_with('a'));
        
        // IDs should be reasonable length (not too short or too long)
        assert!(id1.len() > 1 && id1.len() < 20);
        assert!(id2.len() > 1 && id2.len() < 20);
    }

    #[test]
    fn test_traverse_node_pattern_new_node() {
        let mut plan_ctx = PlanCtx::default();
        let initial_plan = Arc::new(LogicalPlan::Empty);

            let node_pattern = ast::NodePattern {
             name: Some("customer"),
             label: Some("Person"),
             properties: Some(vec![
                 ast::Property::PropertyKV(
                     ast::PropertyKVPair {
                         key: "city",
                         value: ast::Expression::Literal(ast::Literal::String("Boston")),
                     }
                 ),
             ]),
         };

        let result = traverse_node_pattern(&node_pattern, initial_plan.clone(), &mut plan_ctx).unwrap();

        // Should return a GraphNode plan
        match result.as_ref() {
            LogicalPlan::GraphNode(graph_node) => {
                assert_eq!(graph_node.alias, "customer");
                // Input should be a scan
                match graph_node.input.as_ref() {
                    LogicalPlan::Scan(scan) => {
                        assert_eq!(scan.table_alias, "customer");
                        assert_eq!(scan.table_name, None); // generate_scan sets table_name to label, but we pass None
                    },
                    _ => panic!("Expected Scan as input"),
                }
            },
            _ => panic!("Expected GraphNode"),
        }

        // Should have added entry to plan context
        let table_ctx = plan_ctx.get_table_ctx("customer").unwrap();
        assert_eq!(table_ctx.get_label_opt(), Some("Person".to_string()));
        // Note: properties get moved to filters after convert_properties_to_operator_application
        assert!(table_ctx.is_explicit_alias());
    }

    #[test]
    fn test_traverse_node_pattern_existing_node() {
        let mut plan_ctx = PlanCtx::default();
        let initial_plan = Arc::new(LogicalPlan::Empty);

        // Pre-populate plan context with existing node
        plan_ctx.insert_table_ctx(
            "customer".to_string(),
            TableCtx::build(
                "customer".to_string(),
                Some("User".to_string()),
                vec![],
                false,
                true,
            ),
        );

        let node_pattern = ast::NodePattern {
             name: Some("customer"),
             label: Some("Person"), // Different label
             properties: Some(vec![
                 ast::Property::PropertyKV(
                     ast::PropertyKVPair {
                         key: "age",
                         value: ast::Expression::Literal(ast::Literal::Integer(25)),
                     }
                 ),
             ]),
         };

        let result = traverse_node_pattern(&node_pattern, initial_plan.clone(), &mut plan_ctx).unwrap();

        // Should return the same plan (not create new GraphNode)
        assert_eq!(result, initial_plan);

        // Should have updated the existing table context
        let table_ctx = plan_ctx.get_table_ctx("customer").unwrap();
        assert_eq!(table_ctx.get_label_opt(), Some("Person".to_string())); // Label should be updated
        // Note: properties get moved to filters after convert_properties_to_operator_application
    }

    #[test]
    fn test_traverse_node_pattern_empty_node_error() {
        let mut plan_ctx = PlanCtx::default();
        let initial_plan = Arc::new(LogicalPlan::Empty);

        let node_pattern = ast::NodePattern {
            name: None, // Empty node
            label: Some("Person"),
            properties: None,
        };

        let result = traverse_node_pattern(&node_pattern, initial_plan, &mut plan_ctx);
        assert!(result.is_err());
        match result.unwrap_err() {
            LogicalPlanError::EmptyNode => (), // Expected error
            _ => panic!("Expected EmptyNode error"),
        }
    }

    #[test]
    fn test_traverse_connected_pattern_new_connection() {
        let mut plan_ctx = PlanCtx::default();
        let initial_plan = Arc::new(LogicalPlan::Empty);

        let start_node = ast::NodePattern {
            name: Some("user"),
            label: Some("Person"),
            properties: None,
        };

        let end_node = ast::NodePattern {
            name: Some("company"),
            label: Some("Organization"),
            properties: None,
        };

                 let relationship = ast::RelationshipPattern {
             name: Some("works_at"),
             direction: ast::Direction::Outgoing,
             label: Some("WORKS_AT"),
             properties: None,
         };

        let connected_pattern = ast::ConnectedPattern {
            start_node: Rc::new(RefCell::new(start_node)),
            relationship,
            end_node: Rc::new(RefCell::new(end_node)),
        };

        let connected_patterns = vec![connected_pattern];

        let result = traverse_connected_pattern(&connected_patterns, initial_plan, &mut plan_ctx, 0).unwrap();

        // Should return a GraphRel plan
        match result.as_ref() {
            LogicalPlan::GraphRel(graph_rel) => {
                assert_eq!(graph_rel.alias, "works_at");
                assert_eq!(graph_rel.direction, Direction::Outgoing);
                assert_eq!(graph_rel.left_connection, "company");
                assert_eq!(graph_rel.right_connection, "user");
                assert!(!graph_rel.is_rel_anchor);

                // Check left side (end node)
                match graph_rel.left.as_ref() {
                    LogicalPlan::GraphNode(left_node) => {
                        assert_eq!(left_node.alias, "company");
                    },
                    _ => panic!("Expected GraphNode on left"),
                }

                // Check right side (start node)
                match graph_rel.right.as_ref() {
                    LogicalPlan::GraphNode(right_node) => {
                        assert_eq!(right_node.alias, "user");
                    },
                    _ => panic!("Expected GraphNode on right"),
                }
            },
            _ => panic!("Expected GraphRel"),
        }

        // Should have added entries to plan context
        assert!(plan_ctx.get_table_ctx("user").is_ok());
        assert!(plan_ctx.get_table_ctx("company").is_ok());
        assert!(plan_ctx.get_table_ctx("works_at").is_ok());

        let rel_ctx = plan_ctx.get_table_ctx("works_at").unwrap();
        assert!(rel_ctx.is_relation());
    }

    #[test]
    fn test_traverse_connected_pattern_with_existing_start_node() {
        let mut plan_ctx = PlanCtx::default();
        let initial_plan = Arc::new(LogicalPlan::Empty);

        // Pre-populate with existing start node
        plan_ctx.insert_table_ctx(
            "user".to_string(),
            TableCtx::build("user".to_string(), Some("Person".to_string()), vec![], false, true),
        );

        let start_node = ast::NodePattern {
            name: Some("user"), // This exists in plan_ctx
            label: Some("Employee"), // Different label
            properties: None,
        };

        let end_node = ast::NodePattern {
            name: Some("project"),
            label: Some("Project"),
            properties: None,
        };

        let relationship = ast::RelationshipPattern {
             name: Some("assigned_to"),
             direction: ast::Direction::Incoming,
             label: Some("ASSIGNED_TO"),
             properties: None,
         };

        let connected_pattern = ast::ConnectedPattern {
            start_node: Rc::new(RefCell::new(start_node)),
            relationship,
            end_node: Rc::new(RefCell::new(end_node)),
        };

        let connected_patterns = vec![connected_pattern];

        let result = traverse_connected_pattern(&connected_patterns, initial_plan, &mut plan_ctx, 0).unwrap();

        // Should return a GraphRel plan with different structure
        match result.as_ref() {
            LogicalPlan::GraphRel(graph_rel) => {
                assert_eq!(graph_rel.alias, "assigned_to");
                assert_eq!(graph_rel.direction, Direction::Incoming);
                assert_eq!(graph_rel.left_connection, "project");
                assert_eq!(graph_rel.right_connection, "user");

                // Left should be the new end node
                match graph_rel.left.as_ref() {
                    LogicalPlan::GraphNode(left_node) => {
                        assert_eq!(left_node.alias, "project");
                    },
                    _ => panic!("Expected GraphNode on left"),
                }
            },
            _ => panic!("Expected GraphRel"),
        }

        // Existing start node should have updated label
        let user_ctx = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(user_ctx.get_label_opt(), Some("Employee".to_string()));
    }

    #[test]
    fn test_traverse_connected_pattern_disconnected_error() {
        let mut plan_ctx = PlanCtx::default();
        let initial_plan = Arc::new(LogicalPlan::Empty);

        let start_node = ast::NodePattern {
            name: Some("user1"),
            label: Some("Person"),
            properties: None,
        };

        let end_node = ast::NodePattern {
            name: Some("user2"),
            label: Some("Person"),
            properties: None,
        };

        let relationship = ast::RelationshipPattern {
            name: Some("knows"),
            direction: ast::Direction::Either,
            label: Some("KNOWS"),
            properties: None,
        };

        let connected_pattern = ast::ConnectedPattern {
            start_node: Rc::new(RefCell::new(start_node)),
            relationship,
            end_node: Rc::new(RefCell::new(end_node)),
        };

        let connected_patterns = vec![connected_pattern];

        // Pass path_pattern_idx > 0 to simulate second pattern that's disconnected
        let result = traverse_connected_pattern(&connected_patterns, initial_plan, &mut plan_ctx, 1);

        assert!(result.is_err());
        match result.unwrap_err() {
            LogicalPlanError::DisconnectedPatternFound => (), // Expected error
            _ => panic!("Expected DisconnectedPatternFound error"),
        }
    }

    #[test]
    fn test_evaluate_match_clause_with_node_and_connected_pattern() {
        let mut plan_ctx = PlanCtx::default();
        let initial_plan = Arc::new(LogicalPlan::Empty);

        // Create a match clause with both node pattern and connected pattern
        let node_pattern = ast::NodePattern {
            name: Some("admin"),
            label: Some("User"),
            properties: Some(vec![
                ast::Property::PropertyKV(
                    ast::PropertyKVPair {
                        key: "role",
                        value: ast::Expression::Literal(ast::Literal::String("administrator")),
                    }
                ),
            ]),
        };

        let start_node = ast::NodePattern {
            name: Some("admin"), // Same as above - should connect
            label: None,
            properties: None,
        };

        let end_node = ast::NodePattern {
            name: Some("system"),
            label: Some("System"),
            properties: None,
        };

        let relationship = ast::RelationshipPattern {
            name: Some("manages"),
            direction: ast::Direction::Outgoing,
            label: Some("MANAGES"),
            properties: None,
        };

        let connected_pattern = ast::ConnectedPattern {
            start_node: Rc::new(RefCell::new(start_node)),
            relationship,
            end_node: Rc::new(RefCell::new(end_node)),
        };

        let match_clause = ast::MatchClause {
            path_patterns: vec![
                ast::PathPattern::Node(node_pattern),
                ast::PathPattern::ConnectedPattern(vec![connected_pattern]),
            ],
        };

        let result = evaluate_match_clause(&match_clause, initial_plan, &mut plan_ctx).unwrap();

        // Should return a GraphRel plan
        match result.as_ref() {
            LogicalPlan::GraphRel(graph_rel) => {
                assert_eq!(graph_rel.alias, "manages");
                assert_eq!(graph_rel.direction, Direction::Outgoing);
            },
            _ => panic!("Expected GraphRel at top level"),
        }

        // Properties should have been converted to filters
        let admin_ctx = plan_ctx.get_table_ctx("admin").unwrap();
        assert_eq!(admin_ctx.get_filters().len(), 1);
        assert!(admin_ctx.should_use_edge_list()); // Should be true because properties were found
    }

    #[test]
    fn test_convert_properties_to_operator_application() {
        let mut plan_ctx = PlanCtx::default();
        
        // Add table context with properties
        let properties = vec![
            Property::PropertyKV(PropertyKVPair {
                key: "status".to_string(),
                value: Literal::String("active".to_string()),
            }),
        ];
        
        let table_ctx = TableCtx::build(
            "user".to_string(),
            Some("Person".to_string()),
            properties,
            false,
            true,
        );
        
        plan_ctx.insert_table_ctx("user".to_string(), table_ctx);

        // Before conversion, table should have no filters
        let table_ctx_before = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(table_ctx_before.get_filters().len(), 0);
        assert!(!table_ctx_before.should_use_edge_list());

        // Convert properties
        let result = convert_properties_to_operator_application(&mut plan_ctx);
        assert!(result.is_ok());

        // After conversion, properties should be moved to filters
        let table_ctx_after = plan_ctx.get_table_ctx("user").unwrap();
        assert_eq!(table_ctx_after.get_filters().len(), 1); // Filter added
        assert!(table_ctx_after.should_use_edge_list()); // use_edge_list should be true

        // Check the filter predicate
        match &table_ctx_after.get_filters()[0] {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                assert_eq!(op_app.operator, Operator::Equal);
                match &op_app.operands[0] {
                    LogicalExpr::Column(col) => assert_eq!(col.0, "status"),
                    _ => panic!("Expected Column"),
                }
            },
            _ => panic!("Expected OperatorApplication"),
        }
    }

    #[test]
    fn test_generate_scan() {
        let scan = generate_scan("customers".to_string(), Some("Customer".to_string()));
        
        match scan.as_ref() {
            LogicalPlan::Scan(scan_plan) => {
                assert_eq!(scan_plan.table_alias, "customers");
                assert_eq!(scan_plan.table_name, Some("Customer".to_string()));
            },
            _ => panic!("Expected Scan plan"),
        }
    }
}


