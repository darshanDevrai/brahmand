use crate::{
    open_cypher_parser::ast::{Expression, ReturnClause, ReturnItem},
    query_engine::types::{LogicalPlan, ReturnItemData},
};

use super::errors::PlannerError;

pub fn evaluate_return_clause<'a>(
    mut logical_plan: LogicalPlan<'a>,
    return_clause: ReturnClause<'a>,
    add_in_overall: bool,
) -> Result<LogicalPlan<'a>, PlannerError> {
    for current_return_item in return_clause.return_items {
        // add in alias to return item lookup

        if let Some(alias_str) = current_return_item.alias {
            logical_plan
                .return_item_by_alias
                .insert(alias_str, current_return_item.clone());
        }

        // handle expression here
        match &current_return_item.expression.clone() {
            Expression::Literal(_) => {
                // add it in combined return item
                let return_item_data = ReturnItemData {
                    return_item: current_return_item,
                    belongs_to_table: "",
                };
                logical_plan.overall_return_items.push(return_item_data);
            }
            Expression::Variable(var) => {
                // tag it to particular table. If it is not table_name name throw error

                if *var == "*" {
                    let return_item_data = ReturnItemData {
                        return_item: current_return_item,
                        belongs_to_table: "",
                    };
                    logical_plan.overall_return_items.push(return_item_data);
                } else {
                    let uid = logical_plan
                        .entity_name_uid_map
                        .get(*var)
                        .ok_or(PlannerError::InvalidVariableInReturnClause)?;
                    if let Some(table_data) = logical_plan.table_data_by_uid.get_mut(uid) {
                        table_data.return_items.push(current_return_item.clone());

                        if add_in_overall {
                            let return_item_data = ReturnItemData {
                                return_item: current_return_item,
                                belongs_to_table: var,
                            };
                            logical_plan.overall_return_items.push(return_item_data);
                        }
                    } else {
                        // thorw error
                        return Err(PlannerError::OrphanPropertyInReturnClause);
                    }
                }
            }
            Expression::FunctionCallExp(function_call) => {
                // check for fn calls for attaching to particular table.
                // check the argument expression type. It has to be of type variable(table name) or property access
                // we are still keeping it in combined because we just want this node to select the required property.

                let attach_table_name = "";
                let mut return_items: Vec<ReturnItem> = vec![];
                for arg in &function_call.args {
                    // if arg is just a variable with entity name or '*' then skip
                    // e.g. MATCH (p:Post) RETURN COUNT(p)  Here p is entity name
                    if let Expression::Variable(var) = arg.clone() {
                        if logical_plan.entity_name_uid_map.contains_key(var) || var == "*" {
                            continue;
                        }
                    }

                    let return_item = ReturnItem {
                        expression: arg.clone(),
                        alias: None,
                    };
                    return_items.push(return_item);
                }
                logical_plan =
                    evaluate_return_clause(logical_plan, ReturnClause { return_items }, false)?;

                if add_in_overall {
                    let return_item_data = ReturnItemData {
                        return_item: current_return_item,
                        belongs_to_table: attach_table_name,
                    };
                    logical_plan.overall_return_items.push(return_item_data);
                }
            }
            Expression::PropertyAccessExp(property_access) => {
                // same here as earlier but with only property access
                let uid = logical_plan
                    .entity_name_uid_map
                    .get(property_access.base)
                    .ok_or(PlannerError::InvalidPropAccessInReturnClause)?;

                let table_data = logical_plan
                    .table_data_by_uid
                    .get_mut(uid)
                    .ok_or(PlannerError::OrphanPropertyAccessInReturnClause)?;

                table_data.return_items.push(current_return_item.clone());

                if add_in_overall {
                    let return_item_data = ReturnItemData {
                        return_item: current_return_item,
                        belongs_to_table: property_access.base,
                    };
                    logical_plan.overall_return_items.push(return_item_data);
                }
            }
            Expression::OperatorApplicationExp(op_app) => {
                let mut return_items: Vec<ReturnItem> = vec![];
                for operand in &op_app.operands {
                    let return_item = ReturnItem {
                        expression: operand.clone(),
                        alias: None,
                    };
                    return_items.push(return_item);
                }
                logical_plan =
                    evaluate_return_clause(logical_plan, ReturnClause { return_items }, false)?;

                if add_in_overall {
                    let return_item_data = ReturnItemData {
                        return_item: current_return_item,
                        belongs_to_table: "",
                    };
                    logical_plan.overall_return_items.push(return_item_data);
                }
            }
            // Expression::PathPattern(path_pattern) => todo!(),
            _ => {
                return Err(PlannerError::UnsupportedItemInReturnClause);
            }
        }
    }

    Ok(logical_plan)
}

#[cfg(test)]
mod tests {

    use uuid::Uuid;

    use crate::{
        open_cypher_parser::ast::{Literal, PropertyAccess},
        query_engine::types::TableData,
    };

    use super::*;

    // evaluate_return_clause

    /// Helper to build a simple PropertyAccessExp
    fn make_prop<'a>(base: &'a str, key: &'a str) -> Expression<'a> {
        Expression::PropertyAccessExp(PropertyAccess { base, key })
    }

    #[test]
    fn literal_return_adds_to_overall() {
        let plan = LogicalPlan::default();
        let rc = ReturnClause {
            return_items: vec![ReturnItem {
                expression: Expression::Literal(Literal::Integer(10)),
                alias: None,
            }],
        };

        let result = evaluate_return_clause(plan, rc, true).unwrap();
        assert_eq!(result.overall_return_items.len(), 1);
        let rid = &result.overall_return_items[0].return_item;
        assert_eq!(rid.expression, Expression::Literal(Literal::Integer(10)));
        assert_eq!(result.overall_return_items[0].belongs_to_table, "");
    }

    #[test]
    fn variable_return_appends_to_table() {
        let mut plan = LogicalPlan::default();
        let uid = Uuid::new_v4();
        plan.entity_name_uid_map.insert("t".to_string(), uid);
        plan.table_data_by_uid.insert(
            uid,
            TableData {
                entity_name: Some("t"),
                table_name: Some("T"),
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );

        let rc = ReturnClause {
            return_items: vec![ReturnItem {
                expression: Expression::Variable("t"),
                alias: Some("alias"),
            }],
        };

        let result = evaluate_return_clause(plan.clone(), rc, false).unwrap();
        // Should have placed the item into table_data_by_uid
        let td = result.table_data_by_uid.get(&uid).unwrap();
        assert_eq!(td.return_items.len(), 1);
        assert_eq!(td.return_items[0].expression, Expression::Variable("t"));
        // overall_return_items remains empty when add_in_overall = false
        assert!(result.overall_return_items.is_empty());
    }

    #[test]
    fn variable_return_with_overall_flag() {
        let mut plan = LogicalPlan::default();
        let uid = Uuid::new_v4();
        plan.entity_name_uid_map.insert("u".to_string(), uid);
        plan.table_data_by_uid.insert(
            uid,
            TableData {
                entity_name: Some("u"),
                table_name: Some("U"),
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );

        let rc = ReturnClause {
            return_items: vec![ReturnItem {
                expression: Expression::Variable("u"),
                alias: None,
            }],
        };

        let result = evaluate_return_clause(plan.clone(), rc, true).unwrap();
        // Table should have the return item
        let td = result.table_data_by_uid.get(&uid).unwrap();
        assert_eq!(td.return_items.len(), 1);
        // overall_return_items should now include one entry
        assert_eq!(result.overall_return_items.len(), 1);
        assert_eq!(
            result.overall_return_items[0].return_item.expression,
            Expression::Variable("u")
        );
        assert_eq!(result.overall_return_items[0].belongs_to_table, "u");
    }

    #[test]
    fn property_access_return_appends_to_table_and_overall() {
        let mut plan = LogicalPlan::default();
        let uid = Uuid::new_v4();
        plan.entity_name_uid_map.insert("x".to_string(), uid);
        plan.table_data_by_uid.insert(
            uid,
            TableData {
                entity_name: Some("x"),
                table_name: Some("X"),
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );

        let rc = ReturnClause {
            return_items: vec![ReturnItem {
                expression: make_prop("x", "field"),
                alias: None,
            }],
        };

        let result = evaluate_return_clause(plan.clone(), rc, true).unwrap();
        let td = result.table_data_by_uid.get(&uid).unwrap();
        assert_eq!(td.return_items.len(), 1);
        assert_eq!(
            td.return_items[0].expression,
            Expression::PropertyAccessExp(PropertyAccess {
                base: "x",
                key: "field"
            })
        );
        // overall_return_items should record it
        assert_eq!(result.overall_return_items.len(), 1);
        assert_eq!(result.overall_return_items[0].belongs_to_table, "x");
    }

    #[test]
    fn invalid_variable_returns_error() {
        let plan = LogicalPlan::default();
        let rc = ReturnClause {
            return_items: vec![ReturnItem {
                expression: Expression::Variable("nope"),
                alias: None,
            }],
        };
        let err = evaluate_return_clause(plan, rc, false).unwrap_err();
        assert!(matches!(err, PlannerError::InvalidVariableInReturnClause));
    }

    #[test]
    fn orphan_variable_returns_error() {
        let mut plan = LogicalPlan::default();
        // Map name but leave out table_data_by_uid
        plan.entity_name_uid_map
            .insert("o".to_string(), Uuid::new_v4());
        let rc = ReturnClause {
            return_items: vec![ReturnItem {
                expression: Expression::Variable("o"),
                alias: None,
            }],
        };
        let err = evaluate_return_clause(plan, rc, false).unwrap_err();
        assert!(matches!(err, PlannerError::OrphanPropertyInReturnClause));
    }

    #[test]
    fn invalid_property_access_returns_error() {
        let plan = LogicalPlan::default();
        let rc = ReturnClause {
            return_items: vec![ReturnItem {
                expression: make_prop("y", "k"),
                alias: None,
            }],
        };
        let err = evaluate_return_clause(plan, rc, false).unwrap_err();
        assert!(matches!(err, PlannerError::InvalidPropAccessInReturnClause));
    }

    #[test]
    fn orphan_property_access_returns_error() {
        let mut plan = LogicalPlan::default();
        let uid = Uuid::new_v4();
        plan.entity_name_uid_map.insert("z".to_string(), uid);
        // Omit table_data_by_uid for "z"
        let rc = ReturnClause {
            return_items: vec![ReturnItem {
                expression: make_prop("z", "k"),
                alias: None,
            }],
        };
        let err = evaluate_return_clause(plan, rc, false).unwrap_err();
        assert!(matches!(
            err,
            PlannerError::OrphanPropertyAccessInReturnClause
        ));
    }

    #[test]
    fn unsupported_expression_returns_error() {
        let plan = LogicalPlan::default();
        let rc = ReturnClause {
            return_items: vec![ReturnItem {
                // e.g., a list literal is not supported in return clause
                expression: Expression::List(vec![]),
                alias: None,
            }],
        };
        let err = evaluate_return_clause(plan, rc, false).unwrap_err();
        assert!(matches!(err, PlannerError::UnsupportedItemInReturnClause));
    }
}
