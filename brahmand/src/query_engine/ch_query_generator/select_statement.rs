use std::collections::HashMap;

use crate::{
    open_cypher_parser::ast::{Expression, Operator, ReturnItem},
    query_engine::types::ReturnItemData,
};

use super::{common::get_literal_to_string, errors::ChQueryGeneratorError};

fn process_return_expression_string(
    expression: &Expression,
    entity_name_node_id_map: &HashMap<String, String>,
    is_final_node: bool,
    fn_arg_or_unary_op: bool,
) -> Result<String, ChQueryGeneratorError> {
    match expression {
        Expression::OperatorApplicationExp(op) => {
            let operator_string: String = op.operator.into();

            if op.operands.len() == 1 {
                // it could be unary or postfix
                // e.g unary = Not, Distinct & postfix = IsNull, IsNotNull
                // e.g distinct name |  e.g. city IS NULL

                let operand = op
                    .operands
                    .first()
                    .ok_or(ChQueryGeneratorError::NoOperandFoundInReturnClause)?;

                let distinct_op_str: String = Operator::Distinct.into();

                let is_not_distinct =
                    operator_string.to_lowercase() != distinct_op_str.to_lowercase();

                let operand_string = process_return_expression_string(
                    &operand.clone(),
                    entity_name_node_id_map,
                    is_final_node,
                    is_not_distinct,
                )?;

                if op.operator == Operator::Distinct || op.operator == Operator::Not {
                    // process unary
                    let condition_string = format!("{} {}", operator_string, operand_string);
                    return Ok(condition_string);
                }
                // else if op.operator == Operator::IsNull || op.operator == Operator::IsNotNull {
                // process postfix
                let condition_string = format!("{} {}", operand_string, operator_string);
                return Ok(condition_string);
                // }
            }

            let first_operand = op
                .operands
                .first()
                .ok_or(ChQueryGeneratorError::NoOperandFoundInReturnClause)?;
            let first_operand_string = process_return_expression_string(
                &first_operand.clone(),
                entity_name_node_id_map,
                is_final_node,
                fn_arg_or_unary_op,
            )?;

            let second_operand = op
                .operands
                .get(1)
                .ok_or(ChQueryGeneratorError::NoOperandFoundInReturnClause)?;
            let second_operand_string = process_return_expression_string(
                &second_operand.clone(),
                entity_name_node_id_map,
                is_final_node,
                fn_arg_or_unary_op,
            )?;

            let condition_string = format!(
                "{} {} {}",
                first_operand_string, operator_string, second_operand_string
            );
            Ok(condition_string)
        }
        Expression::Literal(literal) => Ok(get_literal_to_string(literal)),
        Expression::List(expressions) => {
            let mut new_exprs = Vec::new();
            for sub_expr in expressions {
                let new_expr = process_return_expression_string(
                    sub_expr,
                    entity_name_node_id_map,
                    is_final_node,
                    fn_arg_or_unary_op,
                )?;
                new_exprs.push(new_expr);
            }
            let list_string = format!("[{}]", new_exprs.join(","));
            Ok(list_string)
        }
        Expression::FunctionCallExp(fn_call) => {
            let mut new_args = Vec::new();
            for arg in fn_call.args.clone() {
                let new_expr = process_return_expression_string(
                    &arg,
                    entity_name_node_id_map,
                    is_final_node,
                    true,
                )?;
                new_args.push(new_expr);
            }
            let fn_call_string = format!("{}({})", fn_call.name, new_args.join(","));
            Ok(fn_call_string)
        }
        Expression::PropertyAccessExp(property_access) => {
            if is_final_node {
                Ok(format!("{}.{}", property_access.base, property_access.key))
            } else {
                Ok(property_access.key.to_string())
            }
        }

        Expression::Variable(var) => {
            // variables are usually column names but if it is just a node name then we need to add node id
            // e.g. COUNT(p). Here var will become 'p' in that case we will add 'p.node_id'
            for (entity_name, node_id) in entity_name_node_id_map.iter() {
                if entity_name == var && fn_arg_or_unary_op {
                    return Ok(format!("{}.{}", entity_name, node_id));
                }
            }
            if is_final_node && *var != "*" {
                return Ok(format!("{}.*", var));
            }
            // Ok(var.to_string())
            Ok("*".to_string())
        }
        _ => Err(ChQueryGeneratorError::UnsupportedItemInReturnClause), // Expression::Parameter(_) => todo!(),
                                                                        // Expression::PathPattern(path_pattern) => todo!(),
    }
}

pub fn generate_final_select_statements(
    return_items: Vec<ReturnItemData>,
    entity_name_node_id_map: &HashMap<String, String>,
) -> Result<(String, String), ChQueryGeneratorError> {
    let mut select_items: Vec<String> = vec![];
    // for now we are ignoring only fn call expressions from group by. Ideally we should check for Aggregated fns from clickhouse and ignore only for agg fns.
    let mut group_by_items: Vec<String> = vec![];

    for return_item_data in return_items {
        let mut alias_string = "".to_string();
        let mut alias = "".to_string();
        if let Some(inner_alias) = return_item_data.return_item.alias {
            alias = inner_alias.to_string();
            alias_string = format!(" AS {}", inner_alias.to_owned());
        }

        let mut select_item = process_return_expression_string(
            &return_item_data.return_item.expression,
            entity_name_node_id_map,
            true,
            false,
        )?;

        // variable, fn and prop access
        match &return_item_data.return_item.expression {
            Expression::Variable(_) => {
                if select_item.contains("*") {
                    group_by_items.push(select_item.clone());
                } else {
                    select_item = format!("{}{}", select_item, alias_string);
                }
            }
            Expression::FunctionCallExp(_) => {
                select_item = format!("{}{}", select_item, alias_string);
            }
            Expression::PropertyAccessExp(_) => {
                // let prop_access_select_string = format!("{}.{}{}", belongs_to_table_alias, prop_access.key, alias_string);
                select_item = format!("{}{}", select_item, alias_string);
                if !alias.is_empty() {
                    group_by_items.push(alias);
                } else {
                    group_by_items.push(select_item.clone());
                }
            }
            Expression::Literal(_) => {
                select_item = format!("{}{}", select_item, alias_string);
                group_by_items.push(alias);
            }
            Expression::OperatorApplicationExp(op) => {
                // select_item = format!("{}{}", select_item, alias_string);
                // if operator is unary then add just the operand which in the group by if the alias is not present
                // e.g. DISTINCT u.Id as userId then add userId in group by
                // e.g. DISTINCT u.Id -> add u.Id in group by
                select_item = format!("{}{}", select_item, alias_string);
                // println!("\n aalias_string {:?} select_item {:?}",alias_string, select_item);
                if !alias.is_empty() {
                    group_by_items.push(alias);
                } else if op.operands.len() == 1 {
                    let operand = op
                        .operands
                        .first()
                        .ok_or(ChQueryGeneratorError::NoOperandFoundInWhereClause)?;
                    let operand_str = process_return_expression_string(
                        operand,
                        entity_name_node_id_map,
                        true,
                        false,
                    )?;
                    group_by_items.push(operand_str);
                } else {
                    group_by_items.push(select_item.clone());
                }
            }
            _ => {
                // throw error
                return Err(ChQueryGeneratorError::UnsupportedItemInFinalNodeSelectClause);
            }
        }

        select_items.push(select_item);
    }

    let select_statement = select_items.join(", ");
    let mut group_by_statement = group_by_items.join(", ");
    if !group_by_items.is_empty() {
        group_by_statement = format!("GROUP BY {}", group_by_statement);
    };
    Ok((select_statement, group_by_statement))
}

pub fn generate_node_select_statements(
    return_items: Vec<ReturnItem>,
    node_id_column: &str,
) -> Result<String, ChQueryGeneratorError> {
    // Node Id is present always
    let mut select_items: Vec<String> = vec![node_id_column.to_string()];

    let empty_map = HashMap::new();
    for return_item in &return_items {
        // skip if it's a prop_access and the key matches with node_id_column
        if let Expression::PropertyAccessExp(prop_access) = &return_item.expression {
            if prop_access.key == node_id_column {
                continue;
            }
        }

        // otherwise process and add in the select items
        let select_item =
            process_return_expression_string(&return_item.expression, &empty_map, false, false)?;

        // in intermediate node select statements, if '*' is present then we will drop everything and keep only '*'
        if select_item.contains("*") {
            select_items = vec![select_item];
            break;
        }

        select_items.push(select_item);
    }

    let select_statement = select_items.join(", ");

    Ok(select_statement)
}

pub fn generate_relationship_select_statements(
    return_items: Vec<ReturnItem>,
    node_id_column: &str,
) -> Result<String, ChQueryGeneratorError> {
    // Id is present always
    // let mut select_items: Vec<String> = vec!["from_id".to_string()];
    let mut select_items: Vec<String> = vec![node_id_column.to_string()];

    select_items.push("arrayJoin(bitmapToArray(to_id)) AS to_id".to_string());

    for return_item in return_items {
        // variable, fn and prop access
        match &return_item.expression {
            Expression::Variable(var) => {
                select_items.push(var.to_string());
            }
            Expression::FunctionCallExp(fn_call) => {
                let arg_strings: Vec<String> = fn_call
                    .args
                    .iter()
                    .map(|arg| {
                        if let Expression::Literal(literal) = arg {
                            get_literal_to_string(literal)
                        } else {
                            "".to_string()
                        }
                    })
                    .collect();

                let arguments = arg_strings.join(",");

                let fn_call_string = format!("{}({})", fn_call.name, arguments);
                select_items.push(fn_call_string);
            }
            Expression::PropertyAccessExp(prop_access) => {
                // if it is Id then we have already added it on top
                if prop_access.key != node_id_column {
                    let prop_access_select_string = prop_access.key.to_string();
                    select_items.push(prop_access_select_string);
                }
            }
            _ => {
                // throw error
                return Err(ChQueryGeneratorError::UnsupportedItemInRelSelectClause);
            }
        }
    }

    let select_statement = select_items.join(", ");

    Ok(select_statement)
}

#[cfg(test)]
mod tests {
    use crate::open_cypher_parser::ast::{
        FunctionCall, Literal, OperatorApplication, PropertyAccess,
    };

    use super::*;

    // Helper to build an OperatorApplicationExp
    fn op_app(op: Operator, operands: Vec<Expression>) -> Expression {
        Expression::OperatorApplicationExp(OperatorApplication {
            operator: op,
            operands,
        })
    }

    // Helper to build a FunctionCallExp
    fn fn_call<'a>(name: &'a str, args: Vec<Expression<'a>>) -> Expression<'a> {
        Expression::FunctionCallExp(FunctionCall {
            name: name.to_string(),
            args,
        })
    }

    // Helper to build ReturnItemData
    fn return_data_builder<'a>(expr: Expression<'a>, alias: Option<&'a str>) -> ReturnItemData<'a> {
        ReturnItemData {
            return_item: ReturnItem {
                expression: expr,
                alias,
            },
            belongs_to_table: "T",
        }
    }

    // Build a ReturnItem with given expression and no alias
    fn return_item_builder<'a>(expr: Expression<'a>) -> ReturnItem<'a> {
        ReturnItem {
            expression: expr,
            alias: None,
        }
    }

    // process_return_expression_string

    #[test]
    fn literal_integer_in_return() {
        let expr = Expression::Literal(Literal::Integer(42));
        let out = process_return_expression_string(&expr, &HashMap::new(), false, false).unwrap();
        assert_eq!(out, "42");
    }

    #[test]
    fn list_of_literals() {
        let expr = Expression::List(vec![
            Expression::Literal(Literal::Integer(1)),
            Expression::Literal(Literal::Integer(2)),
        ]);
        let out = process_return_expression_string(&expr, &HashMap::new(), false, false).unwrap();
        assert_eq!(out, "[1,2]");
    }

    #[test]
    fn function_call_in_return() {
        let expr = fn_call(
            "sum",
            vec![
                Expression::Literal(Literal::Integer(3)),
                Expression::Literal(Literal::Integer(5)),
            ],
        );
        let out = process_return_expression_string(&expr, &HashMap::new(), false, false).unwrap();
        assert_eq!(out, "sum(3,5)");
    }

    #[test]
    fn binary_operator_in_return() {
        let expr = op_app(
            Operator::Addition,
            vec![
                Expression::Literal(Literal::Integer(10)),
                Expression::Literal(Literal::Integer(5)),
            ],
        );
        let out = process_return_expression_string(&expr, &HashMap::new(), false, false).unwrap();
        assert_eq!(out, "10 + 5");
    }

    #[test]
    fn unary_distinct_in_final_node() {
        let expr = op_app(Operator::Distinct, vec![Expression::Variable("x")]);
        let mut map = HashMap::new();
        map.insert("x".to_string(), "nid".to_string());
        let out = process_return_expression_string(&expr, &map, true, false).unwrap();
        assert_eq!(out, "DISTINCT x.*");
    }

    #[test]
    fn postfix_is_null_in_return() {
        let expr = op_app(Operator::IsNull, vec![Expression::Variable("city")]);
        let mut map = HashMap::new();
        map.insert("city".to_string(), "nid".to_string());
        let out = process_return_expression_string(&expr, &map, false, false).unwrap();
        assert_eq!(out, "city.nid IS NULL");
    }

    #[test]
    fn property_access_non_final_in_return() {
        let expr = Expression::PropertyAccessExp(PropertyAccess {
            base: "n",
            key: "k",
        });
        let out = process_return_expression_string(&expr, &HashMap::new(), false, false).unwrap();
        assert_eq!(out, "k");
    }

    #[test]
    fn property_access_final_in_return() {
        let expr = Expression::PropertyAccessExp(PropertyAccess {
            base: "n",
            key: "k",
        });
        let out = process_return_expression_string(&expr, &HashMap::new(), true, false).unwrap();
        assert_eq!(out, "n.k");
    }

    #[test]
    fn variable_lookup_and_plain() {
        let mut map = HashMap::new();
        map.insert("p".to_string(), "nid".to_string());
        // Lookup case
        let expr = Expression::Variable("p");
        let out = process_return_expression_string(&expr, &map, false, false).unwrap();
        assert_eq!(out, "*");
        // final node true
        let out = process_return_expression_string(&expr, &map, true, false).unwrap();
        assert_eq!(out, "p.*");
        // Plain variable
        let expr2 = Expression::Variable("z");
        let out2 = process_return_expression_string(&expr2, &map, false, false).unwrap();
        assert_eq!(out2, "*");
    }

    #[test]
    fn unsupported_parameter() {
        let expr = Expression::Parameter("x");
        let err =
            process_return_expression_string(&expr, &HashMap::new(), false, false).unwrap_err();
        assert!(matches!(
            err,
            ChQueryGeneratorError::UnsupportedItemInReturnClause
        ));
    }

    #[test]
    fn no_operand_error_binary() {
        let expr = op_app(Operator::Addition, vec![]);
        let err =
            process_return_expression_string(&expr, &HashMap::new(), false, false).unwrap_err();
        assert!(matches!(
            err,
            ChQueryGeneratorError::NoOperandFoundInReturnClause
        ));
    }

    // generate_final_select_statements

    #[test]
    fn variable_no_alias() {
        let items = vec![return_data_builder(Expression::Variable("x"), None)];
        let map = HashMap::new();
        let (select, group_by) = generate_final_select_statements(items, &map).unwrap();
        assert_eq!(select, "x.*");
        assert_eq!(group_by, "GROUP BY x.*");
    }

    #[test]
    fn property_access_no_alias() {
        let expr = Expression::PropertyAccessExp(PropertyAccess {
            base: "n",
            key: "k",
        });
        let items = vec![return_data_builder(expr, None)];
        let map = HashMap::new();
        let (select, group_by) = generate_final_select_statements(items, &map).unwrap();
        assert_eq!(select, "n.k");
        assert_eq!(group_by, "GROUP BY n.k");
    }

    #[test]
    fn property_access_with_alias() {
        let expr = Expression::PropertyAccessExp(PropertyAccess {
            base: "n",
            key: "k",
        });
        let items = vec![return_data_builder(expr, Some("a1"))];
        let map = HashMap::new();
        let (select, group_by) = generate_final_select_statements(items, &map).unwrap();
        assert_eq!(select, "n.k AS a1");
        assert_eq!(group_by, "GROUP BY a1");
    }

    #[test]
    fn function_call_no_alias() {
        let expr = Expression::FunctionCallExp(FunctionCall {
            name: "sum".to_string(),
            args: vec![Expression::Variable("x")],
        });
        let items = vec![return_data_builder(expr, None)];
        let mut map = HashMap::new();
        map.insert("x".to_string(), "nid".to_string());
        let (select, group_by) = generate_final_select_statements(items, &map).unwrap();
        assert_eq!(select, "sum(x.nid)");
        assert_eq!(group_by, "");
    }

    #[test]
    fn function_call_with_alias() {
        let expr = Expression::FunctionCallExp(FunctionCall {
            name: "count".to_string(),
            args: vec![Expression::Variable("y")],
        });
        let items = vec![return_data_builder(expr, Some("c1"))];
        let mut map = HashMap::new();
        map.insert("y".to_string(), "nid".to_string());
        let (select, group_by) = generate_final_select_statements(items, &map).unwrap();
        assert_eq!(select, "count(y.nid) AS c1");
        assert_eq!(group_by, "");
    }

    #[test]
    fn literal_with_alias() {
        let expr = Expression::Literal(Literal::Integer(7));
        let items = vec![return_data_builder(expr, Some("l1"))];
        let map = HashMap::new();
        let (select, group_by) = generate_final_select_statements(items, &map).unwrap();
        assert_eq!(select, "7 AS l1");
        assert_eq!(group_by, "GROUP BY l1");
    }

    #[test]
    fn unary_operator_distinct() {
        let expr = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Distinct,
            operands: vec![Expression::Variable("y")],
        });
        let items = vec![return_data_builder(expr, None)];
        let map = HashMap::new();
        let (select, group_by) = generate_final_select_statements(items, &map).unwrap();
        assert_eq!(select, "DISTINCT y.*");
        assert_eq!(group_by, "GROUP BY y.*");
    }

    // not sure about this test case now.
    // #[test]
    // fn binary_operator_final_select() {
    //     let expr = Expression::OperatorApplicationExp(OperatorApplication {
    //         operator: Operator::Addition,
    //         operands: vec![
    //             Expression::Variable("a"),
    //             Expression::Literal(Literal::Integer(5)),
    //         ],
    //     });
    //     let items = vec![return_data_builder(expr, None)];
    //     let map = HashMap::new();
    //     let (select, group_by) = generate_final_select_statements(items, &map).unwrap();
    //     assert_eq!(select, "a + 5");
    //     assert_eq!(group_by, "GROUP BY a + 5");
    // }

    #[test]
    fn unsupported_expression_error_in_final_select() {
        let expr = Expression::Parameter("p");
        let items = vec![return_data_builder(expr, None)];
        let map = HashMap::new();
        let err = generate_final_select_statements(items, &map).unwrap_err();
        assert!(matches!(
            err,
            ChQueryGeneratorError::UnsupportedItemInReturnClause
        ));
    }

    // generate_node_select_statements

    #[test]
    fn only_node_id_when_no_return_items() {
        let select = generate_node_select_statements(vec![], "nid").unwrap();
        assert_eq!(select, "nid");
    }

    #[test]
    fn skip_property_access_matching_node_id() {
        let expr = Expression::PropertyAccessExp(PropertyAccess {
            base: "x",
            key: "nid",
        });
        let select =
            generate_node_select_statements(vec![return_item_builder(expr)], "nid").unwrap();
        assert_eq!(select, "nid");
    }

    #[test]
    fn include_property_access_non_matching() {
        let expr = Expression::PropertyAccessExp(PropertyAccess {
            base: "x",
            key: "foo",
        });
        let select =
            generate_node_select_statements(vec![return_item_builder(expr)], "nid").unwrap();
        assert_eq!(select, "nid, foo");
    }

    #[test]
    fn include_variable() {
        let expr = Expression::Variable("col");
        let select =
            generate_node_select_statements(vec![return_item_builder(expr)], "nid").unwrap();
        assert_eq!(select, "*");
    }

    #[test]
    fn include_literal_integer() {
        let expr = Expression::Literal(Literal::Integer(7));
        let select =
            generate_node_select_statements(vec![return_item_builder(expr)], "nid").unwrap();
        assert_eq!(select, "nid, 7");
    }

    #[test]
    fn include_function_call() {
        let expr = Expression::FunctionCallExp(FunctionCall {
            name: "sqrt".to_string(),
            args: vec![Expression::Literal(Literal::Float(2.0))],
        });
        let select =
            generate_node_select_statements(vec![return_item_builder(expr)], "nid").unwrap();
        assert_eq!(select, "nid, sqrt(2)");
    }

    // #[test]
    // fn include_binary_operator() {
    //     let expr = Expression::OperatorApplicationExp(OperatorApplication {
    //         operator: Operator::Subtraction,
    //         operands: vec![
    //             Expression::Variable("a"),
    //             Expression::Literal(Literal::Integer(3)),
    //         ],
    //     });
    //     let select =
    //         generate_node_select_statements(vec![return_item_builder(expr)], "nid").unwrap();
    //     assert_eq!(select, "nid, a - 3");
    // }

    #[test]
    fn error_on_unsupported_expression() {
        // Parameter is not supported
        let expr = Expression::Parameter("p");
        let err =
            generate_node_select_statements(vec![return_item_builder(expr)], "nid").unwrap_err();
        assert!(matches!(
            err,
            ChQueryGeneratorError::UnsupportedItemInReturnClause
        ));
    }

    // generate_relationship_select_statements

    #[test]
    fn base_only_id_and_array_join() {
        let sql = generate_relationship_select_statements(vec![], "node_id").unwrap();
        assert_eq!(sql, "node_id, arrayJoin(bitmapToArray(to_id)) AS to_id");
    }

    #[test]
    fn variable_appends_plain_var() {
        let items = vec![return_item_builder(Expression::Variable("x"))];
        let sql = generate_relationship_select_statements(items, "id").unwrap();
        assert_eq!(sql, "id, arrayJoin(bitmapToArray(to_id)) AS to_id, x");
    }

    #[test]
    fn function_call_with_literals() {
        let items = vec![return_item_builder(Expression::FunctionCallExp(
            FunctionCall {
                name: "sum".into(),
                args: vec![
                    Expression::Literal(Literal::Integer(1)),
                    Expression::Literal(Literal::Integer(2)),
                ],
            },
        ))];
        let sql = generate_relationship_select_statements(items, "id").unwrap();
        assert_eq!(
            sql,
            "id, arrayJoin(bitmapToArray(to_id)) AS to_id, sum(1,2)"
        );
    }

    #[test]
    fn property_access_skips_node_id_key() {
        let items = vec![return_item_builder(Expression::PropertyAccessExp(
            PropertyAccess {
                base: "x",
                key: "id",
            },
        ))];
        let sql = generate_relationship_select_statements(items, "id").unwrap();
        assert_eq!(sql, "id, arrayJoin(bitmapToArray(to_id)) AS to_id");
    }

    #[test]
    fn property_access_includes_non_key() {
        let items = vec![return_item_builder(Expression::PropertyAccessExp(
            PropertyAccess {
                base: "x",
                key: "val",
            },
        ))];
        let sql = generate_relationship_select_statements(items, "id").unwrap();
        assert_eq!(sql, "id, arrayJoin(bitmapToArray(to_id)) AS to_id, val");
    }

    #[test]
    fn error_on_literal_expression() {
        let items = vec![return_item_builder(Expression::Literal(Literal::Integer(
            5,
        )))];
        let err = generate_relationship_select_statements(items, "id").unwrap_err();
        assert!(matches!(
            err,
            ChQueryGeneratorError::UnsupportedItemInRelSelectClause
        ));
    }

    #[test]
    fn error_on_operator_application() {
        let items = vec![return_item_builder(Expression::OperatorApplicationExp(
            OperatorApplication {
                operator: Operator::Addition,
                operands: vec![
                    Expression::Variable("a"),
                    Expression::Literal(Literal::Integer(2)),
                ],
            },
        ))];
        let err = generate_relationship_select_statements(items, "id").unwrap_err();
        assert!(matches!(
            err,
            ChQueryGeneratorError::UnsupportedItemInRelSelectClause
        ));
    }
}
