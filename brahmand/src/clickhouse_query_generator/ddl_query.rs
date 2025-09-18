use crate::{
    graph_catalog::graph_schema::{
        Direction, GraphSchema, GraphSchemaElement, IndexType, NodeIdSchema, NodeSchema,
        RelationshipIndexSchema, RelationshipSchema,
    },
    open_cypher_parser::ast::{
        ColumnSchema, CreateNodeTableClause, CreateRelTableClause, Expression, Literal,
        OpenCypherQueryAst,
    },
};

use super::{common::get_literal_to_string, errors::ClickhouseQueryGeneratorError};

// CREATE TABLE helloworld.my_first_table
// (
//     user_id UInt32,
//     message String,
//     timestamp DateTime,
//     metric Float32
// )
// ENGINE = MergeTree()
// PRIMARY KEY (user_id, timestamp)

fn get_default_value(
    default_value_expr: &Expression,
) -> Result<String, ClickhouseQueryGeneratorError> {
    match default_value_expr {
        Expression::Literal(literal) => Ok(get_literal_to_string(literal)),
        _ => {
            // throw error
            Err(ClickhouseQueryGeneratorError::UnsupportedDefaultValue)
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeProperties {
    pub primary_keys: String,
    pub node_id: NodeIdSchema, // other props
}

fn get_node_props(
    properties: Vec<Expression>,
    columns: &Vec<ColumnSchema>,
) -> Result<NodeProperties, ClickhouseQueryGeneratorError> {
    let mut primary_keys: Vec<&str> = vec![];
    let mut node_id: Vec<&str> = vec![];

    for prop in properties.iter() {
        if let Expression::FunctionCallExp(function_call) = prop {
            let fn_args: Vec<&str> = function_call
                .args
                .iter()
                .filter_map(|exp| {
                    if let Expression::Variable(var) = exp {
                        Some(*var)
                    } else {
                        None
                    }
                })
                .collect();

            if function_call.name.to_lowercase() == "primary key" {
                primary_keys = fn_args;
            } else if function_call.name.to_lowercase() == "node id" {
                node_id = fn_args;
            }
        }
    }

    if primary_keys.is_empty() {
        return Err(ClickhouseQueryGeneratorError::MissingPrimaryKey);
    }

    if node_id.is_empty() {
        return Err(ClickhouseQueryGeneratorError::MissingNodeId);
    }

    if node_id.len() > 1 {
        return Err(ClickhouseQueryGeneratorError::MultipleNodeIds);
    }

    let node_id_column = node_id.join("");

    let node_id_column_schema = columns
        .iter()
        .find(|column| column.column_name.to_lowercase() == node_id_column.to_lowercase())
        .ok_or(ClickhouseQueryGeneratorError::InvalidNodeId)?;

    if !["Int64", "UInt64"].contains(&node_id_column_schema.column_dtype) {
        return Err(ClickhouseQueryGeneratorError::InvalidNodeIdDType);
    }

    if !primary_keys.contains(&node_id_column.as_str()) {
        primary_keys.push(&node_id_column);
    }

    let props = NodeProperties {
        primary_keys: primary_keys.join(", "),
        node_id: NodeIdSchema {
            column: node_id_column_schema.column_name.to_string(),
            dtype: node_id_column_schema.column_dtype.to_string(),
        },
    };

    Ok(props)
}

#[derive(Debug, Clone)]
pub struct RelProperties {
    pub primary_keys: String,
    pub adj_index: bool, // other props
}

fn get_rel_props(properties: Vec<Expression>, from: &str, to: &str) -> RelProperties {
    let mut primary_keys: Vec<&str> = vec![];
    let mut adj_index: bool = false;

    for prop in properties.iter() {
        if let Expression::FunctionCallExp(function_call) = prop {
            if function_call.name.to_lowercase() == "primary key" {
                let fn_args: Vec<&str> = function_call
                    .args
                    .iter()
                    .filter_map(|exp| {
                        if let Expression::Variable(var) = exp {
                            Some(*var)
                        } else {
                            None
                        }
                    })
                    .collect();
                primary_keys = fn_args;
            } else if function_call.name.to_lowercase() == "adj index"
                && !function_call.args.is_empty()
            {
                if let Expression::Literal(Literal::Boolean(val)) =
                    function_call.args.first().unwrap()
                {
                    adj_index = *val;
                }
            }
        }
    }

    let default_pk = &format!("from_{from}, to_{to}");
    primary_keys.push(default_pk);

    RelProperties {
        primary_keys: primary_keys.join(", "),
        adj_index,
    }
}

fn generate_create_node_table_query(
    create_node_table_clause: CreateNodeTableClause,
) -> Result<(Vec<String>, Vec<GraphSchemaElement>), ClickhouseQueryGeneratorError> {
    let columns_vec: Vec<String> = create_node_table_clause
        .table_schema
        .iter()
        .map(
            |column_schema| -> Result<String, ClickhouseQueryGeneratorError> {
                let column_name = column_schema.column_name;
                let column_type = column_schema.column_dtype;
                if let Some(default_value) = &column_schema.default_value {
                    let default_val = get_default_value(default_value)?;
                    Ok(format!("{column_name} {column_type} DEFAULT {default_val}"))
                } else {
                    Ok(format!("{column_name} {column_type}"))
                }
            },
        )
        .collect::<Result<Vec<String>, ClickhouseQueryGeneratorError>>()?;

    let columns = columns_vec.join(", ");

    // for now only check for primary key. Later we can support multiple properties like skipping indexes etc.
    let node_props = get_node_props(
        create_node_table_clause.table_properties,
        &create_node_table_clause.table_schema,
    )?;

    let table_name = create_node_table_clause.table_name;
    let primary_keys = node_props.primary_keys.clone();
    let create_table_string = format!(
        "CREATE TABLE {table_name} ( {columns} ) ENGINE = MergeTree() PRIMARY KEY ({primary_keys});"
    );

    let column_names: Vec<String> = create_node_table_clause
        .table_schema
        .iter()
        .map(|column_schema| column_schema.column_name.to_string())
        .collect();

    let node_schema = NodeSchema {
        table_name: create_node_table_clause.table_name.to_string(),
        column_names,
        node_id: node_props.node_id,
        primary_keys: node_props.primary_keys,
    };

    Ok((
        vec![create_table_string],
        vec![GraphSchemaElement::Node(node_schema)],
    ))
}

fn generate_create_rel_table_query(
    create_rel_table_clause: CreateRelTableClause,
    current_graph_schema: &GraphSchema,
) -> Result<(Vec<String>, Vec<GraphSchemaElement>), ClickhouseQueryGeneratorError> {
    let from_node = create_rel_table_clause.from;
    let to_node = create_rel_table_clause.to;

    let from_table_schema = current_graph_schema
        .get_node_schema_opt(from_node)
        .ok_or(ClickhouseQueryGeneratorError::UnknownFromTableInRel)?;
    let to_table_schema = current_graph_schema
        .get_node_schema_opt(to_node)
        .ok_or(ClickhouseQueryGeneratorError::UnknownToTableInRel)?;

    let from_node_id_dtype = from_table_schema.node_id.dtype.clone();
    let to_node_id_dtype = to_table_schema.node_id.dtype.clone();

    let columns_vec: Vec<String> = create_rel_table_clause
        .table_schema
        .iter()
        .map(
            |column_schema| -> Result<String, ClickhouseQueryGeneratorError> {
                let column_name = column_schema.column_name;
                let column_type = column_schema.column_dtype;
                if let Some(default_value) = &column_schema.default_value {
                    let default_val = get_default_value(default_value)?;
                    Ok(format!("{column_name} {column_type} DEFAULT {default_val}"))
                } else {
                    Ok(format!("{column_name} {column_type}"))
                }
            },
        )
        .collect::<Result<Vec<String>, ClickhouseQueryGeneratorError>>()?;

    let mut columns = "".to_string();

    if !columns_vec.is_empty() {
        columns = format!(", {}", columns_vec.join(", "));
    }

    let rel_props = get_rel_props(create_rel_table_clause.table_properties, from_node, to_node);

    let primary_keys = rel_props.primary_keys;

    let mut create_table_strings: Vec<String> = vec![];
    let mut graph_schema_elements: Vec<GraphSchemaElement> = vec![];

    let rel_table_name = create_rel_table_clause.table_name;

    // store schema separately so that we can infer on it

    let create_rel_table_string = format!(
        "CREATE TABLE {rel_table_name} (from_{from_node} {from_node_id_dtype}, to_{to_node} {to_node_id_dtype}{columns}) ENGINE = MergeTree() PRIMARY KEY ({primary_keys});"
    );

    create_table_strings.push(create_rel_table_string);

    let column_names: Vec<String> = create_rel_table_clause
        .table_schema
        .iter()
        .map(|column_schema| column_schema.column_name.to_string())
        .collect();

    let relationship_schema = RelationshipSchema {
        table_name: rel_table_name.to_string(),
        column_names,
        from_node: from_node.to_string(),
        to_node: to_node.to_string(),
        from_node_id_dtype: from_table_schema.node_id.dtype.clone(),
        to_node_id_dtype: to_table_schema.node_id.dtype.clone(),
    };

    graph_schema_elements.push(GraphSchemaElement::Rel(relationship_schema));

    if rel_props.adj_index {
        // CREATE TABLE so_graph.edge_posts_to_users
        // (
        //     posts_id UInt32,
        //     users_ids AggregateFunction(groupBitmap, UInt32),
        //     INDEX IDX_edge_posts_to_users (posts_id) TYPE minmax GRANULARITY 1
        // ) ENGINE = AggregatingMergeTree()
        // ORDER BY posts_id;
        let create_outgoing_rel_table_string = format!(
            "CREATE TABLE {rel_table_name}_outgoing (from_id {from_node_id_dtype}, to_id AggregateFunction(groupBitmap, {to_node_id_dtype})) ENGINE = AggregatingMergeTree() ORDER BY from_id;"
        );
        create_table_strings.push(create_outgoing_rel_table_string);
        let create_incoming_rel_table_string = format!(
            "CREATE TABLE {rel_table_name}_incoming (from_id {to_node_id_dtype}, to_id AggregateFunction(groupBitmap, {from_node_id_dtype})) ENGINE = AggregatingMergeTree() ORDER BY from_id;"
        );
        create_table_strings.push(create_incoming_rel_table_string);
        // CREATE MATERIALIZED VIEW so_graph.MV_posts_to_users TO so_graph.edge_posts_to_users AS
        // SELECT
        //     posts_id,
        //     groupBitmapState(users_id) AS users_ids
        // FROM so_graph.raw_edge_posts_and_users
        // GROUP BY posts_id;
        let create_outgoing_rel_mv_string = format!(
            "CREATE MATERIALIZED VIEW mv_{rel_table_name}_outgoing TO {rel_table_name}_outgoing AS SELECT from_{from_node} AS from_id, groupBitmapState(to_{to_node}) AS to_id FROM {rel_table_name} GROUP BY from_id;"
        );
        create_table_strings.push(create_outgoing_rel_mv_string);
        let create_incoming_rel_mv_string = format!(
            "CREATE MATERIALIZED VIEW mv_{rel_table_name}_incoming TO {rel_table_name}_incoming AS SELECT to_{to_node} AS from_id, groupBitmapState(from_{from_node}) AS to_id FROM {rel_table_name} GROUP BY from_id;"
        );
        create_table_strings.push(create_incoming_rel_mv_string);

        let relationship_outgoing_index_schema = RelationshipIndexSchema {
            base_rel_table_name: rel_table_name.to_string(),
            table_name: format!("{}_{}", rel_table_name, Direction::Outgoing),
            direction: Direction::Outgoing,
            index_type: IndexType::Bitmap,
        };

        graph_schema_elements.push(GraphSchemaElement::RelIndex(
            relationship_outgoing_index_schema,
        ));

        let relationship_incoming_index_schema = RelationshipIndexSchema {
            base_rel_table_name: rel_table_name.to_string(),
            table_name: format!("{}_{}", rel_table_name, Direction::Incoming),
            direction: Direction::Incoming,
            index_type: IndexType::Bitmap,
        };

        graph_schema_elements.push(GraphSchemaElement::RelIndex(
            relationship_incoming_index_schema,
        ));
    }

    Ok((create_table_strings, graph_schema_elements))
}

pub fn generate_query(
    query_ast: OpenCypherQueryAst,
    current_graph_schema: &GraphSchema,
) -> Result<(Vec<String>, Vec<GraphSchemaElement>), ClickhouseQueryGeneratorError> {
    if let Some(create_node_table_clause) = query_ast.create_node_table_clause {
        return generate_create_node_table_query(create_node_table_clause);
    }

    if let Some(create_rel_table_clause) = query_ast.create_rel_table_clause {
        return generate_create_rel_table_query(create_rel_table_clause, current_graph_schema);
    }
    // throw error
    Err(ClickhouseQueryGeneratorError::UnsupportedDDLQuery)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::open_cypher_parser::ast::{FunctionCall, Literal};

    use super::*;

    // Helper to build a FunctionCallExp expression
    fn fn_call<'a>(name: &'a str, args: Vec<Expression<'a>>) -> Expression<'a> {
        Expression::FunctionCallExp(FunctionCall {
            name: name.to_string(),
            args,
        })
    }

    // get_node_props

    // Happy path: primary key == node id
    #[test]
    fn get_node_props_happy_path() {
        let props = vec![
            fn_call("primary key", vec![Expression::Variable("id")]),
            fn_call("node id", vec![Expression::Variable("id")]),
        ];
        let cols = vec![ColumnSchema {
            column_name: "id",
            column_dtype: "Int64",
            default_value: None,
        }];

        let out = get_node_props(props, &cols).unwrap();
        assert_eq!(out.primary_keys, "id");
        assert_eq!(
            out.node_id,
            NodeIdSchema {
                column: "id".into(),
                dtype: "Int64".into()
            }
        );
    }

    // If primary key missing
    #[test]
    fn err_missing_primary_key() {
        let props = vec![fn_call("node id", vec![Expression::Variable("id")])];
        let cols = vec![ColumnSchema {
            column_name: "id",
            column_dtype: "Int64",
            default_value: None,
        }];
        let err = get_node_props(props, &cols).unwrap_err();
        assert!(matches!(
            err,
            ClickhouseQueryGeneratorError::MissingPrimaryKey
        ));
    }

    // If node id missing
    #[test]
    fn err_missing_node_id() {
        let props = vec![fn_call("primary key", vec![Expression::Variable("pk")])];
        let cols = vec![ColumnSchema {
            column_name: "pk",
            column_dtype: "Int64",
            default_value: None,
        }];
        let err = get_node_props(props, &cols).unwrap_err();
        assert!(matches!(err, ClickhouseQueryGeneratorError::MissingNodeId));
    }

    // If multiple node ids specified
    #[test]
    fn err_multiple_node_ids() {
        let props = vec![
            fn_call("primary key", vec![Expression::Variable("pk")]),
            fn_call(
                "node id",
                vec![Expression::Variable("id1"), Expression::Variable("id2")],
            ),
        ];
        let cols = vec![
            ColumnSchema {
                column_name: "id1",
                column_dtype: "Int64",
                default_value: None,
            },
            ColumnSchema {
                column_name: "id2",
                column_dtype: "Int64",
                default_value: None,
            },
        ];
        let err = get_node_props(props, &cols).unwrap_err();
        assert!(matches!(
            err,
            ClickhouseQueryGeneratorError::MultipleNodeIds
        ));
    }

    // If node id column not found in schema
    #[test]
    fn err_invalid_node_id() {
        let props = vec![
            fn_call("primary key", vec![Expression::Variable("pk")]),
            fn_call("node id", vec![Expression::Variable("unknown")]),
        ];
        let cols = vec![ColumnSchema {
            column_name: "pk",
            column_dtype: "Int64",
            default_value: None,
        }];
        let err = get_node_props(props, &cols).unwrap_err();
        assert!(matches!(err, ClickhouseQueryGeneratorError::InvalidNodeId));
    }

    // If node id column has wrong dtype
    #[test]
    fn err_invalid_node_id_dtype() {
        let props = vec![
            fn_call("primary key", vec![Expression::Variable("id")]),
            fn_call("node id", vec![Expression::Variable("id")]),
        ];
        let cols = vec![ColumnSchema {
            column_name: "id",
            column_dtype: "String",
            default_value: None,
        }];
        let err = get_node_props(props, &cols).unwrap_err();
        assert!(matches!(
            err,
            ClickhouseQueryGeneratorError::InvalidNodeIdDType
        ));
    }

    // If primary key does not include node id, ensure node id is appended
    #[test]
    fn appends_node_id_to_primary_keys() {
        let props = vec![
            fn_call("primary key", vec![Expression::Variable("pk")]),
            fn_call("node id", vec![Expression::Variable("id")]),
        ];
        let cols = vec![
            ColumnSchema {
                column_name: "pk",
                column_dtype: "UInt64",
                default_value: None,
            },
            ColumnSchema {
                column_name: "id",
                column_dtype: "UInt64",
                default_value: None,
            },
        ];

        let out = get_node_props(props, &cols).unwrap();
        // node id should be appended to existing primary key
        assert_eq!(out.primary_keys, "pk, id");
        assert_eq!(
            out.node_id,
            NodeIdSchema {
                column: "id".into(),
                dtype: "UInt64".into()
            }
        );
    }

    // get_rel_primary_key

    // #[test]
    // fn default_when_no_properties() {
    //     let result = get_rel_primary_key(vec![], "FromTbl", "ToTbl");
    //     assert_eq!(result, "from_FromTbl, to_ToTbl");
    // }

    // #[test]
    // fn default_when_non_pk_function_present() {
    //     let props = vec![
    //         fn_call("not_primary_key", vec![Expression::Variable("x")]),
    //         fn_call("another_fn", vec![Expression::Variable("y")]),
    //     ];
    //     let result = get_rel_primary_key(props, "A", "B");
    //     assert_eq!(result, "from_A, to_B");
    // }

    // #[test]
    // fn default_when_pk_has_no_args() {
    //     let props = vec![fn_call("primary key", vec![])];
    //     let result = get_rel_primary_key(props, "X", "Y");
    //     assert_eq!(result, "from_X, to_Y");
    // }

    // #[test]
    // fn single_arg_appended_before_from_to() {
    //     let props = vec![fn_call("PRIMARY KEY", vec![Expression::Variable("id")])];
    //     let result = get_rel_primary_key(props, "Foo", "Bar");
    //     // "id" + "from_Foo, to_Bar"
    //     assert_eq!(result, "id, from_Foo, to_Bar");
    // }

    // #[test]
    // fn multiple_args_appended_before_from_to() {
    //     let props = vec![fn_call(
    //         "primary key",
    //         vec![Expression::Variable("c1"), Expression::Variable("c2")],
    //     )];
    //     let result = get_rel_primary_key(props, "U", "V");
    //     // "c1,c2" + "from_U, to_V"
    //     assert_eq!(result, "c1, c2, from_U, to_V");
    // }

    // #[test]
    // fn filters_non_variable_args() {
    //     let props = vec![fn_call(
    //         "primary key",
    //         vec![
    //             Expression::Literal(Literal::Integer(42)),
    //             Expression::Variable("only_var"),
    //         ],
    //     )];
    //     let result = get_rel_primary_key(props, "M", "N");
    //     // Only "only_var" is picked up
    //     assert_eq!(result, "only_var, from_M, to_N");
    // }

    // #[test]
    // fn stops_at_first_primary_key_function() {
    //     let props = vec![
    //         fn_call("primary key", vec![Expression::Variable("first")]),
    //         fn_call("primary key", vec![Expression::Variable("second")]),
    //     ];
    //     let result = get_rel_primary_key(props, "P", "Q");
    //     // Only the first PK call is considered
    //     assert_eq!(result, "first, from_P, to_Q");
    // }

    // generate_create_node_table_query

    // #[test]
    // fn happy_path_without_defaults() {
    //     let clause = CreateNodeTableClause {
    //         table_name: "Test",
    //         table_schema: vec![ColumnSchema {
    //             column_name: "id",
    //             column_dtype: "Int64",
    //             default_value: None,
    //         }],
    //         table_properties: vec![
    //             fn_call("primary key", vec![Expression::Variable("id")]),
    //             fn_call("node id", vec![Expression::Variable("id")]),
    //         ],
    //     };

    //     let (queries, schema_elem) = generate_create_node_table_query(clause).unwrap();

    //     let expected_sql = "CREATE TABLE Test ( id Int64 ) ENGINE = MergeTree() PRIMARY KEY (id);";
    //     assert_eq!(queries, vec![expected_sql.to_string()]);

    //     match schema_elem {
    //         GraphSchemaElement::Node(ns) => {
    //             assert_eq!(ns.table_name, "Test");
    //             assert_eq!(ns.column_names, vec!["id"]);
    //             assert_eq!(ns.primary_keys, "id");
    //             assert_eq!(
    //                 ns.node_id,
    //                 NodeIdSchema {
    //                     column: "id".to_string(),
    //                     dtype: "Int64".to_string()
    //                 }
    //             );
    //         }
    //         _ => panic!("Expected a Node schema element"),
    //     }
    // }

    // #[test]
    // fn happy_path_with_defaults() {
    //     let clause = CreateNodeTableClause {
    //         table_name: "Foo",
    //         table_schema: vec![
    //             ColumnSchema {
    //                 column_name: "id",
    //                 column_dtype: "UInt64",
    //                 default_value: None,
    //             },
    //             ColumnSchema {
    //                 column_name: "count",
    //                 column_dtype: "Int64",
    //                 default_value: Some(Expression::Literal(Literal::Integer(42))),
    //             },
    //         ],
    //         table_properties: vec![
    //             fn_call("node id", vec![Expression::Variable("id")]),
    //             fn_call("primary key", vec![Expression::Variable("id")]),
    //         ],
    //     };

    //     let (queries, schema_elem) = generate_create_node_table_query(clause).unwrap();

    //     let expected_sql = "CREATE TABLE Foo ( id UInt64, count Int64 DEFAULT 42 ) ENGINE = MergeTree() PRIMARY KEY (id);";
    //     assert_eq!(queries, vec![expected_sql.to_string()]);

    //     match schema_elem {
    //         GraphSchemaElement::Node(ns) => {
    //             assert_eq!(ns.table_name, "Foo");
    //             assert_eq!(ns.column_names, vec!["id", "count"]);
    //             assert_eq!(ns.primary_keys, "id");
    //             assert_eq!(
    //                 ns.node_id,
    //                 NodeIdSchema {
    //                     column: "id".to_string(),
    //                     dtype: "UInt64".to_string()
    //                 }
    //             );
    //         }
    //         _ => panic!("Expected a Node schema element"),
    //     }
    // }

    #[test]
    fn error_on_missing_primary_key() {
        let clause = CreateNodeTableClause {
            table_name: "Bad",
            table_schema: vec![ColumnSchema {
                column_name: "x",
                column_dtype: "Int64",
                default_value: None,
            }],
            table_properties: vec![fn_call("node id", vec![Expression::Variable("x")])],
        };

        let err = generate_create_node_table_query(clause).unwrap_err();
        assert!(matches!(
            err,
            ClickhouseQueryGeneratorError::MissingPrimaryKey
        ));
    }

    #[test]
    fn error_on_missing_node_id() {
        let clause = CreateNodeTableClause {
            table_name: "Bad",
            table_schema: vec![ColumnSchema {
                column_name: "x",
                column_dtype: "Int64",
                default_value: None,
            }],
            table_properties: vec![fn_call("primary key", vec![Expression::Variable("x")])],
        };

        let err = generate_create_node_table_query(clause).unwrap_err();
        assert!(matches!(err, ClickhouseQueryGeneratorError::MissingNodeId));
    }

    #[test]
    fn error_on_invalid_node_id_column() {
        let clause = CreateNodeTableClause {
            table_name: "Bad",
            table_schema: vec![ColumnSchema {
                column_name: "a",
                column_dtype: "Int64",
                default_value: None,
            }],
            table_properties: vec![
                fn_call("primary key", vec![Expression::Variable("a")]),
                fn_call("node id", vec![Expression::Variable("b")]),
            ],
        };

        let err = generate_create_node_table_query(clause).unwrap_err();
        assert!(matches!(err, ClickhouseQueryGeneratorError::InvalidNodeId));
    }

    #[test]
    fn error_on_invalid_node_id_dtype() {
        let clause = CreateNodeTableClause {
            table_name: "Bad",
            table_schema: vec![ColumnSchema {
                column_name: "key",
                column_dtype: "String",
                default_value: None,
            }],
            table_properties: vec![
                fn_call("primary key", vec![Expression::Variable("key")]),
                fn_call("node id", vec![Expression::Variable("key")]),
            ],
        };

        let err = generate_create_node_table_query(clause).unwrap_err();
        assert!(matches!(
            err,
            ClickhouseQueryGeneratorError::InvalidNodeIdDType
        ));
    }

    // generate_create_rel_table_query

    /// A minimal schema with two node types "User" and "Post", both using UInt64 IDs.
    fn make_schema() -> GraphSchema {
        let mut nodes = HashMap::new();
        nodes.insert(
            "User".to_string(),
            NodeSchema {
                table_name: "User".to_string(),
                column_names: vec!["user_id".to_string()],
                primary_keys: "user_id".to_string(),
                node_id: NodeIdSchema {
                    column: "user_id".to_string(),
                    dtype: "UInt64".to_string(),
                },
            },
        );
        nodes.insert(
            "Post".to_string(),
            NodeSchema {
                table_name: "Post".to_string(),
                column_names: vec!["post_id".to_string()],
                primary_keys: "post_id".to_string(),
                node_id: NodeIdSchema {
                    column: "post_id".to_string(),
                    dtype: "UInt64".to_string(),
                },
            },
        );
        GraphSchema::build(1, nodes, HashMap::new(), HashMap::new())
    }

    // #[test]
    // fn default_pk_happy_path_concrete() {
    //     let clause = CreateRelTableClause {
    //         table_name: "follows",
    //         from: "User",
    //         to: "Post",
    //         table_schema: vec![],
    //         table_properties: vec![],
    //     };

    //     let (queries, elem) = generate_create_rel_table_query(clause, &make_schema()).unwrap();
    //     assert_eq!(queries.len(), 5);

    //     let expected_base = "CREATE TABLE follows (from_User UInt64, to_Post UInt64) ENGINE = MergeTree() PRIMARY KEY (from_User, to_Post);";
    //     let expected_out = "CREATE TABLE follows_outgoing (from_id UInt64, to_id AggregateFunction(groupBitmap, UInt64)) ENGINE = AggregatingMergeTree() ORDER BY from_id;";
    //     let expected_in = "CREATE TABLE follows_incoming (from_id UInt64, to_id AggregateFunction(groupBitmap, UInt64)) ENGINE = AggregatingMergeTree() ORDER BY from_id;";
    //     let expected_mv_out = "CREATE MATERIALIZED VIEW mv_follows_outgoing TO follows_outgoing AS SELECT from_User AS from_id, groupBitmapState(to_Post) AS to_id FROM follows GROUP BY from_id;";
    //     let expected_mv_in = "CREATE MATERIALIZED VIEW mv_follows_incoming TO follows_incoming AS SELECT to_Post AS from_id, groupBitmapState(from_User) AS to_id FROM follows GROUP BY from_id;";

    //     assert_eq!(&queries[0], expected_base);
    //     assert_eq!(&queries[1], expected_out);
    //     assert_eq!(&queries[2], expected_in);
    //     assert_eq!(&queries[3], expected_mv_out);
    //     assert_eq!(&queries[4], expected_mv_in);

    //     match elem {
    //         GraphSchemaElement::Rel(rs) => {
    //             assert_eq!(rs.table_name, "follows");
    //             assert_eq!(rs.from_node, "User");
    //             assert_eq!(rs.to_node, "Post");
    //             assert_eq!(rs.from_node_id_dtype, "UInt64");
    //             assert_eq!(rs.to_node_id_dtype, "UInt64");
    //             assert!(rs.column_names.is_empty());
    //         }
    //         _ => panic!("Expected GraphSchemaElement::Rel"),
    //     }
    // }

    #[test]
    fn respects_pk_fn_with_args_full_query_concrete() {
        let clause = CreateRelTableClause {
            table_name: "follows",
            from: "User",
            to: "Post",
            table_schema: vec![],
            table_properties: vec![fn_call(
                "primary key",
                vec![Expression::Variable("c1"), Expression::Variable("c2")],
            )],
        };

        let (queries, _) = generate_create_rel_table_query(clause, &make_schema()).unwrap();
        // Check the entire base query string exactly
        let expected_base = "CREATE TABLE follows (from_User UInt64, to_Post UInt64) ENGINE = MergeTree() PRIMARY KEY (c1, c2, from_User, to_Post);";
        assert_eq!(&queries[0], expected_base);
    }

    #[test]
    fn includes_default_value_in_columns_full_query_concrete() {
        let clause = CreateRelTableClause {
            table_name: "follows",
            from: "User",
            to: "Post",
            table_schema: vec![ColumnSchema {
                column_name: "count",
                column_dtype: "Int32",
                default_value: Some(Expression::Literal(Literal::Integer(99))),
            }],
            table_properties: vec![],
        };

        let (queries, _) = generate_create_rel_table_query(clause, &make_schema()).unwrap();
        let expected_base = "CREATE TABLE follows (from_User UInt64, to_Post UInt64, count Int32 DEFAULT 99) ENGINE = MergeTree() PRIMARY KEY (from_User, to_Post);";
        assert_eq!(&queries[0], expected_base);
    }

    #[test]
    fn error_unknown_from() {
        let clause = CreateRelTableClause {
            table_name: "Bad",
            from: "X", // not in schema
            to: "B",
            table_schema: vec![],
            table_properties: vec![],
        };
        let err = generate_create_rel_table_query(clause, &make_schema()).unwrap_err();
        assert!(matches!(
            err,
            ClickhouseQueryGeneratorError::UnknownFromTableInRel
        ));
    }

    #[test]
    fn error_unknown_to_concrete() {
        let clause = CreateRelTableClause {
            table_name: "BadRel",
            from: "User",  // valid in schema
            to: "Comment", // not present in make_schema()
            table_schema: vec![],
            table_properties: vec![],
        };
        let err = generate_create_rel_table_query(clause, &make_schema()).unwrap_err();
        assert!(matches!(
            err,
            ClickhouseQueryGeneratorError::UnknownToTableInRel
        ));
    }

    #[test]
    fn generate_query_unsupported() {
        // AST with no DDL clauses
        let ast = OpenCypherQueryAst {
            match_clause: None,
            with_clause: None,
            where_clause: None,
            create_clause: None,
            create_node_table_clause: None,
            create_rel_table_clause: None,
            set_clause: None,
            remove_clause: None,
            delete_clause: None,
            return_clause: None,
            order_by_clause: None,
            skip_clause: None,
            limit_clause: None,
        };

        let err = generate_query(ast, &make_schema()).unwrap_err();
        assert!(matches!(
            err,
            ClickhouseQueryGeneratorError::UnsupportedDDLQuery
        ));
    }
}
