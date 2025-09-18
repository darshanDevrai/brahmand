use ast::{
    CreateClause, CreateNodeTableClause, CreateRelTableClause, DeleteClause, LimitClause,
    MatchClause, OpenCypherQueryAst, OrderByClause, RemoveClause, ReturnClause, SetClause,
    SkipClause, WhereClause, WithClause,
};
use common::ws;
use errors::OpenCypherParsingError;
use nom::bytes::complete::tag;
use nom::character::complete::multispace0;
use nom::combinator::{cut, opt};
use nom::error::context;
use nom::sequence::terminated;
use nom::{IResult, Parser};

pub mod ast;
mod common;
mod create_clause;
mod create_node_table_clause;
mod create_rel_table_clause;
mod create_table_schema;
mod delete_clause;
pub(crate) mod errors;
mod expression;
mod limit_clause;
mod match_clause;
mod order_by_clause;
mod path_pattern;
mod remove_clause;
mod return_clause;
mod set_clause;
mod skip_clause;
mod where_clause;
mod with_clause;

pub fn parse_statement(input: &'_ str) -> IResult<&'_ str, OpenCypherQueryAst<'_>, OpenCypherParsingError<'_>> {
    context(
        "missing semicolon",
        cut(terminated(parse_query_with_nom, ws(tag(";")))),
    )
    .parse(input)
}

pub fn parse_query_with_nom(
    input: &'_ str,
) -> IResult<&'_ str, OpenCypherQueryAst<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = multispace0.parse(input)?;

    let (input, match_clause): (&str, Option<MatchClause>) =
        opt(match_clause::parse_match_clause).parse(input)?;
    let (input, with_clause): (&str, Option<WithClause>) =
        opt(with_clause::parse_with_clause).parse(input)?;
    let (input, where_clause): (&str, Option<WhereClause>) =
        opt(where_clause::parse_where_clause).parse(input)?;
    let (input, create_node_table_clause): (&str, Option<CreateNodeTableClause>) =
        opt(create_node_table_clause::parse_create_node_table_clause).parse(input)?;
    let (input, create_rel_table_clause): (&str, Option<CreateRelTableClause>) =
        opt(create_rel_table_clause::parse_create_rel_table_clause).parse(input)?;
    let (input, create_clause): (&str, Option<CreateClause>) =
        opt(create_clause::parse_create_clause).parse(input)?;
    let (input, set_clause): (&str, Option<SetClause>) =
        opt(set_clause::parse_set_clause).parse(input)?;
    let (input, remove_clause): (&str, Option<RemoveClause>) =
        opt(remove_clause::parse_remove_clause).parse(input)?;
    let (input, delete_clause): (&str, Option<DeleteClause>) =
        opt(delete_clause::parse_delete_clause).parse(input)?;
    let (input, return_clause): (&str, Option<ReturnClause>) =
        opt(return_clause::parse_return_clause).parse(input)?;
    let (input, order_by_clause): (&str, Option<OrderByClause>) =
        opt(order_by_clause::parse_order_by_clause).parse(input)?;
    let (input, skip_clause): (&str, Option<SkipClause>) =
        opt(skip_clause::parse_skip_clause).parse(input)?;
    let (input, limit_clause): (&str, Option<LimitClause>) =
        opt(limit_clause::parse_limit_clause).parse(input)?;

    let cypher_query = OpenCypherQueryAst {
        match_clause,
        with_clause,
        where_clause,
        create_clause,
        create_node_table_clause,
        create_rel_table_clause,
        set_clause,
        remove_clause,
        delete_clause,
        return_clause,
        order_by_clause,
        skip_clause,
        limit_clause,
    };

    Ok((input, cypher_query))
}

pub fn parse_query(input: &'_ str) -> Result<OpenCypherQueryAst<'_>, OpenCypherParsingError<'_>> {
    match parse_statement(input) {
        // if remainder is present then either show error or do something with it
        Ok((_, query_ast)) => Ok(query_ast),
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => Err(e),
        Err(nom::Err::Incomplete(_)) => Err(OpenCypherParsingError {
            errors: vec![("", "")],
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use crate::open_cypher_parser::ast::{
        ColumnSchema, ConnectedPattern, Direction, Expression, FunctionCall, Literal, NodePattern,
        Operator, OperatorApplication, OrderByItem, OrerByOrder, PathPattern, Property,
        PropertyAccess, PropertyKVPair, RelationshipPattern, ReturnItem, WithItem,
    };

    use super::*;

    #[test]
    fn test_parse_full_query() {
        let query = "
            MATCH (a) 
            WITH a 
            WHERE a = 1 
            CREATE (b) 
            SET b.name = 'John', b.age = 30 
            REMOVE b.temp 
            DELETE a 
            RETURN a, b.name AS name 
            ORDER BY a ASC, b DESC 
            SKIP 5 
            LIMIT 10 ;";
        let parsed = parse_query(query);
        match parsed {
            Ok(ast) => {
                // Ensure each clause is present.
                assert!(ast.match_clause.is_some(), "Expected MATCH clause");
                assert!(ast.with_clause.is_some(), "Expected WITH clause");
                assert!(ast.where_clause.is_some(), "Expected WHERE clause");
                assert!(ast.create_clause.is_some(), "Expected CREATE clause");
                assert!(ast.set_clause.is_some(), "Expected SET clause");
                assert!(ast.remove_clause.is_some(), "Expected REMOVE clause");
                assert!(ast.delete_clause.is_some(), "Expected DELETE clause");
                assert!(ast.return_clause.is_some(), "Expected RETURN clause");
                assert!(ast.order_by_clause.is_some(), "Expected ORDER BY clause");
                assert!(ast.skip_clause.is_some(), "Expected SKIP clause");
                assert!(ast.limit_clause.is_some(), "Expected LIMIT clause");

                let match_clause = ast.match_clause.unwrap();

                if let PathPattern::Node(node) = &match_clause.path_patterns[0] {
                    assert_eq!(node.name, Some("a"));
                } else {
                    panic!("Expected MATCH clause to contain a Node pattern");
                }

                let with_clause = ast.with_clause.unwrap();
                assert_eq!(with_clause.with_items.len(), 1);
                let with_item = &with_clause.with_items[0];
                assert_eq!(with_item.expression, Expression::Variable("a"));
                assert_eq!(with_item.alias, None);

                let where_clause = ast.where_clause.unwrap();

                if let Expression::OperatorApplicationExp(operator_application) =
                    where_clause.conditions
                {
                    assert_eq!(operator_application.operator, Operator::Equal);
                    assert_eq!(operator_application.operands.len(), 2);
                    assert_eq!(operator_application.operands[0], Expression::Variable("a"));
                    assert_eq!(
                        operator_application.operands[1],
                        Expression::Literal(Literal::Integer(1))
                    );
                } else {
                    panic!("Expected Where clause to contain a Expression::OperatorApplicationExp");
                }

                let create_clause = ast.create_clause.unwrap();
                if let PathPattern::Node(node) = &create_clause.path_patterns[0] {
                    assert_eq!(node.name, Some("b"));
                } else {
                    panic!("Expected CREATE clause to contain a Node pattern");
                }

                let set_clause = ast.set_clause.unwrap();
                assert_eq!(set_clause.set_items.len(), 2);

                assert_eq!(set_clause.set_items[0].operator, Operator::Equal);
                if let Expression::PropertyAccessExp(prop) = &set_clause.set_items[0].operands[0] {
                    assert_eq!(prop.base, "b");
                    assert_eq!(prop.key, "name");
                } else {
                    panic!("Expected first operand of SET item to be a property access");
                }
                assert_eq!(
                    set_clause.set_items[0].operands[1],
                    Expression::Literal(Literal::String("John"))
                );

                assert_eq!(set_clause.set_items[1].operator, Operator::Equal);
                if let Expression::PropertyAccessExp(prop) = &set_clause.set_items[1].operands[0] {
                    assert_eq!(prop.base, "b");
                    assert_eq!(prop.key, "age");
                } else {
                    panic!("Expected first operand of second SET item to be a property access");
                }
                assert_eq!(
                    set_clause.set_items[1].operands[1],
                    Expression::Literal(Literal::Integer(30))
                );

                let remove_clause = ast.remove_clause.unwrap();
                assert_eq!(remove_clause.remove_items.len(), 1);
                let remove_item = &remove_clause.remove_items[0];
                assert_eq!(remove_item.base, "b");
                assert_eq!(remove_item.key, "temp");

                let delete_clause = ast.delete_clause.unwrap();
                assert_eq!(delete_clause.is_detach, false);
                assert_eq!(delete_clause.delete_items.len(), 1);
                assert_eq!(delete_clause.delete_items[0], Expression::Variable("a"));

                let return_clause = ast.return_clause.unwrap();
                assert_eq!(return_clause.return_items.len(), 2);
                let return_item1 = &return_clause.return_items[0];
                assert_eq!(return_item1.expression, Expression::Variable("a"));
                assert_eq!(return_item1.alias, None);
                let return_item2 = &return_clause.return_items[1];
                if let Expression::PropertyAccessExp(prop) = &return_item2.expression {
                    assert_eq!(prop.base, "b");
                    assert_eq!(prop.key, "name");
                } else {
                    panic!("Expected second RETURN item to be a property access");
                }
                assert_eq!(return_item2.alias, Some("name"));

                let order_by_clause = ast.order_by_clause.unwrap();
                assert_eq!(order_by_clause.order_by_items.len(), 2);
                let order_item1 = &order_by_clause.order_by_items[0];
                assert_eq!(order_item1.expression, Expression::Variable("a"));
                assert_eq!(order_item1.order, OrerByOrder::Asc);
                let order_item2 = &order_by_clause.order_by_items[1];
                assert_eq!(order_item2.expression, Expression::Variable("b"));
                assert_eq!(order_item2.order, OrerByOrder::Desc);

                let skip_clause = ast.skip_clause.unwrap();
                assert_eq!(skip_clause.skip_item, 5);

                let limit_clause = ast.limit_clause.unwrap();
                assert_eq!(limit_clause.limit_item, 10);
            }
            Err(e) => panic!("Full query parsing failed: {:?}", e),
        }
    }

    #[test]
    fn test_parse_partial_query() {
        let query = "MATCH (a) WHERE a = 1 RETURN a;";
        let parsed = parse_query(query);
        match parsed {
            Ok(ast) => {
                // These clauses should be present.
                assert!(ast.match_clause.is_some(), "Expected MATCH clause");
                assert!(ast.where_clause.is_some(), "Expected WHERE clause");
                assert!(ast.return_clause.is_some(), "Expected RETURN clause");
                // The rest should be None.
                assert!(ast.with_clause.is_none(), "Expected WITH clause to be None");
                assert!(
                    ast.create_clause.is_none(),
                    "Expected CREATE clause to be None"
                );
                assert!(ast.set_clause.is_none(), "Expected SET clause to be None");
                assert!(
                    ast.remove_clause.is_none(),
                    "Expected REMOVE clause to be None"
                );
                assert!(
                    ast.delete_clause.is_none(),
                    "Expected DELETE clause to be None"
                );
                assert!(
                    ast.order_by_clause.is_none(),
                    "Expected ORDER BY clause to be None"
                );
                assert!(ast.skip_clause.is_none(), "Expected SKIP clause to be None");
                assert!(
                    ast.limit_clause.is_none(),
                    "Expected LIMIT clause to be None"
                );
            }
            Err(e) => panic!("Partial query parsing failed: {:?}", e),
        }
    }

    #[test]
    fn test_parse_full_read_query() {
        let input = "
        MATCH (david {name: 'David'})-[]-(otherPerson)-[]->(b)
        WITH otherPerson, count(*) AS foaf
        WHERE foaf > 1 and (fof is not null or a + b)
        RETURN otherPerson.name AS otherName
        ORDER BY otherPerson.name DESC
        Skip 10
        LIMIT 20;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(query_ast.match_clause.is_some(), "Expected MATCH clause");
        let match_clause = query_ast.match_clause.unwrap();

        let expected_match_clause = MatchClause {
            path_patterns: vec![PathPattern::ConnectedPattern(vec![
                ConnectedPattern {
                    start_node: Rc::new(RefCell::new(NodePattern {
                        name: Some("david"),
                        label: None,
                        properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                            key: "name",
                            value: Expression::Literal(Literal::String("David")),
                        })]),
                    })),
                    relationship: RelationshipPattern {
                        name: None,
                        direction: Direction::Either,
                        label: None,
                        properties: None,
                    },
                    end_node: Rc::new(RefCell::new(NodePattern {
                        name: Some("otherPerson"),
                        label: None,
                        properties: None,
                    })),
                },
                ConnectedPattern {
                    start_node: Rc::new(RefCell::new(NodePattern {
                        name: Some("otherPerson"),
                        label: None,
                        properties: None,
                    })),
                    relationship: RelationshipPattern {
                        name: None,
                        direction: Direction::Outgoing,
                        label: None,
                        properties: None,
                    },
                    end_node: Rc::new(RefCell::new(NodePattern {
                        name: Some("b"),
                        label: None,
                        properties: None,
                    })),
                },
            ])],
        };

        assert_eq!(match_clause, expected_match_clause);

        assert!(query_ast.with_clause.is_some(), "Expected WITH clause");
        let with_clause = query_ast.with_clause.unwrap();

        let expected_with_clause = WithClause {
            with_items: vec![
                WithItem {
                    expression: Expression::Variable("otherPerson"),
                    alias: None,
                },
                WithItem {
                    expression: Expression::FunctionCallExp(FunctionCall {
                        name: "count".to_string(),
                        args: vec![Expression::Variable("*")],
                    }),
                    alias: Some("foaf"),
                },
            ],
        };
        assert_eq!(with_clause, expected_with_clause);

        assert!(query_ast.where_clause.is_some(), "Expected WHERE clause");
        let where_clause = query_ast.where_clause.unwrap();
        let expected_where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::And,
                operands: vec![
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::GreaterThan,
                        operands: vec![
                            Expression::Variable("foaf"),
                            Expression::Literal(Literal::Integer(1)),
                        ],
                    }),
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Or,
                        operands: vec![
                            Expression::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::IsNotNull,
                                operands: vec![Expression::Variable("fof")],
                            }),
                            Expression::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::Addition,
                                operands: vec![
                                    Expression::Variable("a"),
                                    Expression::Variable("b"),
                                ],
                            }),
                        ],
                    }),
                ],
            }),
        };
        assert_eq!(where_clause, expected_where_clause);

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            return_items: vec![ReturnItem {
                expression: Expression::PropertyAccessExp(PropertyAccess {
                    base: "otherPerson",
                    key: "name",
                }),
                alias: Some("otherName"),
            }],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.order_by_clause.is_some(),
            "Expected ORDER BY clause"
        );
        let order_by_clause = query_ast.order_by_clause.unwrap();
        let expected_order_by_clause = OrderByClause {
            order_by_items: vec![OrderByItem {
                expression: Expression::PropertyAccessExp(PropertyAccess {
                    base: "otherPerson",
                    key: "name",
                }),
                order: OrerByOrder::Desc,
            }],
        };
        assert_eq!(order_by_clause, expected_order_by_clause);

        assert!(query_ast.skip_clause.is_some(), "Expected SKIP clause");
        let skip_clause = query_ast.skip_clause.unwrap();
        let expected_skip_clause = SkipClause { skip_item: 10 };
        assert_eq!(skip_clause, expected_skip_clause);

        assert!(query_ast.limit_clause.is_some(), "Expected LIMIT clause");
        let limit_clause = query_ast.limit_clause.unwrap();
        let expected_limit_clause = LimitClause { limit_item: 20 };
        assert_eq!(limit_clause, expected_limit_clause);

        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
    }

    #[test]
    fn test_parse_full_read_query_person_movie() {
        let input = "
            MATCH (p:Person {name: 'Tom Hardy' })-[r:ACTED_IN]->(movie:Movie)<-[:DIRECTED]-(director:Person)
            WHERE p Is not null and movie.name = 'Batman'
            RETURN p as tom_hardy, movie.name AS movieName, (a)-[]->(c)
        ;";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(query_ast.match_clause.is_some(), "Expected MATCH clause");
        let match_clause = query_ast.match_clause.unwrap();
        let expected_match_clause = MatchClause {
            path_patterns: vec![PathPattern::ConnectedPattern(vec![
                // (p:Person {name: 'Tom Hardy'})-[r:ACTED_IN]->(movie:Movie)
                ConnectedPattern {
                    start_node: Rc::new(RefCell::new(NodePattern {
                        name: Some("p"),
                        label: Some("Person"),
                        properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                            key: "name",
                            value: Expression::Literal(Literal::String("Tom Hardy")),
                        })]),
                    })),
                    relationship: RelationshipPattern {
                        name: Some("r"),
                        direction: Direction::Outgoing,
                        label: Some("ACTED_IN"),
                        properties: None,
                    },
                    end_node: Rc::new(RefCell::new(NodePattern {
                        name: Some("movie"),
                        label: Some("Movie"),
                        properties: None,
                    })),
                },
                // (movie:Movie)<-[:DIRECTED]-(director:Person)
                ConnectedPattern {
                    start_node: Rc::new(RefCell::new(NodePattern {
                        name: Some("movie"),
                        label: Some("Movie"),
                        properties: None,
                    })),
                    relationship: RelationshipPattern {
                        name: None,
                        direction: Direction::Incoming,
                        label: Some("DIRECTED"),
                        properties: None,
                    },
                    end_node: Rc::new(RefCell::new(NodePattern {
                        name: Some("director"),
                        label: Some("Person"),
                        properties: None,
                    })),
                },
            ])],
        };
        assert_eq!(match_clause, expected_match_clause);

        assert!(query_ast.where_clause.is_some(), "Expected WHERE clause");
        let where_clause = query_ast.where_clause.unwrap();
        let expected_where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::And,
                operands: vec![
                    // p IS NOT NULL
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::IsNotNull,
                        operands: vec![Expression::Variable("p")],
                    }),
                    // movie.name = 'Batman'
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            Expression::PropertyAccessExp(PropertyAccess {
                                base: "movie",
                                key: "name",
                            }),
                            Expression::Literal(Literal::String("Batman")),
                        ],
                    }),
                ],
            }),
        };
        assert_eq!(where_clause, expected_where_clause);

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            return_items: vec![
                // p as tom_hardy
                ReturnItem {
                    expression: Expression::Variable("p"),
                    alias: Some("tom_hardy"),
                },
                // movie.name AS movieName
                ReturnItem {
                    expression: Expression::PropertyAccessExp(PropertyAccess {
                        base: "movie",
                        key: "name",
                    }),
                    alias: Some("movieName"),
                },
                // (a)-[]->(c)
                ReturnItem {
                    expression: Expression::PathPattern(PathPattern::ConnectedPattern(vec![
                        ConnectedPattern {
                            start_node: Rc::new(RefCell::new(NodePattern {
                                name: Some("a"),
                                label: None,
                                properties: None,
                            })),
                            relationship: RelationshipPattern {
                                name: None,
                                direction: Direction::Outgoing,
                                label: None,
                                properties: None,
                            },
                            end_node: Rc::new(RefCell::new(NodePattern {
                                name: Some("c"),
                                label: None,
                                properties: None,
                            })),
                        },
                    ])),
                    alias: None,
                },
            ],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    #[test]
    fn test_parse_full_create_query() {
        let input = "
            MATCH (a:Person), (b:Person)
            WHERE a.name = 'Node A' AND b.name = 'Node B'
            CREATE (a)-[r:RELTYPE {name: a.name }]->(b)
            RETURN r;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(query_ast.match_clause.is_some(), "Expected MATCH clause");
        let match_clause = query_ast.match_clause.unwrap();
        let expected_match_clause = MatchClause {
            path_patterns: vec![
                PathPattern::Node(NodePattern {
                    name: Some("a"),
                    label: Some("Person"),
                    properties: None,
                }),
                PathPattern::Node(NodePattern {
                    name: Some("b"),
                    label: Some("Person"),
                    properties: None,
                }),
            ],
        };
        assert_eq!(match_clause, expected_match_clause);

        assert!(query_ast.where_clause.is_some(), "Expected WHERE clause");
        let where_clause = query_ast.where_clause.unwrap();
        let expected_where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::And,
                operands: vec![
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            Expression::PropertyAccessExp(PropertyAccess {
                                base: "a",
                                key: "name",
                            }),
                            Expression::Literal(Literal::String("Node A")),
                        ],
                    }),
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            Expression::PropertyAccessExp(PropertyAccess {
                                base: "b",
                                key: "name",
                            }),
                            Expression::Literal(Literal::String("Node B")),
                        ],
                    }),
                ],
            }),
        };
        assert_eq!(where_clause, expected_where_clause);

        assert!(query_ast.create_clause.is_some(), "Expected CREATE clause");
        let create_clause = query_ast.create_clause.unwrap();
        let expected_create_clause = CreateClause {
            path_patterns: vec![PathPattern::ConnectedPattern(vec![ConnectedPattern {
                start_node: Rc::new(RefCell::new(NodePattern {
                    name: Some("a"),
                    label: None,
                    properties: None,
                })),
                relationship: RelationshipPattern {
                    name: Some("r"),
                    direction: Direction::Outgoing,
                    label: Some("RELTYPE"),
                    properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                        key: "name",
                        value: Expression::PropertyAccessExp(PropertyAccess {
                            base: "a",
                            key: "name",
                        }),
                    })]),
                },
                end_node: Rc::new(RefCell::new(NodePattern {
                    name: Some("b"),
                    label: None,
                    properties: None,
                })),
            }])],
        };
        assert_eq!(create_clause, expected_create_clause);

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            return_items: vec![ReturnItem {
                expression: Expression::Variable("r"),
                alias: None,
            }],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    #[test]
    fn test_parse_full_set_query() {
        let input = "
            MATCH (n {name: 'Andres'})
            SET n = $props, n.rage = 'blah'
            RETURN n;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(query_ast.match_clause.is_some(), "Expected MATCH clause");
        let match_clause = query_ast.match_clause.unwrap();
        let expected_match_clause = MatchClause {
            path_patterns: vec![PathPattern::Node(NodePattern {
                name: Some("n"),
                label: None,
                properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                    key: "name",
                    value: Expression::Literal(Literal::String("Andres")),
                })]),
            })],
        };
        assert_eq!(match_clause, expected_match_clause);

        assert!(query_ast.set_clause.is_some(), "Expected SET clause");
        let set_clause = query_ast.set_clause.unwrap();
        assert_eq!(set_clause.set_items.len(), 2, "Expected two SET items");

        let expected_set_item1 = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![Expression::Variable("n"), Expression::Parameter("props")],
        };

        let expected_set_item2 = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                Expression::PropertyAccessExp(PropertyAccess {
                    base: "n",
                    key: "rage",
                }),
                Expression::Literal(Literal::String("blah")),
            ],
        };
        assert_eq!(set_clause.set_items[0], expected_set_item1);
        assert_eq!(set_clause.set_items[1], expected_set_item2);

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            return_items: vec![ReturnItem {
                expression: Expression::Variable("n"),
                alias: None,
            }],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.where_clause.is_none(),
            "Expected WHERE clause to be None"
        );
        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    #[test]
    fn test_parse_full_delete_query() {
        let input = "
            MATCH (n {name: 'Andres'})
            DETACH DELETE n;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(query_ast.match_clause.is_some(), "Expected MATCH clause");
        let match_clause = query_ast.match_clause.unwrap();
        let expected_match_clause = MatchClause {
            path_patterns: vec![PathPattern::Node(NodePattern {
                name: Some("n"),
                label: None,
                properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                    key: "name",
                    value: Expression::Literal(Literal::String("Andres")),
                })]),
            })],
        };
        assert_eq!(match_clause, expected_match_clause);

        assert!(query_ast.delete_clause.is_some(), "Expected DELETE clause");
        let delete_clause = query_ast.delete_clause.unwrap();
        let expected_delete_clause = DeleteClause {
            is_detach: true,
            delete_items: vec![Expression::Variable("n")],
        };
        assert_eq!(delete_clause, expected_delete_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.where_clause.is_none(),
            "Expected WHERE clause to be None"
        );
        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.return_clause.is_none(),
            "Expected RETURN clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    #[test]
    fn test_parse_full_remove_query() {
        let input = "
            MATCH (andres {name: 'Andres'})
            REMOVE andres.age, andres.address
            RETURN andres;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(query_ast.match_clause.is_some(), "Expected MATCH clause");
        let match_clause = query_ast.match_clause.unwrap();
        let expected_match_clause = MatchClause {
            path_patterns: vec![PathPattern::Node(NodePattern {
                name: Some("andres"),
                label: None,
                properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                    key: "name",
                    value: Expression::Literal(Literal::String("Andres")),
                })]),
            })],
        };
        assert_eq!(match_clause, expected_match_clause);

        assert!(query_ast.remove_clause.is_some(), "Expected REMOVE clause");
        let remove_clause = query_ast.remove_clause.unwrap();
        let expected_remove_clause = RemoveClause {
            remove_items: vec![
                PropertyAccess {
                    base: "andres",
                    key: "age",
                },
                PropertyAccess {
                    base: "andres",
                    key: "address",
                },
            ],
        };
        assert_eq!(remove_clause, expected_remove_clause);

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            return_items: vec![ReturnItem {
                expression: Expression::Variable("andres"),
                alias: None,
            }],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.where_clause.is_none(),
            "Expected WHERE clause to be None"
        );
        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    #[test]
    fn test_parse_full_logical_operators_query() {
        let input = "
            MATCH (p:Person)
            WHERE p.name IN ['Alice', 'Bob'] AND (p.age > 30 OR p.age < 20)
            RETURN p;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(query_ast.match_clause.is_some(), "Expected MATCH clause");
        let match_clause = query_ast.match_clause.unwrap();
        let expected_match_clause = MatchClause {
            path_patterns: vec![PathPattern::Node(NodePattern {
                name: Some("p"),
                label: Some("Person"),
                properties: None,
            })],
        };
        assert_eq!(match_clause, expected_match_clause);

        assert!(query_ast.where_clause.is_some(), "Expected WHERE clause");
        let where_clause = query_ast.where_clause.unwrap();
        let expected_where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::And,
                operands: vec![
                    // Left operand: p.name IN ['Alice', 'Bob']
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::In,
                        operands: vec![
                            Expression::PropertyAccessExp(PropertyAccess {
                                base: "p",
                                key: "name",
                            }),
                            Expression::List(vec![
                                Expression::Literal(Literal::String("Alice")),
                                Expression::Literal(Literal::String("Bob")),
                            ]),
                        ],
                    }),
                    // Right operand: (p.age > 30 OR p.age < 20)
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Or,
                        operands: vec![
                            // p.age > 30
                            Expression::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::GreaterThan,
                                operands: vec![
                                    Expression::PropertyAccessExp(PropertyAccess {
                                        base: "p",
                                        key: "age",
                                    }),
                                    Expression::Literal(Literal::Integer(30)),
                                ],
                            }),
                            // p.age < 20
                            Expression::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::LessThan,
                                operands: vec![
                                    Expression::PropertyAccessExp(PropertyAccess {
                                        base: "p",
                                        key: "age",
                                    }),
                                    Expression::Literal(Literal::Integer(20)),
                                ],
                            }),
                        ],
                    }),
                ],
            }),
        };
        assert_eq!(where_clause, expected_where_clause);

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            return_items: vec![ReturnItem {
                expression: Expression::Variable("p"),
                alias: None,
            }],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    #[test]
    fn test_parse_full_query_with_and_or_in_not_in() {
        let input = "
            MATCH (p:Person)
            WHERE p.name IN ['Alice', 'Bob'] AND 
                  p.city NOT IN ['Chicago', 'Miami'] AND 
                  (p.age > 30 OR p.age < 20)
            RETURN p;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        // --- MATCH clause ---
        assert!(query_ast.match_clause.is_some(), "Expected MATCH clause");
        let match_clause = query_ast.match_clause.unwrap();
        let expected_match_clause = MatchClause {
            path_patterns: vec![PathPattern::Node(NodePattern {
                name: Some("p"),
                label: Some("Person"),
                properties: None,
            })],
        };
        assert_eq!(match_clause, expected_match_clause);

        assert!(query_ast.where_clause.is_some(), "Expected WHERE clause");
        let where_clause = query_ast.where_clause.unwrap();

        let expected_where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::And,
                operands: vec![
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::And,
                        operands: vec![
                            Expression::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::In,
                                operands: vec![
                                    Expression::PropertyAccessExp(PropertyAccess {
                                        base: "p",
                                        key: "name",
                                    }),
                                    Expression::List(vec![
                                        Expression::Literal(Literal::String("Alice")),
                                        Expression::Literal(Literal::String("Bob")),
                                    ]),
                                ],
                            }),
                            Expression::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::NotIn,
                                operands: vec![
                                    Expression::PropertyAccessExp(PropertyAccess {
                                        base: "p",
                                        key: "city",
                                    }),
                                    Expression::List(vec![
                                        Expression::Literal(Literal::String("Chicago")),
                                        Expression::Literal(Literal::String("Miami")),
                                    ]),
                                ],
                            }),
                        ],
                    }),
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Or,
                        operands: vec![
                            // p.age > 30
                            Expression::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::GreaterThan,
                                operands: vec![
                                    Expression::PropertyAccessExp(PropertyAccess {
                                        base: "p",
                                        key: "age",
                                    }),
                                    Expression::Literal(Literal::Integer(30)),
                                ],
                            }),
                            // p.age < 20
                            Expression::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::LessThan,
                                operands: vec![
                                    Expression::PropertyAccessExp(PropertyAccess {
                                        base: "p",
                                        key: "age",
                                    }),
                                    Expression::Literal(Literal::Integer(20)),
                                ],
                            }),
                        ],
                    }),
                ],
            }),
        };
        assert_eq!(where_clause, expected_where_clause);

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            return_items: vec![ReturnItem {
                expression: Expression::Variable("p"),
                alias: None,
            }],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    #[test]
    fn test_create_node_table_clause() {
        let input: &str =
            "CREATE NODE TABLE Product (title STRING, price INT64, PRIMARY KEY (title, price));";
        let query_ast = parse_query(input).expect("Query parsing failed");

        // --- CREATE NODE TABLE clause ---
        assert!(
            query_ast.create_node_table_clause.is_some(),
            "Expected CREATE NODE TABLE clause"
        );
        let create_node_table_clause = query_ast.create_node_table_clause.unwrap();
        let expected_created_node_table_clause = CreateNodeTableClause {
            table_name: "Product",
            table_schema: vec![
                ColumnSchema {
                    column_name: "title",
                    column_dtype: "STRING",
                    default_value: None,
                },
                ColumnSchema {
                    column_name: "price",
                    column_dtype: "INT64",
                    default_value: None,
                },
            ],
            table_properties: vec![Expression::FunctionCallExp(FunctionCall {
                name: "PRIMARY KEY".to_string(),
                args: vec![Expression::Variable("title"), Expression::Variable("price")],
            })],
        };
        assert_eq!(create_node_table_clause, expected_created_node_table_clause);
    }

    #[test]
    fn test_create_rel_table_clause() {
        let input = "CREATE REL TABLE Follows (FROM User TO User, since DATE, age INT64, PRIMARY KEY (since));";
        let query_ast = parse_query(input).expect("Query parsing failed");

        // --- CREATE REL TABLE clause ---
        assert!(
            query_ast.create_rel_table_clause.is_some(),
            "Expected CREATE REL TABLE clause"
        );
        let create_rel_table_clause = query_ast.create_rel_table_clause.unwrap();

        let expected_create_rel_table_clause = CreateRelTableClause {
            table_name: "Follows",
            from: "User",
            to: "User",
            table_schema: vec![
                ColumnSchema {
                    column_name: "since",
                    column_dtype: "DATE",
                    default_value: None,
                },
                ColumnSchema {
                    column_name: "age",
                    column_dtype: "INT64",
                    default_value: None,
                },
            ],
            table_properties: vec![Expression::FunctionCallExp(FunctionCall {
                name: "PRIMARY KEY".to_string(),
                args: vec![Expression::Variable("since")],
            })],
        };

        assert_eq!(create_rel_table_clause, expected_create_rel_table_clause);
    }
}
