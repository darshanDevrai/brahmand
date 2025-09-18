use nom::combinator::cut;
use nom::error::context;
use nom::{IResult, Parser, bytes::complete::tag_no_case};

use super::ast::{ColumnSchema, CreateNodeTableClause, Expression};
use super::common::ws;
use super::create_table_schema::parse_node_table_properties_list;
use super::errors::OpenCypherParsingError;
use super::expression::parse_identifier;

// (table_name,(schema, properties))
type ParsedNodeTableSchema<'a> = (&'a str, (Vec<ColumnSchema<'a>>, Vec<Expression<'a>>));

pub fn parse_node_table_schema(input: &str) -> IResult<&str, ParsedNodeTableSchema> {
    let (input, table_name) = ws(parse_identifier).parse(input)?;

    let (input, (schema, properties)) = parse_node_table_properties_list(input)?;
    Ok((input, (table_name, (schema, properties))))
}

pub fn parse_create_node_table_clause(
    input: &str,
) -> IResult<&str, CreateNodeTableClause<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = ws(tag_no_case("CREATE")).parse(input)?;
    let (input, _) = ws(tag_no_case("NODE")).parse(input)?;
    let (input, _) = ws(tag_no_case("TABLE")).parse(input)?;

    let (input, (table_name, (schema, properties))) = context(
        "Error in create node table clause",
        cut(node_table_schema_parser),
    )
    .parse(input)?;

    let create_node_table_clause = CreateNodeTableClause {
        table_name,
        table_schema: schema,
        table_properties: properties,
    };

    Ok((input, create_node_table_clause))
}

fn node_table_schema_parser(
    input: &'_ str,
) -> IResult<&'_ str, ParsedNodeTableSchema<'_>, OpenCypherParsingError<'_>> {
    parse_node_table_schema(input).map_err(|e| match e {
        nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
        nom::Err::Error(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
        nom::Err::Failure(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
    })
}

#[cfg(test)]
mod tests {
    use crate::open_cypher_parser::ast::{FunctionCall, Literal};

    use super::*;
    use nom::Err;

    #[test]
    fn test_create_node_table_clause() {
        let input =
            "CREATE NODE TABLE Product (title STRING, price INT64, PRIMARY KEY (title, price))";
        let (remaining, ast) = parse_create_node_table_clause(input).unwrap();

        assert!(remaining.trim().is_empty());

        let expected = CreateNodeTableClause {
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

        assert_eq!(ast, expected);
    }
    #[test]
    fn test_create_node_table_clause_with_default_value() {
        let input = "CREATE NODE TABLE User (name STRING, age INT64 DEFAULT 0, PRIMARY KEY (name))";
        let (remaining, ast) = parse_create_node_table_clause(input).unwrap();

        assert!(remaining.trim().is_empty());

        let expected = CreateNodeTableClause {
            table_name: "User",
            table_schema: vec![
                ColumnSchema {
                    column_name: "name",
                    column_dtype: "STRING",
                    default_value: None,
                },
                ColumnSchema {
                    column_name: "age",
                    column_dtype: "INT64",
                    default_value: Some(Expression::Literal(Literal::Integer(0))),
                },
            ],
            table_properties: vec![Expression::FunctionCallExp(FunctionCall {
                name: "PRIMARY KEY".to_string(),
                args: vec![Expression::Variable("name")],
            })],
        };

        assert_eq!(ast, expected);
    }

    #[test]
    fn test_parse_create_node_table_clause_missing_table_schema() {
        let input = "CREATE NODE TABLE";
        let result = parse_create_node_table_clause(input);
        match result {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected failure because no path pattern is provided.
            }
            Ok((rem, clause)) => {
                panic!(
                    "Expected failure due to missing schema, but got remaining: {:?} clause: {:?}",
                    rem, clause
                )
            }
            Err(e) => {
                panic!("Unexpected error type: {:?}", e)
            }
        }
    }

    #[test]
    fn test_parse_create_node_table_clause_wrong_keyword() {
        let input = "CREATE Product (title STRING, price INT64, PRIMARY KEY (title, price))";
        let result = parse_create_node_table_clause(input);
        match result {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected an error because the clause keyword is incorrect.
            }
            Ok((rem, clause)) => {
                panic!(
                    "Expected failure for wrong clause keyword, but got remaining: {:?} clause: {:?}",
                    rem, clause
                )
            }
            Err(_) => todo!(),
        }
    }
}
