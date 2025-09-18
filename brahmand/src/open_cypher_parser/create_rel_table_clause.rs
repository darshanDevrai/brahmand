use nom::branch::alt;
use nom::character::complete::char;
use nom::combinator::{cut, map};
use nom::error::context;
use nom::sequence::delimited;
use nom::{IResult, Parser, bytes::complete::tag_no_case};

use super::ast::{ColumnSchema, CreateRelTableClause, Expression};
use super::common::ws;
use super::create_table_schema::parse_rel_table_properties_list;
use super::errors::OpenCypherParsingError;
// use super::create_table_schema::parse_table_properties_list;
use super::expression::parse_identifier;

// (table_name, (from, to), (schema, properties))
type ParsedRelTableSchema<'a> = (
    &'a str,
    (&'a str, &'a str),
    (Vec<ColumnSchema<'a>>, Vec<Expression<'a>>),
);

fn parse_rel_table_schema(input: &'_ str) -> IResult<&'_ str, ParsedRelTableSchema<'_>> {
    let (input, table_name) = ws(parse_identifier).parse(input)?;
    // Inside the parentheses, first parse the connection.
    let (input, (from_to, table_schema_prop)) = delimited(
        ws(char('(')),
        alt((
            // Case: the connection followed by a comma and properties list.
            map(
                (
                    parse_rel_connection,
                    ws(char(',')),
                    parse_rel_table_properties_list,
                ),
                |(connection, _, props)| (connection, Some(props)),
            ),
            // Case: the connection only.
            map(parse_rel_connection, |connection| (connection, None)),
        )),
        ws(char(')')),
    )
    .parse(input)?;

    let (schema, properties) = table_schema_prop.unwrap_or((Vec::new(), Vec::new()));
    let (from, to) = from_to;
    Ok((input, (table_name, (from, to), (schema, properties))))
}

/// Parse the relationship connection clause: "FROM table TO table"
fn parse_rel_connection(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, _) = ws(tag_no_case("FROM")).parse(input)?;
    let (input, from) = ws(parse_identifier).parse(input)?;
    let (input, _) = ws(tag_no_case("TO")).parse(input)?;
    let (input, to) = ws(parse_identifier).parse(input)?;
    Ok((input, (from, to)))
}

pub fn parse_create_rel_table_clause(
    input: &str,
) -> IResult<&str, CreateRelTableClause<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = ws(tag_no_case("CREATE")).parse(input)?;
    let (input, _) = ws(tag_no_case("REL")).parse(input)?;
    let (input, _) = ws(tag_no_case("TABLE")).parse(input)?;

    let (input, (table_name, (from, to), (schema, properties))) = context(
        "Error in create rel table clause",
        cut(rel_table_schema_parser),
    )
    .parse(input)?;

    let create_rel_table_clause = CreateRelTableClause {
        table_name,
        from,
        to,
        table_schema: schema,
        table_properties: properties,
    };

    Ok((input, create_rel_table_clause))
}

fn rel_table_schema_parser(
    input: &'_ str,
) -> IResult<&'_ str, ParsedRelTableSchema<'_>, OpenCypherParsingError<'_>> {
    parse_rel_table_schema(input).map_err(|e| match e {
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
    fn test_create_rel_table_clause() {
        let input = "CREATE REL TABLE Follows(FROM User TO User, since DATE, age INT64, PRIMARY KEY (since))";
        let (remaining, ast) = parse_create_rel_table_clause(input).unwrap();
        assert!(remaining.trim().is_empty());
        let expected = CreateRelTableClause {
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
        assert_eq!(ast, expected);
    }

    #[test]
    fn test_create_rel_table_clause_with_default() {
        let input = "CREATE REL TABLE Follows(FROM User TO User, since DATE DEFAULT 'today', PRIMARY KEY (since))";
        let (remaining, ast) = parse_create_rel_table_clause(input).unwrap();
        assert!(remaining.trim().is_empty());
        let expected = CreateRelTableClause {
            table_name: "Follows",
            from: "User",
            to: "User",
            table_schema: vec![ColumnSchema {
                column_name: "since",
                column_dtype: "DATE",
                default_value: Some(Expression::Literal(Literal::String("today"))),
            }],
            table_properties: vec![Expression::FunctionCallExp(FunctionCall {
                name: "PRIMARY KEY".to_string(),
                args: vec![Expression::Variable("since")],
            })],
        };
        assert_eq!(ast, expected);
    }

    #[test]
    fn test_create_rel_table_clause_missing_connection() {
        let input = "CREATE REL TABLE Follows (since DATE, PRIMARY KEY (since))";
        let result = parse_create_rel_table_clause(input);
        match result {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected failure because the connection clause ("FROM ... TO ...") is missing.
            }
            Ok((rem, clause)) => {
                panic!(
                    "Expected failure due to missing connection, but got remaining: {:?} clause: {:?}",
                    rem, clause
                )
            }
            Err(_) => todo!(),
        }
    }
}
