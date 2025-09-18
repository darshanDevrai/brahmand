use nom::{
    IResult, Parser,
    bytes::complete::tag_no_case,
    character::complete::{char, multispace0},
    combinator::{cut, opt},
    error::context,
    multi::separated_list1,
    sequence::{delimited, preceded},
};

use super::{
    ast::{WithClause, WithItem},
    common::ws,
    errors::OpenCypherParsingError,
    expression::{parse_expression, parse_identifier},
};

fn parse_with_item(input: &'_ str) -> IResult<&'_ str, WithItem<'_>> {
    let (input, expression) = parse_expression.parse(input)?;
    let (input, alias) = opt(preceded(ws(tag_no_case("AS")), ws(parse_identifier))).parse(input)?;

    let with_item = WithItem { expression, alias };
    Ok((input, with_item))
}

pub fn parse_with_clause(
    input: &'_ str,
) -> IResult<&'_ str, WithClause<'_>, OpenCypherParsingError<'_>> {
    // Parse the RETURN statement

    let (input, _) = ws(tag_no_case("WITH")).parse(input)?;

    let (input, with_items) = context(
        "Error in with clause",
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            cut(with_item_parser),
        ),
    )
    .parse(input)?;

    let with_clause = WithClause { with_items };

    Ok((input, with_clause))
}

fn with_item_parser(input: &str) -> IResult<&str, WithItem<'_>, OpenCypherParsingError<'_>> {
    parse_with_item(input).map_err(|e| match e {
        nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
        nom::Err::Error(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
        nom::Err::Failure(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
    })
}

#[cfg(test)]
mod tests {
    use crate::open_cypher_parser::ast::Expression;

    use super::*;
    use nom::Err;

    #[test]
    fn test_parse_with_item_no_alias() {
        let input = "a";
        let res = parse_with_item(input);
        match res {
            Ok((remaining, with_item)) => {
                assert_eq!(remaining, "");
                let expected = WithItem {
                    expression: Expression::Variable("a"),
                    alias: None,
                };
                assert_eq!(&with_item, &expected);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_with_item_with_alias() {
        let input = "a AS alias";
        let res = parse_with_item(input);
        match res {
            Ok((remaining, with_item)) => {
                assert_eq!(remaining, "");
                let expected = WithItem {
                    expression: Expression::Variable("a"),
                    alias: Some("alias"),
                };
                assert_eq!(&with_item, &expected);
            }
            Err(e) => panic!("Expected successful parse with alias, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_with_clause_multiple_items() {
        let input = "WITH a, b AS aliasB, c";
        let res = parse_with_clause(input);
        match res {
            Ok((remaining, with_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(with_clause.with_items.len(), 3);
                let expected_item1 = WithItem {
                    expression: Expression::Variable("a"),
                    alias: None,
                };
                let expected_item2 = WithItem {
                    expression: Expression::Variable("b"),
                    alias: Some("aliasB"),
                };
                let expected_item3 = WithItem {
                    expression: Expression::Variable("c"),
                    alias: None,
                };
                assert_eq!(&with_clause.with_items[0], &expected_item1);
                assert_eq!(&with_clause.with_items[1], &expected_item2);
                assert_eq!(&with_clause.with_items[2], &expected_item3);
            }
            Err(e) => panic!(
                "Expected successful parse for multiple items, got error: {:?}",
                e
            ),
        }
    }

    #[test]
    fn test_parse_with_clause_extra_whitespace() {
        let input = "   WITH   a   AS  aliasA  ,   b  ,  c AS aliasC   ";
        let res = parse_with_clause(input);
        match res {
            Ok((remaining, with_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(with_clause.with_items.len(), 3);
                let expected_item1 = WithItem {
                    expression: Expression::Variable("a"),
                    alias: Some("aliasA"),
                };
                let expected_item2 = WithItem {
                    expression: Expression::Variable("b"),
                    alias: None,
                };
                let expected_item3 = WithItem {
                    expression: Expression::Variable("c"),
                    alias: Some("aliasC"),
                };
                assert_eq!(&with_clause.with_items[0], &expected_item1);
                assert_eq!(&with_clause.with_items[1], &expected_item2);
                assert_eq!(&with_clause.with_items[2], &expected_item3);
            }
            Err(e) => panic!(
                "Expected successful parse with extra whitespace, got error: {:?}",
                e
            ),
        }
    }

    #[test]
    fn test_parse_with_clause_missing_keyword() {
        let input = "MATCH a, b AS aliasB";
        let res = parse_with_clause(input);
        match res {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected failure since the input does not begin with "WITH".
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure due to missing WITH keyword, but got remaining: {:?} clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
}
