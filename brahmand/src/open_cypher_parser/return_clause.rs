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
    ast::{ReturnClause, ReturnItem},
    common::ws,
    errors::OpenCypherParsingError,
    expression::{parse_expression, parse_identifier},
};

fn parse_return_item(input: &'_ str) -> IResult<&'_ str, ReturnItem<'_>> {
    let (input, expression) = parse_expression.parse(input)?;
    let (input, alias) = opt(preceded(ws(tag_no_case("AS")), ws(parse_identifier))).parse(input)?;

    let return_item = ReturnItem { expression, alias };
    Ok((input, return_item))
}

pub fn parse_return_clause(input: &'_ str) -> IResult<&'_ str, ReturnClause<'_>, OpenCypherParsingError<'_>> {
    // Parse the RETURN statement

    let (input, _) = ws(tag_no_case("RETURN")).parse(input)?;

    let (input, return_items) = context(
        "Error in return clause",
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            cut(return_item_parser),
        ),
    )
    .parse(input)?;

    let return_clause = ReturnClause { return_items };

    Ok((input, return_clause))
}

fn return_item_parser(input: &str) -> IResult<&str, ReturnItem<'_>, OpenCypherParsingError<'_>> {
    parse_return_item(input).map_err(|e| match e {
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
    fn test_parse_return_item_no_alias() {
        let input = "a";
        let res = parse_return_item(input);
        match res {
            Ok((remaining, return_item)) => {
                assert_eq!(remaining, "");
                let expected = ReturnItem {
                    expression: Expression::Variable("a"),
                    alias: None,
                };
                assert_eq!(&return_item, &expected);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_return_item_with_alias() {
        let input = "a AS alias";
        let res = parse_return_item(input);
        match res {
            Ok((remaining, return_item)) => {
                assert_eq!(remaining, "");
                let expected = ReturnItem {
                    expression: Expression::Variable("a"),
                    alias: Some("alias"),
                };
                assert_eq!(&return_item, &expected);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_return_clause_single_item() {
        let input = "RETURN a";
        let res = parse_return_clause(input);
        match res {
            Ok((remaining, return_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(return_clause.return_items.len(), 1);
                let expected_item = ReturnItem {
                    expression: Expression::Variable("a"),
                    alias: None,
                };
                assert_eq!(&return_clause.return_items[0], &expected_item);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_return_clause_multiple_items() {
        let input = "RETURN a, b AS aliasB, c";
        let res = parse_return_clause(input);
        match res {
            Ok((remaining, return_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(return_clause.return_items.len(), 3);
                let expected_item1 = ReturnItem {
                    expression: Expression::Variable("a"),
                    alias: None,
                };
                let expected_item2 = ReturnItem {
                    expression: Expression::Variable("b"),
                    alias: Some("aliasB"),
                };
                let expected_item3 = ReturnItem {
                    expression: Expression::Variable("c"),
                    alias: None,
                };
                assert_eq!(&return_clause.return_items[0], &expected_item1);
                assert_eq!(&return_clause.return_items[1], &expected_item2);
                assert_eq!(&return_clause.return_items[2], &expected_item3);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_return_clause_extra_whitespace() {
        let input = "   RETURN   a   AS  a_alias  ,   b   ,   c  AS c_alias  ";
        let res = parse_return_clause(input);
        match res {
            Ok((remaining, return_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(return_clause.return_items.len(), 3);
                let expected_item1 = ReturnItem {
                    expression: Expression::Variable("a"),
                    alias: Some("a_alias"),
                };
                let expected_item2 = ReturnItem {
                    expression: Expression::Variable("b"),
                    alias: None,
                };
                let expected_item3 = ReturnItem {
                    expression: Expression::Variable("c"),
                    alias: Some("c_alias"),
                };
                assert_eq!(&return_clause.return_items[0], &expected_item1);
                assert_eq!(&return_clause.return_items[1], &expected_item2);
                assert_eq!(&return_clause.return_items[2], &expected_item3);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_return_clause_missing_return_keyword() {
        let input = "Match a, b AS aliasB";
        let res = parse_return_clause(input);
        match res {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected failure because the input does not start with "RETURN".
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure due to missing RETURN keyword, but got remaining: {:?} and clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
}
