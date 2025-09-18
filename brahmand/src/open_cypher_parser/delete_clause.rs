use nom::{
    IResult, Parser,
    bytes::complete::tag_no_case,
    character::complete::{char, multispace0},
    combinator::{cut, opt},
    error::context,
    multi::separated_list1,
    sequence::delimited,
};

use super::{
    ast::{DeleteClause, Expression},
    common::ws,
    errors::OpenCypherParsingError,
    expression::parse_literal_or_variable_expression,
};

pub fn parse_delete_clause(
    input: &'_ str,
) -> IResult<&'_ str, DeleteClause<'_>, OpenCypherParsingError<'_>> {
    // Optionally consume "DETACH" keyword.
    let (input, detach_opt) = opt(ws(tag_no_case("DETACH"))).parse(input)?;
    let is_detach = detach_opt.is_some();

    let (input, _) = ws(tag_no_case("DELETE")).parse(input)?;

    let (input, delete_items) = context(
        "Error in delete clause",
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            ws(cut(literal_or_variable_expression_parser)),
        ),
    )
    .parse(input)?;

    let delete_clause = DeleteClause {
        is_detach,
        delete_items,
    };

    Ok((input, delete_clause))
}

fn literal_or_variable_expression_parser(
    input: &str,
) -> IResult<&str, Expression<'_>, OpenCypherParsingError<'_>> {
    parse_literal_or_variable_expression(input).map_err(|e| match e {
        nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
        nom::Err::Error(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
        nom::Err::Failure(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::Err;

    #[test]
    fn test_parse_delete_clause_simple() {
        let input = "DELETE a";
        let res = parse_delete_clause(input);
        match res {
            Ok((remaining, delete_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(delete_clause.is_detach, false);
                assert_eq!(delete_clause.delete_items.len(), 1);
                assert_eq!(&delete_clause.delete_items[0], &Expression::Variable("a"));
            }
            Err(e) => {
                panic!("Parsing failed unexpectedly: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_delete_clause_with_detach() {
        let input = "DETACH DELETE a, b";
        let res = parse_delete_clause(input);
        match res {
            Ok((remaining, delete_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(delete_clause.is_detach, true);
                assert_eq!(delete_clause.delete_items.len(), 2);
                assert_eq!(&delete_clause.delete_items[0], &Expression::Variable("a"));
                assert_eq!(&delete_clause.delete_items[1], &Expression::Variable("b"));
            }
            Err(e) => {
                panic!("Parsing failed unexpectedly: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_delete_clause_multiple_items_with_spaces() {
        let input = "DETACH DELETE   a  ,   b  , c";
        let res = parse_delete_clause(input);
        match res {
            Ok((remaining, delete_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(delete_clause.is_detach, true);
                assert_eq!(delete_clause.delete_items.len(), 3);
                assert_eq!(&delete_clause.delete_items[0], &Expression::Variable("a"));
                assert_eq!(&delete_clause.delete_items[1], &Expression::Variable("b"));
                assert_eq!(&delete_clause.delete_items[2], &Expression::Variable("c"));
            }
            Err(e) => {
                panic!("Parsing failed unexpectedly: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_delete_clause_error_no_items() {
        let input = "DELETE";
        let res = parse_delete_clause(input);
        match res {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected: error due to missing delete items.
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected error due to missing delete items, but got remaining: {:?} and clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_delete_clause_wrong_keyword() {
        let input = "REMOVE a";
        let res = parse_delete_clause(input);
        match res {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected error because DELETE keyword is missing.
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure for wrong keyword, but got remaining: {:?} and clause: {:?}",
                    remaining, clause
                );
            }
            Err(_) => todo!(),
        }
    }
}
