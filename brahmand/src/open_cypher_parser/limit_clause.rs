use nom::{IResult, Parser, bytes::complete::tag_no_case, combinator::cut, error::context};

use super::{
    ast::{Expression, LimitClause, Literal},
    common::ws,
    errors::OpenCypherParsingError,
    expression::parse_expression,
};

pub fn parse_limit_clause(
    input: &'_ str,
) -> IResult<&'_ str, LimitClause, OpenCypherParsingError<'_>> {
    // Parse the MATCH statement

    let (input, _) = ws(tag_no_case("LIMIT")).parse(input)?;

    let (input, expression) =
        context("Error in limit clause", cut(expression_parser)).parse(input)?;

    if let Expression::Literal(Literal::Integer(limit)) = expression {
        let limit_clause = LimitClause { limit_item: limit };

        Ok((input, limit_clause))
    } else {
        // return error
        Err(nom::Err::Failure(OpenCypherParsingError {
            errors: vec![(
                "Value of limit clause should be integer",
                "Error in limit clause",
            )],
        }))
    }
}

fn expression_parser(input: &str) -> IResult<&str, Expression<'_>, OpenCypherParsingError<'_>> {
    parse_expression(input).map_err(|e| match e {
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
    fn test_parse_limit_clause_valid() {
        let input = "LIMIT 100";
        let res = parse_limit_clause(input);
        match res {
            Ok((remaining, limit_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(limit_clause.limit_item, 100);
            }
            Err(e) => panic!("Expected valid limit clause, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_limit_clause_valid_with_whitespace_and_lowercase() {
        let input = "   limit    200   ";
        let res = parse_limit_clause(input);
        match res {
            Ok((remaining, limit_clause)) => {
                // After trimming, remaining input should be empty.
                assert_eq!(remaining, "");
                assert_eq!(limit_clause.limit_item, 200);
            }
            Err(e) => panic!(
                "Expected valid limit clause with whitespace, got error: {:?}",
                e
            ),
        }
    }

    #[test]
    fn test_parse_limit_clause_invalid_non_integer() {
        let input = "LIMIT abc";
        let res = parse_limit_clause(input);
        match res {
            Ok((_, clause)) => {
                panic!(
                    "Expected failure for non-integer limit clause, but got: {:?}",
                    clause
                );
            }
            Err(Err::Failure(e)) => {
                let error_str = format!("{:?}", e);
                assert!(
                    error_str.contains("Value of limit clause should be integer"),
                    "Error message does not mention integer requirement: {}",
                    error_str
                );
            }
            Err(e) => {
                panic!("Expected failure error, but got: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_limit_clause_invalid_float() {
        let input = "LIMIT 10.5";
        let res = parse_limit_clause(input);
        match res {
            Ok((_, clause)) => {
                panic!(
                    "Expected failure for non-integer (float) limit clause, but got: {:?}",
                    clause
                );
            }
            Err(Err::Failure(e)) => {
                let error_str = format!("{:?}", e);
                assert!(
                    error_str.contains("Value of limit clause should be integer"),
                    "Error message does not mention integer requirement: {}",
                    error_str
                );
            }
            Err(e) => {
                panic!("Expected failure error, but got: {:?}", e);
            }
        }
    }
}
