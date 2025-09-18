use nom::{IResult, Parser, bytes::complete::tag_no_case, combinator::cut, error::context};

use super::{
    ast::{Expression, Literal, SkipClause},
    common::ws,
    errors::OpenCypherParsingError,
    expression::parse_expression,
};

pub fn parse_skip_clause(
    input: &'_ str,
) -> IResult<&'_ str, SkipClause, OpenCypherParsingError<'_>> {
    let (input, _) = ws(tag_no_case("SKIP")).parse(input)?;

    let (input, expression) = context("Error in skip clause", cut(parse_expression))
        .parse(input)
        .map_err(|e| match e {
            nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
            nom::Err::Error(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
            nom::Err::Failure(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
        })?;

    if let Expression::Literal(Literal::Integer(skip_by)) = expression {
        let skip_clause = SkipClause { skip_item: skip_by };

        Ok((input, skip_clause))
    } else {
        // return error
        Err(nom::Err::Failure(OpenCypherParsingError {
            errors: vec![(
                "Value of skip clause should be integer",
                "Error in skip clause",
            )],
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::Err;

    #[test]
    fn test_parse_skip_clause_valid() {
        let input = "SKIP 5";
        let res = parse_skip_clause(input);
        match res {
            Ok((remaining, skip_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(skip_clause.skip_item, 5);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_skip_clause_valid_with_whitespace() {
        let input = "   SKIP    42   ";
        let res = parse_skip_clause(input);
        match res {
            Ok((remaining, skip_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(skip_clause.skip_item, 42);
            }
            Err(e) => panic!("Expected valid parse with whitespace, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_skip_clause_invalid_non_integer() {
        let input = "SKIP abc";
        let res = parse_skip_clause(input);
        match res {
            Err(Err::Failure(e)) => {
                let error_str = format!("{:?}", e);
                assert!(
                    error_str.contains("Value of skip clause should be integer"),
                    "Error message does not mention integer requirement: {}",
                    error_str
                );
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure for non-integer skip clause, but got remaining: {:?} and clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_skip_clause_invalid_float() {
        let input = "SKIP 3.14";
        let res = parse_skip_clause(input);
        match res {
            Err(Err::Failure(e)) => {
                let error_str = format!("{:?}", e);
                assert!(
                    error_str.contains("Value of skip clause should be integer"),
                    "Error message does not mention integer requirement: {}",
                    error_str
                );
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure for float literal, but got remaining: {:?} and clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_skip_clause_missing_expression() {
        let input = "SKIP";
        let res = parse_skip_clause(input);
        match res {
            Err(Err::Failure(_)) | Err(Err::Error(_)) => {
                // Expected failure due to missing expression.
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure for missing expression, but got remaining: {:?} and clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }
}
