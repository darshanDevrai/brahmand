use nom::{IResult, Parser, bytes::complete::tag_no_case, combinator::cut, error::context};

use super::{
    ast::WhereClause, common::ws, errors::OpenCypherParsingError, expression::parse_expression,
};

pub fn parse_where_clause(input: &'_ str) -> IResult<&'_ str, WhereClause<'_>, OpenCypherParsingError<'_>> {
    // Parse the WHERE statement

    let (input, _) = ws(tag_no_case("WHERE")).parse(input)?;

    // let (input, pattern_part) = parse_pattern.parse(input)?;
    let (input, expression) = context("Error in where clause", cut(parse_expression))
        .parse(input)
        .map_err(|e| match e {
            nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
            nom::Err::Error(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
            nom::Err::Failure(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
        })?;

    let where_clause = WhereClause {
        conditions: expression,
    };
    Ok((input, where_clause))
}

#[cfg(test)]
mod tests {
    use crate::open_cypher_parser::ast::{Expression, Literal, Operator, OperatorApplication};

    use super::*;
    use nom::Err;

    #[test]
    fn test_parse_where_clause_valid() {
        let input = "WHERE a = 1";
        let result = parse_where_clause(input);
        match result {
            Ok((remaining, where_clause)) => {
                assert_eq!(remaining, "");
                let expected_operator_application =
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            Expression::Variable("a"),
                            Expression::Literal(Literal::Integer(1)),
                        ],
                    });
                let expected = WhereClause {
                    conditions: expected_operator_application,
                };
                assert_eq!(&where_clause, &expected);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_where_clause_valid_extra_whitespace() {
        let input = "   WHERE    a = 1   ";
        let result = parse_where_clause(input);
        match result {
            Ok((remaining, where_clause)) => {
                assert_eq!(remaining, "");
                let expected_operator_application =
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            Expression::Variable("a"),
                            Expression::Literal(Literal::Integer(1)),
                        ],
                    });
                let expected = WhereClause {
                    conditions: expected_operator_application,
                };
                assert_eq!(&where_clause, &expected);
            }
            Err(e) => panic!(
                "Expected successful parse with extra whitespace, got error: {:?}",
                e
            ),
        }
    }

    #[test]
    fn test_parse_where_clause_invalid_condition() {
        let input = "WHERE a";
        let result = parse_where_clause(input);
        match result {
            Err(Err::Failure(e)) => {
                let error_str = format!("{:?}", e);
                assert!(
                    error_str.contains("Value of where clause should be a condition"),
                    "Error message does not mention condition requirement: {}",
                    error_str
                );
            }
            Ok((remaining, clause)) => {
                if let WhereClause {
                    conditions: Expression::OperatorApplicationExp(op),
                } = clause
                {
                    panic!(
                        "Expected failure for non-condition input, but got remaining: {:?} and condition: {:?}",
                        remaining, op
                    );
                }
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_where_clause_missing_expression() {
        let input = "WHERE";
        let result = parse_where_clause(input);
        match result {
            Err(Err::Failure(_)) | Err(Err::Error(_)) => {
                // Expected failure due to missing expression.
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure for missing expression, but got remaining: {:?} and clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
}
