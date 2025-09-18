use nom::{
    IResult, Parser,
    bytes::complete::tag_no_case,
    character::complete::{char, multispace0},
    combinator::cut,
    error::context,
    multi::separated_list1,
    sequence::delimited,
};

use super::{
    ast::{Expression, OperatorApplication, SetClause},
    common::ws,
    errors::OpenCypherParsingError,
    expression::parse_expression,
};

pub fn parse_set_clause(input: &'_ str) -> IResult<&'_ str, SetClause<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = ws(tag_no_case("SET")).parse(input)?;

    let (input, set_items) = context(
        "Error in set clause",
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            cut(set_item_parser),
        ),
    )
    .parse(input)?;

    let set_clause = SetClause { set_items };

    Ok((input, set_clause))
}

fn set_item_parser(input: &'_ str) -> IResult<&'_ str, OperatorApplication<'_>, OpenCypherParsingError<'_>> {
    let (input, expression) = parse_expression.parse(input).map_err(|e| match e {
        nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
        nom::Err::Error(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
        nom::Err::Failure(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
    })?;

    if let Expression::OperatorApplicationExp(operator_application) = expression {
        Ok((input, operator_application))
    } else {
        // return error
        Err(nom::Err::Failure(OpenCypherParsingError {
            errors: vec![(
                "Value of set clause should be binary application",
                "Error in set clause",
            )],
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::open_cypher_parser::ast::{Literal, Operator};

    use super::*;
    use nom::Err;

    #[test]
    fn test_parse_set_clause_single_item() {
        let input = "SET a = 1";
        let res = parse_set_clause(input);
        match res {
            Ok((remaining, set_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(set_clause.set_items.len(), 1);

                let expected_operator_application = OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        Expression::Variable("a"),
                        Expression::Literal(Literal::Integer(1)),
                    ],
                };

                let actual_operator_application = &set_clause.set_items[0];
                assert_eq!(actual_operator_application, &expected_operator_application);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_set_clause_multiple_items() {
        let input = "SET a = 1, b = 2";
        let res = parse_set_clause(input);
        match res {
            Ok((remaining, set_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(set_clause.set_items.len(), 2);

                let expected_operator_application1 = OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        Expression::Variable("a"),
                        Expression::Literal(Literal::Integer(1)),
                    ],
                };
                let expected_operator_application2 = OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        Expression::Variable("b"),
                        Expression::Literal(Literal::Integer(2)),
                    ],
                };

                assert_eq!(&set_clause.set_items[0], &expected_operator_application1);
                assert_eq!(&set_clause.set_items[1], &expected_operator_application2);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_set_clause_extra_whitespace() {
        let input = "  SET   a = 1   ,   b = 2  ";
        let res = parse_set_clause(input);
        match res {
            Ok((remaining, set_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(set_clause.set_items.len(), 2);

                let expected_operator_application1 = OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        Expression::Variable("a"),
                        Expression::Literal(Literal::Integer(1)),
                    ],
                };
                let expected_operator_application2 = OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        Expression::Variable("b"),
                        Expression::Literal(Literal::Integer(2)),
                    ],
                };

                assert_eq!(&set_clause.set_items[0], &expected_operator_application1);
                assert_eq!(&set_clause.set_items[1], &expected_operator_application2);
            }
            Err(e) => panic!(
                "Expected successful parse with extra whitespace, got error: {:?}",
                e
            ),
        }
    }

    #[test]
    fn test_parse_set_clause_invalid_item() {
        let input = "SET a";
        let res = parse_set_clause(input);
        match res {
            Err(Err::Failure(e)) => {
                let error_str = format!("{:?}", e);
                assert!(
                    error_str.contains("Value of set clause should be binary application"),
                    "Error message does not mention binary application requirement: {}",
                    error_str
                );
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure for invalid set item, but got remaining: {:?} and clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
}
