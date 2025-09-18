use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until, take_while1},
    character::complete::{alphanumeric1, multispace0},
    combinator::{map, not, peek},
    error::{Error, ErrorKind},
    multi::{separated_list0, separated_list1},
    sequence::{delimited, preceded, terminated},
};

use nom::character::complete::char;

use crate::open_cypher_parser::common::{self, ws};

use super::{
    ast::{Expression, FunctionCall, Literal, Operator, OperatorApplication, PropertyAccess},
    path_pattern,
};

pub fn parse_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, expression) = parse_logical_or.parse(input)?;
    Ok((input, expression))
}

pub fn parse_path_pattern_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, path_pattern) = path_pattern::parse_path_pattern(input)?;
    Ok((input, Expression::PathPattern(path_pattern)))
}

// parse_postfix_expression
fn parse_postfix_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // First, parse a basic primary expression: literal, variable, or parenthesized expression.
    let (input, expr) = alt((
        parse_parameter,
        parse_property_access,
        parse_literal_or_variable_expression,
        delimited(ws(char('(')), parse_expression, ws(char(')'))),
    ))
    .parse(input)?;

    // Then, optionally, parse the postfix operator "IS NULL" or "IS NOT NULL".
    let (input, opt_op) = nom::combinator::opt(preceded(
        ws(tag_no_case("IS")),
        alt((
            map(
                preceded(ws(tag_no_case("NOT")), ws(tag_no_case("NULL"))),
                |_| Operator::IsNotNull,
            ),
            map(ws(tag_no_case("NULL")), |_| Operator::IsNull),
        )),
    ))
    .parse(input)?;

    if let Some(op) = opt_op {
        Ok((
            input,
            Expression::OperatorApplicationExp(OperatorApplication {
                operator: op,
                operands: vec![expr],
            }),
        ))
    } else {
        Ok((input, expr))
    }
}

fn parse_primary(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    alt((
        parse_path_pattern_expression,
        parse_function_call,
        parse_postfix_expression,
        parse_property_access,
        parse_list_literal,
        parse_parameter,
        parse_literal_or_variable_expression,
        delimited(ws(char('(')), parse_expression, ws(char(')'))),
    ))
    .parse(input)
}

pub fn parse_operator_symbols(input: &str) -> IResult<&str, Operator> {
    alt((
        map(tag_no_case(">="), |_| Operator::GreaterThanEqual),
        map(tag_no_case("<="), |_| Operator::LessThanEqual),
        map(tag_no_case("<>"), |_| Operator::NotEqual),
        map(tag_no_case(">"), |_| Operator::GreaterThan),
        map(tag_no_case("<"), |_| Operator::LessThan),
        map(tag_no_case("="), |_| Operator::Equal),
        map(tag_no_case("+"), |_| Operator::Addition),
        map(tag_no_case("-"), |_| Operator::Subtraction),
        map(tag_no_case("*"), |_| Operator::Multiplication),
        map(tag_no_case("/"), |_| Operator::Division),
        map(tag_no_case("%"), |_| Operator::ModuloDivision),
        map(tag_no_case("^"), |_| Operator::Exponentiation),
        map(tag_no_case("IN"), |_| Operator::In),
        map(tag_no_case("NOT IN"), |_| Operator::NotIn),
    ))
    .parse(input)
}

fn parse_unary_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    alt((
        map(
            preceded(ws(tag_no_case("NOT")), parse_unary_expression),
            |expr| {
                Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Not,
                    operands: vec![expr],
                })
            },
        ),
        map(
            preceded(ws(tag_no_case("DISTINCT")), parse_unary_expression),
            |expr| {
                Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Distinct,
                    operands: vec![expr],
                })
            },
        ),
        parse_primary, // fallback to a primary expression
    ))
    .parse(input)
}

fn parse_binary_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // parse the left-hand side.
    let (input, lhs) = parse_unary_expression(input)?;

    let mut remaining_input = input;
    let mut final_expression = lhs;

    loop {
        // Try to parse an operator and a right-hand side.
        let res = (ws(parse_operator_symbols), parse_unary_expression).parse(remaining_input);
        match res {
            Ok((new_input, (op, rhs))) => {
                // Build a new expression by moving the previous result into the operator application.
                final_expression = Expression::OperatorApplicationExp(OperatorApplication {
                    operator: op,
                    operands: vec![final_expression, rhs],
                });
                remaining_input = new_input;
            }
            // If no more operator/unary expression pair is found, break out.
            Err(nom::Err::Error(_)) => break,
            Err(e) => return Err(e),
        }
    }
    Ok((remaining_input, final_expression))
}

fn parse_logical_and(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, lhs) = parse_binary_expression(input)?;

    let mut remaining_input = input;
    let mut final_expression = lhs;

    loop {
        // Try to parse an "AND" followed by a binary expression.
        let res = preceded(ws(tag_no_case("AND")), parse_binary_expression).parse(remaining_input);
        match res {
            Ok((new_input, rhs)) => {
                // Build a new expression by moving `expr` and `rhs` into a new operator application.
                final_expression = Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    // final_expression here is lhs
                    operands: vec![final_expression, rhs],
                });
                remaining_input = new_input;
            }
            // If we don't match "AND", exit the loop.
            Err(nom::Err::Error(_)) => break,
            Err(e) => return Err(e),
        }
    }
    Ok((remaining_input, final_expression))
}

// fn parse_logical_or(input: &str) -> IResult<&str, Expression> {
//     let (input, left) = parse_logical_and(input)?;
//     fold_many0(
//         // parse only "OR" and not "ORDER"
//         preceded(ws(terminated(tag_no_case("OR"), not(peek(alphanumeric1)))), parse_logical_and),
//         move || left.clone(),
//         |acc, rhs| Expression::OperatorApplicationExp(OperatorApplication {
//             operator: Operator::Or,
//             operands: vec![acc, rhs],
//         }),
//     ).parse(input)
// }

fn parse_logical_or(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, lhs) = parse_logical_and(input)?;

    let mut remaining_input = input;
    let mut final_expression = lhs;

    loop {
        let res = preceded(
            // parse only "OR" and not "ORDER"
            ws(terminated(tag_no_case("OR"), not(peek(alphanumeric1)))),
            parse_logical_and,
        )
        .parse(remaining_input);

        match res {
            Ok((new_input, rhs)) => {
                final_expression = Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Or,
                    // final_expression here is lhs
                    operands: vec![final_expression, rhs],
                });
                remaining_input = new_input;
            }
            Err(nom::Err::Error(_)) => break,
            Err(e) => return Err(e),
        }
    }

    Ok((remaining_input, final_expression))
}

fn is_identifier_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

// Parse an identifier and return it as a String.
pub fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(is_identifier_char).parse(input)
}

pub fn parse_function_call(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // First, parse the function name.
    let (input, name) = ws(parse_identifier).parse(input)?;
    // Then parse the comma-separated arguments within parentheses.
    let (input, args) = delimited(
        ws(char('(')),
        separated_list0(ws(char(',')), parse_expression),
        ws(char(')')),
    )
    .parse(input)?;

    Ok((
        input,
        Expression::FunctionCallExp(FunctionCall {
            name: name.to_string(),
            args,
        }),
    ))
}

pub fn parse_list_literal(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse content within [ ... ] as a comma-separated list of expressions.
    let (input, exprs) = delimited(
        // Opening bracket with optional whitespace afterwards
        delimited(multispace0, char('['), multispace0),
        // Zero or more expressions separated by commas (with optional whitespace)
        separated_list0(
            delimited(multispace0, char(','), multispace0),
            parse_expression,
        ),
        // Closing bracket with optional whitespace preceding it
        delimited(multispace0, char(']'), multispace0),
    )
    .parse(input)?;

    Ok((input, Expression::List(exprs)))
}

pub fn parse_property_access(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, property_access_pair) =
        separated_list1(char('.'), common::parse_alphanumeric_with_underscore).parse(input)?;

    if property_access_pair.len() != 2 {
        return Err(nom::Err::Error(Error::new(input, ErrorKind::Float)));
    }

    let base = match parse_literal_or_variable_expression(property_access_pair[0]) {
        Ok((_, Expression::Variable(base))) => base,
        _ => return Err(nom::Err::Error(Error::new(input, ErrorKind::Float))),
    };

    let key = match parse_literal_or_variable_expression(property_access_pair[1]) {
        Ok((_, Expression::Variable(key))) => key,
        _ => return Err(nom::Err::Error(Error::new(input, ErrorKind::Float))),
    };

    let property_access = Expression::PropertyAccessExp(PropertyAccess { base, key });

    Ok((input, property_access))
}

/// Helper function to determine if a character is valid in a parameter name.
fn is_param_char(c: char) -> bool {
    c.is_alphanumeric() //|| c == '_'
}

pub fn parse_parameter(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // println!("Input in parse_parameter {:?}", input);

    let (input, param) = preceded(tag("$"), take_while1(is_param_char)).parse(input)?;

    Ok((input, Expression::Parameter(param)))
}

pub fn parse_parameter_property_access_literal_variable_expression(
    input: &'_ str,
) -> IResult<&'_ str, Expression<'_>> {
    // println!("Input in parse_literal_variable_parameter_expression {:?}", input);

    let (input, expression) = alt((
        parse_parameter,
        parse_property_access,
        parse_literal_or_variable_expression,
    ))
    .parse(input)?;
    Ok((input, expression))
}

pub fn parse_literal_or_variable_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    alt((
        map(ws(parse_string_literal), Expression::Literal),
        map(
            ws(common::parse_alphanumeric_with_underscore_dot_star),
            |s: &str| {
                if s.eq_ignore_ascii_case("null") {
                    Expression::Literal(Literal::Null)
                } else if s.eq_ignore_ascii_case("true") {
                    Expression::Literal(Literal::Boolean(true))
                } else if s.eq_ignore_ascii_case("false") {
                    Expression::Literal(Literal::Boolean(false))
                } else if let Ok(i) = s.parse::<i64>() {
                    Expression::Literal(Literal::Integer(i))
                } else if let Ok(f) = s.parse::<f64>() {
                    Expression::Literal(Literal::Float(f))
                } else {
                    // string literal is covered already in parse_string_literal fn. Any other string is variable now.
                    Expression::Variable(s)
                }
            },
        ),
    ))
    .parse(input)
}

pub fn parse_string_literal(input: &'_ str) -> IResult<&'_ str, Literal<'_>> {
    let (input, s) = delimited(char('\''), take_until("\'"), char('\'')).parse(input)?;

    Ok((input, Literal::String(s)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_operator_symbols() {
        let (rem, op) = parse_operator_symbols(">=").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::GreaterThanEqual);

        let (rem, op) = parse_operator_symbols("<=").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::LessThanEqual);

        let (rem, op) = parse_operator_symbols("<>").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::NotEqual);

        let (rem, op) = parse_operator_symbols(">").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::GreaterThan);

        let (rem, op) = parse_operator_symbols("<").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::LessThan);

        let (rem, op) = parse_operator_symbols("=").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Equal);

        let (rem, op) = parse_operator_symbols("+").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Addition);

        let (rem, op) = parse_operator_symbols("-").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Subtraction);

        let (rem, op) = parse_operator_symbols("*").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Multiplication);

        let (rem, op) = parse_operator_symbols("/").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Division);

        let (rem, op) = parse_operator_symbols("%").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::ModuloDivision);

        let (rem, op) = parse_operator_symbols("^").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Exponentiation);

        let (rem, op) = parse_operator_symbols("IN").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::In);
    }

    // postfix
    #[test]
    fn test_parse_postfix_expression_is_null() {
        let (rem, expr) = parse_postfix_expression("a IS NULL").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::IsNull,
            operands: vec![Expression::Variable("a")],
        });
        assert_eq!(&expr, &expected);
    }

    #[test]
    fn test_parse_postfix_expression_is_not_null() {
        let (rem, expr) = parse_postfix_expression("a IS NOT NULL").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::IsNotNull,
            operands: vec![Expression::Variable("a")],
        });
        assert_eq!(&expr, &expected);
    }

    // unary
    #[test]
    fn test_parse_unary_expression_not() {
        let (rem, expr) = parse_unary_expression("NOT a").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Not,
            operands: vec![Expression::Variable("a")],
        });
        assert_eq!(&expr, &expected);
    }

    // binary
    #[test]
    fn test_parse_binary_expression_addition() {
        let (rem, expr) = parse_binary_expression("a + b").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![Expression::Variable("a"), Expression::Variable("b")],
        });
        assert_eq!(&expr, &expected);
    }

    // and
    #[test]
    fn test_parse_logical_and() {
        let (rem, expr) = parse_logical_and("a AND b").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: vec![Expression::Variable("a"), Expression::Variable("b")],
        });
        assert_eq!(&expr, &expected);
    }

    // or
    #[test]
    fn test_parse_logical_or() {
        let (rem, expr) = parse_logical_or("a OR b").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Or,
            operands: vec![Expression::Variable("a"), Expression::Variable("b")],
        });
        assert_eq!(&expr, &expected);
    }

    // fn call
    #[test]
    fn test_parse_function_call() {
        // Testing a function call: foo(a, b)
        let (rem, expr) = parse_function_call("foo(a, b)").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::FunctionCallExp(FunctionCall {
            name: "foo".to_string(),
            args: vec![Expression::Variable("a"), Expression::Variable("b")],
        });
        assert_eq!(&expr, &expected);
    }

    #[test]
    fn test_parse_function_cal_count() {
        // Testing a function call: foo(a, b)
        let (rem, expr) = parse_function_call("count(*)").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::FunctionCallExp(FunctionCall {
            name: "count".to_string(),
            args: vec![Expression::Variable("*")],
        });
        assert_eq!(&expr, &expected);
    }

    // list
    #[test]
    fn test_parse_list_literal() {
        let (rem, expr) = parse_list_literal("[a, b]").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::List(vec![Expression::Variable("a"), Expression::Variable("b")]);
        assert_eq!(&expr, &expected);
    }

    //  property access
    #[test]
    fn test_parse_property_access() {
        let (rem, expr) = parse_property_access("user.name").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::PropertyAccessExp(PropertyAccess {
            base: "user",
            key: "name",
        });
        assert_eq!(&expr, &expected);
    }

    // fn_call + operator
    #[test]
    fn test_parse_expression_fn_call_expression() {
        // Example: foo(a, b) + c
        let (rem, expr) = parse_expression("foo(a, b) + c").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                Expression::FunctionCallExp(FunctionCall {
                    name: "foo".to_string(),
                    args: vec![Expression::Variable("a"), Expression::Variable("b")],
                }),
                Expression::Variable("c"),
            ],
        });
        assert_eq!(&expr, &expected);
    }
}
