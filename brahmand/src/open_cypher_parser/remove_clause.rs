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
    ast::{Expression, PropertyAccess, RemoveClause},
    common::ws,
    errors::OpenCypherParsingError,
    expression::parse_expression,
};

pub fn parse_remove_clause(
    input: &'_ str,
) -> IResult<&'_ str, RemoveClause<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = ws(tag_no_case("REMOVE")).parse(input)?;

    let (input, remove_items) = context(
        "Error in remove clause",
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            cut(remove_item_parser),
        ),
    )
    .parse(input)?;

    let remove_clause = RemoveClause { remove_items };

    Ok((input, remove_clause))
}

fn remove_item_parser(
    input: &'_ str,
) -> IResult<&'_ str, PropertyAccess<'_>, OpenCypherParsingError<'_>> {
    let (input, expression) = parse_expression.parse(input).map_err(|e| match e {
        nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
        nom::Err::Error(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
        nom::Err::Failure(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
    })?;

    if let Expression::PropertyAccessExp(property_access) = expression {
        Ok((input, property_access))
    } else {
        // return error
        Err(nom::Err::Failure(OpenCypherParsingError {
            errors: vec![(
                "Value of remove clause should be property access",
                "Error in remove clause",
            )],
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::Err;

    #[test]
    fn test_parse_remove_clause_single_item() {
        let input = "REMOVE user.name";
        let res = parse_remove_clause(input);
        match res {
            Ok((remaining, remove_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(remove_clause.remove_items.len(), 1);
                let prop_access = &remove_clause.remove_items[0];
                let expected = PropertyAccess {
                    base: "user",
                    key: "name",
                };
                assert_eq!(prop_access, &expected);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_remove_clause_multiple_items() {
        let input = "REMOVE user.name, order.date";
        let res = parse_remove_clause(input);
        match res {
            Ok((remaining, remove_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(remove_clause.remove_items.len(), 2);
                let expected1 = PropertyAccess {
                    base: "user",
                    key: "name",
                };
                let expected2 = PropertyAccess {
                    base: "order",
                    key: "date",
                };
                assert_eq!(&remove_clause.remove_items[0], &expected1);
                assert_eq!(&remove_clause.remove_items[1], &expected2);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_remove_clause_extra_whitespace() {
        let input = "   REMOVE    user.name  ,   order.date";
        let res = parse_remove_clause(input);
        match res {
            Ok((remaining, remove_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(remove_clause.remove_items.len(), 2);
                let expected1 = PropertyAccess {
                    base: "user",
                    key: "name",
                };
                let expected2 = PropertyAccess {
                    base: "order",
                    key: "date",
                };
                assert_eq!(&remove_clause.remove_items[0], &expected1);
                assert_eq!(&remove_clause.remove_items[1], &expected2);
            }
            Err(e) => panic!(
                "Expected successful parse with extra whitespace, got error: {:?}",
                e
            ),
        }
    }

    #[test]
    fn test_parse_remove_clause_invalid_item() {
        let input = "REMOVE a";
        let res = parse_remove_clause(input);
        match res {
            Err(Err::Failure(e)) => {
                let error_str = format!("{:?}", e);
                assert!(
                    error_str.contains("Value of remove clause should be property access"),
                    "Error message does not mention property access requirement: {}",
                    error_str
                );
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure for non-property access, but got remaining: {:?}, clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
}
