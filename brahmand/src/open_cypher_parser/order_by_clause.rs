use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::tag_no_case,
    character::complete::{char, multispace0},
    combinator::{cut, map, opt},
    error::context,
    multi::separated_list1,
    sequence::delimited,
};

use super::{
    ast::{OrderByClause, OrderByItem, OrerByOrder},
    common::ws,
    errors::OpenCypherParsingError,
    expression::parse_expression,
};

pub fn parse_order_by_item(input: &'_ str) -> IResult<&'_ str, OrderByItem<'_>> {
    let (input, expression) = parse_expression(input)?;

    let (input, order_opt) = opt(ws(alt((
        map(tag_no_case("ASC"), |_| OrerByOrder::Asc),
        map(tag_no_case("DESC"), |_| OrerByOrder::Desc),
    ))))
    .parse(input)?;

    // Default to ASCE if no order keyword is provided.
    let order = order_opt.unwrap_or(OrerByOrder::Asc);
    Ok((input, OrderByItem { expression, order }))
}

pub fn parse_order_by_clause(
    input: &'_ str,
) -> IResult<&'_ str, OrderByClause<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = ws(tag_no_case("ORDER BY")).parse(input)?;

    let (input, order_by_items) = context(
        "Error in order by clause",
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            cut(order_by_item_parser),
        ),
    )
    .parse(input)?;

    let order_by_clause = OrderByClause { order_by_items };

    Ok((input, order_by_clause))
}

fn order_by_item_parser(input: &str) -> IResult<&str, OrderByItem<'_>, OpenCypherParsingError<'_>> {
    parse_order_by_item(input).map_err(|e| match e {
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
    fn test_parse_order_by_item_with_asc() {
        let input = "a ASC";
        let res = parse_order_by_item(input);
        match res {
            Ok((remaining, order_by_item)) => {
                assert_eq!(remaining, "");
                let expected = OrderByItem {
                    expression: Expression::Variable("a"),
                    order: OrerByOrder::Asc,
                };
                assert_eq!(&order_by_item, &expected);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_order_by_item_with_desc() {
        let input = "b DESC";
        let res = parse_order_by_item(input);
        match res {
            Ok((remaining, order_by_item)) => {
                assert_eq!(remaining, "");
                let expected = OrderByItem {
                    expression: Expression::Variable("b"),
                    order: OrerByOrder::Desc,
                };
                assert_eq!(&order_by_item, &expected);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_order_by_item_default_order() {
        let input = "c";
        let res = parse_order_by_item(input);
        match res {
            Ok((remaining, order_by_item)) => {
                assert_eq!(remaining, "");
                let expected = OrderByItem {
                    expression: Expression::Variable("c"),
                    order: OrerByOrder::Asc,
                };
                assert_eq!(&order_by_item, &expected);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_order_by_clause_single_item() {
        let input = "ORDER BY a DESC";
        let res = parse_order_by_clause(input);
        match res {
            Ok((remaining, order_by_clause)) => {
                assert_eq!(remaining, "");
                // Expect one item in the clause.
                assert_eq!(order_by_clause.order_by_items.len(), 1);
                let expected_item = OrderByItem {
                    expression: Expression::Variable("a"),
                    order: OrerByOrder::Desc,
                };
                assert_eq!(&order_by_clause.order_by_items[0], &expected_item);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_order_by_clause_multiple_items() {
        let input = "ORDER BY a ASC, b, c DESC";
        let res = parse_order_by_clause(input);
        match res {
            Ok((remaining, order_by_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(order_by_clause.order_by_items.len(), 3);

                let expected_item1 = OrderByItem {
                    expression: Expression::Variable("a"),
                    order: OrerByOrder::Asc,
                };

                let expected_item2 = OrderByItem {
                    expression: Expression::Variable("b"),
                    order: OrerByOrder::Asc,
                };

                let expected_item3 = OrderByItem {
                    expression: Expression::Variable("c"),
                    order: OrerByOrder::Desc,
                };
                assert_eq!(&order_by_clause.order_by_items[0], &expected_item1);
                assert_eq!(&order_by_clause.order_by_items[1], &expected_item2);
                assert_eq!(&order_by_clause.order_by_items[2], &expected_item3);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_order_by_clause_with_extra_whitespace() {
        let input = "  ORDER BY   a   DESC ,  b   ASC  ";
        let res = parse_order_by_clause(input);
        match res {
            Ok((remaining, order_by_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(order_by_clause.order_by_items.len(), 2);
                let expected_item1 = OrderByItem {
                    expression: Expression::Variable("a"),
                    order: OrerByOrder::Desc,
                };
                let expected_item2 = OrderByItem {
                    expression: Expression::Variable("b"),
                    order: OrerByOrder::Asc,
                };
                assert_eq!(&order_by_clause.order_by_items[0], &expected_item1);
                assert_eq!(&order_by_clause.order_by_items[1], &expected_item2);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_order_by_clause_missing_keyword() {
        let input = "SORT BY a ASC";
        let res = parse_order_by_clause(input);
        match res {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected failure because the input does not begin with the "ORDER BY" keyword.
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure due to wrong keyword, but got remaining: {:?} clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
}
