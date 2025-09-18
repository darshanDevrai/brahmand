use nom::character::complete::char;
use nom::combinator::cut;
use nom::error::context;
use nom::{
    IResult, Parser, bytes::complete::tag_no_case, character::complete::multispace0,
    multi::separated_list1, sequence::delimited,
};

use super::ast::{CreateClause, PathPattern};
use super::errors::OpenCypherParsingError;
use super::path_pattern;

pub fn parse_create_clause(input: &'_ str) -> IResult<&'_ str, CreateClause<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = tag_no_case("CREATE")(input)?;

    let (input, pattern_parts) = context(
        "Error in create clause",
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            cut(path_parser),
        ),
    )
    .parse(input)?;

    let create_clause = CreateClause {
        path_patterns: pattern_parts,
    };

    Ok((input, create_clause))
}

fn path_parser(input: &str) -> IResult<&str, PathPattern<'_>, OpenCypherParsingError<'_>> {
    path_pattern::parse_path_pattern(input).map_err(|e| match e {
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
    fn test_parse_create_clause_single_pattern() {
        let input = "CREATE ()";
        let result = parse_create_clause(input);
        match result {
            Ok((remaining, create_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(create_clause.path_patterns.len(), 1);
                match &create_clause.path_patterns[0] {
                    PathPattern::Node(node) => {
                        assert_eq!(node.name, None);
                        assert_eq!(node.label, None);
                        assert_eq!(node.properties, None);
                    }
                    other => panic!("Expected Node variant, got: {:?}", other),
                }
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_create_clause_multiple_patterns() {
        let input = "CREATE () , ()";
        let result = parse_create_clause(input);
        match result {
            Ok((remaining, create_clause)) => {
                assert_eq!(remaining, "");
                // Expect two path patterns.
                assert_eq!(create_clause.path_patterns.len(), 2);
                for pattern in create_clause.path_patterns.iter() {
                    match pattern {
                        PathPattern::Node(node) => {
                            assert_eq!(node.name, None);
                            assert_eq!(node.label, None);
                            assert_eq!(node.properties, None);
                        }
                        other => panic!("Expected Node variant, got: {:?}", other),
                    }
                }
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_create_clause_missing_pattern() {
        let input = "CREATE";
        let result = parse_create_clause(input);
        match result {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected failure because no path pattern is provided.
            }
            Ok((rem, clause)) => {
                panic!(
                    "Expected failure due to missing pattern, but got remaining: {:?} clause: {:?}",
                    rem, clause
                )
            }
            Err(e) => {
                panic!("Unexpected error type: {:?}", e)
            }
        }
    }

    #[test]
    fn test_parse_create_clause_wrong_keyword() {
        let input = "Match ()";
        let result = parse_create_clause(input);
        match result {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected an error because the clause keyword is incorrect.
            }
            Ok((rem, clause)) => {
                panic!(
                    "Expected failure for wrong clause keyword, but got remaining: {:?} clause: {:?}",
                    rem, clause
                )
            }
            Err(_) => todo!(),
        }
    }
}
