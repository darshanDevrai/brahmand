use nom::character::complete::char;
use nom::combinator::cut;
use nom::error::context;
use nom::{
    IResult, Parser, bytes::complete::tag_no_case, character::complete::multispace0,
    multi::separated_list1, sequence::delimited,
};

use super::ast::{MatchClause, PathPattern};
use super::errors::OpenCypherParsingError;
use super::path_pattern;

pub fn parse_match_clause(
    input: &'_ str,
) -> IResult<&'_ str, MatchClause<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = tag_no_case("MATCH").parse(input)?;

    let (input, pattern_parts) = context(
        "Error in match clause",
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            cut(path_parser),
        ),
    )
    .parse(input)?;

    let match_clause = MatchClause {
        path_patterns: pattern_parts,
    };

    Ok((input, match_clause))
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
    use crate::open_cypher_parser::ast::NodePattern;

    use super::*;
    use nom::Err;

    // Test that a simple MATCH clause with one node pattern is parsed correctly.
    #[test]
    fn test_parse_match_clause_single_pattern() {
        let input = "MATCH ()";
        let result = parse_match_clause(input);
        match result {
            Ok((remaining, match_clause)) => {
                assert_eq!(remaining, "");
                // We expect one path pattern.
                assert_eq!(match_clause.path_patterns.len(), 1);
                match &match_clause.path_patterns[0] {
                    PathPattern::Node(node) => {
                        // Expected empty node: no name, no label, no properties.
                        let expected = NodePattern {
                            name: None,
                            label: None,
                            properties: None,
                        };
                        assert_eq!(node, &expected);
                    }
                    other => {
                        panic!("Expected PathPattern::Node, got {:?}", other);
                    }
                }
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_match_clause_multiple_patterns() {
        let input = "MATCH () , ()";
        let result = parse_match_clause(input);
        match result {
            Ok((remaining, match_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(match_clause.path_patterns.len(), 2);
                for pattern in &match_clause.path_patterns {
                    match pattern {
                        PathPattern::Node(node) => {
                            let expected = NodePattern {
                                name: None,
                                label: None,
                                properties: None,
                            };
                            assert_eq!(node, &expected);
                        }
                        other => {
                            panic!("Expected PathPattern::Node, got {:?}", other);
                        }
                    }
                }
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_match_clause_missing_match_keyword() {
        let input = "MERGE ()";
        let result = parse_match_clause(input);
        match result {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected failure because the input does not start with "MATCH".
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure for missing MATCH keyword, but got remaining: {:?} and clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_match_clause_invalid_pattern() {
        let input = "MATCH xyz";
        let result = parse_match_clause(input);
        match result {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected error due to an invalid path pattern.
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure for invalid path pattern, but got remaining: {:?} and clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
}
