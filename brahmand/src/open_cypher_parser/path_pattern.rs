use std::cell::RefCell;
use std::rc::Rc;
use std::vec;

use nom::character::complete::char;
use nom::combinator::peek;
use nom::error::ErrorKind;
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::tag,
    character::complete::{alphanumeric1, multispace0, space0},
    combinator::{map, opt},
    error::Error,
    multi::separated_list0,
    sequence::{delimited, separated_pair},
};

use super::ast::{
    ConnectedPattern, Direction, Expression, NodePattern, PathPattern, Property, PropertyKVPair,
    RelationshipPattern,
};
use super::common::ws;
use super::expression::parse_parameter;
use super::{common, expression};

pub fn parse_path_pattern(input: &'_ str) -> IResult<&'_ str, PathPattern<'_>> {
    let (input, start_node_pattern) = parse_node_pattern.parse(input)?;

    let (_, is_start_of_relation) = is_start_of_a_relationship.parse(input)?;

    if is_start_of_relation {
        let (input, relationship_end_node_pair_inner_option) =
            parse_relationship_and_connected_node.parse(input)?;

        match relationship_end_node_pair_inner_option {
            Some((first_relationship, end_node_pattern)) => {
                let first_connected_pattern = ConnectedPattern {
                    start_node: Rc::new(RefCell::new(start_node_pattern)),
                    relationship: first_relationship,
                    end_node: Rc::new(RefCell::new(end_node_pattern)),
                };

                let mut connected_nodes_pattern: Vec<ConnectedPattern> = vec![];
                connected_nodes_pattern.push(first_connected_pattern);

                // let mut last_end_node = end_node_pattern;
                let (input, consecutive_relations_end_nodes_vec) =
                    parse_consecutive_relationships(input)?;

                for (consecutive_relationship, consecutive_end_node_pattern) in
                    consecutive_relations_end_nodes_vec
                {
                    let last_pushed = connected_nodes_pattern.last().unwrap();
                    let connected_pattern = ConnectedPattern {
                        start_node: last_pushed.end_node.clone(),
                        relationship: consecutive_relationship,
                        end_node: Rc::new(RefCell::new(consecutive_end_node_pattern)),
                    };
                    connected_nodes_pattern.push(connected_pattern);
                    // last_end_node = consecutive_end_node_pattern;
                }

                Ok((
                    input,
                    PathPattern::ConnectedPattern(connected_nodes_pattern),
                ))
            }
            // This is only a placeholder error. Replace it with actual custom error later.
            None => Err(nom::Err::Failure(Error::new(input, ErrorKind::Satisfy))),
        }
    } else {
        Ok((input, PathPattern::Node(start_node_pattern)))
    }
}

fn parse_relationship_and_connected_node(
    input: &'_ str,
) -> IResult<&'_ str, Option<(RelationshipPattern<'_>, NodePattern<'_>)>> {
    let (input, relationship_pattern) = parse_relationship_pattern(input)?;

    match relationship_pattern {
        Some(rel_pattern) => {
            let (input, end_node_pattern) = parse_node_pattern.parse(input)?;
            Ok((input, Some((rel_pattern, end_node_pattern))))
        }
        None => Ok((input, None)),
    }
}

// Parses a single `-` (dash) without a direction
fn parse_single_dash(input: &str) -> IResult<&str, bool> {
    map((char('-'), multispace0, char('[')), |_| true).parse(input)
}

// Parses `<-` with spaces allowed in between
fn parse_incoming(input: &str) -> IResult<&str, bool> {
    map((char('<'), multispace0, char('-')), |_| true).parse(input)
}

// Parses `->` with spaces allowed in between
fn parse_outgoing(input: &str) -> IResult<&str, bool> {
    map((char('-'), multispace0, char('>')), |_| true).parse(input)
}

// Main parser that checks for `<-`, `->`, or `-`
fn is_start_of_a_relationship(input: &str) -> IResult<&str, bool> {
    let (input, _) = multispace0(input)?;

    let (_, found_relationship_start) = opt(peek(alt((
        parse_incoming,
        parse_outgoing,
        parse_single_dash,
    ))))
    .parse(input)?;
    let is_start = found_relationship_start.is_some();
    Ok((input, is_start))
}

fn get_relation_node(
    input: &'_ str,
) -> IResult<&'_ str, Option<(RelationshipPattern<'_>, NodePattern<'_>)>> {
    // Try to detect the start of a relationship pattern.
    let (_, is_start_of_relation) = is_start_of_a_relationship(input)?;
    if is_start_of_relation {
        parse_relationship_and_connected_node(input)
    } else {
        Ok((input, None))
    }
}

fn parse_consecutive_relationships(
    input: &'_ str,
) -> IResult<&'_ str, Vec<(RelationshipPattern<'_>, NodePattern<'_>)>> {
    let (input, maybe_relation_node) = get_relation_node(input)?;

    // If we got a relation-node, accumulate it and continue recursively.
    if let Some(relation_node) = maybe_relation_node {
        let mut result = vec![relation_node];
        let (input, mut rest) = parse_consecutive_relationships(input)?;
        result.append(&mut rest);
        Ok((input, result))
    } else {
        // No more relation-nodes found, so return an empty vector.
        Ok((input, Vec::new()))
    }
}

// {name: 'Oliver Stone', age: 52}
pub fn parse_properties(input: &'_ str) -> IResult<&'_ str, Vec<Property<'_>>> {
    alt((
        // Property map: requires curly braces and key-value pairs.
        delimited(
            delimited(space0, char('{'), space0),
            separated_list0(
                delimited(space0, char(','), space0),
                map(
                    separated_pair(
                        delimited(space0, alphanumeric1, space0), // key
                        delimited(space0, char(':'), space0),
                        common::parse_alphanumeric_with_underscore_dot_star, // value
                    ),
                    |(key, value)| {
                        // println!("\n key : {:?}, value : {:?}\n", key, value);
                        let value_expression = match expression::parse_parameter_property_access_literal_variable_expression(value) {
                            Ok((_, expression)) => expression,
                            _ => unreachable!(),
                        };
                        Property::PropertyKV(PropertyKVPair {
                            key,
                            value: value_expression,
                        })
                    }
                )
            ),
            delimited(space0, char('}'), space0)
        ),
        // Parameter variant: no curly braces are expected.
        map(ws(parse_parameter), |expr| {
            if let Expression::Parameter(s) = expr {
                vec![Property::Param(s)]
            } else {
                unreachable!()
            }
        })
    )).parse(input)
}

fn parse_name_or_label_with_properties(
    input: &'_ str,
) -> IResult<&'_ str, (Option<&'_ str>, Option<Vec<Property<'_>>>)> {
    let (remainder, node_label) =
        ws(opt(common::parse_alphanumeric_with_underscore)).parse(input)?;
    let (remainder, node_properties) = opt(parse_properties).parse(remainder)?;
    Ok((remainder, (node_label, node_properties)))
}

type NameOrLabelWithProperties<'a> = (Option<&'a str>, Option<Vec<Property<'a>>>);

fn parse_name_label(
    input: &'_ str,
) -> IResult<&'_ str, (NameOrLabelWithProperties<'_>, NameOrLabelWithProperties<'_>)> {
    let (input, _) = multispace0(input)?;

    separated_pair(
        parse_name_or_label_with_properties,
        opt(char(':')),
        parse_name_or_label_with_properties,
    )
    .parse(input)
}

// fn parse_comma(input: &str) -> IResult<&str, Option<&str>> {
//     opt(tag_no_case(",")).parse(input)
// }

fn parse_node_pattern(input: &'_ str) -> IResult<&'_ str, NodePattern<'_>> {
    let (input, _) = multispace0(input)?;

    let empty_node_parser = map(delimited(ws(char('(')), space0, ws(char(')'))), |_| {
        NodePattern {
            name: None,
            label: None,
            properties: None,
        }
    });

    let node_parser = map(
        delimited(ws(char('(')), parse_name_label, ws(char(')'))),
        |((node_name, properties_with_node_name), (node_label, properties_with_node_label))| {
            NodePattern {
                name: node_name,
                label: node_label,
                properties: properties_with_node_name.map_or(properties_with_node_label, Some),
                // .map_or(properties_with_node_label, |v| Some(v)),
            }
        },
    );

    alt((empty_node_parser, node_parser)).parse(input)
}

fn parse_relationship_internals(
    input: &'_ str,
) -> IResult<&'_ str, (NameOrLabelWithProperties<'_>, NameOrLabelWithProperties<'_>)> {
    delimited(ws(char('[')), parse_name_label, ws(char(']'))).parse(input)
}

// Parse relationships - e.g -
//  '<-[ name:KIND ]-' , '-[ name:KIND ]->' '-[ name:KIND ]-',
// '<-[name]-', '-[name]->', '-[name]-'
// '<-[]', '-[]->', '-[]-'
fn parse_relationship_pattern(input: &'_ str) -> IResult<&'_ str, Option<RelationshipPattern<'_>>> {
    let empty_incoming_relationship_parser =
        map(delimited(ws(tag("<-")), space0, ws(tag("-"))), |_| {
            RelationshipPattern {
                direction: Direction::Incoming,
                name: None,
                label: None,
                properties: None,
            }
        });

    let incoming_relationship_with_props_parser = map(
        delimited(tag("<-"), parse_relationship_internals, tag("-")),
        |(
            (relationship_name, properties_with_relationship_name),
            (relationship_label, properties_with_relationship_label),
        )| RelationshipPattern {
            direction: Direction::Incoming,
            name: relationship_name,
            label: relationship_label,
            properties: properties_with_relationship_name
                .map_or(properties_with_relationship_label, Some),
        },
    );

    let empty_outgoing_relationship_parser =
        map(delimited(ws(tag("-")), space0, ws(tag("->"))), |_| {
            RelationshipPattern {
                direction: Direction::Outgoing,
                name: None,
                label: None,
                properties: None,
            }
        });

    let outgoing_relationship_with_props_parser = map(
        delimited(tag("-"), parse_relationship_internals, tag("->")),
        |(
            (relationship_name, properties_with_relationship_name),
            (relationship_label, properties_with_relationship_label),
        )| RelationshipPattern {
            direction: Direction::Outgoing,
            name: relationship_name,
            label: relationship_label,
            properties: properties_with_relationship_name
                .map_or(properties_with_relationship_label, Some),
        },
    );

    let empty_either_relationship_parser =
        map(delimited(ws(tag("-")), space0, ws(tag("-"))), |_| {
            RelationshipPattern {
                direction: Direction::Either,
                name: None,
                label: None,
                properties: None,
            }
        });

    let either_relationship_with_props_parser = map(
        delimited(tag("-"), parse_relationship_internals, tag("-")),
        |(
            (relationship_name, properties_with_relationship_name),
            (relationship_label, properties_with_relationship_label),
        )| RelationshipPattern {
            direction: Direction::Either,
            name: relationship_name,
            label: relationship_label,
            properties: properties_with_relationship_name
                .map_or(properties_with_relationship_label, Some),
        },
    );

    opt(alt((
        empty_incoming_relationship_parser,
        empty_outgoing_relationship_parser,
        empty_either_relationship_parser,
        incoming_relationship_with_props_parser,
        outgoing_relationship_with_props_parser,
        either_relationship_with_props_parser,
    )))
    .parse(input)
}

#[cfg(test)]
mod tests {
    use crate::open_cypher_parser::ast::Literal;

    use super::*;
    use nom::{
        Err,
        error::{Error, ErrorKind},
    };
    use std::rc::Rc;

    #[test]
    fn test_parse_path_pattern_single_node() {
        let input = "()";
        let result = parse_path_pattern(input);
        match result {
            Ok((remaining, PathPattern::Node(node))) => {
                assert_eq!(remaining, "");
                let expected = NodePattern {
                    name: None,
                    label: None,
                    properties: None,
                };
                assert_eq!(&node, &expected);
            }
            Ok((_, other)) => {
                panic!("Expected a Node variant, got: {:?}", other);
            }
            Err(e) => {
                panic!("Parsing failed with error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_path_pattern_connected_single_relationship() {
        let input = "()- [ ] -> ()";
        let result = parse_path_pattern(input);
        match result {
            Ok((remaining, PathPattern::ConnectedPattern(connected_patterns))) => {
                assert_eq!(remaining, "");
                assert_eq!(connected_patterns.len(), 1);
                let connected_pattern: &ConnectedPattern<'_> = &connected_patterns[0];
                // The start and end nodes are parsed as empty nodes.
                let expected_node = Rc::new(RefCell::new(NodePattern {
                    name: None,
                    label: None,
                    properties: None,
                }));
                // For this test, we expect an outgoing relationship without properties.
                let expected_relationship = RelationshipPattern {
                    direction: Direction::Outgoing,
                    name: None,
                    label: None,
                    properties: None,
                };
                // Compare start node.
                assert_eq!(
                    format!("{:?}", connected_pattern.start_node),
                    format!("{:?}", expected_node)
                );
                // Compare relationship.
                assert_eq!(&connected_pattern.relationship, &expected_relationship);
                // Compare end node.
                assert_eq!(
                    format!("{:?}", connected_pattern.end_node),
                    format!("{:?}", Rc::new(expected_node))
                );
            }
            Ok((_, other)) => {
                panic!("Expected a ConnectedPattern variant, got: {:?}", other);
            }
            Err(e) => {
                panic!("Parsing failed with error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_path_pattern_multiple_patterns() {
        let input = "()- [ ] -> (), ()";
        let result = parse_path_pattern(input);
        match result {
            Ok((remaining, path_pattern)) => {
                match path_pattern {
                    PathPattern::Node(node_pattern) => {
                        assert_eq!(remaining, "");
                        let expected_node = Rc::new(RefCell::new(NodePattern {
                            name: None,
                            label: None,
                            properties: None,
                        }));
                        assert_eq!(
                            format!("{:?}", node_pattern),
                            format!("{:?}", expected_node)
                        );
                    }
                    PathPattern::ConnectedPattern(connected_patterns) => {
                        assert_eq!(connected_patterns.len(), 1);
                        let connected_pattern: &ConnectedPattern<'_> = &connected_patterns[0];
                        // The start and end nodes are parsed as empty nodes.
                        let expected_node = Rc::new(RefCell::new(NodePattern {
                            name: None,
                            label: None,
                            properties: None,
                        }));
                        // For this test, we expect an outgoing relationship without properties.
                        let expected_relationship = RelationshipPattern {
                            direction: Direction::Outgoing,
                            name: None,
                            label: None,
                            properties: None,
                        };
                        // Compare start node.
                        assert_eq!(
                            format!("{:?}", connected_pattern.start_node),
                            format!("{:?}", expected_node)
                        );
                        // Compare relationship.
                        assert_eq!(&connected_pattern.relationship, &expected_relationship);
                        // Compare end node.
                        assert_eq!(
                            format!("{:?}", connected_pattern.end_node),
                            format!("{:?}", Rc::new(expected_node))
                        );
                    }
                }
            }
            Err(e) => {
                panic!("Parsing failed with error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_path_pattern_connected_multiple_relationships() {
        let input = "()-[]->()<-[]-()";
        let result = parse_path_pattern(input);
        match result {
            Ok((remaining, PathPattern::ConnectedPattern(connected_patterns))) => {
                assert_eq!(remaining, "");
                // Expect two connected patterns.
                assert_eq!(connected_patterns.len(), 2);
                let expected_node = Rc::new(RefCell::new(NodePattern {
                    name: None,
                    label: None,
                    properties: None,
                }));
                let expected_relationship_1 = RelationshipPattern {
                    direction: Direction::Outgoing,
                    name: None,
                    label: None,
                    properties: None,
                };
                // First connected pattern: from node1 to node2.
                let connected_pattern_1: &ConnectedPattern<'_> = &connected_patterns[0];
                assert_eq!(
                    format!("{:?}", connected_pattern_1.start_node),
                    format!("{:?}", expected_node)
                );
                assert_eq!(&connected_pattern_1.relationship, &expected_relationship_1);
                let start_node_2nd = connected_pattern_1.end_node.clone();
                // Second connected pattern: from node2 to node3.
                let connected_pattern_2 = &connected_patterns[1];
                assert_eq!(
                    format!("{:?}", connected_pattern_2.start_node),
                    format!("{:?}", start_node_2nd)
                );
                let expected_relationship_2 = RelationshipPattern {
                    direction: Direction::Incoming,
                    name: None,
                    label: None,
                    properties: None,
                };
                assert_eq!(&connected_pattern_2.relationship, &expected_relationship_2);
                assert_eq!(
                    format!("{:?}", connected_pattern_2.end_node),
                    format!("{:?}", Rc::new(expected_node))
                );
            }
            Ok((_, other)) => {
                panic!("Expected a ConnectedPattern variant, got: {:?}", other);
            }
            Err(e) => {
                panic!("Parsing failed with error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_path_pattern_connected_multiple_relationships_props_and_labels() {
        let input =
            "(a:IamA {name: 'IamA'} )-[:Pointing]->(b)<-[pointing {what: $dontKnow}]-(:IamC)";
        let result = parse_path_pattern(input);
        match result {
            Ok((remaining, PathPattern::ConnectedPattern(connected_patterns))) => {
                assert_eq!(remaining, "");
                // Expect two connected patterns.
                assert_eq!(connected_patterns.len(), 2);

                let expected_node_a = Rc::new(RefCell::new(NodePattern {
                    name: Some("a"),
                    label: Some("IamA"),
                    properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                        key: "name",
                        value: Expression::Literal(Literal::String("IamA")),
                    })]),
                }));

                let expected_node_b = Rc::new(RefCell::new(NodePattern {
                    name: Some("b"),
                    label: None,
                    properties: None,
                }));

                let expected_node_c = Rc::new(RefCell::new(NodePattern {
                    name: None,
                    label: Some("IamC"),
                    properties: None,
                }));

                let expected_relationship_1 = RelationshipPattern {
                    direction: Direction::Outgoing,
                    name: None,
                    label: Some("Pointing"),
                    properties: None,
                };

                let expected_relationship_2 = RelationshipPattern {
                    direction: Direction::Incoming,
                    name: Some("pointing"),
                    label: None,
                    properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                        key: "what",
                        value: Expression::Parameter("dontKnow"),
                    })]),
                };
                // First connected pattern: from a to b.
                let connected_pattern_1: &ConnectedPattern<'_> = &connected_patterns[0];
                assert_eq!(
                    format!("{:?}", connected_pattern_1.start_node),
                    format!("{:?}", expected_node_a)
                );
                assert_eq!(
                    format!("{:?}", connected_pattern_1.end_node),
                    format!("{:?}", expected_node_b)
                );
                assert_eq!(&connected_pattern_1.relationship, &expected_relationship_1);
                // Second connected pattern: from b to c.
                let connected_pattern_2 = &connected_patterns[1];
                assert_eq!(
                    format!("{:?}", connected_pattern_2.start_node),
                    format!("{:?}", connected_pattern_1.end_node)
                );
                assert_eq!(
                    format!("{:?}", connected_pattern_2.end_node),
                    format!("{:?}", expected_node_c)
                );
                assert_eq!(&connected_pattern_2.relationship, &expected_relationship_2);
            }
            Ok((_, other)) => {
                panic!("Expected a ConnectedPattern variant, got: {:?}", other);
            }
            Err(e) => {
                panic!("Parsing failed with error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_path_pattern_placeholder_error() {
        let input = "()-[";
        let result = parse_path_pattern(input);
        match result {
            Err(Err::Failure(Error { code, .. })) => {
                // Change this later with actual custom error.s
                assert_eq!(code, ErrorKind::Satisfy);
            }
            _ => {
                panic!("Expected failure error for incomplete relationship pattern");
            }
        }
    }
}
