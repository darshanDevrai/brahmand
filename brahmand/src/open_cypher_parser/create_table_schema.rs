use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::combinator::{map, opt};
use nom::sequence::preceded;
use nom::{
    IResult, Parser, character::complete::multispace0, multi::separated_list1, sequence::delimited,
};

use super::ast::{ColumnSchema, Expression, FunctionCall};
use super::common::ws;
use super::expression::{parse_expression, parse_identifier};

// Parse a "multiword identifier" by joining one or more identifiers separated by whitespace.
// This is for table property like "Primary Key ()"
fn parse_multiword_identifier(input: &str) -> IResult<&str, Vec<&str>> {
    separated_list1(multispace0, parse_identifier).parse(input)
}

pub fn parse_property_function_call(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, fn_name_parts) = ws(parse_multiword_identifier).parse(input)?;
    // parse args
    let (input, args) = delimited(
        ws(char('(')),
        separated_list1(ws(char(',')), ws(parse_expression)),
        ws(char(')')),
    )
    .parse(input)?;
    Ok((
        input,
        Expression::FunctionCallExp(FunctionCall {
            name: fn_name_parts.join(" "),
            args,
        }),
    ))
}

//Parse a column schema item: e.g. "title STRING"
fn parse_column_schema(input: &'_ str) -> IResult<&'_ str, ColumnSchema<'_>> {
    let (input, col_name) = ws(parse_identifier).parse(input)?;
    let (input, col_dtype) = ws(parse_identifier).parse(input)?;
    let (input, default_value) =
        opt(preceded(ws(tag_no_case("DEFAULT")), ws(parse_expression))).parse(input)?;
    Ok((
        input,
        ColumnSchema {
            column_name: col_name,
            column_dtype: col_dtype,
            default_value,
        },
    ))
}
#[derive(Debug, PartialEq, Clone)]
pub enum SchemaItem<'a> {
    Column(ColumnSchema<'a>),
    Property(Expression<'a>),
}
// Parse one item inside the table definition.
// We try to parse a column definition first; if that fails, we parse a property function call.
pub fn parse_property_item(input: &'_ str) -> IResult<&'_ str, SchemaItem<'_>> {
    alt((
        map(parse_property_function_call, SchemaItem::Property),
        map(parse_column_schema, |col| {
            // println!("col {:?}", col);
            SchemaItem::Column(col)
        }),
    ))
    .parse(input)
}

// Parse the list inside parentheses that follows the table name.
pub fn parse_node_table_properties_list(
    input: &str,
) -> IResult<&str, (Vec<ColumnSchema<'_>>, Vec<Expression<'_>>)> {
    let (input, items) = delimited(
        ws(char('(')),
        separated_list1(ws(char(',')), ws(parse_property_item)),
        ws(char(')')),
    )
    .parse(input)?;
    // println!("items -> {:?}",items);
    let mut schema = Vec::new();
    let mut properties = Vec::new();
    for item in items {
        match item {
            SchemaItem::Column(col) => schema.push(col),
            SchemaItem::Property(expr) => properties.push(expr),
        }
    }

    Ok((input, (schema, properties)))
}

// in rel node case, paranthesis are already parsed.
pub fn parse_rel_table_properties_list(
    input: &str,
) -> IResult<&str, (Vec<ColumnSchema<'_>>, Vec<Expression<'_>>)> {
    let (input, items) = separated_list1(ws(char(',')), ws(parse_property_item)).parse(input)?;
    // println!("items -> {:?}",items);
    let mut schema = Vec::new();
    let mut properties = Vec::new();
    for item in items {
        match item {
            SchemaItem::Column(col) => schema.push(col),
            SchemaItem::Property(expr) => properties.push(expr),
        }
    }

    Ok((input, (schema, properties)))
}
