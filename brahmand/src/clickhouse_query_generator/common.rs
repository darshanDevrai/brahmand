use crate::open_cypher_parser::ast::Literal;

pub fn get_literal_to_string(literal: &Literal) -> String {
    match literal {
        Literal::Integer(i) => i.to_string(),
        Literal::Float(f) => f.to_string(),
        Literal::Boolean(b) => b.to_string(),
        Literal::String(s) => {
            format!("'{}'", s)
        } // clickhouse uses single quotes for string literals
        Literal::Null => "null".to_string(),
    }
}
