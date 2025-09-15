#[derive(Debug, PartialEq, Clone)]
pub enum QueryType {
    Ddl,
    Read,
    Update,
    Delete,
}
