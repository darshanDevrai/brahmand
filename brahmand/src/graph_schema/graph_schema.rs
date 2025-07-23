use std::collections::HashMap;

use serde::{Deserialize, Serialize};





#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeSchema {
    pub table_name: String,
    pub column_names: Vec<String>,
    pub primary_keys: String,
    pub node_id: NodeIdSchema,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelationshipSchema {
    pub table_name: String,
    pub column_names: Vec<String>,
    pub from_node: String,
    pub to_node: String,
    pub from_node_id_dtype: String,
    pub to_node_id_dtype: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GraphSchema {
    pub version: u32,
    pub nodes: HashMap<String, NodeSchema>,
    pub relationships: HashMap<String, RelationshipSchema>,
}

#[derive(Debug, Clone)]
pub enum GraphSchemaElement {
    Node(NodeSchema),
    Rel(RelationshipSchema),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeIdSchema {
    pub column: String,
    pub dtype: String,
}

#[derive(Debug, Clone)]
pub struct EntityProperties {
    pub primary_keys: String,
    pub node_id: NodeIdSchema, // other props
}
