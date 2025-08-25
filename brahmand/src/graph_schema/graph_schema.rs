use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::errors::GraphSchemaError;





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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GraphSchema {
    version: u32,
    nodes: HashMap<String, NodeSchema>,
    relationships: HashMap<String, RelationshipSchema>,
}

impl GraphSchema {

    pub fn build(version: u32, nodes: HashMap<String, NodeSchema>, relationships: HashMap<String, RelationshipSchema>,) -> GraphSchema {
        GraphSchema {
            version,
            nodes,
            relationships
        }
    }

    pub fn insert_node_schema(&mut self, node_label: String, node_schema:NodeSchema) {
        self.nodes.insert(node_label, node_schema);
    }

    pub fn insert_rel_schema(&mut self, rel_label: String, rel_schema:RelationshipSchema) {
        self.relationships.insert(rel_label, rel_schema);
    }

    pub fn get_version(&self) -> u32 {
        self.version
    }

    pub fn increment_version(&mut self) {
        self.version += 1;
    }

    pub fn get_node_schema(&self, node_label: &str) -> Result<&NodeSchema, GraphSchemaError> {
        self.nodes.get(node_label).ok_or(GraphSchemaError::NoNodeSchemaFound{node_label: node_label.to_string()})
    }

    pub fn get_rel_schema(&self, rel_label: &str) -> Result<&RelationshipSchema, GraphSchemaError> {
        self.relationships.get(rel_label).ok_or(GraphSchemaError::NoRelationSchemaFound{rel_label: rel_label.to_string()})
    }

    pub fn get_relationships_schemas(&self) -> &HashMap<String, RelationshipSchema> {
        &self.relationships
    }

    pub fn get_nodes_schemas(&self) -> &HashMap<String, NodeSchema> {
        &self.nodes
    }

    pub fn get_node_schema_opt(&self, node_label: &str) -> Option<&NodeSchema> {
        self.nodes.get(node_label)
    }

    pub fn get_relationships_schema_opt(&self, rel_label: &str) -> Option<&RelationshipSchema> {
        self.relationships.get(rel_label)
    }

}