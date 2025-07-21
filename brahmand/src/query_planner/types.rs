use std::{collections::HashMap, fmt};

use crate::open_cypher_parser::ast::{
    Direction, Expression, OperatorApplication, OrderByItem, ReturnItem,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub enum TraversalMode {
    TempTable,
    Cte,
}

#[derive(Debug, PartialEq, Clone)]
pub struct TableData<'a> {
    pub entity_name: Option<&'a str>,
    pub table_name: Option<&'a str>,
    pub return_items: Vec<ReturnItem<'a>>,
    pub where_conditions: Vec<OperatorApplication<'a>>,
    pub order_by_items: Vec<OrderByItem<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ConnectedTraversal {
    pub id: Uuid,
    pub start_node: Uuid,
    pub relationship: Uuid,
    pub direction: Direction,
    pub end_node: Uuid,
}

#[derive(Debug, PartialEq, Clone)]
pub enum QueryType {
    Ddl,
    Read,
    Update,
    Delete,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReturnItemData<'a> {
    pub return_item: ReturnItem<'a>,
    pub belongs_to_table: &'a str,
}

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalPlan<'a> {
    pub connected_traversals: Vec<ConnectedTraversal>,
    pub node_traversals: Vec<Uuid>,
    pub overall_condition: Option<Expression<'a>>,
    pub overall_return_items: Vec<ReturnItemData<'a>>,
    pub table_data_by_uid: HashMap<Uuid, TableData<'a>>,
    pub entity_name_uid_map: HashMap<String, Uuid>,
    pub return_item_by_alias: HashMap<&'a str, ReturnItem<'a>>,
    pub order_by_items: Vec<OrderByItem<'a>>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
}

impl LogicalPlan<'_> {
    pub fn default() -> Self {
        LogicalPlan {
            connected_traversals: vec![],
            node_traversals: vec![],
            overall_condition: None,
            overall_return_items: vec![],
            entity_name_uid_map: HashMap::new(),
            table_data_by_uid: HashMap::new(),
            return_item_by_alias: HashMap::new(),
            order_by_items: vec![],
            skip: None,
            limit: None,
        }
    }
}

impl fmt::Display for LogicalPlan<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalPlan")?;
        writeln!(
            f,
            "├── connected_traversals: {:#?}",
            self.connected_traversals
        )?;
        writeln!(f, "├── node_traversals: {:#?}", self.node_traversals)?;
        writeln!(f, "├── overall_condition: {:#?}", self.overall_condition)?;
        writeln!(
            f,
            "├── overall_return_items: {:#?}",
            self.overall_return_items
        )?;
        writeln!(f, "├── table_data_by_uid: {:#?}", self.table_data_by_uid)?;
        writeln!(
            f,
            "├── entity_name_uid_map: {:#?}",
            self.entity_name_uid_map
        )?;
        writeln!(f, "├── order_by_items: {:#?}", self.order_by_items)?;
        writeln!(f, "├── skip: {:#?}", self.skip)?;
        writeln!(f, "├── limit: {:#?}", self.limit)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinCondition<'a> {
    pub node_id: String,
    pub table_uid: Uuid,
    pub table_name: String,
    pub table_alias: String,
    pub temp_table_name: String,
    pub column_name: &'a str,
}

#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalPlanTableData<'a> {
    pub id: Uuid,
    pub node_id: String,
    pub table_alias: String,
    pub table_name: String,
    pub temp_table_name: String,
    pub is_eagerly_evaluated: bool,
    pub is_relationship: bool,
    pub join_condition: Option<JoinCondition<'a>>,
    pub forward_join_condition: Option<JoinCondition<'a>>,
}
impl fmt::Display for PhysicalPlanTableData<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalPlanTableData")?;
        writeln!(f, "├── id: {:#?}", self.id)?;
        writeln!(f, "├── node_id: {:#?}", self.node_id)?;
        writeln!(f, "├── table_alias: {:#?}", self.table_alias)?;
        writeln!(f, "├── table_name: {:#?}", self.table_name)?;
        writeln!(f, "├── temp_table_name: {:#?}", self.temp_table_name)?;
        writeln!(
            f,
            "├── is_eagerly_evaluated: {:#?}",
            self.is_eagerly_evaluated
        )?;
        writeln!(f, "├── is_relationship: {:#?}", self.is_relationship)?;
        writeln!(f, "├── join_condition: {:#?}", self.join_condition)?;
        writeln!(
            f,
            "├── forward_join_condition: {:#?}",
            self.forward_join_condition
        )?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalConnectedTraversal<'a> {
    pub id: Uuid,
    pub start_node: PhysicalPlanTableData<'a>,
    pub relationship: PhysicalPlanTableData<'a>,
    pub direction: Direction,
    pub end_node: PhysicalPlanTableData<'a>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalPlan<'a> {
    // pub physical_table_data_by_uid: HashMap<Uuid, PhysicalPlanTableData<'a>>,
    // pub traversal_sequence: Vec<Uuid>,
    pub physical_connected_traversals: Vec<PhysicalConnectedTraversal<'a>>,
    pub physcial_node_traversals: Vec<PhysicalPlanTableData<'a>>,
    pub entity_name_node_id_map: HashMap<String, String>,
}

impl fmt::Display for PhysicalPlan<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalPlan")?;
        // writeln!(f, "├── physical_table_data_by_uid: {:#?}", self.physical_table_data_by_uid)?;
        // writeln!(f, "├── traversal_sequence: {:#?}", self.traversal_sequence)?;
        writeln!(
            f,
            "├── physical_connected_traversals: {:#?}",
            self.physical_connected_traversals
        )?;
        writeln!(
            f,
            "├── physcial_node_traversals: {:#?}",
            self.physcial_node_traversals
        )?;
        writeln!(
            f,
            "├── entity_name_node_id_map: {:#?}",
            self.entity_name_node_id_map
        )?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct QueryIR<'a> {
    pub query_type: QueryType,
    pub logical_plan: LogicalPlan<'a>,
    pub physical_plan: PhysicalPlan<'a>,
}

impl fmt::Display for QueryIR<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "QueryIR")?;
        writeln!(f, "├── query_type: {:#?}", self.query_type)?;
        writeln!(f, "├── logical_plan: {:#?}", self.logical_plan)?;
        writeln!(f, "├── physical_plan: {:#?}", self.physical_plan)?;
        Ok(())
    }
}

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
