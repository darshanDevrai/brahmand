use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum ClickhouseQueryGeneratorError {
    #[error(
        "Distinct node and connected patterns found. Currently the cross join is not supported."
    )]
    DistinctNodeConnectedPattern,
    #[error("No physical plan.")]
    NoPhysicalPlan,
    #[error("No logical table data found for a given uid.")]
    NoLogicalTableDataForUid,
    #[error("No operand found in where clause.")]
    NoOperandFoundInWhereClause,
    #[error("No operand found in return clause.")]
    NoOperandFoundInReturnClause,
    #[error("Unsupported expression found in where clause.")]
    UnsupportedItemInWhereClause,
    #[error("Unsupported expression found in return clause.")]
    UnsupportedItemInReturnClause,
    #[error("Unsupported expression found in order by clause.")]
    UnsupportedItemInOrderByClause,
    #[error("Unsupported expression found in relationship select clause.")]
    UnsupportedItemInRelSelectClause,
    #[error("Unsupported expression found in final node select clause.")]
    UnsupportedItemInFinalNodeSelectClause,
    #[error("Unsupported DDL query found.")]
    UnsupportedDDLQuery,
    #[error("Unsupported expression found for default value.")]
    UnsupportedDefaultValue,
    #[error("Primary key is missing in DDL.")]
    MissingPrimaryKey,
    #[error("Node id column is missing in DDL.")]
    MissingNodeId,
    #[error("Multiple node ids found. Only one node id is allowed in DDL.")]
    MultipleNodeIds,
    #[error("Invalid node id data type found. Only Int64 and UInt64 are allowed as a node id.")]
    InvalidNodeIdDType,
    #[error("Invalid node id found. Make sure to add node id column in the table schema as well.")]
    InvalidNodeId,
    #[error(
        "Unknow 'From' table found in relationship defination. Make sure to create nodes first before creating relationship."
    )]
    UnknownFromTableInRel,
    #[error(
        "Unknow 'To' table found in relationship defination. Make sure to create nodes first before creating relationship."
    )]
    UnknownToTableInRel,
}
