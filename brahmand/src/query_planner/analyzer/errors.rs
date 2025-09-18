use std::fmt::Display;

use thiserror::Error;

use crate::{
    graph_catalog::errors::GraphSchemaError, query_planner::plan_ctx::errors::PlanCtxError,
};

#[derive(Debug, Clone, Error, PartialEq)]
pub enum Pass {
    // DuplicateScansRemoving,
    FilterTagging,
    GraphJoinInference,
    GraphTraversalPlanning,
    // GroupByBuilding,
    ProjectionTagging,
    SchemaInference,
    // PlanSanitization,
    QueryValidation,
}

impl Display for Pass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pass::FilterTagging => write!(f, "FilterTagging"),
            // Pass::DuplicateScansRemoving => write!(f, "DuplicateScansRemoving"),
            Pass::GraphJoinInference => write!(f, "GraphJoinInference"),
            Pass::GraphTraversalPlanning => write!(f, "GraphTraversalPlanning"),
            // Pass::GroupByBuilding => write!(f, "GroupByBuilding"),
            Pass::ProjectionTagging => write!(f, "ProjectionTagging"),
            Pass::SchemaInference => write!(f, "SchemaInference"),
            // Pass::PlanSanitization => write!(f, "PlanSanitization"),
            Pass::QueryValidation => write!(f, "QueryValidation"),
        }
    }
}

#[derive(Debug, Clone, Error, PartialEq)]
pub enum AnalyzerError {
    #[error(
        " {pass}: No relation label found. Currently we need label to identify the relationship table. This will change in future."
    )]
    MissingRelationLabel { pass: Pass },

    #[error(
        " {pass}: Not enough information. Labels are required to identify nodes and relationships."
    )]
    NotEnoughLabels { pass: Pass },

    #[error(
        " {pass}: Alias `{alias}` not found in Match Clause. Alias should be from Match Clause."
    )]
    OrphanAlias { pass: Pass, alias: String },

    #[error("PlanCtxError: {pass}: {source}.")]
    PlanCtx {
        pass: Pass, //&'static str,
        #[source]
        source: PlanCtxError,
    },

    #[error("GraphSchema: {pass}: {source}.")]
    GraphSchema {
        pass: Pass,
        #[source]
        source: GraphSchemaError,
    },

    #[error("Invalid relation query - {rel}")]
    InvalidRelationInQuery { rel: String },
}
