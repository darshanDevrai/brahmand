

use std::sync::Arc;

use crate::query_engine_v2::{logical_plan::{self, logical_plan::LogicalPlan, plan_ctx::PlanCtx}, render_plan::{errors::RenderBuildError, plan_builder::Builder, render_plan::{CteItems, SelectItems}}};


impl Builder<SelectItems> for SelectItems {
    fn build(&self, logical_plan: Arc<LogicalPlan>, plan_ctx: &mut PlanCtx) -> Result<SelectItems,RenderBuildError>  {
        match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                Ok(projection.clone().into())
            },
            _ => Err(RenderBuildError::SelectItemsBuilder)
        }
    }
}


