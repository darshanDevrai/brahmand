use std::sync::Arc;

use crate::query_engine_v2::logical_plan::logical_plan::LogicalPlan;




#[derive(Debug, PartialEq, Clone)]
pub enum Transformed<T> {
    Yes(T),
    No(T)
}

impl<T> Transformed<T> {
    pub fn get_plan(self) -> T {
        match self {
            Transformed::Yes(plan) | Transformed::No(plan) => plan,
        }
    }

    pub fn is_yes(&self) -> bool {
        matches!(self, Transformed::Yes(_))
    }
}