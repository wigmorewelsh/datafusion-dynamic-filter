use datafusion::common::Result as DataFusionResult;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use std::fmt;
use std::sync::Arc;

pub struct ExtendableQueryPlanner {
    planner: DefaultPhysicalPlanner,
}

impl ExtendableQueryPlanner {
    pub fn new(extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>) -> Self {
        Self {
            planner: DefaultPhysicalPlanner::with_extension_planners(extension_planners),
        }
    }
}

impl fmt::Debug for ExtendableQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtendableQueryPlanner").finish()
    }
}

#[async_trait::async_trait]
impl QueryPlanner for ExtendableQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
