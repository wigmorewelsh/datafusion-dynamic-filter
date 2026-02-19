pub mod logical;
pub mod param_resolver;
pub mod parameterized_statement;
pub mod physical;
pub mod planner;
pub mod rule;

pub use logical::DynamicFilterNode;
pub use param_resolver::{PARAM_RESOLVER_UDF_NAME, ParamResolverUDF};
pub use parameterized_statement::ParameterizedStatement;
pub use physical::DynamicFilterExec;
pub use planner::DynamicFilterExtensionPlanner;
pub use rule::DynamicFilterRule;

use datafusion::common::Result as DataFusionResult;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use std::fmt;
use std::sync::Arc;

pub struct SimpleQueryPlanner {
    planner: DefaultPhysicalPlanner,
}

impl SimpleQueryPlanner {
    pub fn new(extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>) -> Self {
        Self {
            planner: DefaultPhysicalPlanner::with_extension_planners(extension_planners),
        }
    }
}

impl fmt::Debug for SimpleQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimpleQueryPlanner").finish()
    }
}

#[async_trait::async_trait]
impl QueryPlanner for SimpleQueryPlanner {
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
