use super::{DynamicFilterExec, DynamicFilterNode};
use datafusion::common::Result as DataFusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::ExtensionPlanner;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct DynamicFilterExtensionPlanner;

impl DynamicFilterExtensionPlanner {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl ExtensionPlanner for DynamicFilterExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn datafusion::physical_planner::PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(dynamic_filter) = node.as_any().downcast_ref::<DynamicFilterNode>() {
            if physical_inputs.len() != 1 {
                return Ok(None);
            }

            let input_dfschema = dynamic_filter.input.schema();

            let exec = DynamicFilterExec::new(
                dynamic_filter.predicate.clone(),
                input_dfschema.clone(),
                physical_inputs[0].clone(),
            );

            return Ok(Some(Arc::new(exec)));
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DynamicFilterNode;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::expr::Placeholder;
    use datafusion::logical_expr::{col, lit, Expr, LogicalPlanBuilder};
    use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

    #[tokio::test]
    async fn planner_converts_dynamic_filter_node_to_exec() {
        let placeholder = Expr::Placeholder(Placeholder::new("$1".to_string(), None));
        let predicate = col("id").eq(placeholder);
        let input_plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1).alias("id")])
            .unwrap()
            .build()
            .unwrap();

        let dynamic_filter = DynamicFilterNode::new(predicate, Arc::new(input_plan)).unwrap();

        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let default_planner = DefaultPhysicalPlanner::default();
        let physical_input = default_planner
            .create_physical_plan(&dynamic_filter.input, &session_state)
            .await
            .unwrap();

        let planner = DynamicFilterExtensionPlanner::new();
        let result = planner
            .plan_extension(
                &default_planner,
                &dynamic_filter,
                &[],
                &[physical_input],
                &session_state,
            )
            .await
            .unwrap();

        assert!(result.is_some());
        let exec = result.unwrap();
        assert_eq!(exec.name(), "DynamicFilterExec");
    }

    #[tokio::test]
    async fn planner_returns_none_for_wrong_input_count() {
        let placeholder = Expr::Placeholder(Placeholder::new("$1".to_string(), None));
        let predicate = col("id").eq(placeholder);
        let input_plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1).alias("id")])
            .unwrap()
            .build()
            .unwrap();

        let dynamic_filter = DynamicFilterNode::new(predicate, Arc::new(input_plan)).unwrap();

        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let default_planner = DefaultPhysicalPlanner::default();

        let planner = DynamicFilterExtensionPlanner::new();
        let result = planner
            .plan_extension(&default_planner, &dynamic_filter, &[], &[], &session_state)
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn planner_preserves_predicate() {
        let placeholder = Expr::Placeholder(Placeholder::new("$1".to_string(), None));
        let predicate = col("id").eq(placeholder.clone());
        let input_plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1).alias("id")])
            .unwrap()
            .build()
            .unwrap();

        let dynamic_filter = DynamicFilterNode::new(predicate, Arc::new(input_plan)).unwrap();

        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let default_planner = DefaultPhysicalPlanner::default();
        let physical_input = default_planner
            .create_physical_plan(&dynamic_filter.input, &session_state)
            .await
            .unwrap();

        let planner = DynamicFilterExtensionPlanner::new();
        let result = planner
            .plan_extension(
                &default_planner,
                &dynamic_filter,
                &[],
                &[physical_input],
                &session_state,
            )
            .await
            .unwrap();

        assert!(result.is_some());
        let exec = result.unwrap();
        let dynamic_exec = exec
            .as_any()
            .downcast_ref::<DynamicFilterExec>()
            .expect("should be DynamicFilterExec");

        assert_eq!(dynamic_exec.predicate(), &col("id").eq(placeholder));
    }
}
