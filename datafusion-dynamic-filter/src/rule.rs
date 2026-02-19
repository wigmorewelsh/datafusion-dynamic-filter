use super::DynamicFilterNode;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct DynamicFilterRule;

impl DynamicFilterRule {
    pub fn new() -> Self {
        Self
    }

    fn contains_placeholder(expr: &Expr) -> bool {
        let mut has_placeholder = false;

        expr.apply(|e| {
            if matches!(e, Expr::Placeholder(_)) {
                has_placeholder = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .ok();

        has_placeholder
    }

    fn try_rewrite_filter(&self, plan: &LogicalPlan) -> DataFusionResult<Option<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(None);
        };

        if !Self::contains_placeholder(&filter.predicate) {
            return Ok(None);
        }

        let dynamic_filter =
            DynamicFilterNode::new(filter.predicate.clone(), filter.input.clone())?;

        Ok(Some(LogicalPlan::Extension(
            datafusion::logical_expr::Extension {
                node: Arc::new(dynamic_filter),
            },
        )))
    }
}

impl OptimizerRule for DynamicFilterRule {
    fn name(&self) -> &str {
        "dynamic_filter_rule"
    }

    fn apply_order(&self) -> Option<datafusion::optimizer::ApplyOrder> {
        Some(datafusion::optimizer::ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        if let Some(rewritten) = self.try_rewrite_filter(&plan)? {
            return Ok(Transformed::yes(rewritten));
        }

        plan.map_children(|child| self.rewrite(child, _config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::expr::Placeholder;
    use datafusion::logical_expr::{col, lit, LogicalPlanBuilder};

    #[test]
    fn rule_name_is_dynamic_filter_rule() {
        let rule = DynamicFilterRule::new();
        assert_eq!(rule.name(), "dynamic_filter_rule");
    }

    #[test]
    fn filter_without_placeholder_is_unchanged() {
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1).alias("id"), lit("test").alias("name")])
            .unwrap()
            .filter(col("id").eq(lit(42)))
            .unwrap()
            .build()
            .unwrap();

        let rule = DynamicFilterRule::new();
        let config = datafusion::optimizer::OptimizerContext::new();
        let result = rule.rewrite(plan.clone(), &config).unwrap();

        assert!(!result.transformed);
    }

    #[test]
    fn filter_with_placeholder_becomes_dynamic_filter() {
        let placeholder = Expr::Placeholder(Placeholder::new("$1".to_string(), None));
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1).alias("id"), lit("test").alias("name")])
            .unwrap()
            .filter(col("id").eq(placeholder))
            .unwrap()
            .build()
            .unwrap();

        let rule = DynamicFilterRule::new();
        let config = datafusion::optimizer::OptimizerContext::new();
        let result = rule.rewrite(plan.clone(), &config).unwrap();

        assert!(result.transformed);
    }

    #[test]
    fn dynamic_filter_node_preserves_predicate() {
        let placeholder = Expr::Placeholder(Placeholder::new("$1".to_string(), None));
        let predicate = col("id").eq(placeholder);
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1).alias("id"), lit("test").alias("name")])
            .unwrap()
            .filter(predicate.clone())
            .unwrap()
            .build()
            .unwrap();

        let rule = DynamicFilterRule::new();
        let config = datafusion::optimizer::OptimizerContext::new();
        let result = rule.rewrite(plan, &config).unwrap();

        if let LogicalPlan::Extension(ext) = &result.data {
            let dynamic_filter = ext
                .node
                .as_any()
                .downcast_ref::<DynamicFilterNode>()
                .unwrap();
            assert_eq!(dynamic_filter.predicate, predicate);
        } else {
            panic!("Expected Extension node with DynamicFilterNode");
        }
    }

    #[test]
    fn nested_placeholder_is_detected() {
        let placeholder1 = Expr::Placeholder(Placeholder::new("$1".to_string(), None));
        let placeholder2 = Expr::Placeholder(Placeholder::new("$2".to_string(), None));
        let predicate = col("id").gt(placeholder1).and(col("id").lt(placeholder2));

        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1).alias("id"), lit("test").alias("name")])
            .unwrap()
            .filter(predicate)
            .unwrap()
            .build()
            .unwrap();

        let rule = DynamicFilterRule::new();
        let config = datafusion::optimizer::OptimizerContext::new();
        let result = rule.rewrite(plan, &config).unwrap();

        assert!(result.transformed);
    }

    #[test]
    fn placeholder_in_or_expression_is_detected() {
        let placeholder = Expr::Placeholder(Placeholder::new("$1".to_string(), None));
        let predicate = col("id").eq(placeholder).or(col("name").eq(lit("test")));

        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1).alias("id"), lit("test").alias("name")])
            .unwrap()
            .filter(predicate)
            .unwrap()
            .build()
            .unwrap();

        let rule = DynamicFilterRule::new();
        let config = datafusion::optimizer::OptimizerContext::new();
        let result = rule.rewrite(plan, &config).unwrap();

        assert!(result.transformed);
    }
}
