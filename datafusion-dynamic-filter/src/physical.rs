use super::param_resolver::ParamResolverUDF;
use super::param_resolver::{replace_placeholders, PARAM_RESOLVER_UDF_NAME};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DFSchema, Result as DataFusionResult};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::{
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion_physical_expr::{create_physical_expr, PhysicalExpr};
use std::fmt;
use std::sync::Arc;

pub struct DynamicFilterExec {
    predicate: Expr,
    input: Arc<dyn ExecutionPlan>,
    input_dfschema: Arc<DFSchema>,
    schema: SchemaRef,
    properties: PlanProperties,
    dynamic_filter: Option<Arc<dyn PhysicalExpr>>,
    metrics: ExecutionPlanMetricsSet,
}

impl DynamicFilterExec {
    pub fn new(
        predicate: Expr,
        input_dfschema: Arc<DFSchema>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        use datafusion::physical_plan::execution_plan::Boundedness;
        use datafusion::physical_plan::execution_plan::EmissionType;

        let schema = input.schema();
        let input_eq_properties = input.properties().equivalence_properties().clone();
        let properties = PlanProperties::new(
            input_eq_properties,
            input.properties().output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            predicate,
            input,
            input_dfschema,
            schema,
            properties,
            dynamic_filter: None,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn predicate(&self) -> &Expr {
        &self.predicate
    }

    fn resolve_params(&self, context: &Arc<datafusion::execution::TaskContext>) -> Expr {
        let Some(udf) = context.scalar_functions().get(PARAM_RESOLVER_UDF_NAME) else {
            return self.predicate.clone();
        };

        let Some(param_resolver) = udf.inner().as_any().downcast_ref::<ParamResolverUDF>() else {
            return self.predicate.clone();
        };

        let params = param_resolver.get_params();
        replace_placeholders(&self.predicate, params).unwrap_or_else(|_| self.predicate.clone())
    }

    fn update_dynamic_filter(
        &self,
        physical_predicate: Arc<dyn PhysicalExpr>,
    ) -> DataFusionResult<()> {
        let Some(dynamic_filter) = &self.dynamic_filter else {
            return Ok(());
        };

        let Some(df) = dynamic_filter
            .as_any()
            .downcast_ref::<DynamicFilterPhysicalExpr>()
        else {
            return Ok(());
        };

        df.update(physical_predicate)
    }

    fn create_placeholder_physical_expr(
        &self,
    ) -> (Arc<dyn PhysicalExpr>, Vec<Arc<dyn PhysicalExpr>>) {
        use datafusion::common::tree_node::{Transformed, TreeNode};

        let predicate_with_nulls = self
            .predicate
            .clone()
            .transform(|e| {
                if let Expr::Placeholder(placeholder) = &e {
                    let null_value = placeholder
                        .data_type
                        .as_ref()
                        .and_then(|dt| datafusion::common::ScalarValue::try_from(dt).ok());

                    if let Some(null) = null_value {
                        return Ok(Transformed::yes(Expr::Literal(null, None)));
                    }
                }
                Ok(Transformed::no(e))
            })
            .unwrap_or_else(|_| {
                Transformed::no(datafusion::logical_expr::Expr::Literal(
                    datafusion::common::ScalarValue::Boolean(Some(true)),
                    None,
                ))
            })
            .data;

        let physical_expr = create_physical_expr(
            &predicate_with_nulls,
            &self.input_dfschema,
            &Default::default(),
        )
        .unwrap_or_else(|_| {
            Arc::new(datafusion_physical_expr::expressions::Literal::new(
                datafusion::common::ScalarValue::Boolean(Some(true)),
            ))
        });

        let children: Vec<Arc<dyn PhysicalExpr>> =
            datafusion_physical_expr::utils::collect_columns(&physical_expr)
                .into_iter()
                .map(|col| Arc::new(col) as Arc<dyn PhysicalExpr>)
                .collect();
        (physical_expr, children)
    }

    fn extract_dynamic_filter_from_pushdown(
        child_pushdown_result: &ChildPushdownResult,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        let filters = child_pushdown_result.self_filters.first()?;

        let filter = filters.iter().find(|f| {
            matches!(
                f.discriminant,
                datafusion::physical_plan::filter_pushdown::PushedDown::Yes
            )
        })?;

        filter
            .predicate
            .as_any()
            .downcast_ref::<DynamicFilterPhysicalExpr>()?;

        Some(filter.predicate.clone())
    }

    fn reset_dynamic_filter(&self) -> DataFusionResult<()> {
        let Some(dynamic_filter) = &self.dynamic_filter else {
            return Ok(());
        };

        let Some(df) = dynamic_filter
            .as_any()
            .downcast_ref::<DynamicFilterPhysicalExpr>()
        else {
            return Ok(());
        };

        let placeholder = Arc::new(datafusion_physical_expr::expressions::Literal::new(
            datafusion::common::ScalarValue::Boolean(Some(true)),
        )) as Arc<dyn PhysicalExpr>;

        df.update(placeholder)
    }
}

impl fmt::Debug for DynamicFilterExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DynamicFilterExec: predicate={:?}", self.predicate)
    }
}

impl DisplayAs for DynamicFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DynamicFilterExec: predicate={:?}", self.predicate)
    }
}

impl ExecutionPlan for DynamicFilterExec {
    fn name(&self) -> &str {
        "DynamicFilterExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return datafusion::common::internal_err!(
                "DynamicFilterExec expects exactly one child"
            );
        }

        Ok(Arc::new(Self {
            predicate: self.predicate.clone(),
            input_dfschema: self.input_dfschema.clone(),
            input: children.into_iter().next().unwrap(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
            dynamic_filter: self.dynamic_filter.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn statistics(&self) -> DataFusionResult<datafusion::common::Statistics> {
        Ok(datafusion::common::Statistics::new_unknown(&self.schema))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let predicate_with_params = self.resolve_params(&context);

        if self.dynamic_filter.is_some() {
            let physical_predicate = create_physical_expr(
                &predicate_with_params,
                &self.input_dfschema,
                &Default::default(),
            )?;
            self.update_dynamic_filter(physical_predicate)?;
            return self.input.execute(partition, context);
        }

        let physical_predicate = create_physical_expr(
            &predicate_with_params,
            &self.input_dfschema,
            &Default::default(),
        )?;
        let filter_exec = FilterExec::try_new(physical_predicate, self.input.clone())?;
        filter_exec.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DataFusionResult<FilterDescription> {
        if phase == FilterPushdownPhase::Post {
            let child_desc =
                datafusion::physical_plan::filter_pushdown::ChildFilterDescription::from_child(
                    &parent_filters,
                    &self.input,
                )?;
            return Ok(FilterDescription::new().with_child(child_desc));
        }

        let dynamic_filter = self.dynamic_filter.clone().unwrap_or_else(|| {
            let (physical_placeholder, children) = self.create_placeholder_physical_expr();
            Arc::new(DynamicFilterPhysicalExpr::new(
                children,
                physical_placeholder,
            )) as Arc<dyn PhysicalExpr>
        });

        let child_desc =
            datafusion::physical_plan::filter_pushdown::ChildFilterDescription::from_child(
                &parent_filters,
                &self.input,
            )?
            .with_self_filter(dynamic_filter);

        Ok(FilterDescription::new().with_child(child_desc))
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DataFusionResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        if phase == FilterPushdownPhase::Post {
            let propagation = FilterPushdownPropagation::if_all(child_pushdown_result);
            return Ok(propagation.with_updated_node(Arc::new(Self {
                predicate: self.predicate.clone(),
                input: self.input.clone(),
                input_dfschema: self.input_dfschema.clone(),
                schema: self.schema.clone(),
                properties: self.properties.clone(),
                dynamic_filter: self.dynamic_filter.clone(),
                metrics: self.metrics.clone(),
            })));
        }

        let dynamic_filter = Self::extract_dynamic_filter_from_pushdown(&child_pushdown_result);

        let new_exec = Self {
            predicate: self.predicate.clone(),
            input: self.input.clone(),
            input_dfschema: self.input_dfschema.clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
            dynamic_filter,
            metrics: self.metrics.clone(),
        };

        let propagation = FilterPushdownPropagation::if_all(child_pushdown_result);
        Ok(propagation.with_updated_node(Arc::new(new_exec)))
    }

    fn reset_state(self: Arc<Self>) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.reset_dynamic_filter()?;
        let children = self.children().into_iter().cloned().collect();
        self.with_new_children(children)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_physical_expr::expressions::Literal;

    #[test]
    fn dynamic_filter_exec_can_be_created() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::empty::EmptyExec;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let input = Arc::new(EmptyExec::new(schema.clone()));
        let dfschema = Arc::new(DFSchema::try_from(schema.as_ref().clone()).unwrap());
        let predicate = Expr::Literal(datafusion::common::ScalarValue::Boolean(Some(true)), None);

        let exec = DynamicFilterExec::new(predicate, dfschema, input);
        assert_eq!(exec.name(), "DynamicFilterExec");
    }

    #[test]
    fn with_new_children_preserves_dynamic_filter() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::empty::EmptyExec;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let input = Arc::new(EmptyExec::new(schema.clone()));
        let dfschema = Arc::new(DFSchema::try_from(schema.as_ref().clone()).unwrap());
        let predicate = Expr::Literal(datafusion::common::ScalarValue::Boolean(Some(true)), None);

        let placeholder = Arc::new(Literal::new(datafusion::common::ScalarValue::Boolean(
            Some(true),
        ))) as Arc<dyn PhysicalExpr>;
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(vec![], placeholder));

        let mut exec = DynamicFilterExec::new(predicate, dfschema.clone(), input);
        exec.dynamic_filter = Some(dynamic_filter.clone() as Arc<dyn PhysicalExpr>);

        let new_input = Arc::new(EmptyExec::new(schema.clone()));
        let result = Arc::new(exec)
            .with_new_children(vec![new_input as Arc<dyn ExecutionPlan>])
            .unwrap();

        let new_exec = result.as_any().downcast_ref::<DynamicFilterExec>().unwrap();
        assert!(new_exec.dynamic_filter.is_some());
    }

    #[test]
    fn reset_state_succeeds() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::empty::EmptyExec;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let input = Arc::new(EmptyExec::new(schema.clone()));
        let dfschema = Arc::new(DFSchema::try_from(schema.as_ref().clone()).unwrap());
        let predicate = Expr::Literal(datafusion::common::ScalarValue::Boolean(Some(true)), None);

        let exec = DynamicFilterExec::new(predicate, dfschema, input);
        let result = Arc::new(exec).reset_state();
        assert!(result.is_ok());
    }
}
