use super::param_resolver::ParamResolverUDF;
use super::param_resolver::{PARAM_RESOLVER_UDF_NAME, replace_placeholders};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DFSchema, Result as DataFusionResult};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
};
use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
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
        let predicate_with_params = if let Some(udf) =
            context.scalar_functions().get(PARAM_RESOLVER_UDF_NAME)
        {
            if let Some(param_resolver) = udf.inner().as_any().downcast_ref::<ParamResolverUDF>() {
                let params = param_resolver.get_params();
                replace_placeholders(&self.predicate, params)?
            } else {
                self.predicate.clone()
            }
        } else {
            self.predicate.clone()
        };

        let physical_predicate = create_physical_expr(
            &predicate_with_params,
            &self.input_dfschema,
            &Default::default(),
        )?;

        if let Some(dynamic_filter) = &self.dynamic_filter {
            if let Some(df) = dynamic_filter
                .as_any()
                .downcast_ref::<DynamicFilterPhysicalExpr>()
            {
                df.update(physical_predicate.clone())?;
            }
            return self.input.execute(partition, context);
        }

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

        let dynamic_filter = if let Some(dynamic_filter) = &self.dynamic_filter {
            dynamic_filter.clone()
        } else {
            let placeholder = Arc::new(datafusion_physical_expr::expressions::Literal::new(
                datafusion::common::ScalarValue::Boolean(Some(true)),
            )) as Arc<dyn PhysicalExpr>;
            Arc::new(DynamicFilterPhysicalExpr::new(vec![], placeholder)) as Arc<dyn PhysicalExpr>
        };

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

        let dynamic_filter = child_pushdown_result
            .self_filters
            .first()
            .and_then(|filters| {
                filters.iter().find_map(|f| {
                    f.predicate
                        .as_any()
                        .downcast_ref::<DynamicFilterPhysicalExpr>()
                        .map(|_| f.predicate.clone())
                })
            });

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
        if let Some(dynamic_filter) = &self.dynamic_filter {
            if let Some(df) = dynamic_filter
                .as_any()
                .downcast_ref::<DynamicFilterPhysicalExpr>()
            {
                let placeholder = Arc::new(datafusion_physical_expr::expressions::Literal::new(
                    datafusion::common::ScalarValue::Boolean(Some(true)),
                )) as Arc<dyn PhysicalExpr>;
                df.update(placeholder)?;
            }
        }
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
