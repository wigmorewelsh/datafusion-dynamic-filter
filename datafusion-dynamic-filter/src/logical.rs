use datafusion::common::{DFSchema, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::fmt;
use std::sync::Arc;

#[derive(Clone)]
pub struct DynamicFilterNode {
    pub predicate: Expr,
    pub input: Arc<LogicalPlan>,
    pub schema: Arc<DFSchema>,
}

impl DynamicFilterNode {
    pub fn new(predicate: Expr, input: Arc<LogicalPlan>) -> DataFusionResult<Self> {
        let schema = input.schema().clone();

        Ok(Self {
            predicate,
            input,
            schema,
        })
    }
}

impl std::fmt::Debug for DynamicFilterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicFilterNode")
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl PartialEq for DynamicFilterNode {
    fn eq(&self, other: &Self) -> bool {
        self.predicate == other.predicate && self.input == other.input
    }
}

impl Eq for DynamicFilterNode {}

impl std::hash::Hash for DynamicFilterNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.predicate.hash(state);
        self.input.hash(state);
    }
}

impl PartialOrd for DynamicFilterNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DynamicFilterNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_ptr = Arc::as_ptr(&self.input) as usize;
        let other_ptr = Arc::as_ptr(&other.input) as usize;
        self_ptr.cmp(&other_ptr)
    }
}

impl UserDefinedLogicalNodeCore for DynamicFilterNode {
    fn name(&self) -> &str {
        "DynamicFilter"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &Arc<DFSchema> {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.predicate.clone()]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DynamicFilter: predicate={:?}", self.predicate)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if exprs.len() != 1 {
            return datafusion::common::internal_err!(
                "DynamicFilterNode expects exactly one expression"
            );
        }
        if inputs.len() != 1 {
            return datafusion::common::internal_err!(
                "DynamicFilterNode expects exactly one input"
            );
        }

        let predicate = exprs.into_iter().next().unwrap();
        let input = Arc::new(inputs.into_iter().next().unwrap());

        Self::new(predicate, input)
    }
}
