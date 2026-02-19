use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan, ScalarUDF};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{PARAM_RESOLVER_UDF_NAME, ParamResolverUDF};

#[derive(Debug)]
pub struct ParameterizedStatement {
    plan: Arc<dyn ExecutionPlan>,
    session_state: SessionState,
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    aggregate_functions: HashMap<String, Arc<datafusion::logical_expr::AggregateUDF>>,
    window_functions: HashMap<String, Arc<datafusion::logical_expr::WindowUDF>>,
}

impl ParameterizedStatement {
    pub fn new(
        logical_plan: &LogicalPlan,
        physical_plan: Arc<dyn ExecutionPlan>,
        session_state: SessionState,
    ) -> Self {
        let (scalar_funcs, aggregate_funcs, window_funcs) =
            Self::collect_function_names(logical_plan);

        let mut scalar_functions = HashMap::new();
        for name in scalar_funcs {
            if let Some(func) = session_state.scalar_functions().get(&name) {
                scalar_functions.insert(name, Arc::clone(func));
            }
        }

        let mut aggregate_functions = HashMap::new();
        for name in aggregate_funcs {
            if let Some(func) = session_state.aggregate_functions().get(&name) {
                aggregate_functions.insert(name, Arc::clone(func));
            }
        }

        let mut window_functions = HashMap::new();
        for name in window_funcs {
            if let Some(func) = session_state.window_functions().get(&name) {
                window_functions.insert(name, Arc::clone(func));
            }
        }

        Self {
            plan: physical_plan,
            session_state,
            scalar_functions,
            aggregate_functions,
            window_functions,
        }
    }

    pub fn plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.plan
    }

    pub async fn execute(
        &self,
        params: HashMap<String, ScalarValue>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let param_resolver = Arc::new(ScalarUDF::new_from_impl(ParamResolverUDF::new(params)));

        let mut scalar_functions = self.scalar_functions.clone();
        scalar_functions.insert(PARAM_RESOLVER_UDF_NAME.to_string(), param_resolver);

        let aggregate_functions = self.aggregate_functions.clone();
        let window_functions = self.window_functions.clone();

        let task_context = Arc::new(TaskContext::new(
            None,
            self.session_state.session_id().to_string(),
            self.session_state.config().clone(),
            scalar_functions,
            aggregate_functions,
            window_functions,
            Arc::clone(self.session_state.runtime_env()),
        ));

        self.plan.execute(0, task_context)
    }

    fn collect_function_names(
        plan: &LogicalPlan,
    ) -> (HashSet<String>, HashSet<String>, HashSet<String>) {
        let mut scalar_funcs = HashSet::new();
        let mut aggregate_funcs = HashSet::new();
        let mut window_funcs = HashSet::new();

        Self::collect_from_plan(
            plan,
            &mut scalar_funcs,
            &mut aggregate_funcs,
            &mut window_funcs,
        );

        (scalar_funcs, aggregate_funcs, window_funcs)
    }

    fn collect_from_plan(
        plan: &LogicalPlan,
        scalar_funcs: &mut HashSet<String>,
        aggregate_funcs: &mut HashSet<String>,
        window_funcs: &mut HashSet<String>,
    ) {
        plan.expressions().iter().for_each(|expr| {
            Self::collect_from_expr(expr, scalar_funcs, aggregate_funcs, window_funcs);
        });

        plan.inputs().iter().for_each(|child| {
            Self::collect_from_plan(child, scalar_funcs, aggregate_funcs, window_funcs);
        });
    }

    fn collect_from_expr(
        expr: &Expr,
        scalar_funcs: &mut HashSet<String>,
        aggregate_funcs: &mut HashSet<String>,
        window_funcs: &mut HashSet<String>,
    ) {
        struct FunctionCollector<'a> {
            scalar_funcs: &'a mut HashSet<String>,
            aggregate_funcs: &'a mut HashSet<String>,
            window_funcs: &'a mut HashSet<String>,
        }

        impl<'a> TreeNodeVisitor<'_> for FunctionCollector<'a> {
            type Node = Expr;

            fn f_down(
                &mut self,
                node: &Self::Node,
            ) -> datafusion::common::Result<TreeNodeRecursion> {
                match node {
                    Expr::ScalarFunction(func) => {
                        self.scalar_funcs.insert(func.name().to_string());
                    }
                    Expr::AggregateFunction(func) => {
                        self.aggregate_funcs.insert(func.func.name().to_string());
                    }
                    Expr::WindowFunction(func) => {
                        self.window_funcs.insert(func.fun.to_string());
                    }
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            }
        }

        let mut collector = FunctionCollector {
            scalar_funcs,
            aggregate_funcs,
            window_funcs,
        };
        expr.visit(&mut collector).ok();
    }
}
