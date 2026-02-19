use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

pub const PARAM_RESOLVER_UDF_NAME: &str = "__temporal_param_resolver";

pub struct ParamResolverUDF {
    params: HashMap<String, ScalarValue>,
    signature: Signature,
}

impl ParamResolverUDF {
    pub fn new(params: HashMap<String, ScalarValue>) -> Self {
        Self {
            params,
            signature: Signature::any(0, Volatility::Immutable),
        }
    }

    pub fn empty() -> Self {
        Self::new(HashMap::new())
    }

    pub fn get_params(&self) -> &HashMap<String, ScalarValue> {
        &self.params
    }
}

impl std::fmt::Debug for ParamResolverUDF {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParamResolverUDF").finish()
    }
}

impl PartialEq for ParamResolverUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

impl Eq for ParamResolverUDF {}

impl Hash for ParamResolverUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for ParamResolverUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        PARAM_RESOLVER_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Null)
    }

    fn invoke_with_args(
        &self,
        _args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DataFusionResult<ColumnarValue> {
        unreachable!("ParamResolverUDF should never be invoked")
    }
}

pub fn replace_placeholders(
    expr: &datafusion::logical_expr::Expr,
    params: &HashMap<String, ScalarValue>,
) -> DataFusionResult<datafusion::logical_expr::Expr> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::logical_expr::Expr;

    expr.clone()
        .transform(|e| {
            if let Expr::Placeholder(placeholder) = &e {
                if let Some(value) = params.get(&placeholder.id) {
                    Ok(Transformed::yes(Expr::Literal(value.clone(), None)))
                } else {
                    Err(datafusion::common::DataFusionError::Execution(format!(
                        "Placeholder '{}' was not provided a value for execution",
                        placeholder.id
                    )))
                }
            } else {
                Ok(Transformed::no(e))
            }
        })
        .map(|t| t.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::expr::Placeholder;
    use datafusion::logical_expr::{col, lit, Expr};

    #[test]
    fn param_resolver_udf_name_is_correct() {
        let udf = ParamResolverUDF::empty();
        assert_eq!(udf.name(), PARAM_RESOLVER_UDF_NAME);
    }

    #[test]
    fn param_resolver_stores_and_retrieves_params() {
        let mut params = HashMap::new();
        params.insert("$1".to_string(), ScalarValue::Int32(Some(42)));

        let udf = ParamResolverUDF::new(params.clone());
        let retrieved = udf.get_params();

        assert_eq!(retrieved.get("$1"), Some(&ScalarValue::Int32(Some(42))));
    }

    #[test]
    fn replace_placeholders_replaces_single_placeholder() {
        let mut params = HashMap::new();
        params.insert("$1".to_string(), ScalarValue::Int32(Some(42)));

        let expr = col("id").eq(Expr::Placeholder(Placeholder::new("$1".to_string(), None)));
        let result = replace_placeholders(&expr, &params).unwrap();

        assert_eq!(result, col("id").eq(lit(ScalarValue::Int32(Some(42)))));
    }

    #[test]
    fn replace_placeholders_replaces_multiple_placeholders() {
        let mut params = HashMap::new();
        params.insert("$1".to_string(), ScalarValue::Int32(Some(10)));
        params.insert("$2".to_string(), ScalarValue::Int32(Some(20)));

        let expr = col("id")
            .gt(Expr::Placeholder(Placeholder::new("$1".to_string(), None)))
            .and(col("id").lt(Expr::Placeholder(Placeholder::new("$2".to_string(), None))));

        let result = replace_placeholders(&expr, &params).unwrap();

        assert_eq!(
            result,
            col("id")
                .gt(lit(ScalarValue::Int32(Some(10))))
                .and(col("id").lt(lit(ScalarValue::Int32(Some(20)))))
        );
    }

    #[test]
    fn replace_placeholders_errors_on_missing_param() {
        let params = HashMap::new();
        let expr = col("id").eq(Expr::Placeholder(Placeholder::new("$1".to_string(), None)));

        let result = replace_placeholders(&expr, &params);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Placeholder '$1' was not provided a value"));
    }

    #[test]
    fn replace_placeholders_preserves_non_placeholder_expressions() {
        let params = HashMap::new();
        let expr = col("id").eq(lit(42));

        let result = replace_placeholders(&expr, &params).unwrap();

        assert_eq!(result, expr);
    }
}
