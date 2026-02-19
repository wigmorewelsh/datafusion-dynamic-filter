use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;

use crate::ParameterizedStatement;

#[async_trait]
pub trait PreparableSessionContext {
    async fn prepare(&self, sql: &str) -> DataFusionResult<ParameterizedStatement>;
}

#[async_trait]
impl PreparableSessionContext for SessionContext {
    async fn prepare(&self, sql: &str) -> DataFusionResult<ParameterizedStatement> {
        let plan = self.state().create_logical_plan(sql).await?;

        if let LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(_)) = &plan {
            return Err(DataFusionError::Plan(
                "Cannot prepare DDL statement".to_string(),
            ));
        }

        if let LogicalPlan::Ddl(DdlStatement::CreateIndex(_)) = &plan {
            return Err(DataFusionError::Plan(
                "Cannot prepare DDL statement".to_string(),
            ));
        }

        let physical_plan = self.state().create_physical_plan(&plan).await?;

        Ok(ParameterizedStatement::new(
            &plan,
            physical_plan,
            self.state(),
        ))
    }
}
