use datafusion::prelude::SessionContext;
use datafusion_dynamic_filter::PreparableSessionContext;

#[tokio::test]
async fn test_trait_is_implemented_for_session_context() {
    let ctx = SessionContext::new();

    let result = ctx.prepare("SELECT 1").await;

    assert!(result.is_ok());
}
