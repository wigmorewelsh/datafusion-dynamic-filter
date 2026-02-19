use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_dynamic_filter::{
    DynamicFilterExtensionPlanner, DynamicFilterRule, ExtendableQueryPlanner,
    PreparableSessionContext,
};
use std::collections::HashMap;
use std::sync::Arc;

fn create_session_context() -> SessionContext {
    let extension_planners: Vec<
        Arc<dyn datafusion::physical_planner::ExtensionPlanner + Send + Sync>,
    > = vec![Arc::new(DynamicFilterExtensionPlanner::new())];

    let config = SessionConfig::new().with_target_partitions(1);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_optimizer_rule(Arc::new(DynamicFilterRule::new()))
        .with_query_planner(Arc::new(ExtendableQueryPlanner::new(extension_planners)))
        .build();

    SessionContext::new_with_state(state)
}

fn create_test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Int32, false),
    ]));

    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let name_array = StringArray::from(vec!["alice", "bob", "charlie", "diana", "eve"]);
    let amount_array = Int32Array::from(vec![100, 200, 300, 400, 500]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(amount_array),
        ],
    )
    .unwrap()
}

async fn register_memtable(ctx: &SessionContext, table_name: &str, batch: RecordBatch) {
    let schema = batch.schema();
    let partitions = vec![vec![batch]];
    let table = MemTable::try_new(schema, partitions).unwrap();
    ctx.register_table(table_name, Arc::new(table)).unwrap();
}

#[tokio::test]
async fn test_trait_is_implemented_for_session_context() {
    let ctx = SessionContext::new();
    let result = ctx.prepare("SELECT 1").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_parameterized_query_single_parameter() {
    let ctx = create_session_context();
    let batch = create_test_batch();
    register_memtable(&ctx, "users", batch).await;

    let stmt = ctx
        .prepare("SELECT id, name, amount FROM users WHERE id = $1")
        .await
        .unwrap();

    let mut params = HashMap::new();
    params.insert("$1".to_string(), ScalarValue::Int32(Some(3)));

    let result = stmt.execute(params).await.unwrap();
    let batches = datafusion::physical_plan::common::collect(result)
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    let batch = &batches[0];
    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let name_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(id_col.value(0), 3);
    assert_eq!(name_col.value(0), "charlie");
}

#[tokio::test]
async fn test_parameterized_query_multiple_executions() {
    let ctx = create_session_context();
    let batch = create_test_batch();
    register_memtable(&ctx, "users", batch).await;

    let stmt = ctx
        .prepare("SELECT id, name FROM users WHERE id = $1")
        .await
        .unwrap();

    let mut params1 = HashMap::new();
    params1.insert("$1".to_string(), ScalarValue::Int32(Some(1)));
    let result1 = stmt.execute(params1).await.unwrap();
    let batches1 = datafusion::physical_plan::common::collect(result1)
        .await
        .unwrap();

    assert_eq!(batches1.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let id_col1 = batches1[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(id_col1.value(0), 1);

    let mut params2 = HashMap::new();
    params2.insert("$1".to_string(), ScalarValue::Int32(Some(5)));
    let result2 = stmt.execute(params2).await.unwrap();
    let batches2 = datafusion::physical_plan::common::collect(result2)
        .await
        .unwrap();

    assert_eq!(batches2.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let id_col2 = batches2[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(id_col2.value(0), 5);
}

#[tokio::test]
async fn test_parameterized_query_string_parameter() {
    let ctx = create_session_context();
    let batch = create_test_batch();
    register_memtable(&ctx, "users", batch).await;

    let stmt = ctx
        .prepare("SELECT id, name FROM users WHERE name = $1")
        .await
        .unwrap();

    let mut params = HashMap::new();
    params.insert("$1".to_string(), ScalarValue::Utf8(Some("bob".to_string())));

    let result = stmt.execute(params).await.unwrap();
    let batches = datafusion::physical_plan::common::collect(result)
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    let batch = &batches[0];
    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let name_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(id_col.value(0), 2);
    assert_eq!(name_col.value(0), "bob");
}

#[tokio::test]
async fn test_parameterized_query_multiple_parameters() {
    let ctx = create_session_context();
    let batch = create_test_batch();
    register_memtable(&ctx, "users", batch).await;

    let stmt = ctx
        .prepare("SELECT id, name, amount FROM users WHERE id >= $1 AND id <= $2")
        .await
        .unwrap();

    let mut params = HashMap::new();
    params.insert("$1".to_string(), ScalarValue::Int32(Some(2)));
    params.insert("$2".to_string(), ScalarValue::Int32(Some(4)));

    let result = stmt.execute(params).await.unwrap();
    let batches = datafusion::physical_plan::common::collect(result)
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    let batch = &batches[0];
    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    assert_eq!(id_col.value(0), 2);
    assert_eq!(id_col.value(1), 3);
    assert_eq!(id_col.value(2), 4);
}

#[tokio::test]
async fn test_parameterized_query_no_matches() {
    let ctx = create_session_context();
    let batch = create_test_batch();
    register_memtable(&ctx, "users", batch).await;

    let stmt = ctx
        .prepare("SELECT id, name FROM users WHERE id = $1")
        .await
        .unwrap();

    let mut params = HashMap::new();
    params.insert("$1".to_string(), ScalarValue::Int32(Some(999)));

    let result = stmt.execute(params).await.unwrap();
    let batches = datafusion::physical_plan::common::collect(result)
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_parameterized_query_projection_subset() {
    let ctx = create_session_context();
    let batch = create_test_batch();
    register_memtable(&ctx, "users", batch).await;

    let stmt = ctx
        .prepare("SELECT name FROM users WHERE id = $1")
        .await
        .unwrap();

    let mut params = HashMap::new();
    params.insert("$1".to_string(), ScalarValue::Int32(Some(4)));

    let result = stmt.execute(params).await.unwrap();
    let batches = datafusion::physical_plan::common::collect(result)
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 1);
    let name_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name_col.value(0), "diana");
}

#[tokio::test]
async fn test_parameterized_query_with_aggregation() {
    use datafusion::arrow::array::Int64Array;

    let ctx = create_session_context();
    let batch = create_test_batch();
    register_memtable(&ctx, "users", batch).await;

    let stmt = ctx
        .prepare("SELECT COUNT(*) as count FROM users WHERE id >= $1")
        .await
        .unwrap();

    let mut params = HashMap::new();
    params.insert("$1".to_string(), ScalarValue::Int32(Some(3)));

    let result = stmt.execute(params).await.unwrap();
    let batches = datafusion::physical_plan::common::collect(result)
        .await
        .unwrap();

    let batch = &batches[0];
    let count_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count_col.value(0), 3);
}
