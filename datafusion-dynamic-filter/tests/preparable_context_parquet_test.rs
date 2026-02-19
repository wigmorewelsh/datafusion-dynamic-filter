use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_dynamic_filter::{
    DynamicFilterExtensionPlanner, DynamicFilterRule, ExtendableQueryPlanner,
    PreparableSessionContext,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

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

fn create_parquet_file(dir: &TempDir) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
    ]));

    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let value_array = StringArray::from(vec!["alpha", "beta", "gamma", "delta", "epsilon"]);
    let score_array = Int32Array::from(vec![10, 20, 30, 40, 50]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array),
            Arc::new(value_array),
            Arc::new(score_array),
        ],
    )
    .unwrap();

    let path = dir.path().join("records.parquet");
    let file = File::create(&path).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    path.to_str().unwrap().to_string()
}

async fn register_parquet_table(ctx: &SessionContext, table_name: &str, path: &str) {
    let table_path = ListingTableUrl::parse(path).unwrap();
    let file_format = ParquetFormat::default();
    let listing_options = ListingOptions::new(Arc::new(file_format));

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .infer_schema(&ctx.state())
        .await
        .unwrap();

    let table = ListingTable::try_new(config).unwrap();
    ctx.register_table(table_name, Arc::new(table)).unwrap();
}

#[tokio::test]
async fn test_parameterized_query_select_by_primary_key_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let parquet_path = create_parquet_file(&temp_dir);

    let ctx = create_session_context();
    register_parquet_table(&ctx, "records", &parquet_path).await;

    let stmt = ctx
        .prepare("SELECT id, value, score FROM records WHERE id = $1")
        .await
        .unwrap();

    let mut params = HashMap::new();
    params.insert("$1".to_string(), ScalarValue::Int32(Some(2)));

    let result = stmt.execute(params).await.unwrap();
    let batches = datafusion::physical_plan::common::collect(result)
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 3);

    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let value_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    let score_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    assert_eq!(id_col.value(0), 2);
    assert_eq!(value_col.value(0), "beta");
    assert_eq!(score_col.value(0), 20);
}

#[tokio::test]
async fn test_parameterized_query_multiple_executions_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let parquet_path = create_parquet_file(&temp_dir);

    let ctx = create_session_context();
    register_parquet_table(&ctx, "records", &parquet_path).await;

    let stmt = ctx
        .prepare("SELECT id, value, score FROM records WHERE id = $1")
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
    let value_col1 = batches1[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert_eq!(id_col1.value(0), 1);
    assert_eq!(value_col1.value(0), "alpha");

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
    let value_col2 = batches2[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert_eq!(id_col2.value(0), 5);
    assert_eq!(value_col2.value(0), "epsilon");
}
