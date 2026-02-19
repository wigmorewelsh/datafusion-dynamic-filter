use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::print_batches;
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

    let mut config = SessionConfig::new().with_target_partitions(1);
    config.options_mut().execution.parquet.pushdown_filters = true;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_optimizer_rule(Arc::new(DynamicFilterRule::new()))
        .with_query_planner(Arc::new(ExtendableQueryPlanner::new(extension_planners)))
        .build();

    SessionContext::new_with_state(state)
}

fn create_sample_data() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int32, false),
        Field::new("username", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    let user_ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let usernames = StringArray::from(vec!["alice", "bob", "charlie", "diana", "eve"]);
    let emails = StringArray::from(vec![
        "alice@example.com",
        "bob@example.com",
        "charlie@example.com",
        "diana@example.com",
        "eve@example.com",
    ]);
    let ages = Int32Array::from(vec![25, 30, 35, 28, 42]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(user_ids),
            Arc::new(usernames),
            Arc::new(emails),
            Arc::new(ages),
        ],
    )
    .unwrap()
}

fn create_parquet_file(dir: &TempDir) -> String {
    let batch = create_sample_data();
    let schema = batch.schema();

    let path = dir.path().join("users.parquet");
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

async fn example_single_parameter(ctx: &SessionContext) -> Result<(), Box<dyn std::error::Error>> {
    let stmt = ctx
        .prepare("SELECT user_id, username, email FROM users WHERE user_id = $1")
        .await?;

    let mut params = HashMap::new();
    params.insert("$1".to_string(), ScalarValue::Int32(Some(2)));
    let result = stmt.execute(params).await?;
    let batches = datafusion::physical_plan::common::collect(result).await?;
    print_batches(&batches)?;

    let mut params = HashMap::new();
    params.insert("$1".to_string(), ScalarValue::Int32(Some(5)));
    let result = stmt.execute(params).await?;
    let batches = datafusion::physical_plan::common::collect(result).await?;
    print_batches(&batches)?;

    Ok(())
}

async fn example_multiple_parameters(
    ctx: &SessionContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let stmt = ctx
        .prepare("SELECT user_id, username, age FROM users WHERE user_id >= $1 AND age <= $2")
        .await?;

    let mut params = HashMap::new();
    params.insert("$1".to_string(), ScalarValue::Int32(Some(2)));
    params.insert("$2".to_string(), ScalarValue::Int32(Some(35)));

    let result = stmt.execute(params).await?;
    let batches = datafusion::physical_plan::common::collect(result).await?;
    print_batches(&batches)?;

    Ok(())
}

async fn example_string_parameter(ctx: &SessionContext) -> Result<(), Box<dyn std::error::Error>> {
    let stmt = ctx
        .prepare("SELECT user_id, username, email FROM users WHERE username = $1")
        .await?;

    let mut params = HashMap::new();
    params.insert(
        "$1".to_string(),
        ScalarValue::Utf8View(Some("charlie".to_string())),
    );

    let result = stmt.execute(params).await?;
    let batches = datafusion::physical_plan::common::collect(result).await?;
    print_batches(&batches)?;

    Ok(())
}

async fn example_aggregation_with_parameter(
    ctx: &SessionContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let stmt = ctx
        .prepare("SELECT COUNT(*) as user_count FROM users WHERE age >= $1")
        .await?;

    let mut params = HashMap::new();
    params.insert("$1".to_string(), ScalarValue::Int32(Some(30)));

    let result = stmt.execute(params).await?;
    let batches = datafusion::physical_plan::common::collect(result).await?;
    print_batches(&batches)?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let parquet_path = create_parquet_file(&temp_dir);

    let ctx = create_session_context();
    register_parquet_table(&ctx, "users", &parquet_path).await;

    example_single_parameter(&ctx).await?;
    example_multiple_parameters(&ctx).await?;
    example_string_parameter(&ctx).await?;
    example_aggregation_with_parameter(&ctx).await?;

    Ok(())
}
