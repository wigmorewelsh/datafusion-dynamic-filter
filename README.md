This project is a demonstration and probably shouldn't be used in production.

Datafusion can pre-compute a logical plan skipping parse and logical optimize phase, but it doesn't pre-compute all the way down to the physical layer. This is normally fine for analytical workloads where there generally isn't a benefit to pre-computing complicated plans. But for very simple queries, like selecting a single row or joining two tables, the overhead of parsing and optimizing the query can be greater than running the query in the first place. This library allows SQL placeholders to be supported in the physical plan rather than just the logical plan.

```rust
let stmt = ctx
    .prepare("SELECT user_id, username, email FROM users WHERE user_id = $1")
    .await?;

// run first time
let mut params = HashMap::new();
params.insert("$1".to_string(), ScalarValue::Int32(Some(1)));

let result = stmt.execute(params).await?;

// run second time
let mut params2 = HashMap::new();
params2.insert("$1".to_string(), ScalarValue::Int32(Some(2)));

let result = stmt.execute(params2).await?;
```

This library works over re-using the dynamic filter, added to datafusion last year to optimize hashjoins [https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/] to replace placeholders in sql queries at the physical layer.

## Benchmark

The benchmarks measure the performance difference between prepared statements, precomputed logical plans, and unprepared statements when executing 50 queries against a table with 1,000 records.

| Query Type | Time | Speedup |
|-----------|------|---------|
| Prepared Statement | 2.93 ms | 13.3x faster |
| Precomputed Logical Plan | 40.53 ms | 1.0x (similar) |
| Unprepared Statement | 39.07 ms | baseline |

The prepared statement approach pre-computes the physical plan, avoiding both parsing and logical optimization on each execution. The precomputed logical plan approach (using `with_param_values`) skips parsing but still performs logical optimization and physical planning for each query, offering minimal performance improvement over unprepared queries.

The difference improves further for data providers that can optimize DynamicFilters:

Querying for 50 random keys with temporal database:

| Query Type | Time | Speedup |
|-----------|------|---------|
| Prepared Statement | 591.80 µs | 55.2x faster |
| Unprepared Statement | 32.68 ms | baseline |

Querying using IN clause with temporal database:

| Query Type | Time | Speedup |
|-----------|------|---------|
| Prepared Statement | 72.35 µs | 116.9x faster |
| Unprepared Statement | 8.46 ms | baseline |


## Usage

You'll need add the rule/planner to the session builder, as well as set the target_partitions to 1 to avoid a concurrency issue. See the examples.

```rust
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
```
