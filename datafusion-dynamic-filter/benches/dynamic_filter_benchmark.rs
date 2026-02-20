use criterion::{black_box, criterion_group, criterion_main, Criterion};
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
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

const RECORD_COUNT: u32 = 1_000;
const QUERY_KEY_COUNT: usize = 50;
const RANDOM_SEED: u64 = 42;
const SAMPLE_SIZE: usize = 10;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

fn generate_random_keys() -> Vec<u32> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(RANDOM_SEED);
    let mut keys: Vec<u32> = (0..RECORD_COUNT).collect();
    keys.shuffle(&mut rng);
    keys.into_iter().take(QUERY_KEY_COUNT).collect()
}

trait PreparedStatementBench {
    fn setup() -> Self;
    fn populate(&mut self);
    fn query_random_keys_prepared(&self, keys: &[u32]) -> usize;
    fn query_random_keys_unprepared(&self, keys: &[u32]) -> usize;
    fn query_random_keys_logical_plan(&self, keys: &[u32]) -> usize;
}

struct DataFusionAdapter {
    ctx: SessionContext,
}

impl PreparedStatementBench for DataFusionAdapter {
    fn setup() -> Self {
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

        let ctx = SessionContext::new_with_state(state);
        Self { ctx }
    }

    fn populate(&mut self) {
        let runtime = get_runtime();

        runtime.block_on(async {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("amount", DataType::Int32, false),
            ]));

            let mut ids = Vec::with_capacity(RECORD_COUNT as usize);
            let mut names = Vec::with_capacity(RECORD_COUNT as usize);
            let mut amounts = Vec::with_capacity(RECORD_COUNT as usize);

            for id in 0..RECORD_COUNT {
                ids.push(id as i32);
                names.push(format!("user_{}", id));
                amounts.push((id * 10) as i32);
            }

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(StringArray::from(names)),
                    Arc::new(Int32Array::from(amounts)),
                ],
            )
            .unwrap();

            let partitions = vec![vec![batch]];
            let table = MemTable::try_new(schema, partitions).unwrap();
            self.ctx.register_table("users", Arc::new(table)).unwrap();
        });
    }

    fn query_random_keys_prepared(&self, keys: &[u32]) -> usize {
        let runtime = get_runtime();

        runtime.block_on(async {
            let stmt = self
                .ctx
                .prepare("SELECT id, name, amount FROM users WHERE id = $1")
                .await
                .unwrap();

            let mut total_rows = 0;

            for key in keys {
                let mut params = HashMap::new();
                params.insert("$1".to_string(), ScalarValue::Int32(Some(*key as i32)));

                let result = stmt.execute(params).await.unwrap();
                let batches = datafusion::physical_plan::common::collect(result)
                    .await
                    .unwrap();
                total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
            }

            total_rows
        })
    }

    fn query_random_keys_unprepared(&self, keys: &[u32]) -> usize {
        let runtime = get_runtime();

        runtime.block_on(async {
            let mut total_rows = 0;

            for key in keys {
                let sql = format!("SELECT id, name, amount FROM users WHERE id = {}", key);
                let batches = self.ctx.sql(&sql).await.unwrap().collect().await.unwrap();
                total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
            }

            total_rows
        })
    }

    fn query_random_keys_logical_plan(&self, keys: &[u32]) -> usize {
        let runtime = get_runtime();

        runtime.block_on(async {
            let logical_plan = self
                .ctx
                .state()
                .create_logical_plan("SELECT id, name, amount FROM users WHERE id = $1")
                .await
                .unwrap();

            let mut total_rows = 0;

            for key in keys {
                let plan_with_params = logical_plan
                    .clone()
                    .with_param_values(vec![ScalarValue::Int32(Some(*key as i32))])
                    .unwrap();

                let optimized_plan = self.ctx.state().optimize(&plan_with_params).unwrap();
                let physical_plan = self
                    .ctx
                    .state()
                    .create_physical_plan(&optimized_plan)
                    .await
                    .unwrap();
                let task_ctx = self.ctx.task_ctx();
                let stream =
                    datafusion::physical_plan::execute_stream(physical_plan, task_ctx).unwrap();
                let batches = datafusion::physical_plan::common::collect(stream)
                    .await
                    .unwrap();
                total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
            }

            total_rows
        })
    }
}

fn benchmark_prepared_statement_lookup(c: &mut Criterion) {
    fn scenario_prepared(db: &impl PreparedStatementBench, keys: &[u32]) {
        let count = db.query_random_keys_prepared(keys);
        black_box(count);
    }

    fn scenario_unprepared(db: &impl PreparedStatementBench, keys: &[u32]) {
        let count = db.query_random_keys_unprepared(keys);
        black_box(count);
    }

    fn scenario_logical_plan(db: &impl PreparedStatementBench, keys: &[u32]) {
        let count = db.query_random_keys_logical_plan(keys);
        black_box(count);
    }

    let random_keys = generate_random_keys();

    let mut group = c.benchmark_group("prepared_statement_lookup");
    group.sample_size(SAMPLE_SIZE);

    group.bench_function("datafusion_prepared", |b| {
        let mut datafusion_db = DataFusionAdapter::setup();
        datafusion_db.populate();
        b.iter(|| scenario_prepared(&datafusion_db, &random_keys))
    });

    group.bench_function("datafusion_logical_plan", |b| {
        let mut datafusion_db = DataFusionAdapter::setup();
        datafusion_db.populate();
        b.iter(|| scenario_logical_plan(&datafusion_db, &random_keys))
    });

    group.bench_function("datafusion_unprepared", |b| {
        let mut datafusion_db = DataFusionAdapter::setup();
        datafusion_db.populate();
        b.iter(|| scenario_unprepared(&datafusion_db, &random_keys))
    });

    group.finish();
}

fn dynamic_filter_benchmark_suite(c: &mut Criterion) {
    benchmark_prepared_statement_lookup(c);
}

criterion_group!(benches, dynamic_filter_benchmark_suite);
criterion_main!(benches);
