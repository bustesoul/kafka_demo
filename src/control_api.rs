// src/control_api.rs

use axum::{
    extract::{Query, State},
    routing::{get, post},
    Json, Router,
};
use deadpool_redis::{Pool, Config as RedisPoolConfig, Runtime};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::Arc;
use clap::Parser;
use kafka_demo::redis_state::*;
use sqlx::QueryBuilder;
use kafka_demo::args::ControlApiArgs;
use kafka_demo::common::Config;

#[allow(dead_code)]
#[tokio::main]
async fn main() {
    let mut args =  ControlApiArgs::parse();

    // 合并 config.yaml
    let config_path = std::env::current_dir().unwrap().join("config.yaml");
    if config_path.exists() {
        if let Ok(file) = std::fs::File::open(config_path) {
            if let Ok(config) = serde_yaml::from_reader::<_, Config>(file) {
                let (ca, server) = (config.control_api, config.server);

                args.redis_url = args.redis_url
                    .or_else(|| ca.as_ref().and_then(|x| x.redis_url.clone()))
                    .or_else(|| server.as_ref().and_then(|s| s.redis_url.clone()));
                args.pg_dsn = args.pg_dsn
                    .or_else(|| ca.as_ref().and_then(|x| x.pg_dsn.clone()))
                    .or_else(|| server.as_ref().and_then(|s| s.pg_dsn.clone()));
            }
        }
    }

    let args_redis_url = args.redis_url.expect("Missing --redis-url or config.control_api.redis_url");
    let args_pg_dsn = args.pg_dsn.expect("Missing --pg-dsn or config.control_api.pg_dsn");

    let redis_cfg = RedisPoolConfig::from_url(args_redis_url);
    let redis_pool = Arc::new(
        redis_cfg
            .create_pool(Some(Runtime::Tokio1))
            .expect("Failed to create Redis pool"),
    );

    let pg_pool = Arc::new(
        PgPoolOptions::new()
        .max_connections(5)
        .connect(&args_pg_dsn)
        .await
        .expect("PostgreSQL connect fail")
    );

    println!("[Control API] 启动成功");

    let state = ApiState {
        redis: redis_pool,
        pg: pg_pool,
    };
    
    // 构建 app
    let app = routes(state);

    // 新推荐写法
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Control API started at 0.0.0.0:3000");
    axum::serve(listener, app).await.unwrap();
}

// ------------- 结构体和API实现部分保持不变 -------------

#[derive(Clone)]
pub struct ApiState {
    pub redis: Arc<Pool>,
    pub pg: Arc<PgPool>,
}

#[derive(Deserialize)]
pub struct SetStockInput {
    pub value: i64,
}
#[derive(Serialize)]
pub struct StatusDetail {
    pub stock: Option<i64>,
    pub over: bool,
    pub version: Option<i64>,
    pub success: Option<i64>,
    pub fail: Option<i64>,
    pub timeout: Option<i64>,
}
#[derive(Deserialize)]
pub struct SyncFromDbInput {
    pub activity_id: i64,
}

// 1. 设置库存，重置状态、递增版本并 PUBLISH
pub async fn set_stock_api(
    State(state): State<ApiState>,
    Json(input): Json<SetStockInput>,
) -> Json<bool> {
    let ok = set_stock(&state.redis, input.value).await;
    Json(ok)
}

// 2. 结束活动，PUBLISH 广播
pub async fn finish_activity_api(State(state): State<ApiState>) -> Json<bool> {
    let ok = set_activity_finish(&state.redis).await;
    Json(ok)
}

// 3. 查询全部状态
pub async fn status_api(State(state): State<ApiState>) -> Json<StatusDetail> {
    let stock = get_stock(&state.redis).await;
    let over = is_activity_over(&state.redis).await;
    let version = get_version(&state.redis).await;
    let success = get_stats(&state.redis, "stats:success").await;
    let fail = get_stats(&state.redis, "stats:fail").await;
    let timeout = get_stats(&state.redis, "stats:timeout").await;
    Json(StatusDetail {
        stock,
        over,
        version,
        success,
        fail,
        timeout,
    })
}

// 4. 从数据库同步活动库存到 Redis（并递增版本，PUBLISH）
pub async fn sync_from_db_api(
    State(state): State<ApiState>,
    Query(q): Query<SyncFromDbInput>,
) -> Json<bool> {
    if let Some(stock) = get_activity_stock(&state.pg, q.activity_id).await {
        let ok = sync_redis_from_db(&state.redis, stock).await;
        Json(ok)
    } else {
        Json(false)
    }
}

pub fn routes(state: ApiState) -> Router {
    Router::new()
        .route("/set_stock", post(set_stock_api))
        .route("/finish_activity", post(finish_activity_api))
        .route("/status", get(status_api))
        .route("/sync_from_db", post(sync_from_db_api))
        .with_state(state)
}

pub async fn get_activity_stock(pg: &PgPool, activity_id: i64) -> Option<i64> {
    let mut builder = QueryBuilder::new("SELECT stock FROM seckill_activity WHERE id = ");
    builder.push_bind(activity_id);

    let row = builder
        .build_query_as::<(i64,)>()
        .fetch_one(pg)
        .await
        .ok()?;
    Some(row.0)
}