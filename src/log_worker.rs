// src/log_worker.rs
use clap::Parser;
use deadpool_redis::{Config as RedisPoolConfig, Runtime};
use kafka_demo::common::SeckillRecord;
use kafka_demo::redis_state::{pop_log_detail};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
use chrono::DateTime;
use serde::Deserialize;
use tokio::time::sleep;
use sqlx::QueryBuilder;


/// 日志落库 Worker（消费 logs:detail）
#[derive(Parser, Debug, Clone, Deserialize)]
#[serde(default)]
#[command(author, version, about)]
struct Args {
    #[arg(long)]
    redis_url: Option<String>,

    #[arg(long)]
    pg_dsn: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Config {
    log_worker: Option<Args>,
    server: Args,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            redis_url: None,
            pg_dsn: None,
        }
    }
}

#[tokio::main]
async fn main() {
    let mut args = Args::parse();

    // 合并 config.yaml
    let config_path = std::env::current_dir().unwrap().join("config.yaml");
    if config_path.exists() {
        if let Ok(file) = std::fs::File::open(config_path) {
            if let Ok(config) = serde_yaml::from_reader::<_, Config>(file) {
                let fallback = config.log_worker.unwrap_or(config.server);

                args.redis_url = args.redis_url.or(fallback.redis_url);
                args.pg_dsn = args.pg_dsn.or(fallback.pg_dsn);
            }
        }
    }

    let args_redis_url = args.redis_url.expect("Missing --redis-url or config.log_worker.redis_url");
    let args_pg_dsn = args.pg_dsn.expect("Missing --pg-dsn or config.log_worker.pg_dsn");

    let redis_cfg = RedisPoolConfig::from_url(args_redis_url);
    let redis_pool = Arc::new(
        redis_cfg
            .create_pool(Some(Runtime::Tokio1))
            .expect("Failed to create Redis pool"),
    );

    let pg_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&args_pg_dsn)
        .await
        .expect("PostgreSQL connect fail");

    println!("[LogWorker] 启动成功");

    loop {
        let mut batch = Vec::new();

        for _ in 0..100 {
            if let Some(json) = pop_log_detail(&redis_pool, 2).await {
                println!("[LogWorker] pop -> {}", json);
                match serde_json::from_str::<SeckillRecord>(&json) {
                    Ok(rec) => batch.push(rec),
                    Err(e) => eprintln!("[LogWorker] ❌ JSON 解析失败: {e}"),
                }
            }
        }

        println!("[LogWorker] 当前 batch 条数: {}", batch.len());

        if !batch.is_empty() {
            match insert_batch(&pg_pool, &batch).await {
                Ok(_) => println!("[LogWorker] ✅ 批量写入 {} 条", batch.len()),
                Err(e) => eprintln!("[LogWorker] ❌ 写入失败: {e}"),
            }
        } else {
            sleep(Duration::from_millis(300)).await;
        }
    }
}

async fn insert_batch(pool: &sqlx::PgPool, records: &[SeckillRecord]) -> Result<(), sqlx::Error> {
    println!("[LogWorker] 🔨 批量构建 INSERT 语句: {} 条", records.len());

    let mut tx = pool.begin().await?;

    let mut builder = QueryBuilder::new(
        "INSERT INTO seckill_record (user_id, activity_id, cost_ms, status, ts) ",
    );

    builder.push_values(records, |mut b, r| {
        b.push_bind(r.user_id as i64)
            .push_bind(r.activity_id as i64)
            .push_bind(r.cost_ms as i32)
            .push_bind(&r.status)
            .push_bind(DateTime::from_timestamp(
                r.timestamp as i64,
                0,
            ).unwrap_or_else(|| {
                eprintln!("[LogWorker] ⚠️ timestamp 非法值: {}", r.timestamp);
                DateTime::from_timestamp(0, 0)
                    .expect("timestamp 0 竟然不合法")
            }));
    });

    builder.build().execute(&mut *tx).await?;
    tx.commit().await?;

    Ok(())
}