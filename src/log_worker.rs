use chrono::DateTime;
// src/log_worker.rs
use clap::Parser;
use deadpool_redis::{Config as RedisPoolConfig, Runtime};
use kafka_demo::common::SeckillRecord;
use kafka_demo::redis_state::pop_log_detail;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::QueryBuilder;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, Instant};

/// 日志落库 Worker（消费 logs:detail）
#[derive(Parser, Debug, Clone, Deserialize, Default)]
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

#[allow(dead_code)]
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

    // 批量窗口参数
    const BATCH_SIZE: usize = 100;
    const BATCH_INTERVAL_MS: u64 = 10_000; // 10 秒
    const STATUS_REPORT_INTERVAL_MS: u64 = 60_000; // 每 60 秒输出一次状态

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut last_flush = Instant::now();
    let mut last_status_report = Instant::now(); // 💡 新增状态报告时间记录

    loop {
        let mut did_pop = false;

        // 一轮尽量填满 batch
        for _ in 0..(BATCH_SIZE - batch.len()) {
            if let Some(json) = pop_log_detail(&redis_pool, 2).await {
                did_pop = true;
                println!("[LogWorker] pop -> {}", json);
                match serde_json::from_str::<SeckillRecord>(&json) {
                    Ok(rec) => batch.push(rec),
                    Err(e) => eprintln!("[LogWorker] ❌ JSON 解析失败: {e}"),
                }
            } else {
                break;
            }
        }

        let time_due = last_flush.elapsed() >= Duration::from_millis(BATCH_INTERVAL_MS);
        let size_due = batch.len() >= BATCH_SIZE;

        if !batch.is_empty() && (size_due || time_due) {
            println!("[LogWorker] flush batch: {} 条，距离上次写入 {:?}", batch.len(), last_flush.elapsed());
            match insert_batch(&pg_pool, &batch).await {
                Ok(_) => println!("[LogWorker] ✅ 批量写入 {} 条", batch.len()),
                Err(e) => eprintln!("[LogWorker] ❌ 写入失败: {e}"),
            }
            batch.clear();
            last_flush = Instant::now();
        }

        // 没有新数据就 sleep，避免空转
        if !did_pop {
            sleep(Duration::from_millis(200)).await;
        }
        
        // 定期状态日志（如心跳）
        if last_status_report.elapsed() >= Duration::from_millis(STATUS_REPORT_INTERVAL_MS) {
            println!(
                "[LogWorker] 🟢 状态报告：当前 batch 中有 {} 条数据，Redis连接状态: {:?}",
                batch.len(),
                redis_pool.status()
            );
            last_status_report = Instant::now();
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