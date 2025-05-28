// src/server_kafka.rs
use clap::Parser;
use deadpool_redis::{Config as RedisPoolConfig, Runtime};
use futures::StreamExt;
use kafka_demo::args::ServerArgs;
use kafka_demo::common::*;
use kafka_demo::redis_state::*;
use rand::{Rng, rng};
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use sqlx::types::chrono::{DateTime, Utc};
use std::error::Error;
use std::fs;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 读取配置文件
    let config_content = fs::read_to_string("config.yaml").expect("Failed to read config.yaml");
    let config: Config =
        serde_yaml::from_str(&config_content).expect("Failed to parse config.yaml");
    // 解析命令行参数
    let mut args = ServerArgs::parse();
    // 合并配置：命令行优先于配置文件
    if let Some(server_cfg) = &config.server {
        args.brokers = args.brokers.or(server_cfg.brokers.clone());
        args.topic = args.topic.or(server_cfg.topic.clone());
        args.stock = args.stock.or(server_cfg.stock.clone());
        args.timeout = args.timeout.or(server_cfg.timeout);
        args.consumers = args.consumers.or(server_cfg.consumers);
        args.status_interval = args.status_interval.or(server_cfg.status_interval);
        args.reset_offset = args.reset_offset.or(server_cfg.reset_offset);
        args.group_id = args.group_id.or(server_cfg.group_id.clone());
        args.redis_url = args.redis_url.or(server_cfg.redis_url.clone());
    }
    // 其他字段使用命令行默认值或配置文件值
    println!("启动 server，配置：{args:?}");

    let args_group_id = args
        .group_id
        .unwrap_or_else(|| "seckill_server_group".to_string());
    let args_redis_url = args
        .redis_url
        .unwrap_or_else(|| "redis://127.0.0.1/".to_string());
    let args_consumers = args.consumers.unwrap_or(3);
    let args_status_interval = args.status_interval.unwrap_or(10);
    let args_reset_offset = args.reset_offset.unwrap_or(false);

    let redis_cfg = RedisPoolConfig::from_url(args_redis_url);
    let redis_pool = redis_cfg
        .create_pool(Some(Runtime::Tokio1))
        .expect("Failed to create Redis connection pool");
    let redis_pool = Arc::new(redis_pool);
    // Provide a default HashMap if args.stock is None
    let initial_stocks = args.stock.unwrap_or_else(|| {
        let mut default_map = std::collections::HashMap::new();
        default_map.insert(1001, 1000); // Default item 1001 with 1000 stock
        default_map
    });
    init_state(&redis_pool, initial_stocks.clone()).await; // Pass the HashMap

    println!("[Server] Redis 初始化成功，库存: {:?}", initial_stocks);

    let request_counter = Arc::new(AtomicUsize::new(0));
    // 新增三个原子计数器
    let success_counter = Arc::new(AtomicUsize::new(0));
    let timeout_counter = Arc::new(AtomicUsize::new(0));
    let fail_counter = Arc::new(AtomicUsize::new(0));
    let mut rng_producer = rng();
    let delay_ms = rng_producer.random_range(5..=15);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", args_group_id)
        .set(
            "bootstrap.servers",
            args.brokers.as_deref().unwrap_or("localhost:9092"),
        ) // fallback
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        // 参数优化 ↓↓↓
        .set("fetch.max.bytes", "10485760") // 10MB
        .set("max.partition.fetch.bytes", "10485760")
        .set("queued.max.messages.kbytes", "1048576")
        .set("queued.min.messages", "10000")
        .set("max.poll.interval.ms", "30000")
        .set("session.timeout.ms", "20000")
        .set("receive.message.max.bytes", "16777216")
        .set("fetch.wait.max.ms", "100")
        .set("fetch.min.bytes", "1")
        .create()
        .expect("Consumer creation error");
    consumer.subscribe(&[args.topic.as_deref().unwrap_or("default_topic")])?;
    // reset_offset 逻辑
    if args_reset_offset {
        use rdkafka::TopicPartitionList;
        use rdkafka::{Offset, consumer::Consumer};
        use std::time::Duration;
        let mut tpl = TopicPartitionList::new();
        let topic = args.topic.as_deref().unwrap_or("default_topic");
        let md = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(3))
            .expect("metadata");
        if let Some(topic_md) = md.topics().iter().find(|t| t.name() == topic) {
            for p in topic_md.partitions() {
                tpl.add_partition_offset(topic, p.id(), Offset::End)
                    .unwrap();
            }
        }
        consumer.assign(&tpl).expect("assign partitions");
        println!("重置所有分区 offset 到最新成功，仅消费新消息！");
    }
    let consumer = Arc::new(consumer);
    let mut handles = vec![];
    for i in 0..args_consumers {
        let consumer_clone = Arc::clone(&consumer);
        let request_counter_clone = Arc::clone(&request_counter);
        let timeout_ms = args.timeout;
        let success_counter = Arc::clone(&success_counter);
        let timeout_counter = Arc::clone(&timeout_counter);
        let fail_counter = Arc::clone(&fail_counter);
        let redis_pool_for_consumer = Arc::clone(&redis_pool);
        handles.push(tokio::spawn(async move {
            let message_stream = consumer_clone.stream();
            println!("Server Consumer #{i} 启动！");
            let concurrency = 32;

            let pool = Arc::clone(&redis_pool_for_consumer);
            message_stream
                .for_each_concurrent(concurrency, move |result| {
                    let fail_counter_inner = Arc::clone(&fail_counter);
                    let timeout_counter_inner = Arc::clone(&timeout_counter);
                    let success_counter_inner = Arc::clone(&success_counter);
                    let request_counter_clone_inner = Arc::clone(&request_counter_clone);
                    let pool = Arc::clone(&pool);
                    async move {
                        if let Ok(msg) = result {
                            if let Some(payload) = msg.payload() {
                                if let Ok(req) = serde_json::from_slice::<SeckillRequest>(payload) {
                                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms))
                                        .await;
                                    let now_ms = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis();
                                    let cost_ms =
                                        now_ms.saturating_sub(req.request_initiation_time);

                                    if let Some(timeout_ms) = timeout_ms {
                                        if cost_ms > timeout_ms {
                                            // eprintln!("[Server] 用户{} 请求超时：用时 {}ms", req.user_id, cost_ms);
                                            timeout_counter_inner.fetch_add(1, Ordering::Relaxed);
                                            return;
                                        }
                                    }
                                    if is_activity_over(&pool).await {
                                        // eprintln!("[Server] 用户{} 抢购失败：活动已结束", req.user_id);
                                        fail_counter_inner.fetch_add(1, Ordering::Relaxed);
                                        return;
                                    }

                                    let script_execution_result =
                                        run_seckill_script(&pool, &req.items).await;

                                    match script_execution_result {
                                        Ok(result_code) => {
                                            if result_code == -1 {
                                                // Activity over
                                                fail_counter_inner.fetch_add(1, Ordering::Relaxed);
                                                // eprintln!("[Server] 用户{} 抢购失败：活动已结束", req.user_id);
                                            } else if result_code == -2 {
                                                // Not enough stock for some item
                                                fail_counter_inner.fetch_add(1, Ordering::Relaxed);
                                                // eprintln!("[Server] 用户{} 抢购失败：库存不足", req.user_id);
                                            } else if result_code == 1 {
                                                // Success
                                                request_counter_clone_inner
                                                    .fetch_add(1, Ordering::Relaxed);
                                                success_counter_inner
                                                    .fetch_add(1, Ordering::Relaxed);

                                                println!("[Server] 用户{} 抢购成功！", req.user_id);

                                                // ✨ 写入 Redis 日志列表 for each item
                                                let now_ts = SystemTime::now()
                                                    .duration_since(UNIX_EPOCH)
                                                    .unwrap()
                                                    .as_secs();
                                                for (item_id, quantity) in &req.items {
                                                    let log = SeckillRecord {
                                                        user_id: req.user_id as u64,
                                                        activity_id: 1, // Assuming a default activity_id
                                                        item_id: *item_id as u64, // New field
                                                        quantity: *quantity as u64, // New field
                                                        cost_ms,
                                                        status: "success".into(),
                                                        timestamp: now_ts,
                                                    };
                                                    let payload = serde_json::to_string(&log)
                                                        .expect("Failed to serialize log");

                                                    let mut redis_conn =
                                                        pool.get().await.expect("Redis conn");
                                                    let _: () = redis::cmd("LPUSH")
                                                        .arg("seckill:logs")
                                                        .arg(payload)
                                                        .query_async(&mut redis_conn)
                                                        .await
                                                        .expect("Failed to LPUSH log");
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "[Server] User {}: Lua script execution error: {}",
                                                req.user_id, e
                                            );
                                            fail_counter_inner.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                        }
                    }
                })
                .await;
        }));
    }

    // 定时状态简报任务
    {
        let request_counter = Arc::clone(&request_counter);
        let success_counter = Arc::clone(&success_counter);
        let timeout_counter = Arc::clone(&timeout_counter);
        let fail_counter = Arc::clone(&fail_counter);
        let redis_pool = Arc::clone(&redis_pool); // ✅ 加这个
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(args_status_interval)).await;

                // For status report, we might want to get total stock or specific item stocks
                // For now, let's just get stock for a default item (e.g., 1001) or sum up if needed.
                // This part needs a decision on how to display multi-item stock.
                // For simplicity, let's try to get stock for item 1001 if it exists.
                let left_stock = get_stock(&redis_pool, 1001).await; // Assuming item 1001 for status report
                let over = is_activity_over(&redis_pool).await;
                let total = request_counter.load(Ordering::Relaxed)
                    + timeout_counter.load(Ordering::Relaxed)
                    + fail_counter.load(Ordering::Relaxed);
                let succ = success_counter.load(Ordering::Relaxed);
                let timeout = timeout_counter.load(Ordering::Relaxed);
                let fail = fail_counter.load(Ordering::Relaxed);
                let now = SystemTime::now();
                let readable: DateTime<Utc> = now.into();
                println!(
                    "[状态简报] 时间: {} | 剩余库存: {:?} | 活动结束: {} | 成功: {} | 超时: {} | 失败: {} | 总: {} | 消费者数: {}",
                    readable, left_stock, over, succ, timeout, fail, total, args_consumers
                );
            }
        });
    }

    // Keep the existing Ctrl+C handling
    println!("Server 正在持续运行中，按 Ctrl+C 结束服务...");
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for event");
    println!("收到 Ctrl+C，服务关闭，等待所有 consumer 线程结束...");

    Ok(()) // MODIFIED: Added Ok(()) at the end
}
