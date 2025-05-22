// src/server_kafka.rs
use clap::Parser;
use futures::StreamExt;
use kafka_demo::common::*;
use rand::{rng, Rng};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::ClientConfig;
use rdkafka::Message;
use std::sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as TokioMutex;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(long)]
    brokers: String,
    #[arg(long)]
    topic: String,
    #[arg(long)]
    stock: usize,
    #[arg(long)]
    timeout: u128,
    #[arg(long, default_value_t = 3)]
    consumers: usize,
    #[arg(long, default_value_t = 10)]
    status_interval: u64,
    #[arg(long, default_value_t = false)]
    reset_offset: bool,
    #[arg(long, default_value = "seckill_server_group")]
    group_id: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("启动 server，配置：{args:?}");

    let stock = Arc::new(TokioMutex::new(args.stock));
    let activity_over = Arc::new(AtomicBool::new(false));
    let request_counter = Arc::new(AtomicUsize::new(0));
    // 新增三个原子计数器
    let success_counter = Arc::new(AtomicUsize::new(0));
    let timeout_counter = Arc::new(AtomicUsize::new(0));
    let fail_counter = Arc::new(AtomicUsize::new(0));
    let mut rng_producer = rng();
    let delay_ms = rng_producer.random_range(5..=40);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &args.group_id)
        .set("bootstrap.servers", &args.brokers)
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        // 参数优化 ↓↓↓
        .set("fetch.max.bytes", "10485760") // 10MB
        .set("max.partition.fetch.bytes", "10485760")
        .set("queued.max.messages.kbytes", "1048576")
        .set("queued.min.messages", "10000")
        .set("max.poll.interval.ms", "60000")
        .set("session.timeout.ms", "20000")
        .set("receive.message.max.bytes", "16777216")
        .set("fetch.wait.max.ms", "500")
        .set("fetch.min.bytes", "1")
        .create()
        .expect("Consumer creation error");
    consumer.subscribe(&[&args.topic]).unwrap();
    // reset_offset 逻辑
    if args.reset_offset {
        use rdkafka::{consumer::Consumer, Offset};
        use rdkafka::TopicPartitionList;
        use std::time::Duration;
        let mut tpl = TopicPartitionList::new();
        let md = consumer.fetch_metadata(Some(&args.topic), Duration::from_secs(3)).expect("metadata");
        if let Some(topic_md) = md.topics().iter().find(|t| t.name() == args.topic) {
            for p in topic_md.partitions() {
                tpl.add_partition_offset(&args.topic, p.id(), Offset::End).unwrap();
            }
        }
        consumer.assign(&tpl).expect("assign partitions");
        println!("重置所有分区 offset 到最新成功，仅消费新消息！");
    }
    let consumer = Arc::new(consumer);

    let mut handles = vec![];
    for i in 0..args.consumers {
        let consumer_clone = Arc::clone(&consumer);
        let stock_clone = Arc::clone(&stock);
        let activity_over_clone = Arc::clone(&activity_over);
        let request_counter_clone = Arc::clone(&request_counter);
        let timeout_ms = args.timeout;
        let success_counter = Arc::clone(&success_counter);
        let timeout_counter = Arc::clone(&timeout_counter);
        let fail_counter = Arc::clone(&fail_counter);
        handles.push(tokio::spawn(async move {
            let message_stream = consumer_clone.stream();
            println!("Server Consumer #{i} 启动！");
            let concurrency = 32; // 并发处理数量，可调整
            message_stream.for_each_concurrent(concurrency, |result| async {
                if let Ok(msg) = result {
                    if let Some(payload) = msg.payload() {
                        if let Ok(req) = serde_json::from_slice::<SeckillRequest>(payload) {
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                            let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                            let cost_ms = now_ms.saturating_sub(req.request_initiation_time);
                            if activity_over_clone.load(Ordering::Relaxed) {
                                // println!("[Server] 活动结束，user{} 拒绝", req.user_id);
                                fail_counter.fetch_add(1, Ordering::Relaxed);
                            } else if cost_ms > timeout_ms {
                                // println!("[Server] 用户{} 超时", req.user_id);
                                timeout_counter.fetch_add(1, Ordering::Relaxed);
                            } else {
                                let mut stock_guard = stock_clone.lock().await;
                                if *stock_guard > 0 {
                                    *stock_guard -= 1;
                                    println!("[Server] 用户{} 抢购成功，剩余库存{}", req.user_id, *stock_guard);
                                    request_counter_clone.fetch_add(1, Ordering::Relaxed);
                                    success_counter.fetch_add(1, Ordering::Relaxed);
                                    if *stock_guard == 0 {
                                        activity_over_clone.store(true, Ordering::SeqCst);
                                        println!("*** 库存抢光，活动结束！");
                                    }
                                } else {
                                    activity_over_clone.store(true, Ordering::SeqCst);
                                    println!("[Server] 用户{} 已无库存", req.user_id);
                                    fail_counter.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                }
            }).await;
        }));
    }

    // 定时状态简报任务
    {
        let stock = Arc::clone(&stock);
        let activity_over = Arc::clone(&activity_over);
        let request_counter = Arc::clone(&request_counter);
        let success_counter = Arc::clone(&success_counter);
        let timeout_counter = Arc::clone(&timeout_counter);
        let fail_counter = Arc::clone(&fail_counter);
        let consumers = args.consumers;
        let status_interval = args.status_interval;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(status_interval)).await;
                let left_stock = *stock.lock().await;
                let over = activity_over.load(Ordering::Relaxed);
                let total = request_counter.load(Ordering::Relaxed)
                    + timeout_counter.load(Ordering::Relaxed)
                    + fail_counter.load(Ordering::Relaxed);
                let succ = success_counter.load(Ordering::Relaxed);
                let timeout = timeout_counter.load(Ordering::Relaxed);
                let fail = fail_counter.load(Ordering::Relaxed);
                println!(
                    "[状态简报] 时间: {:?} | 剩余库存: {} | 活动结束: {} | 成功: {} | 超时: {} | 失败: {} | 总: {} | 消费者数: {}",
                    std::time::SystemTime::now(),
                    left_stock, over, succ, timeout, fail, total, consumers
                );
            }
        });
    }

    println!("Server 正在持续运行中，按 Ctrl+C 结束服务...");
    tokio::signal::ctrl_c().await.expect("failed to listen for event");
    println!("收到 Ctrl+C，服务关闭，等待所有 consumer 线程结束...");
}