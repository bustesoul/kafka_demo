// src/client_kafka.rs
use kafka_demo::common::*;
use clap::Parser;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use futures::future::join_all;
use rand::rng;
use rand::seq::SliceRandom;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(long)]
    brokers: String,
    #[arg(long)]
    topic: String,
    #[arg(long)]
    user_count: usize,
    #[arg(long, default_value_t = 100)]
    rate: usize, // 每秒最大并发数
    #[arg(long, default_value_t = 0)]
    delay: u64, // 每条消息间隔ms
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("启动 client，配置：{args:?}");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &args.brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let mut user_ids: Vec<usize> = (1..=args.user_count).collect();
    user_ids.shuffle(&mut rng());

    let mut futs = Vec::new();
    for user_id in user_ids {
        let req = SeckillRequest {
            user_id,
            request_initiation_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis(),
        };
        let payload = serde_json::to_vec(&req).unwrap();
        let topic = args.topic.clone();
        let producer = producer.clone();
        futs.push(tokio::spawn(async move {
            let record: FutureRecord<'_, (), [u8]> = FutureRecord::to(&topic).payload(&payload);
            let _ = producer.send(record, Duration::from_secs(3)).await;
        }));
        if args.delay > 0 {
            tokio::time::sleep(Duration::from_millis(args.delay)).await;
        }
        if user_id % args.rate == 0 {
            // 简单限流：每rate个用户sleep一秒
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    join_all(futs).await;
    println!("client 全部用户请求已发送。");
}