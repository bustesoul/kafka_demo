// src/client_kafka.rs

use kafka_demo::common::*;
use clap::Parser;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use futures::future::join_all;
use rand::rng;
use rand::seq::SliceRandom;
use serde::Deserialize;

#[derive(Parser, Debug, Clone, Deserialize)]
#[serde(default)]
#[command(author, version, about)]
struct Args {
    #[arg(long)]
    brokers: Option<String>,
    #[arg(long)]
    topic: Option<String>,
    #[arg(long)]
    user_count: Option<usize>,
    #[arg(long)]
    rate: Option<usize>, // 每秒最大并发数
    #[arg(long)]
    delay: Option<u64>, // 每条消息间隔ms
}

impl Default for Args {
    fn default() -> Self {
        Self {
            brokers: None,
            topic: None,
            user_count: None,
            rate: None,
            delay: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct Config {
    client: Args,
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
                args.brokers = args.brokers.or(config.client.brokers);
                args.topic = args.topic.or(config.client.topic);
                args.user_count = args.user_count.or(config.client.user_count);
                args.rate = args.rate.or(config.client.rate);
                args.delay = args.delay.or(config.client.delay);
            }
        }
    }

    // 最终值，带默认
    let brokers = args.brokers.expect("Missing --brokers or config.client.brokers");
    let topic = args.topic.expect("Missing --topic or config.client.topic");
    let user_count = args.user_count.expect("Missing --user-count or config.client.user_count");
    let rate = args.rate.unwrap_or(100);
    let delay = args.delay.unwrap_or(0);

    println!(
        "启动 client，配置：brokers={}, topic={}, user_count={}, rate={}, delay={}",
        brokers, topic, user_count, rate, delay
    );

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let mut user_ids: Vec<usize> = (1..=user_count).collect();
    user_ids.shuffle(&mut rng());

    let mut futs = Vec::new();
    for user_id in user_ids {
        let req = SeckillRequest {
            user_id,
            request_initiation_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        };
        let payload = serde_json::to_vec(&req).unwrap();
        let topic = topic.clone();
        let producer = producer.clone();
        futs.push(tokio::spawn(async move {
            let record: FutureRecord<'_, (), [u8]> = FutureRecord::to(&topic).payload(&payload);
            let _ = producer.send(record, Duration::from_secs(3)).await;
        }));
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        if user_id % rate == 0 {
            // 简单限流
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    join_all(futs).await;
    println!("client 全部用户请求已发送。");
}