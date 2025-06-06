// src/client_kafka.rs

use std::collections::HashMap;
use kafka_demo::common::*;
use clap::Parser;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use futures::future::join_all;
use rand::{rng, seq::SliceRandom, Rng};
use rand::seq::IndexedRandom;
use kafka_demo::args::ClientArgs;

#[allow(dead_code)]
#[tokio::main]
async fn main() {
    let mut args = ClientArgs::parse();

    // 合并 config.yaml
    let config_path = std::env::current_dir().unwrap().join("config.yaml");
    if config_path.exists() {
        if let Ok(file) = std::fs::File::open(config_path) {
            if let Ok(config) = serde_yaml::from_reader::<_, Config>(file) {
                if let Some(client_cfg) = config.client {
                    args.brokers = args.brokers.or(client_cfg.brokers);
                    args.topic = args.topic.or(client_cfg.topic);
                    args.user_count = args.user_count.or(client_cfg.user_count);
                    args.rate = args.rate.or(client_cfg.rate);
                    args.delay = args.delay.or(client_cfg.delay);
                }
            }
        }
    }

    // 最终值，带默认
    let brokers = args.brokers.expect("Missing --brokers or config.client.brokers");
    let topic = args.topic.expect("Missing --topic or config.client.topic");
    let user_count = args.user_count.expect("Missing --user-count or config.client.user_count");
    let rate = args.rate.unwrap_or(100);
    let delay = args.delay.unwrap_or(0);
    // 模拟数据
    let mut rng = rng();
    let item_ids = vec![1001, 1002, 1003, 1004, 1005];
    let min_count = 1;
    let max_count = 5;

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
    user_ids.shuffle(&mut rng);

    let mut futs = Vec::new();
    for user_id in user_ids {
        // 随机 item id + 数量
        let item_id = *item_ids.choose(&mut rng).unwrap();
        let count = rng.random_range(min_count..=max_count);
        let mut items = HashMap::new();
        items.insert(item_id, count);
        let req = SeckillRequest {
            user_id,
            items,
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