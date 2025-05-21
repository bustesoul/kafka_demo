use futures::StreamExt;
use rand::{rng, seq::SliceRandom, Rng};
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use rdkafka::Message;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex};
use std::thread;
use std::time::SystemTime;
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::sync::Mutex as TokioMutex;
use crate::QueueBackend::Simulate;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SeckillRequest {
    user_id: usize,
    request_initiation_time: u128, // 用u128存timestamp，Kafka不能用Instant
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SeckillResult {
    Success { user_id: usize, cost_ms: u128 },
    Fail { user_id: usize, cost_ms: u128, reason: String },
    PendingTimeout { user_id: usize, cost_ms: u128 },
}

#[derive(Debug, Clone)]
enum QueueBackend {
    Simulate, // 内存队列
    Kafka {
        // 这里可扩展Kafka相关配置，如brokers、topic等
        brokers: String,
        topic: String,
        // 可根据实际需求添加认证、分区等配置项
    },
}

#[derive(Clone, Debug)]
struct SeckillConfig {
    user_count: usize,
    initial_stock: usize,
    consumer_speeds: Vec<u64>,
    timeout_ms: u128,
    queue_backend: QueueBackend,
}

fn kafka_consumer(
    queue: Arc<Mutex<VecDeque<SeckillRequest>>>,
    stock: Arc<Mutex<usize>>,
    results: Arc<Mutex<Vec<SeckillResult>>>,
    speed_factor: u64,
    timeout_ms: u128,
    producing_active: Arc<AtomicBool>,
    activity_officially_over: Arc<AtomicBool>,
) {
    let mut rng = rng();
    loop {
        let req_opt = {
            let mut q = queue.lock().unwrap();
            q.pop_front()
        };

        if let Some(req) = req_opt {
            // 检查活动是否已全局标记为结束
            if activity_officially_over.load(Ordering::Relaxed) {
                // 活动已结束，快速处理路径
                let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                let cost_ms = now_ms.saturating_sub(req.request_initiation_time);
                if cost_ms > timeout_ms {
                    results.lock().unwrap().push(SeckillResult::PendingTimeout {
                        user_id: req.user_id,
                        cost_ms,
                    });
                } else {
                    results.lock().unwrap().push(SeckillResult::Fail {
                        user_id: req.user_id,
                        cost_ms,
                        reason: "活动已结束 (快速通道拒绝)".into(),
                    });
                }
            } else {
                // 活动尚未标记为结束（或当前线程未知），进行正常处理模拟
                thread::sleep(Duration::from_millis(rng.random_range(5..=speed_factor)));
                let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                let cost_ms = now_ms.saturating_sub(req.request_initiation_time);

                // 1. 首先检查是否处理超时 (这是高并发下最先可能发生的情况)
                if cost_ms > timeout_ms {
                    results.lock().unwrap().push(SeckillResult::PendingTimeout {
                        user_id: req.user_id,
                        cost_ms,
                    });
                }
                // 2. 如果没超时，再检查活动是否在此期间被其他线程标记为结束
                else if activity_officially_over.load(Ordering::Relaxed) {
                    results.lock().unwrap().push(SeckillResult::Fail {
                        user_id: req.user_id,
                        cost_ms,
                        reason: "活动已结束 (处理期间标记更新)".into(),
                    });
                }
                // 3. 如果活动仍未结束且未超时，则尝试扣减库存
                else {
                    let mut s_guard = stock.lock().unwrap();
                    if *s_guard > 0 {
                        *s_guard -= 1;
                        let stock_after_grab = *s_guard;
                        drop(s_guard); // 尽快释放锁

                        if stock_after_grab == 0 {
                            // 当前线程抢到了最后一件商品
                            if !activity_officially_over.swap(true, Ordering::SeqCst) {
                                println!("\n>>> 库存已由 User{} 抢光! 系统将通知后续用户活动已结束. <<<\n", req.user_id);
                            }
                        }
                        results.lock().unwrap().push(SeckillResult::Success {
                            user_id: req.user_id,
                            cost_ms,
                        });
                    } else {
                        // 获取锁后发现库存为0 (可能在检查 activity_officially_over 后，其他线程抢光了)
                        drop(s_guard); // 释放锁
                        // 确保活动结束标志被设置
                        if !activity_officially_over.swap(true, Ordering::SeqCst) {
                            println!("\n>>> User{} 发现库存为0 (尝试获取锁后)! 系统将通知后续用户活动已结束. <<<\n", req.user_id);
                        }
                        results.lock().unwrap().push(SeckillResult::Fail {
                            user_id: req.user_id,
                            cost_ms,
                            reason: "已无库存 (获取锁后发现)".into(),
                        });
                    }
                }
            }
        } else { // 队列为空
            if !producing_active.load(Ordering::Relaxed) && queue.lock().unwrap().is_empty() {
                break; // 生产者已停止且队列为空，消费者退出
            } else {
                thread::sleep(Duration::from_millis(10)); // 稍作等待，避免忙轮询
            }
        }
    }
}

fn kafka_produce(queue: &Arc<Mutex<VecDeque<SeckillRequest>>>, user_id: usize, request_initiation_time: u128) {
    let mut q = queue.lock().unwrap();
    q.push_back(SeckillRequest { user_id, request_initiation_time });
}

fn enqueue_requests(
    user_count: usize,
    queue: &Arc<Mutex<VecDeque<SeckillRequest>>>,
    producing_active: Arc<AtomicBool>,
) {
    let mut user_ids: Vec<usize> = (1..=user_count).collect();
    let mut main_rng = rng();
    user_ids.shuffle(&mut main_rng);

    let early_bird_ratio = 0.15;
    let early_bird_count = ((user_count as f64) * early_bird_ratio).round() as usize;

    let mut handles = vec![];

    println!("开始生成 {} 个早鸟用户请求 (模拟瞬时高峰)...", early_bird_count);
    for i in 0..early_bird_count {
        let user_id = user_ids[i];
        let queue_clone = Arc::clone(queue);
        let handle = thread::spawn(move || {
            let mut rng_producer = rng();
            let delay_ms = rng_producer.random_range(0..=5);
            thread::sleep(Duration::from_millis(delay_ms));
            let request_initiation_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
            kafka_produce(&queue_clone, user_id, request_initiation_time);
        });
        handles.push(handle);
    }

    for handle in handles.drain(..) { // 使用 drain 来消耗 handles
        handle.join().unwrap();
    }
    println!("{} 个早鸟用户请求已发送至队列。", early_bird_count);

    let pause_duration = Duration::from_secs(5);
    println!("模拟活动进行中，{} 秒后后续用户开始进入...", pause_duration.as_secs());
    thread::sleep(pause_duration);

    let late_comer_count = user_count - early_bird_count;
    if late_comer_count > 0 {
        handles.clear(); // 清空 handles 以便复用
        println!("开始生成 {} 个后续用户请求...", late_comer_count);
        for i in early_bird_count..user_count {
            let user_id = user_ids[i];
            let queue_clone = Arc::clone(queue);
            let handle = thread::spawn(move || {
                let mut rng_producer = rng();
                let delay_ms = rng_producer.random_range(10..=100);
                thread::sleep(Duration::from_millis(delay_ms));
                let request_initiation_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                kafka_produce(&queue_clone, user_id, request_initiation_time);
            });
            handles.push(handle);
        }

        for handle in handles { // 直接消耗
            handle.join().unwrap();
        }
        println!("{} 个后续用户请求已发送至队列。", late_comer_count);
    }

    producing_active.store(false, Ordering::SeqCst);
    println!("所有用户请求已发送至队列，生产者停止。");
}


fn start_consumers(
    queue: Arc<Mutex<VecDeque<SeckillRequest>>>,
    stock: Arc<Mutex<usize>>,
    results: Arc<Mutex<Vec<SeckillResult>>>,
    consumer_speeds: &[u64],
    timeout_ms: u128,
    producing_active: Arc<AtomicBool>,
    activity_officially_over: Arc<AtomicBool>,
) -> Vec<thread::JoinHandle<()>> {
    let mut handles = vec![];
    for (i, &speed) in consumer_speeds.iter().enumerate() {
        let queue_clone = Arc::clone(&queue);
        let stock_clone = Arc::clone(&stock);
        let results_clone = Arc::clone(&results);
        let producing_active_clone = Arc::clone(&producing_active);
        let activity_over_clone = Arc::clone(&activity_officially_over);
        let handle = thread::spawn(move || {
            println!("消费者 #{} (速度上限 {}ms) 启动...", i + 1, speed);
            kafka_consumer(
                queue_clone,
                stock_clone,
                results_clone,
                speed,
                timeout_ms,
                producing_active_clone,
                activity_over_clone,
            );
            println!("消费者 #{} (速度上限 {}ms) 退出.", i + 1, speed);
        });
        handles.push(handle);
    }
    handles
}

fn print_and_stat_results_with_timeout(results_arc: &Arc<Mutex<Vec<SeckillResult>>>, _timeout_val_param: u128) {
    let mut stats = (0, 0, 0); // success, fail, timeout
    let mut success_users = Vec::new();
    let mut all_results = results_arc.lock().unwrap().clone();

    // 按用户ID排序，方便查看特定用户的最终结果 (如果需要详细打印)
    all_results.sort_by_key(|r| match r {
        SeckillResult::Success { user_id, .. } => *user_id,
        SeckillResult::Fail { user_id, .. } => *user_id,
        SeckillResult::PendingTimeout { user_id, .. } => *user_id,
    });

    println!("\n----- 抢购结果 -----");
    for r in &all_results {
        match r {
            SeckillResult::Success { user_id, cost_ms } => {
                stats.0 += 1;
                success_users.push(*user_id);
                // 详细打印 (可选)
                println!("[成功] User{:04} 用时{:4}ms", user_id, cost_ms);
            }
            SeckillResult::Fail { user_id: _, cost_ms: _, reason: _ } => {
                stats.1 += 1;
                // 详细打印 (可选)
                // println!("[失败] User{:04} 用时{:4}ms，原因：{}", user_id, cost_ms, reason);
            }
            SeckillResult::PendingTimeout { user_id: _, cost_ms: _ } => {
                stats.2 += 1;
                // 详细打印 (可选) - 注意这里如果使用 timeout_val_param 需要去掉下划线
                // println!("[处理超时] User{:04} 用时{:4}ms (队列等待+处理超过阈值 {}ms)", user_id, cost_ms, timeout_val_param);
            }
        }
    }
    println!("----- 总结 -----");
    let total_requests = all_results.len();
    println!("总请求数: {}", total_requests);
    println!("成功: {}, 失败(含活动结束/无库存): {}, 处理超时: {}", stats.0, stats.1, stats.2);

    // 校验统计数量
    if stats.0 + stats.1 + stats.2 != total_requests {
        eprintln!("警告: 结果统计数量 ({}) 与总请求数 ({}) 不匹配!", stats.0 + stats.1 + stats.2, total_requests);
    }

    // 打印成功用户列表 (可选)
    // if !success_users.is_empty() {
    //     success_users.sort();
    //     println!("成功抢到用户ID: {:?}", success_users);
    // }
}

fn run_simulate_backend(config: &SeckillConfig) {
    let stock = Arc::new(Mutex::new(config.initial_stock));
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let results = Arc::new(Mutex::new(Vec::new()));
    let producing_active = Arc::new(AtomicBool::new(true)); // 生产者是否还在生成请求
    let activity_officially_over = Arc::new(AtomicBool::new(false)); // 活动是否已正式结束 (库存为0)

    println!(
        "模拟开始：{} 用户，{} 库存，{} 消费者 (各速度上限 {:?}ms)，超时 {}ms. 活动结束条件：库存为0.",
        config.user_count, config.initial_stock, config.consumer_speeds.len(), config.consumer_speeds, config.timeout_ms
    );

    // 启动消费者线程
    let consumer_handles = start_consumers(
        Arc::clone(&queue),
        Arc::clone(&stock),
        Arc::clone(&results),
        &config.consumer_speeds,
        config.timeout_ms,
        Arc::clone(&producing_active),
        Arc::clone(&activity_officially_over),
    );

    // 启动生产者线程 (模拟用户请求入队)
    let producer_handle = thread::spawn({
        let queue_clone = Arc::clone(&queue);
        let producing_active_clone = Arc::clone(&producing_active);
        let user_count = config.user_count;
        move || {
            enqueue_requests(user_count, &queue_clone, producing_active_clone);
        }
    });

    // 等待生产者完成所有请求的入队
    producer_handle.join().unwrap();
    println!("所有生产者任务完成 (请求已入队)。等待消费者处理剩余队列...");

    // 等待所有消费者线程处理完毕并退出
    for handle in consumer_handles {
        handle.join().unwrap();
    }
    println!("所有消费者已退出。");

    // 打印并统计最终结果
    print_and_stat_results_with_timeout(&results, config.timeout_ms);

    let final_stock = *stock.lock().unwrap();
    println!("最终剩余库存: {}", final_stock);
    let successful_grabs = results.lock().unwrap().iter().filter(|r| matches!(r, SeckillResult::Success {..})).count();

    // 断言检查
    if config.initial_stock > 0 && final_stock == 0 { // 如果初始有库存且最终库存为0
        assert_eq!(successful_grabs, config.initial_stock, "成功抢购数 ({}) 与初始库存消耗 ({}) 不匹配!", successful_grabs, config.initial_stock);
    } else { // 其他情况 (如初始库存为0，或未抢光)
        assert_eq!(config.initial_stock.saturating_sub(successful_grabs), final_stock, "库存计算不一致！");
    }

    if final_stock == 0 && config.initial_stock > 0 {
        assert!(activity_officially_over.load(Ordering::Relaxed), "库存为0但活动结束标志未设置!");
    }
    println!("模拟结束。");
}


async fn kafka_produce_kafka(
    producer: &FutureProducer,
    topic: &str,
    req: &SeckillRequest,
) {
    let payload = serde_json::to_vec(req).unwrap();
    let record: FutureRecord<'_, (), [u8]> = FutureRecord::to(topic).payload(&payload);
    // .key(Some(&req.user_id.to_string()))
    let send_res = producer.send(record, Duration::from_secs(5)).await;
    match send_res {
        Ok(delivery) => {
            // 这里可以只打印部分用户，比如每100条
            if req.user_id % 500 == 0 {
                println!("[Kafka Produce] 成功发送User{}消息到Kafka", req.user_id);
            }
        }
        Err(e) => {
            println!("[Kafka Produce] 发送消息失败: User{} 错误: {:?}", req.user_id, e);
        }
    }
}

async fn kafka_consumer_kafka(
    consumer: std::sync::Arc<StreamConsumer>,
    config: SeckillConfig,
    result_sender: tokio::sync::mpsc::UnboundedSender<SeckillResult>,
    stock: Arc<tokio::sync::Mutex<usize>>,
    activity_over: Arc<AtomicBool>,
) {
    let mut message_stream = consumer.stream();
    let mut msg_count = 0usize;
    println!("[Kafka Consumer] 消费者启动, 等待消息...");

    while let Some(Ok(msg)) = message_stream.next().await {
        msg_count += 1;
        if msg_count % 100 == 0 {
            println!("[Kafka Consumer] 已处理{}条消息", msg_count);
        }
        if let Some(payload) = msg.payload() {
            if let Ok(req) = serde_json::from_slice::<SeckillRequest>(payload) {
                let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                let cost_ms = now_ms.saturating_sub(req.request_initiation_time);
                // 下面保持原逻辑，可补充细致日志
                if activity_over.load(Ordering::Relaxed) {
                    if cost_ms > config.timeout_ms {
                        let _ = result_sender.send(SeckillResult::PendingTimeout {
                            user_id: req.user_id,
                            cost_ms,
                        });
                        println!("[Kafka Consumer] 用户{} 消费超时", req.user_id);
                    } else {
                        let _ = result_sender.send(SeckillResult::Fail {
                            user_id: req.user_id,
                            cost_ms,
                            reason: "活动已结束 (Kafka快速拒绝)".to_string(),
                        });
                        // println!("[Kafka Consumer] 用户{} 活动已结束(快速拒绝)", req.user_id);
                    }
                } else {
                    let mut stock_guard = stock.lock().await;
                    if *stock_guard > 0 {
                        *stock_guard -= 1;
                        if *stock_guard == 0 {
                            activity_over.store(true, Ordering::SeqCst);
                            println!("[Kafka Consumer] 用户{} 抢到最后一件库存！活动标记结束", req.user_id);
                        }
                        drop(stock_guard);
                        let _ = result_sender.send(SeckillResult::Success {
                            user_id: req.user_id,
                            cost_ms,
                        });
                        println!("[Kafka Consumer] 用户{} 抢购成功", req.user_id);
                    } else {
                        activity_over.store(true, Ordering::SeqCst);
                        let _ = result_sender.send(SeckillResult::Fail {
                            user_id: req.user_id,
                            cost_ms,
                            reason: "已无库存 (Kafka分布式锁后)".to_string(),
                        });
                        println!("[Kafka Consumer] 用户{} 已无库存", req.user_id);
                    }
                }
            } else {
                println!("[Kafka Consumer] 反序列化消息失败");
            }
        } else {
            println!("[Kafka Consumer] 收到无payload消息");
        }
        if activity_over.load(Ordering::Relaxed) && *stock.lock().await == 0 {
            println!("[Kafka Consumer] 活动已结束且库存为0，消费者主动退出。");
            break;
        }
    }
    println!("[Kafka Consumer] 消费者退出，消息流关闭，已处理{}条消息", msg_count);
}

async fn run_kafka_backend(config: &SeckillConfig) {
    let (brokers, topic) = match &config.queue_backend {
        QueueBackend::Kafka { brokers, topic, .. } => (brokers.as_str(), topic.as_str()),
        _ => unreachable!(),
    };
    println!("[Kafka] 开始创建Producer, brokers={}, topic={}", brokers, topic);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");
    println!("[Kafka] Producer创建成功");

    println!("[Kafka] 开始创建Consumer");
    let group_id = "seckill_consumer_group";
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation error");
    consumer.subscribe(&[topic]).unwrap();
    let consumer = Arc::new(consumer);
    println!("[Kafka] Consumer创建并订阅成功，启动消费者任务");

    // 并发环境下的库存和结束标记
    let stock = Arc::new(TokioMutex::new(config.initial_stock));
    let activity_over = Arc::new(AtomicBool::new(false));

    // 用mpsc传递结果
    let (result_sender, mut result_receiver) = mpsc::unbounded_channel();

    // 启动消费者任务
    let mut consumer_handles = vec![];
    for i in 0..config.consumer_speeds.len() {
        println!("[Kafka] 启动第{}个消费者任务", i + 1);
        let consumer_clone = Arc::clone(&consumer);
        let stock_clone = stock.clone();
        let activity_over_clone = activity_over.clone();
        let sender_clone = result_sender.clone();
        let config_clone = config.clone();
        consumer_handles.push(tokio::spawn(kafka_consumer_kafka(
            consumer_clone,
            config_clone,
            sender_clone,
            stock_clone,
            activity_over_clone,
        )));
    }

    println!("[Kafka] 所有消费者任务已启动, 开始批量生产消息...");
    // 生产者批量发消息
    for user_id in 1..=config.user_count {
        let req = SeckillRequest {
            user_id,
            request_initiation_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis(),
        };
        if user_id % 300 == 0 {
            println!("[Kafka] 已生产{}条请求", user_id);
        }
        kafka_produce_kafka(&producer, topic, &req).await;
    }
    println!("[Kafka] 生产者所有消息已发送, 关闭result_sender");

    drop(result_sender); // 通知消费者：不会再有结果发进来了

    println!("[Kafka] 开始接收消费者处理结果...");
    // 收集结果
    let mut all_results = Vec::new();
    while let Some(r) = result_receiver.recv().await {
        if all_results.len() % 100 == 0 {
            println!("[Kafka] 已接收到{}条消费结果", all_results.len());
        }
        all_results.push(r);
    }
    println!("[Kafka] Kafka流程完成，总处理请求数：{}", all_results.len());

    // 等待所有 Kafka consumer 任务退出，防止 runtime 挂起
    // Stop the consumer
    for handle in consumer_handles {
        let _ = handle.await;
    }
    println!("[Kafka] 所有Kafka消费者任务已退出");
}

fn run_seckill_simulation(config: SeckillConfig) {
    match &config.queue_backend {
        QueueBackend::Simulate => {
            run_simulate_backend(&config);
        }
        QueueBackend::Kafka { .. } => {
            run_kafka_backend(&config);
        }
    }
}

#[tokio::main]
async fn main() {
    let config = SeckillConfig {
        user_count: 2000,
        initial_stock: 20,
        consumer_speeds: vec![30, 35, 40, 45, 50],
        timeout_ms: 110,
        queue_backend: QueueBackend::Kafka {
            brokers: "172.20.19.27:9092".to_string(),
            topic: "seckill_test".to_string(),
        },
    };
    println!("[MAIN] 启动秒杀系统，配置: {:?}", config);
    match &config.queue_backend {
        QueueBackend::Simulate => {
            println!("[MAIN] 选择内存模拟后端，准备运行模拟流程...");
            run_simulate_backend(&config)
        }
        QueueBackend::Kafka { .. } => {
            println!("[MAIN] 选择Kafka后端，准备连接Kafka并运行流程...");
            run_kafka_backend(&config).await
        }
    }
    println!("[MAIN] 秒杀系统流程主入口退出。");
}