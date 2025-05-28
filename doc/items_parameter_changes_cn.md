# `items` 参数引入及代码适配中文文档

## 引言

本次代码改动引入了 `SeckillRequest` 结构体中的 `items` 参数，旨在支持用户在一次秒杀请求中购买多种商品及其对应的数量。这一改动使得秒杀系统能够处理更复杂的业务场景，从单一商品秒杀扩展到多商品组合秒杀。

## `items` 参数定义

在 `src/common.rs` 文件中，`SeckillRequest` 结构体新增了 `items` 字段：

```rust
// src/common.rs
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SeckillRequest {
    pub user_id: usize,
    pub items: HashMap<usize, usize>, // 新增字段：商品ID到数量的映射
    pub request_initiation_time: u128, // timestamp in ms
}
```

*   `items`: 这是一个 `HashMap<usize, usize>` 类型，其中 `key` 表示商品的唯一标识符（`item_id`），`value` 表示用户希望购买该商品的数量（`quantity`）。通过 `HashMap`，一个请求可以包含多个不同的商品及其购买数量。

## 相关结构体改动

为了更好地记录多商品秒杀的成功记录，`src/common.rs` 中的 `SeckillRecord` 结构体也进行了更新，新增了 `item_id` 和 `quantity` 字段：

```rust
// src/common.rs
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SeckillRecord {
    pub user_id: u64,
    pub activity_id: u64,
    pub item_id: u64,  // 新增字段：秒杀成功的商品ID
    pub quantity: u64, // 新增字段：秒杀成功的商品数量
    pub cost_ms: u128,
    pub status: String,
    pub timestamp: u64,
}
```

*   `item_id`: 记录本次秒杀成功涉及的具体商品 ID。
*   `quantity`: 记录本次秒杀成功涉及的具体商品数量。

## 客户端 (`src/client_kafka.rs`) 适配

客户端 (`src/client_kafka.rs`) 负责生成秒杀请求并发送到 Kafka。为了适配 `items` 参数，客户端的请求构建逻辑已更新：

```rust
// src/client_kafka.rs (部分代码)
// ...
let mut items = HashMap::new();
items.insert(item_id, count); // 将随机生成的商品ID和数量放入items HashMap
let req = SeckillRequest {
    user_id,
    items, // 包含items字段
    request_initiation_time: SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis(),
};
// ...
```

*   **请求构建**: 客户端现在在创建 `SeckillRequest` 实例时，会填充 `items` 字段。
*   **当前模拟**: 尽管 `items` 字段支持多商品，但当前客户端的模拟逻辑每次只随机选择一个 `item_id` 和 `count` 放入 `HashMap`。这意味着客户端目前模拟的是单商品秒杀请求，但其底层结构已为未来的多商品请求做好了准备。

## 服务器端 (`src/server_kafka.rs`) 适配

服务器端 (`src/server_kafka.rs`) 负责从 Kafka 消费秒杀请求，并进行业务处理。对 `items` 参数的适配主要体现在以下几个方面：

*   **请求反序列化**: 服务器端能够正确地将 Kafka 消息负载反序列化为包含 `items` 字段的 `SeckillRequest`。

    ```rust
    // src/server_kafka.rs (部分代码)
    if let Ok(req) = serde_json::from_slice::<SeckillRequest>(payload) {
        // ...
    }
    ```

*   **秒杀逻辑调用**: `items` 字段被正确地传递给核心的秒杀逻辑处理函数 `run_seckill_script`。

    ```rust
    // src/server_kafka.rs (部分代码)
    let script_execution_result = run_seckill_script(&pool, &req.items).await;
    ```

*   **成功记录写入**: 在秒杀成功的情况下，服务器端会遍历 `req.items` `HashMap` 中的每一个商品，为每个商品创建独立的 `SeckillRecord` 并将其推送到 Redis 的日志队列中。这确保了即使是多商品请求，每个商品的秒杀结果也能被单独记录。

    ```rust
    // src/server_kafka.rs (部分代码)
    for (item_id, quantity) in &req.items {
        let log = SeckillRecord {
            user_id: req.user_id as u64,
            activity_id: 1, // Assuming a default activity_id
            item_id: *item_id as u64, // 新增字段赋值
            quantity: *quantity as u64, // 新增字段赋值
            cost_ms,
            status: "success".into(),
            timestamp: now_ts,
        };
        // ... LPUSH log to Redis
    }
    ```

## Redis 状态管理 (`src/redis_state.rs`) 适配

`src/redis_state.rs` 模块负责与 Redis 交互，管理库存、活动状态等。为了支持 `items` 参数，该模块进行了以下关键改动：

*   **`run_seckill_script` 函数**: 这是核心的秒杀逻辑，通过执行 Lua 脚本在 Redis 中进行原子操作。该函数现在接受 `items: &HashMap<usize, usize>` 参数，并动态生成 Lua 脚本来处理多个商品的库存检查和扣减。

    ```rust
    // src/redis_state.rs (部分代码)
    pub async fn run_seckill_script(
        pool: &Arc<Pool>,
        items: &HashMap<usize, usize>, // 接受多商品参数
    ) -> Result<i64, redis::RedisError> {
        let mut lua_script = String::from(
            r#"
            if redis.call("GET", KEYS[1]) == "1" then return -1 end -- ACTIVITY_OVER_KEY
        "#,
        );

        let mut keys = vec![ACTIVITY_OVER_KEY.to_string()];
        let mut arg_index = 1;

        // 动态生成库存检查逻辑
        for (item_id, quantity) in items {
            let item_stock_key = format!("{ITEM_STOCK_PREFIX}{}", item_id);
            keys.push(item_stock_key.clone());
            arg_index += 1;
            lua_script.push_str(&format!(
                r#"
                local stock_{item_id} = tonumber(redis.call("GET", KEYS[{arg_index}]))
                if stock_{item_id} < {quantity} then
                    return -2 -- Not enough stock for item {item_id}
                end
            "#
            ));
        }

        lua_script.push_str(
            r#"
            -- If all checks pass, decrement stocks and increment success counter
            local total_decremented = 0
        "#,
        );

        // 动态生成库存扣减逻辑
        for (item_id, quantity) in items {
            let item_stock_key = format!("{ITEM_STOCK_PREFIX}{}", item_id);
            let key_idx = keys.iter().position(|k| k == &item_stock_key).unwrap() + 1;
            lua_script.push_str(&format!(
                r#"
                redis.call("DECRBY", KEYS[{key_idx}], {quantity})
                total_decremented = total_decremented + {quantity}
            "#
            ));
        }

        keys.push(STATS_SUCCESS_KEY.to_string());
        let stats_success_key_idx = keys.len();
        lua_script.push_str(&format!(
            r#"
            redis.call("INCRBY", KEYS[{stats_success_key_idx}], total_decremented)
            return 1 -- Success
        "#
        ));

        // ... Lua 脚本执行
    }
    ```

    *   **库存检查**: Lua 脚本在扣减库存前，会遍历 `items` 中的每个商品，检查其当前库存是否充足。如果任何一个商品的库存不足，则立即返回 `-2`。
    *   **库存扣减**: 如果所有商品的库存都充足，Lua 脚本会原子性地扣减 `items` 中所有商品的库存。
    *   **成功计数**: 成功扣减库存后，`STATS_SUCCESS_KEY` 会根据总扣减数量进行累加。

*   **库存初始化和设置函数**: `do_reset_all`、`init_state`、`set_stock` 和 `sync_redis_from_db` 等函数都已更新，接受 `HashMap<usize, i64>` 类型的 `stocks` 参数，从而支持对多个商品进行初始化和重置库存。

    ```rust
    // src/redis_state.rs (部分代码)
    async fn do_reset_all(
        conn: &mut Connection,
        stocks: &HashMap<usize, i64>, // 接受HashMap
        reset_stats: bool,
    ) -> redis::RedisResult<()> {
        let mut pipe = redis::pipe();
        for (item_id, &stock) in stocks {
            pipe.cmd("SET")
                .arg(format!("{ITEM_STOCK_PREFIX}{}", item_id))
                .arg(stock);
        }
        // ...
    }
    ```

*   **`get_stock` 函数**: 现在可以根据 `item_id` 查询特定商品的库存。

    ```rust
    // src/redis_state.rs (部分代码)
    pub async fn get_stock(pool: &Arc<Pool>, item_id: usize) -> Option<i64> {
        // ...
        conn.get(format!("{ITEM_STOCK_PREFIX}{}", item_id))
            .await
            .ok()
    }
    ```

## 系统影响

引入 `items` 参数并进行相应代码适配后，秒杀系统具备了以下能力：

*   **支持多商品秒杀**: 用户可以在一次请求中同时购买多种商品，极大地提升了业务灵活性。
*   **精细化库存管理**: Redis 中现在可以为每个商品维护独立的库存，实现了更精细的库存控制。
*   **原子性操作**: `run_seckill_script` 中的 Lua 脚本确保了多商品库存检查和扣减的原子性，避免了并发问题。
*   **详细日志记录**: `SeckillRecord` 的更新和服务器端对 `items` 的遍历记录，使得秒杀成功日志能够精确到每个商品及其数量。

这些改动为秒杀系统提供了更强大的功能和更好的扩展性，能够应对更复杂的秒杀场景。