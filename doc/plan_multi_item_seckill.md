# 多商品秒杀功能实现计划

## 总体目标
修改 `src/server_kafka.rs` 和 `src/redis_state.rs`，以支持 `SeckillRequest` 中 `items` 字段的多个商品库存管理。

```mermaid
graph TD
    A[开始任务: 处理多商品秒杀] --> B{用户确认: 多商品 HashMap};

    B --> D[阶段 1: 更新 src/args.rs];
    D --> D1[修改 ServerArgs.stock 为 HashMap<usize, i64>];

    D1 --> E[阶段 2: 更新 src/common.rs];
    E --> E1[为 SeckillRecord 添加 item_id 和 quantity 字段];

    E1 --> F[阶段 3: 更新 src/redis_state.rs];
    F --> F1[将 STOCK_KEY 更改为 ITEM_STOCK_PREFIX (例如, "stock:ITEM_ID")];
    F --> F2[更新 do_reset_all, init_state, set_stock, sync_redis_from_db 以接受 HashMap<usize, i64> 作为初始库存];
    F --> F3[重写 run_seckill_script 以使用 Redis 事务/Lua 脚本实现原子性多商品库存扣减];
    F --> F4[更新 get_stock 以接受 item_id 并检索特定商品的库存];

    F4 --> G[阶段 4: 更新 src/server_kafka.rs];
    G --> G1[修改 main 函数以使用新的 ServerArgs.stock 类型并将其传递给 init_state];
    G --> G2[更新消费者循环，遍历 req.items 并调用新的多商品 run_seckill_script];
    G --> G3[调整日志记录和 SeckillRecord 创建，以反映单个商品的购买情况];
    G --> G4[调整状态报告以显示相关的库存信息（例如，所有商品的剩余总库存或按商品的库存）];

    G4 --> H[结束任务: 多商品秒杀功能实现];
```

## 详细步骤

**阶段 1: 更新 `src/args.rs`**
*   **操作:** 修改 `ServerArgs` 结构体，将 `stock` 字段的类型从 `Option<i64>` 更改为 `Option<std::collections::HashMap<usize, i64>>`。这将允许服务器配置多个不同商品的初始库存水平。
*   **原因:** 这是允许服务器从其配置中接受和管理多个商品库存的基础性更改。

**阶段 2: 更新 `src/common.rs`**
*   **操作:** 在 `SeckillRecord` 结构体中添加 `item_id: u64` 和 `quantity: u64` 字段。
*   **原因:** 这将使秒杀成功事件的日志记录更加精细，将其与特定商品和数量关联起来。

**阶段 3: 更新 `src/redis_state.rs`**
*   **操作:** 引入一个新的常量用于商品特定库存键，例如 `const ITEM_STOCK_PREFIX: &str = "stock:";`。
*   **操作:** 修改 `do_reset_all`、`init_state`、`set_stock` 和 `sync_redis_from_db` 函数。它们现在将接受一个 `HashMap<usize, i64>`，表示商品 ID 及其初始库存数量。这些函数将遍历该 Map 并设置单独的 Redis 键（例如，`stock:1001`，`stock:1002`）。
*   **操作:** 重写 `run_seckill_script` 函数及其关联的 Lua 脚本。新的 Lua 脚本将：
    *   接受商品 ID 列表和相应的数量列表作为参数。
    *   在单个 Redis 事务中（使用 `MULTI`/`EXEC` 或更高级的 Lua 脚本）原子性地检查并扣减所有请求商品的库存。
    *   如果任何商品缺货或活动结束，整个事务应失败。
    *   返回明确的成功或失败指示。
*   **操作:** 更新 `get_stock` 函数以接受 `item_id: usize` 并返回该特定商品的库存（例如，`conn.get(format!("{ITEM_STOCK_PREFIX}{item_id}"))`）。
*   **原因:** 此阶段对于使 Redis 后端能够管理和原子性地更新单个商品的库存水平至关重要。Lua 脚本对于在高并发环境中确保原子性是必不可少的。

**阶段 4: 更新 `src/server_kafka.rs`**
*   **操作:** 在 `main` 函数中，调整 `init_state` 调用以传递从 `args.stock` 获取的 `HashMap<usize, i64>`。
*   **操作:** 修改 `message_stream.for_each_concurrent` 块内的消息处理逻辑。不再是单个 `run_seckill_script` 调用，而是遍历 `req.items`。将整个 `req.items` HashMap 传递给更新后的 `run_seckill_script`。
*   **操作:** 根据多商品 `run_seckill_script` 的结果，更新成功/失败计数器。
*   **操作:** 在创建 `SeckillRecord` 时，根据请求中成功处理的商品填充新的 `item_id` 和 `quantity` 字段。如果一个请求涉及多个商品，需要做出决定：是为每个商品记录一条记录，还是聚合为包含商品列表的单个记录。为简单起见，我们可以为每个成功购买的商品记录一条记录。
*   **操作:** 调整状态报告（`[状态简报]`）以反映新的多商品库存管理。这可能涉及报告所有商品的剩余总库存，或最关键商品的摘要。
*   **原因:** 此阶段将新的 Redis 功能集成到服务器的请求处理流程中，确保传入的秒杀请求正确地与商品特定库存管理进行交互。