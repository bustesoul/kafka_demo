// src/redis_state.rs

use deadpool_redis::Connection;
use deadpool_redis::Pool;
use redis::{AsyncCommands, Script};
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;

const ITEM_STOCK_PREFIX: &str = "stock:"; // New constant for item-specific stock keys
const ACTIVITY_OVER_KEY: &str = "activity_over";
const STATS_SUCCESS_KEY: &str = "stats:success";
const VERSION_KEY: &str = "stock_version";
const CONTROL_CHANNEL: &str = "control_channel";

// 统一重置库存、活动状态、版本号和 stats
async fn do_reset_all(
    conn: &mut Connection,
    stocks: &HashMap<usize, i64>, // Changed to HashMap
    reset_stats: bool,
) -> redis::RedisResult<()> {
    let mut pipe = redis::pipe();
    for (item_id, &stock) in stocks {
        pipe.cmd("SET")
            .arg(format!("{ITEM_STOCK_PREFIX}{}", item_id))
            .arg(stock);
    }
    pipe.cmd("SET").arg(ACTIVITY_OVER_KEY).arg("0");
    pipe.cmd("INCR").arg(VERSION_KEY);
    if reset_stats {
        pipe.cmd("SET").arg(STATS_SUCCESS_KEY).arg(0);
    }
    pipe.query_async(conn).await
}

// 初始化：全字段重置（只用一次，通常用于冷启动）
pub async fn init_state(pool: &Arc<Pool>, stocks: HashMap<usize, i64>) {
    // Changed to HashMap
    if let Ok(mut conn) = pool.get().await {
        let _ = do_reset_all(&mut conn, &stocks, true).await;
    }
}

// 控制面：库存重置+通知
pub async fn set_stock(pool: &Arc<Pool>, stocks: HashMap<usize, i64>) -> bool {
    // Changed to HashMap
    if let Ok(mut conn) = pool.get().await {
        let ok = do_reset_all(&mut conn, &stocks, true).await.is_ok();
        if ok {
            let _: Result<(), _> = conn
                .publish(
                    CONTROL_CHANNEL,
                    serde_json::to_string(&serde_json::json!({"cmd":"set_stock","value": stocks}))
                        .unwrap(),
                )
                .await;
        }
        ok
    } else {
        false
    }
}

// 数据库同步库存+通知
pub async fn sync_redis_from_db(pool: &Arc<Pool>, stocks: HashMap<usize, i64>) -> bool {
    // Changed to HashMap
    if let Ok(mut conn) = pool.get().await {
        let ok = do_reset_all(&mut conn, &stocks, true).await.is_ok();
        if ok {
            let _: Result<(), _> = conn
                .publish(
                    CONTROL_CHANNEL,
                    serde_json::to_string(
                        &serde_json::json!({"cmd":"sync_from_db","value": stocks}),
                    )
                    .unwrap(),
                )
                .await;
        }
        ok
    } else {
        false
    }
}

// 只设置活动结束，不重置其它
pub async fn set_activity_finish(pool: &Arc<Pool>) -> bool {
    if let Ok(mut conn) = pool.get().await {
        let set_ok = conn
            .set::<&str, &str, ()>(ACTIVITY_OVER_KEY, "1")
            .await
            .is_ok();
        if set_ok {
            let _: Result<(), _> = conn.publish(CONTROL_CHANNEL, r#"{"cmd":"finish"}"#).await;
        }
        set_ok
    } else {
        false
    }
}

// 查询库存
pub async fn get_stock(pool: &Arc<Pool>, item_id: usize) -> Option<i64> {
    // Added item_id
    if let Ok(mut conn) = pool.get().await {
        conn.get(format!("{ITEM_STOCK_PREFIX}{}", item_id))
            .await
            .ok()
    } else {
        None
    }
}

// 查询活动是否结束
pub async fn is_activity_over(pool: &Arc<Pool>) -> bool {
    if let Ok(mut conn) = pool.get().await {
        conn.get::<_, String>(ACTIVITY_OVER_KEY)
            .await
            .ok()
            .map_or(false, |v| v == "1")
    } else {
        false
    }
}

// 运行秒杀脚本（减库存、判定活动状态、统计成功数）
pub async fn run_seckill_script(
    pool: &Arc<Pool>,
    items: &HashMap<usize, usize>,
) -> Result<i64, redis::RedisError> {
    // Added items parameter
    let mut lua_script = String::from(
        r#"
        if redis.call("GET", KEYS[1]) == "1" then return -1 end -- ACTIVITY_OVER_KEY
    "#,
    );

    let mut keys = vec![ACTIVITY_OVER_KEY.to_string()];
    let mut arg_index = 1;

    for (item_id, quantity) in items {
        let item_stock_key = format!("{ITEM_STOCK_PREFIX}{}", item_id);
        keys.push(item_stock_key.clone());
        arg_index += 1;
        lua_script.push_str(&format!(
            r#"
            local stock_{item_id} = tonumber(redis.call("GET", KEYS[{arg_index}])) or 0
            if stock_{item_id} < {quantity} then
                return -2 -- Not enough stock for item {item_id}
            end
        "#,
        ));
    }

    lua_script.push_str(
        r#"
        -- If all checks pass, decrement stocks and increment success counter
        local total_decremented = 0
    "#,
    );

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

    let mut conn = pool.get().await.map_err(|e| {
        redis::RedisError::from((
            redis::ErrorKind::IoError,
            "deadpool get() failed",
            format!("{:?}", e),
        ))
    })?;

    let script = Script::new(&lua_script);
    let mut script_invocation = script.prepare_invoke();
    for key in &keys {
        script_invocation.key(key);
    }
    script_invocation.invoke_async(&mut conn).await
}

// 从日志队列弹出一条
pub async fn pop_log_detail(pool: &Arc<Pool>, timeout_secs: usize) -> Option<String> {
    if let Ok(mut conn) = pool.get().await {
        redis::cmd("BRPOP")
            .arg("seckill:logs")
            .arg(timeout_secs.to_string())
            .query_async::<Option<(String, String)>>(&mut conn)
            .await
            .ok()
            .flatten()
            .map(|(_key, val)| val)
    } else {
        None
    }
}

// 查询统计字段
pub async fn get_stats(pool: &Arc<Pool>, key: &str) -> Option<i64> {
    if let Ok(mut conn) = pool.get().await {
        conn.get(key).await.ok()
    } else {
        None
    }
}

// 查询当前 Redis 版本号
pub async fn get_version(pool: &Arc<Pool>) -> Option<i64> {
    if let Ok(mut conn) = pool.get().await {
        conn.get(VERSION_KEY).await.ok()
    } else {
        None
    }
}

// 增加版本号（控制面所有修改都用它，返回新版本号）
pub async fn incr_version(pool: &Arc<Pool>) -> Option<i64> {
    if let Ok(mut conn) = pool.get().await {
        conn.incr(VERSION_KEY, 1).await.ok()
    } else {
        None
    }
}