// src/redis_state.rs

use deadpool_redis::Connection;
use deadpool_redis::Pool;
use redis::{AsyncCommands, Script};
use std::sync::Arc;

const STOCK_KEY: &str = "stock";
const ACTIVITY_OVER_KEY: &str = "activity_over";
const STATS_SUCCESS_KEY: &str = "stats:success";
const VERSION_KEY: &str = "stock_version";
const CONTROL_CHANNEL: &str = "control_channel";

// 统一重置库存、活动状态、版本号和 stats
async fn do_reset_all(
    conn: &mut Connection,
    stock: i64,
    reset_stats: bool,
) -> redis::RedisResult<()> {
    let mut pipe = redis::pipe();
    pipe.cmd("SET").arg(STOCK_KEY).arg(stock);
    pipe.cmd("SET").arg(ACTIVITY_OVER_KEY).arg("0");
    pipe.cmd("INCR").arg(VERSION_KEY);
    if reset_stats {
        pipe.cmd("SET").arg(STATS_SUCCESS_KEY).arg(0);
    }
    pipe.query_async(conn).await
}

// 初始化：全字段重置（只用一次，通常用于冷启动）
pub async fn init_state(pool: &Arc<Pool>, stock: i64) {
    if let Ok(mut conn) = pool.get().await {
        let _ = do_reset_all(&mut conn, stock, true).await;
    }
}

// 控制面：库存重置+通知
pub async fn set_stock(pool: &Arc<Pool>, value: i64) -> bool {
    if let Ok(mut conn) = pool.get().await {
        let ok = do_reset_all(&mut conn, value, true).await.is_ok();
        if ok {
            let _: Result<(), _> = conn.publish(
                CONTROL_CHANNEL,
                format!(r#"{{"cmd":"set_stock","value":{}}}"#, value)
            ).await;
        }
        ok
    } else {
        false
    }
}


// 数据库同步库存+通知
pub async fn sync_redis_from_db(pool: &Arc<Pool>, stock: i64) -> bool {
    if let Ok(mut conn) = pool.get().await {
        let ok = do_reset_all(&mut conn, stock, true).await.is_ok();
        if ok {
            let _: Result<(), _> = conn.publish(
                CONTROL_CHANNEL,
                format!(r#"{{"cmd":"sync_from_db","value":{}}}"#, stock)
            ).await;
        }
        ok
    } else {
        false
    }
}

// 只设置活动结束，不重置其它
pub async fn set_activity_finish(pool: &Arc<Pool>) -> bool {
    if let Ok(mut conn) = pool.get().await {
        let set_ok = conn.set::<&str, &str, ()>(ACTIVITY_OVER_KEY, "1").await.is_ok();
        if set_ok {
            let _: Result<(), _> = conn.publish(CONTROL_CHANNEL, r#"{"cmd":"finish"}"#).await;
        }
        set_ok
    } else {
        false
    }
}

// 查询库存
pub async fn get_stock(pool: &Arc<Pool>) -> Option<i64> {
    if let Ok(mut conn) = pool.get().await {
        conn.get(STOCK_KEY).await.ok()
    } else {
        None
    }
}

// 查询活动是否结束
pub async fn is_activity_over(pool: &Arc<Pool>) -> bool {
    if let Ok(mut conn) = pool.get().await {
        conn.get::<_, String>(ACTIVITY_OVER_KEY).await.ok().map_or(false, |v| v == "1")
    } else {
        false
    }
}

// 运行秒杀脚本（减库存、判定活动状态、统计成功数）
pub async fn run_seckill_script(pool: &Arc<Pool>) -> Result<i64, redis::RedisError> {
    let lua_script = r#"
        if redis.call("GET", KEYS[2]) == "1" then return -1 end
        local stock = tonumber(redis.call("GET", KEYS[1]))
        if stock <= 0 then
            redis.call("SET", KEYS[2], "1")
            return -1
        end
        redis.call("DECR", KEYS[1])
        redis.call("INCR", KEYS[3])
        return stock - 1
    "#;

    let mut conn = pool.get().await.map_err(|e| {
        redis::RedisError::from((
            redis::ErrorKind::IoError,
            "deadpool get() failed",
            format!("{:?}", e),
        ))
    })?;

    Script::new(lua_script)
        .key(STOCK_KEY)
        .key(ACTIVITY_OVER_KEY)
        .key(STATS_SUCCESS_KEY)
        .invoke_async(&mut conn)
        .await
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