use deadpool_redis::Pool;
use redis::{AsyncCommands, Script};
use std::sync::Arc;

pub async fn get_stock(pool: &Arc<Pool>) -> Option<i64> {
    if let Ok(mut conn) = pool.get().await {
        conn.get("stock").await.ok()
    } else {
        None
    }
}

pub async fn is_activity_over(pool: &Arc<Pool>) -> bool {
    if let Ok(mut conn) = pool.get().await {
        conn.get::<_, String>("activity_over").await.ok().map_or(false, |v| v == "1")
    } else {
        false
    }
}

pub async fn set_activity_over(pool: &Arc<Pool>) {
    if let Ok(mut conn) = pool.get().await {
        let _: Result<(), _> = conn.set("activity_over", "1").await;
    }
}

pub async fn init_state(pool: &Arc<Pool>, stock: usize) {
    if let Ok(mut conn) = pool.get().await {
        let _: () = conn.set("stock", stock).await.unwrap_or(());
        let _: () = conn.set("activity_over", "0").await.unwrap_or(());
        let _: () = conn.set("stats:success", 0).await.unwrap_or(());
    }
}

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
        .key("stock")
        .key("activity_over")
        .key("stats:success")
        .invoke_async(&mut conn)
        .await
}

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