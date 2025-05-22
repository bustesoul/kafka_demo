// src/common.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SeckillRequest {
    pub user_id: usize,
    pub request_initiation_time: u128, // timestamp in ms
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SeckillResult {
    Success { user_id: usize, cost_ms: u128 },
    Fail { user_id: usize, cost_ms: u128, reason: String },
    PendingTimeout { user_id: usize, cost_ms: u128 },
}