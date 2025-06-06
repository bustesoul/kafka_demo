// src/common.rs
use crate::args::{ClientArgs, ControlApiArgs, LogWorkerArgs, ServerArgs};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct Config {
    pub client: Option<ClientArgs>,
    pub server: Option<ServerArgs>,
    pub control_api: Option<ControlApiArgs>,
    pub log_worker: Option<LogWorkerArgs>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SeckillRequest {
    pub user_id: usize,
    pub items: HashMap<usize, usize>,
    pub request_initiation_time: u128, // timestamp in ms
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SeckillResult {
    Success {
        user_id: usize,
        cost_ms: u128,
    },
    Fail {
        user_id: usize,
        cost_ms: u128,
        reason: String,
    },
    PendingTimeout {
        user_id: usize,
        cost_ms: u128,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SeckillRecord {
    pub user_id: u64,
    pub activity_id: u64,
    pub item_id: u64,  // New field
    pub quantity: u64, // New field
    pub cost_ms: u128,
    pub status: String,
    pub timestamp: u64,
}
