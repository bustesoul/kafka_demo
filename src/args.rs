// src/args.rs
use clap::Parser;
use serde::Deserialize;

#[derive(Parser, Debug, Clone, Deserialize, Default)]
#[serde(default)]
#[command(author, version, about)]
pub struct ClientArgs {
    #[arg(long)]
    pub brokers: Option<String>,
    #[arg(long)]
    pub topic: Option<String>,
    #[arg(long)]
    pub user_count: Option<usize>,
    #[arg(long)]
    pub rate: Option<usize>,         // 每秒最大并发数
    #[arg(long)]
    pub delay: Option<u64>,          // 每条消息间隔ms
}

#[derive(Parser, Debug, Clone, Deserialize, Default)]
#[serde(default)]
#[command(author, version, about)]
pub struct ServerArgs {
    #[arg(long)]
    pub brokers: Option<String>,
    #[arg(long)]
    pub topic: Option<String>,
    #[arg(long)]
    pub stock: Option<usize>,
    #[arg(long)]
    pub timeout: Option<u128>,

    #[arg(long)]
    pub consumers: Option<usize>,
    #[arg(long)]
    pub status_interval: Option<u64>,
    #[arg(long)]
    pub reset_offset: Option<bool>,
    #[arg(long)]
    pub group_id: Option<String>,
    #[arg(long)]
    pub redis_url: Option<String>,
    #[arg(long)]
    pub pg_dsn: Option<String>,
}

#[derive(Parser, Debug, Clone, Deserialize, Default)]
#[serde(default)]
#[command(author, version, about)]
pub struct ControlApiArgs {
    #[arg(long)]
    pub redis_url: Option<String>,
    #[arg(long)]
    pub pg_dsn: Option<String>,
}

#[derive(Parser, Debug, Clone, Deserialize, Default)]
#[serde(default)]
#[command(author, version, about)]
pub struct LogWorkerArgs {
    #[arg(long)]
    pub redis_url: Option<String>,
    #[arg(long)]
    pub pg_dsn: Option<String>,
}