// src/args.rs
use clap::Parser;
use serde::Deserialize;
use clap::builder::{TypedValueParser, ValueParserFactory};
use clap::{Command, Error as ClapError, error::ErrorKind};

#[derive(Clone)]
pub struct HashMapParser;

impl ValueParserFactory for HashMapParser {
    type Parser = Self;

    fn value_parser() -> Self {
        Self
    }
}

impl TypedValueParser for HashMapParser {
    type Value = std::collections::HashMap<usize, i64>;

    fn parse_ref(
        &self,
        cmd: &Command,
        _arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, ClapError> {
        let s = value.to_str().ok_or_else(|| {
            ClapError::raw(ErrorKind::InvalidUtf8, "Invalid UTF-8 in HashMap value").with_cmd(cmd)
        })?;

        let mut map = std::collections::HashMap::new();
        for pair in s.split(',') {
            let parts: Vec<&str> = pair.split(':').collect();
            if parts.len() != 2 {
                return Err(
                    ClapError::raw(
                        ErrorKind::ValueValidation,
                        format!("Invalid HashMap format: expected 'key:value', got '{}'", pair),
                    )
                        .with_cmd(cmd),
                );
            }
            let key = parts[0].parse::<usize>().map_err(|e| {
                ClapError::raw(ErrorKind::ValueValidation, format!("Invalid key in HashMap: {}", e)).with_cmd(cmd)
            })?;
            let val = parts[1].parse::<i64>().map_err(|e| {
                ClapError::raw(ErrorKind::ValueValidation, format!("Invalid value in HashMap: {}", e)).with_cmd(cmd)
            })?;
            map.insert(key, val);
        }
        Ok(map)
    }
}

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
    pub rate: Option<usize>, // 每秒最大并发数
    #[arg(long)]
    pub delay: Option<u64>, // 每条消息间隔ms
}

#[derive(Parser, Debug, Clone, Deserialize, Default)]
#[serde(default)]
#[command(author, version, about)]
pub struct ServerArgs {
    #[arg(long)]
    pub brokers: Option<String>,
    #[arg(long)]
    pub topic: Option<String>,
    #[arg(long, value_parser = HashMapParser::value_parser())]
    pub stock: Option<std::collections::HashMap<usize, i64>>,
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
