#!/bin/zsh

BIN_DIR="target/debug"

usage() {
  cat <<EOF
Usage: $0 [server|client|log_worker|control_api] [options]

Modes:
  server                  启动服务器端 Kafka 模拟器
  client                  启动客户端请求模拟器（默认）
  log_worker              启动日志处理 Worker
  control_api             启动控制中心 API

Options:
  --brokers <BROKERS>     Kafka broker 地址
  --topic <TOPIC>         Kafka topic 名称
  --stock <STOCK>         初始库存数量
  --timeout <TIMEOUT>     用户请求超时时间，单位毫秒
  --user-count <N>        模拟用户数量
  --rate <R>              每秒请求数量
  --delay <MS>            请求发起初始延迟，单位毫秒
  -h, --help              显示此帮助信息

Note: Options override values in config.yaml
EOF
  exit 1
}

if [[ "$1" == "help" || "$1" == "--help" || "$1" == "-h" ]]; then
  usage
fi

echo "Building project..."
cargo build --bin server_kafka --bin client_kafka --bin log_worker --bin control_api || exit 1

MODE="${1:-client}"
shift
case "$MODE" in
  server)
    BIN_PATH="$BIN_DIR/server_kafka"
    ;;
  client)
    BIN_PATH="$BIN_DIR/client_kafka"
    ;;
  log_worker)
    BIN_PATH="$BIN_DIR/log_worker"
    ;;
  control_api)
    BIN_PATH="$BIN_DIR/control_api"
    ;;
  *)
    usage
    ;;
esac

echo "Starting $MODE with config.yaml and overrides: $@"
exec "$BIN_PATH" "$@"