#!/bin/zsh

BIN_DIR="target/debug"
DEFAULT_MODE="client"
DEFAULT_GROUP_ID="202505211808"
BROKERS="172.20.19.27:9092"
TOPIC="seckill_test"
DEFAULT_CLIENT_ARGS=(--user-count 1000 --rate 50 --delay 10)
DEFAULT_SERVER_ARGS=(--stock 10 --timeout 100 --group-id $DEFAULT_GROUP_ID)

usage() {
  cat <<EOF
Usage: $0 [server|client] [options]

Modes:
  server                  启动服务器端 Kafka 模拟器
  client                  启动客户端请求模拟器（默认）

Options:
  --brokers <BROKERS>     Kafka broker 地址 (默认: $BROKERS)
  --topic <TOPIC>         Kafka topic 名称 (默认: $TOPIC)

Server mode default options:
  --stock <STOCK>         初始库存数量 (默认: ${DEFAULT_SERVER_ARGS[1]})
  --timeout <TIMEOUT>     用户请求超时时间，单位毫秒 (默认: ${DEFAULT_SERVER_ARGS[3]})
  --group-id <ID>         Kafka 消费者 group id (默认: $DEFAULT_GROUP_ID)

Client mode default options:
  --user-count <N>        模拟用户数量 (默认: ${DEFAULT_CLIENT_ARGS[1]})
  --rate <R>              每秒请求数量 (默认: ${DEFAULT_CLIENT_ARGS[3]})
  --delay <MS>            请求发起初始延迟，单位毫秒 (默认: ${DEFAULT_CLIENT_ARGS[5]})

Global options:
  -h, --help              显示此帮助信息

Examples:
  $0                      使用默认 client 参数运行
  $0 client --user-count 5000 --rate 1000
  $0 server --stock 20 --timeout 200

EOF
  exit 1
}

if [[ "$1" == "help" || "$1" == "--help" || "$1" == "-h" ]]; then
  usage
fi

echo "Building project..."
cargo build --bin server_kafka --bin client_kafka || exit 1

# 判断参数中是否已包含 --brokers/--topic
contains_arg() {
  local key="$1"
  shift
  for arg in "$@"; do
    if [[ "$arg" == "$key" ]]; then
      return 0
    fi
  done
  return 1
}

if [[ $# -eq 0 ]]; then
  MODE="$DEFAULT_MODE"
  BIN_PATH="$BIN_DIR/client_kafka"
  ARGS=(--brokers "$BROKERS" --topic "$TOPIC" "${DEFAULT_CLIENT_ARGS[@]}")
else
  MODE="$1"
  shift
  case "$MODE" in
    server)
      BIN_PATH="$BIN_DIR/server_kafka"
      if [[ $# -eq 0 ]]; then
        ARGS=(--brokers "$BROKERS" --topic "$TOPIC" "${DEFAULT_SERVER_ARGS[@]}")
      else
        # 检查用户参数
        ARGS=()
        if ! contains_arg --brokers "$@"; then
          ARGS+=(--brokers "$BROKERS")
        fi
        if ! contains_arg --topic "$@"; then
          ARGS+=(--topic "$TOPIC")
        fi
        ARGS+=("$@")
      fi
      ;;
    client)
      BIN_PATH="$BIN_DIR/client_kafka"
      if [[ $# -eq 0 ]]; then
        ARGS=(--brokers "$BROKERS" --topic "$TOPIC" "${DEFAULT_CLIENT_ARGS[@]}")
      else
        # 检查用户参数
        ARGS=()
        if ! contains_arg --brokers "$@"; then
          ARGS+=(--brokers "$BROKERS")
        fi
        if ! contains_arg --topic "$@"; then
          ARGS+=(--topic "$TOPIC")
        fi
        ARGS+=("$@")
      fi
      ;;
    *)
      usage
      ;;
  esac
fi

echo "Starting $MODE..."
exec "$BIN_PATH" "${ARGS[@]}"