#!/usr/bin/env bash
set -e

MODE="container"
PROFILE=""

for arg in "$@"; do
  case "$arg" in
    --llap)
      MODE="llap"
      PROFILE="--profile llap"
      export HIVE_ZOOKEEPER_QUORUM=zookeeper:2181
      export HIVE_LLAP_DAEMON_SERVICE_HOSTS=@llap0
      ;;
    *)
      echo "Unknown option: $arg"
      exit 1
      ;;
  esac
done

export HIVE_EXECUTION_MODE="$MODE"

echo "Starting Hive cluster (mode=$HIVE_EXECUTION_MODE)"

docker compose $PROFILE up -d

docker compose $PROFILE logs -f