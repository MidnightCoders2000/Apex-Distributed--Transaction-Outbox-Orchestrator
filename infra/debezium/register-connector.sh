#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

set -a
source .env
: "${DB_PORT:=5432}"
: "${DB_SSLMODE:=require}"
set +a

envsubst < infra/debezium/outbox-connector.json \
  | curl -s -X POST -H "Content-Type: application/json" \
      --data @- \
      http://localhost:8083/connectors
