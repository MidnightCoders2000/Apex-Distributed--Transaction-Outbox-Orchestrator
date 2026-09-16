#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

set -a
source .env
: "${DB_PORT:=5432}"
: "${DB_SSLMODE:=require}"
# Identity of the real (Neon-targeted) registration. The verification
# script overrides all four with its own values so it can never touch this
# connector, its offsets, or its topics.
: "${CONNECTOR_NAME:=apex-outbox-connector}"
: "${TOPIC_PREFIX:=apex}"
: "${SLOT_NAME:=apex_outbox_slot}"
: "${PUBLICATION_NAME:=apex_outbox_pub}"
set +a

envsubst < infra/debezium/outbox-connector.json \
  | curl -s -X POST -H "Content-Type: application/json" \
      --data @- \
      http://localhost:8083/connectors
