CREATE TABLE transactions (
                              id BIGSERIAL PRIMARY KEY,
                              account_id BIGINT NOT NULL,
                              amount DECIMAL(19, 2) NOT NULL,
                              state VARCHAR(50) NOT NULL
);


CREATE TABLE outbox_event (
                              id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                              aggregate_type text        NOT NULL,
                              aggregate_id   text        NOT NULL,
                              event_type     text        NOT NULL,
                              payload        jsonb       NOT NULL,
                              correlation_id text,
                              created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_event_created_at ON outbox_event (created_at);