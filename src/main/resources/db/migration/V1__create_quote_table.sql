CREATE TABLE quote (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol       VARCHAR(16)              NOT NULL,
    bid_price    NUMERIC(19, 4)           NOT NULL,
    bid_size     BIGINT                   NOT NULL,
    bid_exchange VARCHAR(8),
    ask_price    NUMERIC(19, 4)           NOT NULL,
    ask_size     BIGINT                   NOT NULL,
    ask_exchange VARCHAR(8),
    quoted_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    received_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Kafka delivers at-least-once; this key makes a redelivered message a no-op.
    CONSTRAINT uq_quote_symbol_quoted_at UNIQUE (symbol, quoted_at)
);
