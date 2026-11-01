-- blokli-light stores raw logs only. The three compatibility tables intentionally
-- retain the column names and byte layout used by Blokli and Arrakis bundles.

CREATE TABLE IF NOT EXISTS log (
    id               bigserial PRIMARY KEY,
    tx_index         bigint  NOT NULL,
    log_index        bigint  NOT NULL,
    block_number     bigint  NOT NULL,
    block_hash       bytea   NOT NULL CHECK (octet_length(block_hash) = 32),
    transaction_hash bytea   NOT NULL CHECK (octet_length(transaction_hash) = 32),
    address          bytea   NOT NULL CHECK (octet_length(address) = 20),
    topics           bytea   NOT NULL CHECK (octet_length(topics) % 32 = 0),
    data             bytea   NOT NULL,
    removed          boolean NOT NULL DEFAULT false,
    CONSTRAINT idx_log_composite UNIQUE (block_number, tx_index, log_index)
);

CREATE INDEX IF NOT EXISTS idx_log_address_topic0
    ON log (address, (substring(topics FROM 1 FOR 32)), block_number);

CREATE TABLE IF NOT EXISTS chain_info (
    id                     bigint PRIMARY KEY CHECK (id = 1),
    last_indexed_block     bigint NOT NULL DEFAULT 0,
    last_indexed_tx_index  bigint,
    last_indexed_log_index bigint,
    network                text,
    chain_id               bigint,
    head_block             bigint,
    updated_at             timestamptz NOT NULL DEFAULT now()
);

INSERT INTO chain_info (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS log_topic_info (
    id      bigserial PRIMARY KEY,
    address bytea NOT NULL CHECK (octet_length(address) = 20),
    topic   bytea NOT NULL CHECK (octet_length(topic) = 32),
    CONSTRAINT idx_contract_log_topic UNIQUE (address, topic)
);

CREATE TABLE IF NOT EXISTS source (
    id            bigserial PRIMARY KEY,
    name          text        NOT NULL UNIQUE,
    address       bytea       NOT NULL UNIQUE CHECK (octet_length(address) = 20),
    kind          text        NOT NULL CHECK (kind IN ('builtin', 'custom')),
    from_block    bigint      NOT NULL CHECK (from_block >= 0),
    indexed_to    bigint      NOT NULL,
    events        text[],
    enabled       boolean     NOT NULL DEFAULT true,
    discovered_by text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CHECK (indexed_to >= from_block - 1)
);

CREATE OR REPLACE VIEW indexer_status AS
SELECT
    source.name,
    source.address,
    source.kind,
    source.from_block,
    source.indexed_to,
    chain_info.head_block - source.indexed_to AS lag,
    source.enabled
FROM source
CROSS JOIN chain_info
WHERE chain_info.id = 1;
