CREATE TABLE kafka_outbox (
    outbox_id NVARCHAR(100) NOT NULL PRIMARY KEY,
    sequence_no BIGINT NOT NULL,
    event_id NVARCHAR(100) NOT NULL,
    product_id NVARCHAR(100) NOT NULL,
    event_type NVARCHAR(100) NOT NULL,
    occurred_at_epoch_ms BIGINT NOT NULL,
    created_at_utc DATETIME2 NOT NULL,
    locked_at_utc DATETIME2 NULL,
    lock_owner NVARCHAR(120) NULL,
    published_topic NVARCHAR(255) NULL,
    published_partition INT NULL,
    published_offset BIGINT NULL,
    published_timestamp_utc DATETIME2 NULL,
    published_at_utc DATETIME2 NULL
);

CREATE UNIQUE INDEX ux_kafka_outbox_sequence_no ON kafka_outbox(sequence_no);
CREATE INDEX ix_kafka_outbox_pending ON kafka_outbox(published_at_utc, lock_owner, sequence_no);
