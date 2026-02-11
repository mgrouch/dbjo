-- Generate concrete payload columns from the generated DbMeta for the object type
-- you persist in the outbox (see MsSqlOutboxSqlBuilder). This script is a template.
--
-- sqlcmd variables expected:
--   OUTBOX_TABLE     e.g. dbo.order_outbox
--   PAYLOAD_TABLE    e.g. dbo.orders
--   PAYLOAD_COLUMNS  e.g. order_id, status, customer_id

SELECT TOP (0)
       $(PAYLOAD_COLUMNS)
  INTO $(OUTBOX_TABLE)
  FROM $(PAYLOAD_TABLE);

ALTER TABLE $(OUTBOX_TABLE) ADD
    outbox_id NVARCHAR(100) NOT NULL,
    sequence_no BIGINT NOT NULL,
    lock_owner NVARCHAR(120) NULL,
    locked_at_utc DATETIME2 NULL,
    published_topic NVARCHAR(255) NULL,
    published_partition INT NULL,
    published_offset BIGINT NULL,
    published_timestamp_utc DATETIME2 NULL,
    published_at_utc DATETIME2 NULL,
    created_at_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME();

ALTER TABLE $(OUTBOX_TABLE) ADD CONSTRAINT pk_outbox_id PRIMARY KEY (outbox_id);
CREATE UNIQUE INDEX ux_outbox_sequence_no ON $(OUTBOX_TABLE)(sequence_no);
CREATE INDEX ix_outbox_pending ON $(OUTBOX_TABLE)(published_at_utc, lock_owner, sequence_no);
