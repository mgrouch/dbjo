package org.github.dbjo.kafka.outbox.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.github.dbjo.kafka.outbox.OutboxStateStore;
import org.github.dbjo.kafka.publisher.KafkaPublishReceipt;
import org.github.dbjo.meta.jdbc.DbDialect;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * JDBC outbox store implementation with claim SQL chosen by {@link DbDialect}.
 */
public class JdbcOutboxStore implements OutboxStateStore {
    private static final String CLAIM_NEXT_BATCH_MSSQL_SQL = """
        WITH next_rows AS (
            SELECT TOP (:batchSize) outbox_id, sequence_no, payload_type, partition_key, payload, occurred_at_epoch_ms
            FROM kafka_outbox WITH (ROWLOCK, UPDLOCK, READPAST)
            WHERE published_at_utc IS NULL AND lock_owner IS NULL
            ORDER BY sequence_no ASC
        )
        UPDATE next_rows
           SET lock_owner = :lockOwner,
               locked_at_utc = CURRENT_TIMESTAMP
        OUTPUT inserted.outbox_id,
               inserted.sequence_no,
               inserted.payload_type,
               inserted.partition_key,
               inserted.payload,
               inserted.occurred_at_epoch_ms
        """;

    private static final String CLAIM_NEXT_BATCH_LOCK_SQL = """
        UPDATE kafka_outbox
           SET lock_owner = :lockOwner,
               locked_at_utc = CURRENT_TIMESTAMP
         WHERE outbox_id IN (
               SELECT outbox_id
                 FROM kafka_outbox
                WHERE published_at_utc IS NULL AND lock_owner IS NULL
                ORDER BY sequence_no ASC
                FETCH FIRST :batchSize ROWS ONLY
         )
        """;

    private static final String CLAIM_NEXT_BATCH_SELECT_SQL = """
        SELECT outbox_id, sequence_no, payload_type, partition_key, payload, occurred_at_epoch_ms
          FROM kafka_outbox
         WHERE lock_owner = :lockOwner
           AND published_at_utc IS NULL
         ORDER BY sequence_no ASC
         FETCH FIRST :batchSize ROWS ONLY
        """;

    private static final String INSERT_SQL = """
        INSERT INTO kafka_outbox (
            outbox_id,
            sequence_no,
            payload_type,
            partition_key,
            payload,
            occurred_at_epoch_ms,
            created_at_utc
        )
        VALUES (
            :outboxId,
            :sequenceNo,
            :payloadType,
            :partitionKey,
            :payload,
            :occurredAtEpochMs,
            :createdAtUtc
        )
        """;

    private static final String MARK_PUBLISHED_SQL = """
        UPDATE kafka_outbox
           SET published_topic = :topic,
               published_partition = :partition,
               published_offset = :offset,
               published_timestamp_utc = :publishedTimestampUtc,
               published_at_utc = CURRENT_TIMESTAMP,
               lock_owner = NULL,
               locked_at_utc = NULL
         WHERE outbox_id = :outboxId
        """;

    private final NamedParameterJdbcTemplate jdbc;
    private final DbDialect dialect;
    private final RowMapper<OutboxMessage> rowMapper;

    public JdbcOutboxStore(NamedParameterJdbcTemplate jdbc) {
        this(jdbc, DbDialect.MSSQL);
    }

    public JdbcOutboxStore(NamedParameterJdbcTemplate jdbc, DbDialect dialect) {
        this.jdbc = Objects.requireNonNull(jdbc, "jdbc must not be null");
        this.dialect = Objects.requireNonNull(dialect, "dialect must not be null");
        this.rowMapper = new RowMapper<>() {
            @Override
            public OutboxMessage mapRow(ResultSet rs, int rowNum) throws SQLException {
                return new OutboxMessage(
                    rs.getString("outbox_id"),
                    rs.getLong("sequence_no"),
                    rs.getString("payload_type"),
                    rs.getString("partition_key"),
                    rs.getBytes("payload"),
                    rs.getLong("occurred_at_epoch_ms")
                );
            }
        };
    }

    public void append(OutboxMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("outboxId", message.outboxId())
            .addValue("sequenceNo", message.sequenceNo())
            .addValue("payloadType", message.payloadType())
            .addValue("partitionKey", message.partitionKey())
            .addValue("payload", message.payload())
            .addValue("occurredAtEpochMs", message.occurredAtEpochMs())
            .addValue("createdAtUtc", Timestamp.from(Instant.now()));
        jdbc.update(INSERT_SQL, params);
    }

    public List<OutboxMessage> claimNextBatch(int batchSize, String lockOwner) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0");
        }
        if (lockOwner == null || lockOwner.isBlank()) {
            throw new IllegalArgumentException("lockOwner must not be null or blank");
        }
        Map<String, Object> params = Map.of(
            "batchSize", batchSize,
            "lockOwner", lockOwner
        );

        List<OutboxMessage> messages = switch (dialect) {
            case MSSQL -> jdbc.query(CLAIM_NEXT_BATCH_MSSQL_SQL, params, rowMapper);
            case SYBASE, HSQL, ORACLE -> {
                jdbc.update(CLAIM_NEXT_BATCH_LOCK_SQL, params);
                yield jdbc.query(CLAIM_NEXT_BATCH_SELECT_SQL, params, rowMapper);
            }
        };

        return messages.stream()
            .sorted(Comparator.comparingLong(OutboxMessage::sequenceNo))
            .toList();
    }

    @Override
    public void markPublished(Collection<KafkaPublishReceipt> receipts) {
        Objects.requireNonNull(receipts, "receipts must not be null");
        if (receipts.isEmpty()) {
            return;
        }

        SqlParameterSource[] batch = receipts.stream()
            .map(receipt -> new MapSqlParameterSource()
                .addValue("outboxId", receipt.outboxId())
                .addValue("topic", receipt.topic())
                .addValue("partition", receipt.partition())
                .addValue("offset", receipt.offset())
                .addValue("publishedTimestampUtc", Timestamp.from(Instant.ofEpochMilli(receipt.timestamp()))))
            .toArray(SqlParameterSource[]::new);
        jdbc.batchUpdate(MARK_PUBLISHED_SQL, batch);
    }
}
