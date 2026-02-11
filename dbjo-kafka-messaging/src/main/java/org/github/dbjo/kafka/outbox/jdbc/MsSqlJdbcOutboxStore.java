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
import org.apache.avro.specific.SpecificRecord;
import org.github.dbjo.kafka.outbox.OutboxStateStore;
import org.github.dbjo.kafka.publisher.KafkaPublishReceipt;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * MS SQL outbox store implementation using row locks and READPAST (skip locked).
 */
public class MsSqlJdbcOutboxStore<T extends SpecificRecord> implements OutboxStateStore {
    private static final String CLAIM_NEXT_BATCH_SQL = """
        WITH next_rows AS (
            SELECT TOP (:batchSize) outbox_id, sequence_no, partition_key, payload
            FROM kafka_outbox WITH (ROWLOCK, UPDLOCK, READPAST)
            WHERE published_at_utc IS NULL AND lock_owner IS NULL
            ORDER BY sequence_no ASC
        )
        UPDATE next_rows
           SET lock_owner = :lockOwner,
               locked_at_utc = SYSUTCDATETIME()
        OUTPUT inserted.outbox_id, inserted.sequence_no, inserted.partition_key, inserted.payload
        """;

    private static final String INSERT_SQL = """
        INSERT INTO kafka_outbox (outbox_id, sequence_no, partition_key, payload, created_at_utc)
        VALUES (:outboxId, :sequenceNo, :partitionKey, :payload, :createdAtUtc)
        """;

    private static final String MARK_PUBLISHED_SQL = """
        UPDATE kafka_outbox
           SET published_topic = :topic,
               published_partition = :partition,
               published_offset = :offset,
               published_timestamp_utc = :publishedTimestampUtc,
               published_at_utc = SYSUTCDATETIME(),
               lock_owner = NULL,
               locked_at_utc = NULL
         WHERE outbox_id = :outboxId
        """;

    private final NamedParameterJdbcTemplate jdbc;
    private final OutboxEventCodec<T> codec;
    private final RowMapper<OutboxMessage<T>> rowMapper;

    public MsSqlJdbcOutboxStore(NamedParameterJdbcTemplate jdbc, OutboxEventCodec<T> codec) {
        this.jdbc = Objects.requireNonNull(jdbc, "jdbc must not be null");
        this.codec = Objects.requireNonNull(codec, "codec must not be null");
        this.rowMapper = new RowMapper<>() {
            @Override
            public OutboxMessage<T> mapRow(ResultSet rs, int rowNum) throws SQLException {
                return new OutboxMessage<>(
                    rs.getString("outbox_id"),
                    rs.getLong("sequence_no"),
                    rs.getString("partition_key"),
                    codec.decode(rs.getBytes("payload"))
                );
            }
        };
    }

    public void append(OutboxMessage<T> message) {
        Objects.requireNonNull(message, "message must not be null");
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("outboxId", message.outboxId())
            .addValue("sequenceNo", message.sequenceNo())
            .addValue("partitionKey", message.partitionKey())
            .addValue("payload", codec.encode(message.event()))
            .addValue("createdAtUtc", Timestamp.from(Instant.now()));
        jdbc.update(INSERT_SQL, params);
    }

    public List<OutboxMessage<T>> claimNextBatch(int batchSize, String lockOwner) {
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
        List<OutboxMessage<T>> messages = jdbc.query(CLAIM_NEXT_BATCH_SQL, params, rowMapper);
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
