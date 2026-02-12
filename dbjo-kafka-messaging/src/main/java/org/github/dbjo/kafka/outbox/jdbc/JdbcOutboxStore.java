package org.github.dbjo.kafka.outbox.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.github.dbjo.kafka.outbox.OutboxStateStore;
import org.github.dbjo.kafka.publisher.KafkaPublishReceipt;
import org.github.dbjo.meta.jdbc.DbDialect;
import org.github.dbjo.meta.jdbc.DbMeta;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * JDBC outbox store implementation with payload columns mapped from generated {@link DbMeta}.
 */
public class JdbcOutboxStore<T> implements OutboxStateStore {
    private static final String DEFAULT_OUTBOX_TABLE = "kafka_outbox";

    private final NamedParameterJdbcTemplate jdbc;
    private final DbMeta<T> payloadMeta;
    private final DbDialect dialect;
    private final String claimForUpdateSql;
    private final String markPublishedSql;
    private final String insertSql;
    private final List<String> payloadColumns;
    private final List<String> payloadInsertColumns;
    private final RowMapper<OutboxMessage<T>> rowMapper;

    public JdbcOutboxStore(NamedParameterJdbcTemplate jdbc, DbMeta<T> payloadMeta) {
        this(jdbc, payloadMeta, DEFAULT_OUTBOX_TABLE, DbDialect.MSSQL);
    }

    public JdbcOutboxStore(NamedParameterJdbcTemplate jdbc, DbMeta<T> payloadMeta, DbDialect dialect) {
        this(jdbc, payloadMeta, DEFAULT_OUTBOX_TABLE, dialect);
    }

    public JdbcOutboxStore(
        NamedParameterJdbcTemplate jdbc,
        DbMeta<T> payloadMeta,
        String outboxTableFqn,
        DbDialect dialect
    ) {
        this.jdbc = Objects.requireNonNull(jdbc, "jdbc must not be null");
        this.payloadMeta = Objects.requireNonNull(payloadMeta, "payloadMeta must not be null");
        this.dialect = Objects.requireNonNull(dialect, "dialect must not be null");

        OutboxSqlBuilder.OutboxSql outboxSql = OutboxSqlBuilder.build(payloadMeta, outboxTableFqn, dialect);
        this.claimForUpdateSql = outboxSql.claimForUpdateSql();
        this.markPublishedSql = outboxSql.markPublishedSql();
        this.payloadColumns = outboxSql.payloadColumns();
        this.payloadInsertColumns = OutboxSqlBuilder.parseInsertColumns(payloadMeta.insertSql());
        this.insertSql = buildInsertSql(outboxTableFqn, payloadColumns);

        this.rowMapper = new RowMapper<>() {
            @Override
            public OutboxMessage<T> mapRow(ResultSet rs, int rowNum) throws SQLException {
                return new OutboxMessage<>(
                    rs.getString("outbox_id"),
                    rs.getLong("sequence_no"),
                    rs.getString("partition_key"),
                    payloadMeta.fromRow(rs),
                    rs.getLong("occurred_at_epoch_ms")
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
            .addValue("occurredAtEpochMs", message.occurredAtEpochMs())
            .addValue("createdAtUtc", Timestamp.from(Instant.now()));

        Map<String, Object> payloadValueByColumn = toPayloadColumnValueMap(message.event());
        payloadColumns.forEach(column -> params.addValue(column, payloadValueByColumn.get(column)));

        jdbc.update(insertSql, params);
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

        List<OutboxMessage<T>> messages = jdbc.query(claimForUpdateSql, params, rowMapper);

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
        jdbc.batchUpdate(markPublishedSql, batch);
    }

    private Map<String, Object> toPayloadColumnValueMap(T event) {
        Object[] values = payloadMeta.insertParams(event);
        if (values.length != payloadInsertColumns.size()) {
            throw new IllegalStateException("payloadMeta insertParams size must match parsed insert columns");
        }
        Map<String, Object> byInsertColumn = new HashMap<>(values.length);
        for (int i = 0; i < values.length; i++) {
            byInsertColumn.put(payloadInsertColumns.get(i), values[i]);
        }
        return byInsertColumn;
    }

    private static String buildInsertSql(String outboxTableFqn, List<String> payloadColumns) {
        String payloadColumnSql = payloadColumns.stream().collect(Collectors.joining(",\n            "));
        String payloadValueSql = payloadColumns.stream()
            .map(column -> ":" + column)
            .collect(Collectors.joining(",\n            "));

        return """
            INSERT INTO %s (
                outbox_id,
                sequence_no,
                partition_key,
                occurred_at_epoch_ms,
                created_at_utc,
                %s
            )
            VALUES (
                :outboxId,
                :sequenceNo,
                :partitionKey,
                :occurredAtEpochMs,
                :createdAtUtc,
                %s
            )
            """.formatted(outboxTableFqn, payloadColumnSql, payloadValueSql);
    }
}
