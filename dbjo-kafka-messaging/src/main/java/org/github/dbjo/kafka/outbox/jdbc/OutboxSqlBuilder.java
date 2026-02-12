package org.github.dbjo.kafka.outbox.jdbc;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import org.github.dbjo.meta.jdbc.DbDialect;
import org.github.dbjo.meta.jdbc.DbMeta;

/**
 * Builds outbox SQL from an already-generated {@link DbMeta} payload table mapping.
 */
public final class OutboxSqlBuilder {
    private OutboxSqlBuilder() {}

    public static OutboxSql build(DbMeta<?> payloadMeta, String outboxTableFqn) {
        return build(payloadMeta, outboxTableFqn, DbDialect.MSSQL);
    }

    public static OutboxSql build(DbMeta<?> payloadMeta, String outboxTableFqn, DbDialect dialect) {
        Objects.requireNonNull(payloadMeta, "payloadMeta must not be null");
        Objects.requireNonNull(dialect, "dialect must not be null");
        if (outboxTableFqn == null || outboxTableFqn.isBlank()) {
            throw new IllegalArgumentException("outboxTableFqn must not be blank");
        }

        List<String> payloadColumns = parseSelectColumns(payloadMeta.selectAllBaseSql());
        if (payloadColumns.isEmpty()) {
            throw new IllegalArgumentException("payloadMeta selectAllSql must include at least one payload column");
        }

        String quotedOutboxTable = quoteFqn(dialect, outboxTableFqn);
        String quotedPayloadTable = quoteFqn(dialect, payloadMeta.fqn());
        List<String> quotedPayloadColumns = payloadColumns.stream().map(c -> quoteIdentifier(dialect, c)).toList();

        String payloadProjection = String.join(",\n           ", quotedPayloadColumns);
        String insertedPayloadProjection = quotedPayloadColumns.stream()
            .map(c -> "inserted." + c)
            .collect(Collectors.joining(",\n                   "));

        String outboxId = quoteIdentifier(dialect, "outbox_id");
        String sequenceNo = quoteIdentifier(dialect, "sequence_no");
        String partitionKey = quoteIdentifier(dialect, "partition_key");
        String occurredAtEpochMs = quoteIdentifier(dialect, "occurred_at_epoch_ms");
        String publishedPartition = quoteIdentifier(dialect, "published_partition");
        String publishedOffset = quoteIdentifier(dialect, "published_offset");
        String publishedTimestampUtc = quoteIdentifier(dialect, "published_timestamp_utc");
        String publishedAtUtc = quoteIdentifier(dialect, "published_at_utc");
        String publishedTopic = quoteIdentifier(dialect, "published_topic");
        String createdAtUtc = quoteIdentifier(dialect, "created_at_utc");

        String createSql = switch (dialect) {
            case MSSQL, SYBASE -> """
                SELECT TOP (0)
                       %s,
                       CAST('' AS NVARCHAR(100)) AS %s,
                       CAST(0 AS BIGINT) AS %s,
                       CAST(NULL AS NVARCHAR(40)) AS %s,
                       CAST(0 AS BIGINT) AS %s,
                       CAST(NULL AS INT) AS %s,
                       CAST(NULL AS BIGINT) AS %s,
                       CAST(NULL AS DATETIME2) AS %s,
                       CAST(NULL AS DATETIME2) AS %s,
                       CAST(CURRENT_TIMESTAMP AS DATETIME2) AS %s
                  INTO %s
                  FROM %s;

                CREATE UNIQUE INDEX ux_%s_outbox_id ON %s(%s);
                CREATE UNIQUE INDEX ux_%s_sequence_no ON %s(%s);
                CREATE INDEX ix_%s_pending ON %s(%s, %s);
                """.formatted(
                payloadProjection,
                outboxId,
                sequenceNo,
                partitionKey,
                occurredAtEpochMs,
                publishedPartition,
                publishedOffset,
                publishedTimestampUtc,
                publishedAtUtc,
                createdAtUtc,
                quotedOutboxTable,
                quotedPayloadTable,
                sanitizeForConstraint(outboxTableFqn),
                quotedOutboxTable,
                outboxId,
                sanitizeForConstraint(outboxTableFqn),
                quotedOutboxTable,
                sequenceNo,
                sanitizeForConstraint(outboxTableFqn),
                quotedOutboxTable,
                publishedAtUtc,
                sequenceNo
            );
            case HSQL, ORACLE -> """
                CREATE TABLE %s AS
                SELECT %s,
                       CAST('' AS VARCHAR(100)) AS %s,
                       CAST(0 AS BIGINT) AS %s,
                       CAST(NULL AS VARCHAR(40)) AS %s,
                       CAST(0 AS BIGINT) AS %s,
                       CAST(NULL AS INTEGER) AS %s,
                       CAST(NULL AS BIGINT) AS %s,
                       CAST(NULL AS TIMESTAMP) AS %s,
                       CAST(NULL AS TIMESTAMP) AS %s,
                       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS %s
                  FROM %s
                 WHERE 1 = 0;

                CREATE UNIQUE INDEX ux_%s_outbox_id ON %s(%s);
                CREATE UNIQUE INDEX ux_%s_sequence_no ON %s(%s);
                CREATE INDEX ix_%s_pending ON %s(%s, %s);
                """.formatted(
                quotedOutboxTable,
                payloadProjection,
                outboxId,
                sequenceNo,
                partitionKey,
                occurredAtEpochMs,
                publishedPartition,
                publishedOffset,
                publishedTimestampUtc,
                publishedAtUtc,
                createdAtUtc,
                quotedPayloadTable,
                sanitizeForConstraint(outboxTableFqn),
                quotedOutboxTable,
                outboxId,
                sanitizeForConstraint(outboxTableFqn),
                quotedOutboxTable,
                sequenceNo,
                sanitizeForConstraint(outboxTableFqn),
                quotedOutboxTable,
                publishedAtUtc,
                sequenceNo
            );
        };

        String claimSql = switch (dialect) {
            case MSSQL -> """
                SELECT TOP (:batchSize)
                       %s,
                       %s,
                       %s,
                       %s,
                       %s
                  FROM %s WITH (READPAST)
                 WHERE %s IS NULL
                 ORDER BY %s ASC
                """.formatted(outboxId, sequenceNo, partitionKey, occurredAtEpochMs, payloadProjection, quotedOutboxTable, publishedAtUtc, sequenceNo);
            case SYBASE -> """
                SELECT TOP (:batchSize)
                       %s,
                       %s,
                       %s,
                       %s,
                       %s
                  FROM %s
                 WHERE %s IS NULL
                 ORDER BY %s ASC
                """.formatted(outboxId, sequenceNo, partitionKey, occurredAtEpochMs, payloadProjection, quotedOutboxTable, publishedAtUtc, sequenceNo);
            case HSQL, ORACLE -> """
                SELECT %s,
                       %s,
                       %s,
                       %s,
                       %s
                  FROM %s
                 WHERE %s IS NULL
                 ORDER BY %s ASC
                 FETCH FIRST :batchSize ROWS ONLY
                """.formatted(outboxId, sequenceNo, partitionKey, occurredAtEpochMs, payloadProjection, quotedOutboxTable, publishedAtUtc, sequenceNo);
        };

        String markPublishedSql = """
            UPDATE %s
               SET %s = :topic,
                   %s = :partition,
                   %s = :offset,
                   %s = :publishedTimestampUtc,
                   %s = CURRENT_TIMESTAMP
             WHERE %s = :outboxId
            """.formatted(quotedOutboxTable, publishedTopic, publishedPartition, publishedOffset, publishedTimestampUtc, publishedAtUtc, outboxId);

        return new OutboxSql(createSql, claimSql, markPublishedSql, payloadColumns);
    }

    private static String sanitizeForConstraint(String fqn) {
        return fqn.replace('.', '_').replace('[', '_').replace(']', '_').replace('"', '_');
    }

    private static String quoteFqn(DbDialect dialect, String fqn) {
        return Arrays.stream(fqn.split("\\."))
            .map(String::trim)
            .filter(part -> !part.isEmpty())
            .map(part -> quoteIdentifier(dialect, part))
            .collect(Collectors.joining("."));
    }

    private static String quoteIdentifier(DbDialect dialect, String identifier) {
        String ident = identifier.trim();
        if (dialect == DbDialect.MSSQL || dialect == DbDialect.SYBASE) {
            return "[" + ident + "]";
        }
        return "\"" + ident + "\"";
    }

    static List<String> parseSelectColumns(String selectAllSql) {
        String base = DbMeta.stripTrailingSemicolon(selectAllSql);
        int selectIx = indexOfIgnoreCase(base, "SELECT ");
        int fromIx = indexOfIgnoreCase(base, " FROM ");
        if (selectIx != 0 || fromIx <= 0) {
            throw new IllegalArgumentException("Unsupported selectAllSql format: " + selectAllSql);
        }
        String cols = base.substring("SELECT ".length(), fromIx).trim();
        return Arrays.stream(cols.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    }

    static List<String> parseInsertColumns(String insertSql) {
        String base = DbMeta.stripTrailingSemicolon(insertSql);
        int valuesIx = indexOfIgnoreCase(base, " VALUES");
        int firstParenIx = base.indexOf('(');
        if (firstParenIx <= 0 || valuesIx <= firstParenIx) {
            throw new IllegalArgumentException("Unsupported insertSql format: " + insertSql);
        }
        int closeParenIx = base.lastIndexOf(')', valuesIx);
        if (closeParenIx <= firstParenIx) {
            throw new IllegalArgumentException("Unsupported insertSql format: " + insertSql);
        }
        String cols = base.substring(firstParenIx + 1, closeParenIx).trim();
        return Arrays.stream(cols.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    }

    private static int indexOfIgnoreCase(String s, String search) {
        return s.toUpperCase(Locale.ROOT).indexOf(search.toUpperCase(Locale.ROOT));
    }

    public record OutboxSql(
        String createTableSql,
        String claimForUpdateSql,
        String markPublishedSql,
        List<String> payloadColumns
    ) {}
}
