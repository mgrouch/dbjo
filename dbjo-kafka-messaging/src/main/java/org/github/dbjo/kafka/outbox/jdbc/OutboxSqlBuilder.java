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

        String payloadProjection = String.join(",\n           ", payloadColumns);
        String insertedPayloadProjection = payloadColumns.stream()
            .map(c -> "inserted." + c)
            .collect(Collectors.joining(",\n                   "));
        String createSql = switch (dialect) {
            case MSSQL, SYBASE -> """
                SELECT TOP (0)
                       %s,
                       CAST('' AS NVARCHAR(100)) AS outbox_id,
                       CAST(0 AS BIGINT) AS sequence_no,
                       CAST(NULL AS NVARCHAR(120)) AS lock_owner,
                       CAST(NULL AS DATETIME2) AS locked_at_utc,
                       CAST(NULL AS NVARCHAR(255)) AS published_topic,
                       CAST(NULL AS INT) AS published_partition,
                       CAST(NULL AS BIGINT) AS published_offset,
                       CAST(NULL AS DATETIME2) AS published_timestamp_utc,
                       CAST(NULL AS DATETIME2) AS published_at_utc,
                       CAST(CURRENT_TIMESTAMP AS DATETIME2) AS created_at_utc
                  INTO %s
                  FROM %s;

                CREATE UNIQUE INDEX ux_%s_outbox_id ON %s(outbox_id);
                CREATE UNIQUE INDEX ux_%s_sequence_no ON %s(sequence_no);
                CREATE INDEX ix_%s_pending ON %s(published_at_utc, lock_owner, sequence_no);
                """.formatted(
                payloadProjection,
                outboxTableFqn,
                payloadMeta.fqn(),
                sanitizeForConstraint(outboxTableFqn),
                outboxTableFqn,
                sanitizeForConstraint(outboxTableFqn),
                outboxTableFqn,
                sanitizeForConstraint(outboxTableFqn),
                outboxTableFqn
            );
            case HSQL, ORACLE -> """
                CREATE TABLE %s AS
                SELECT %s,
                       CAST('' AS VARCHAR(100)) AS outbox_id,
                       CAST(0 AS BIGINT) AS sequence_no,
                       CAST(NULL AS VARCHAR(120)) AS lock_owner,
                       CAST(NULL AS TIMESTAMP) AS locked_at_utc,
                       CAST(NULL AS VARCHAR(255)) AS published_topic,
                       CAST(NULL AS INTEGER) AS published_partition,
                       CAST(NULL AS BIGINT) AS published_offset,
                       CAST(NULL AS TIMESTAMP) AS published_timestamp_utc,
                       CAST(NULL AS TIMESTAMP) AS published_at_utc,
                       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_at_utc
                  FROM %s
                 WHERE 1 = 0;

                CREATE UNIQUE INDEX ux_%s_outbox_id ON %s(outbox_id);
                CREATE UNIQUE INDEX ux_%s_sequence_no ON %s(sequence_no);
                CREATE INDEX ix_%s_pending ON %s(published_at_utc, lock_owner, sequence_no);
                """.formatted(
                outboxTableFqn,
                payloadProjection,
                payloadMeta.fqn(),
                sanitizeForConstraint(outboxTableFqn),
                outboxTableFqn,
                sanitizeForConstraint(outboxTableFqn),
                outboxTableFqn,
                sanitizeForConstraint(outboxTableFqn),
                outboxTableFqn
            );
        };

        String claimSql = switch (dialect) {
            case MSSQL -> """
                WITH next_rows AS (
                    SELECT TOP (:batchSize)
                           outbox_id,
                           sequence_no,
                           %s
                    FROM %s WITH (ROWLOCK, UPDLOCK, READPAST)
                    WHERE published_at_utc IS NULL AND lock_owner IS NULL
                    ORDER BY sequence_no ASC
                )
                UPDATE next_rows
                   SET lock_owner = :lockOwner,
                       locked_at_utc = CURRENT_TIMESTAMP
                OUTPUT inserted.outbox_id,
                       inserted.sequence_no,
                       %s
                """.formatted(payloadProjection, outboxTableFqn, insertedPayloadProjection);
            case SYBASE, HSQL, ORACLE -> """
                UPDATE %s
                   SET lock_owner = :lockOwner,
                       locked_at_utc = CURRENT_TIMESTAMP
                 WHERE outbox_id IN (
                       SELECT outbox_id
                         FROM %s
                        WHERE published_at_utc IS NULL AND lock_owner IS NULL
                        ORDER BY sequence_no ASC
                        FETCH FIRST :batchSize ROWS ONLY
                 );

                SELECT outbox_id,
                       sequence_no,
                       %s
                  FROM %s
                 WHERE lock_owner = :lockOwner
                   AND published_at_utc IS NULL
                 ORDER BY sequence_no ASC
                 FETCH FIRST :batchSize ROWS ONLY
                """.formatted(outboxTableFqn, outboxTableFqn, payloadProjection, outboxTableFqn);
        };

        String markPublishedSql = """
            UPDATE %s
               SET published_topic = :topic,
                   published_partition = :partition,
                   published_offset = :offset,
                   published_timestamp_utc = :publishedTimestampUtc,
                   published_at_utc = CURRENT_TIMESTAMP,
                   lock_owner = NULL,
                   locked_at_utc = NULL
             WHERE outbox_id = :outboxId
            """.formatted(outboxTableFqn);

        return new OutboxSql(createSql, claimSql, markPublishedSql, payloadColumns);
    }

    private static String sanitizeForConstraint(String fqn) {
        return fqn.replace('.', '_').replace('[', '_').replace(']', '_').replace('"', '_');
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
