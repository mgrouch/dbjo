package org.github.dbjo.kafka.outbox.jdbc;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import org.github.dbjo.meta.jdbc.DbMeta;

/**
 * Builds MSSQL outbox SQL from an already-generated {@link DbMeta} payload table mapping.
 */
public final class MsSqlOutboxSqlBuilder {
    private MsSqlOutboxSqlBuilder() {}

    public static OutboxSql build(DbMeta<?> payloadMeta, String outboxTableFqn) {
        Objects.requireNonNull(payloadMeta, "payloadMeta must not be null");
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
        String createSql = """
            SELECT TOP (0)
                   %s
              INTO %s
              FROM %s;

            ALTER TABLE %s ADD
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

            ALTER TABLE %s ADD CONSTRAINT pk_%s_outbox_id PRIMARY KEY (outbox_id);
            CREATE UNIQUE INDEX ux_%s_sequence_no ON %s(sequence_no);
            CREATE INDEX ix_%s_pending ON %s(published_at_utc, lock_owner, sequence_no);
            """.formatted(
            payloadProjection,
            outboxTableFqn,
            payloadMeta.fqn(),
            outboxTableFqn,
            outboxTableFqn,
            sanitizeForConstraint(outboxTableFqn),
            sanitizeForConstraint(outboxTableFqn),
            outboxTableFqn,
            sanitizeForConstraint(outboxTableFqn),
            outboxTableFqn
        );

        String claimSql = """
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
                   locked_at_utc = SYSUTCDATETIME()
            OUTPUT inserted.outbox_id,
                   inserted.sequence_no,
                   %s
            """.formatted(payloadProjection, outboxTableFqn, insertedPayloadProjection);

        String markPublishedSql = """
            UPDATE %s
               SET published_topic = :topic,
                   published_partition = :partition,
                   published_offset = :offset,
                   published_timestamp_utc = :publishedTimestampUtc,
                   published_at_utc = SYSUTCDATETIME(),
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
