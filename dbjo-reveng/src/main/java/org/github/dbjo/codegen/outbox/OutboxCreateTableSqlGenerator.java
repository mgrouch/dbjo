package org.github.dbjo.codegen.outbox;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.meta.db.TableRef;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Generates a single SQL script with outbox CREATE TABLE statement based on one selected entity table.
 */
public final class OutboxCreateTableSqlGenerator {
    private final Config cfg;

    public OutboxCreateTableSqlGenerator(Config cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg must not be null");
    }

    public Path generate(List<TableModel> tables) throws IOException {
        TableModel selected = selectTable(tables);
        String outboxTableFqn = resolveOutboxTableFqn(selected.table());

        String payloadProjection = selected.cols().stream()
            .map(c -> c.colName())
            .collect(Collectors.joining(",\n       "));

        String sourceTableFqn = fqn(selected.table());
        String constraintPrefix = sanitizeForConstraint(outboxTableFqn);

        String sql = """
            SELECT TOP (0)
                   %s,
                   CAST('' AS NVARCHAR(100)) AS outbox_id,
                   CAST(0 AS BIGINT) AS sequence_no,
                   CAST(NULL AS NVARCHAR(255)) AS partition_key,
                   CAST(0 AS BIGINT) AS occurred_at_epoch_ms,
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
            """.formatted(payloadProjection, outboxTableFqn, sourceTableFqn,
            constraintPrefix, outboxTableFqn,
            constraintPrefix, outboxTableFqn,
            constraintPrefix, outboxTableFqn);

        Path outputDir = cfg.outboxSqlDir();
        Files.createDirectories(outputDir);

        String fileName = selected.table().table().toLowerCase(Locale.ROOT) + "-outbox-create.sql";
        Path outFile = outputDir.resolve(fileName);
        Files.writeString(outFile, sql, StandardCharsets.UTF_8);
        return outFile;
    }

    private TableModel selectTable(List<TableModel> tables) {
        if (tables == null || tables.isEmpty()) {
            throw new IllegalArgumentException("No table available to generate outbox SQL");
        }

        String configuredEntity = cfg.outboxEntity();
        if (configuredEntity != null && !configuredEntity.isBlank()) {
            String expected = configuredEntity.trim().toLowerCase(Locale.ROOT);
            List<TableModel> matching = tables.stream()
                .filter(t -> t.table().table() != null && t.table().table().toLowerCase(Locale.ROOT).equals(expected))
                .toList();

            if (matching.isEmpty()) {
                throw new IllegalArgumentException("No table matched --outboxEntity=" + configuredEntity);
            }
            if (matching.size() > 1) {
                throw new IllegalArgumentException("Multiple tables matched --outboxEntity=" + configuredEntity +
                    ". Refine filters using --schemaInclude or --tableInclude.");
            }
            return matching.get(0);
        }

        if (tables.size() != 1) {
            throw new IllegalArgumentException("Outbox SQL generation requires one table. " +
                "Use --outboxEntity=<table> or table filters to select a single table.");
        }
        return tables.get(0);
    }

    private String resolveOutboxTableFqn(TableRef src) {
        String configured = cfg.outboxTableFqn();
        if (configured != null && !configured.isBlank()) {
            return configured;
        }

        String tableName = src.table() + "_outbox";
        if (src.schema() == null || src.schema().isBlank()) {
            return tableName;
        }
        return src.schema() + "." + tableName;
    }

    private static String fqn(TableRef tableRef) {
        if (tableRef.schema() == null || tableRef.schema().isBlank()) {
            return tableRef.table();
        }
        return tableRef.schema() + "." + tableRef.table();
    }

    private static String sanitizeForConstraint(String fqn) {
        return fqn.replace('.', '_').replace('[', '_').replace(']', '_').replace('"', '_');
    }
}
