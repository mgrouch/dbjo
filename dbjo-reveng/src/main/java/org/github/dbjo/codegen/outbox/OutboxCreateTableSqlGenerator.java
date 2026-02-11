package org.github.dbjo.codegen.outbox;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.meta.db.TableRef;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generates a single SQL script with outbox CREATE TABLE statement based on one selected entity table.
 */
public final class OutboxCreateTableSqlGenerator {
    private static final Set<String> LENGTH_TYPES = Set.of(
        "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "BINARY", "VARBINARY"
    );
    private static final Set<String> PRECISION_SCALE_TYPES = Set.of(
        "DECIMAL", "NUMERIC"
    );
    private static final Set<String> PRECISION_TYPES = Set.of(
        "FLOAT"
    );

    private final Config cfg;

    public OutboxCreateTableSqlGenerator(Config cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg must not be null");
    }

    public Path generate(List<TableModel> tables) throws IOException {
        TableModel selected = selectTable(tables);
        String outboxTableFqn = resolveOutboxTableFqn(selected.table());

        String payloadColumns = selected.cols().stream()
            .map(this::columnDefinition)
            .collect(Collectors.joining(",\n    "));

        String constraintPrefix = sanitizeForConstraint(outboxTableFqn);

        String sql = """
            CREATE TABLE %s (
                %s,
                outbox_id NVARCHAR(100) NOT NULL,
                sequence_no BIGINT NOT NULL,
                partition_key NVARCHAR(40) NULL,
                occurred_at_epoch_ms BIGINT NOT NULL,
                published_partition INT NULL,
                published_offset BIGINT NULL,
                published_timestamp_utc DATETIME2 NULL,
                published_at_utc DATETIME2 NULL,
                created_at_utc DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE UNIQUE INDEX ux_%s_outbox_id ON %s(outbox_id);
            CREATE UNIQUE INDEX ux_%s_sequence_no ON %s(sequence_no);
            CREATE INDEX ix_%s_pending ON %s(published_at_utc, sequence_no);
            """.formatted(outboxTableFqn, payloadColumns,
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

    private String columnDefinition(Col col) {
        String nullability = col.nullable() ? "NULL" : "NOT NULL";
        return col.colName() + " " + sqlType(col) + " " + nullability;
    }

    private static String sqlType(Col col) {
        String typeName = col.typeName() == null ? "" : col.typeName().trim();
        if (typeName.isEmpty()) {
            return "NVARCHAR(255)";
        }

        String normalized = typeName.toUpperCase(Locale.ROOT);
        if (LENGTH_TYPES.contains(normalized) && col.size() > 0) {
            return normalized + "(" + col.size() + ")";
        }
        if (PRECISION_SCALE_TYPES.contains(normalized) && col.size() > 0) {
            if (col.scale() > 0) {
                return normalized + "(" + col.size() + "," + col.scale() + ")";
            }
            return normalized + "(" + col.size() + ")";
        }
        if (PRECISION_TYPES.contains(normalized) && col.size() > 0) {
            return normalized + "(" + col.size() + ")";
        }

        return normalized;
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

    private static String sanitizeForConstraint(String fqn) {
        return fqn.replace('.', '_').replace('[', '_').replace(']', '_').replace('"', '_');
    }
}
