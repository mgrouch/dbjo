package org.github.dbjo.codegen.outbox;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.meta.db.TableRef;
import org.github.dbjo.meta.jdbc.DbDialect;

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
    private static final Set<String> OUTBOX_COLUMN_NAMES = Set.of(
        "outbox_id",
        "sequence_no",
        "partition_key",
        "occurred_at_epoch_ms",
        "published_partition",
        "published_offset",
        "published_timestamp_utc",
        "published_at_utc",
        "created_at_utc"
    );

    private final Config cfg;
    private final DbDialect dialect;

    public OutboxCreateTableSqlGenerator(Config cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg must not be null");
        this.dialect = detectDialect(cfg);
    }

    public Path generate(List<TableModel> tables) throws IOException {
        TableModel selected = selectTable(tables);
        String outboxTableFqn = resolveOutboxTableFqn(selected.table());
        String quotedOutboxTableFqn = quoteFqn(outboxTableFqn);

        String payloadColumns = selected.cols().stream()
            .filter(col -> !isOutboxColumn(col.colName()))
            .map(this::columnDefinition)
            .collect(Collectors.joining(",\n    "));

        String constraintPrefix = sanitizeForConstraint(outboxTableFqn);

        String sql = """
            CREATE TABLE %s (
                %s,
                %s %s NOT NULL,
                %s %s NOT NULL,
                %s %s NULL,
                %s %s NOT NULL,
                %s %s NULL,
                %s %s NULL,
                %s %s NULL,
                %s %s NULL,
                %s %s NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE UNIQUE INDEX ux_%s_outbox_id ON %s(%s);
            CREATE UNIQUE INDEX ux_%s_sequence_no ON %s(%s);
            CREATE INDEX ix_%s_pending ON %s(%s, %s);
            """.formatted(quotedOutboxTableFqn, payloadColumns,
            quoteId("outbox_id"), stringType(100),
            quoteId("sequence_no"), bigintType(),
            quoteId("partition_key"), stringType(40),
            quoteId("occurred_at_epoch_ms"), bigintType(),
            quoteId("published_partition"), intType(),
            quoteId("published_offset"), bigintType(),
            quoteId("published_timestamp_utc"), timestampType(),
            quoteId("published_at_utc"), timestampType(),
            quoteId("created_at_utc"), timestampType(),
            constraintPrefix, quotedOutboxTableFqn, quoteId("outbox_id"),
            constraintPrefix, quotedOutboxTableFqn, quoteId("sequence_no"),
            constraintPrefix, quotedOutboxTableFqn, quoteId("published_at_utc"), quoteId("sequence_no"));

        Path outputDir = cfg.outboxSqlDir();
        Files.createDirectories(outputDir);

        String fileName = selected.table().table().toLowerCase(Locale.ROOT) + "-outbox-create.sql";
        Path outFile = outputDir.resolve(fileName);
        Files.writeString(outFile, sql, StandardCharsets.UTF_8);
        return outFile;
    }

    private String columnDefinition(Col col) {
        String nullability = col.nullable() ? "NULL" : "NOT NULL";
        return quoteId(col.colName()) + " " + sqlType(col) + " " + nullability;
    }

    private String quoteFqn(String fqn) {
        String[] parts = fqn.split("\\.");
        return java.util.Arrays.stream(parts)
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(this::quoteId)
            .collect(Collectors.joining("."));
    }

    private String quoteId(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("identifier must not be blank");
        }
        if ((id.startsWith("[") && id.endsWith("]")) || (id.startsWith("\"") && id.endsWith("\""))) {
            return id;
        }
        return switch (dialect) {
            case MSSQL, SYBASE -> "[" + id + "]";
            case ORACLE, HSQL -> "\"" + id.replace("\"", "\"\"") + "\"";
        };
    }

    private String stringType(int size) {
        return switch (dialect) {
            case ORACLE -> "VARCHAR2(" + size + " CHAR)";
            case MSSQL, SYBASE, HSQL -> "NVARCHAR(" + size + ")";
        };
    }

    private String bigintType() {
        return dialect == DbDialect.ORACLE ? "NUMBER(19)" : "BIGINT";
    }

    private String intType() {
        return dialect == DbDialect.ORACLE ? "NUMBER(10)" : "INT";
    }

    private String timestampType() {
        return switch (dialect) {
            case MSSQL, SYBASE -> "DATETIME2";
            case ORACLE, HSQL -> "TIMESTAMP";
        };
    }

    private static DbDialect detectDialect(Config cfg) {
        String probe = ((cfg.url() == null ? "" : cfg.url()) + " " + (cfg.driver() == null ? "" : cfg.driver()))
            .toLowerCase(Locale.ROOT);
        if (probe.contains("sqlserver") || probe.contains("mssql") || probe.contains("jtds")) {
            return DbDialect.MSSQL;
        }
        if (probe.contains("sybase")) {
            return DbDialect.SYBASE;
        }
        if (probe.contains("oracle")) {
            return DbDialect.ORACLE;
        }
        return DbDialect.HSQL;
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

    private static boolean isOutboxColumn(String columnName) {
        return columnName != null && OUTBOX_COLUMN_NAMES.contains(columnName.toLowerCase(Locale.ROOT));
    }
}
