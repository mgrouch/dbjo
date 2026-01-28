package org.github.dbjo.rdb.jdbc.catalog;

import java.util.List;
import java.util.Objects;

public record RocksJdbcTableMeta(
        String tableName,
        List<String> columns,
        List<IndexMeta> indexes
) {
    public RocksJdbcTableMeta {
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(columns, "columns");
        Objects.requireNonNull(indexes, "indexes");
    }

    public record IndexMeta(
            String name,
            List<String> columns,
            boolean unique,
            boolean primary
    ) {
        public IndexMeta {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(columns, "columns");
        }
    }
}
