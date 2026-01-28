package org.github.dbjo.rdb.jdbc.catalog;

import java.util.Arrays;
import java.util.Objects;

public record RocksJdbcIndex(
        String indexName,
        boolean unique,
        String[] columnNames // in ordinal position order
) {
    public RocksJdbcIndex {
        Objects.requireNonNull(indexName, "indexName");
        Objects.requireNonNull(columnNames, "columnNames");
        columnNames = columnNames.clone();
    }

    @Override
    public String toString() {
        return "RocksJdbcIndex{" +
                "indexName='" + indexName + '\'' +
                ", unique=" + unique +
                ", columnNames=" + Arrays.toString(columnNames) +
                '}';
    }
}
