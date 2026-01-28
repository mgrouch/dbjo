package org.github.dbjo.rdb.jdbc.catalog;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class RocksJdbcTable {
    private final String tableName;
    private final String cfName;

    private final Class<?> rowClass;

    private final String[] columnNames;
    private final int[] columnSqlTypes;
    private final String[] getterNames;

    private final RocksJdbcDecoder decoder;

    private final String[] names; // aliases for lookup / metadata

    public RocksJdbcTable(
            String tableName,
            String cfName,
            Class<?> rowClass,
            String[] columnNames,
            int[] columnSqlTypes,
            String[] getterNames,
            RocksJdbcDecoder decoder,
            String[] names
    ) {
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.cfName = Objects.requireNonNull(cfName, "cfName");
        this.rowClass = Objects.requireNonNull(rowClass, "rowClass");
        this.columnNames = Objects.requireNonNull(columnNames, "columnNames");
        this.columnSqlTypes = Objects.requireNonNull(columnSqlTypes, "columnSqlTypes");
        this.getterNames = Objects.requireNonNull(getterNames, "getterNames");
        this.decoder = Objects.requireNonNull(decoder, "decoder");
        this.names = (names == null) ? new String[] { tableName } : names.clone();

        if (columnNames.length != columnSqlTypes.length || columnNames.length != getterNames.length) {
            throw new IllegalArgumentException("columnNames/columnSqlTypes/getterNames must have same length");
        }
    }

    public String tableName() { return tableName; }
    public String cfName() { return cfName; }
    public Class<?> rowClass() { return rowClass; }

    public String[] columnNames() { return columnNames.clone(); }
    public int[] columnSqlTypes() { return columnSqlTypes.clone(); }
    public String[] getterNames() { return getterNames.clone(); }

    public RocksJdbcDecoder decoder() { return decoder; }

    public List<String> names() { return List.of(names); }

    @Override
    public String toString() {
        return "RocksJdbcTable{" +
                "tableName='" + tableName + '\'' +
                ", cfName='" + cfName + '\'' +
                ", rowClass=" + rowClass.getName() +
                ", columns=" + Arrays.toString(columnNames) +
                '}';
    }
}
