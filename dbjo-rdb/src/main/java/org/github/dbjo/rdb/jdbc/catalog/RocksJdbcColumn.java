package org.github.dbjo.rdb.jdbc.catalog;

import java.util.Objects;

public record RocksJdbcColumn(
        int pos,                 // 1-based position
        String name,
        int sqlType,
        String typeName,
        int size,
        int scale,
        int nullable,            // DatabaseMetaData.columnNullable / columnNoNulls / columnNullableUnknown
        String isAutoIncrement,  // "YES"/"NO"/""
        String defaultValue,
        String getterName        // Java bean getter on row class
) {
    public RocksJdbcColumn {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(typeName, "typeName");
        Objects.requireNonNull(isAutoIncrement, "isAutoIncrement");
        Objects.requireNonNull(getterName, "getterName");
    }

    public boolean autoIncrement() {
        return "YES".equalsIgnoreCase(isAutoIncrement);
    }

    public boolean nullableBool() {
        return nullable == java.sql.DatabaseMetaData.columnNullable;
    }
}
