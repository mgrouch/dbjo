package org.github.dbjo.meta.db;

public record Col(
        int pos,
        String colName,
        int sqlType,
        String typeName,
        int size,
        int scale,
        int nullable,
        String isAutoIncrement,
        String defaultValue
) {}
