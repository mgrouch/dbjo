package org.github.dbjo.codegen.model;

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
