package org.github.dbjo.rdb.jdbc.remote.dto;

import java.util.Objects;

public record RemoteRocksJdbcColumnDto(
        int pos,
        String name,
        int sqlType,
        String typeName,
        int size,
        int scale,
        boolean nullable,
        String isAutoIncrement,
        String defaultValue,
        String getterName
) {
    public RemoteRocksJdbcColumnDto {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(typeName, "typeName");
        Objects.requireNonNull(isAutoIncrement, "isAutoIncrement");
        Objects.requireNonNull(getterName, "getterName");
    }
}
