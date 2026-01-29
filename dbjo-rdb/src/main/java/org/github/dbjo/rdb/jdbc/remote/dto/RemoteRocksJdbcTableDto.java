package org.github.dbjo.rdb.jdbc.remote.dto;

import java.util.List;
import java.util.Objects;

public record RemoteRocksJdbcTableDto(
        String schemaName,
        String tableName,
        String cfName,
        List<RemoteRocksJdbcColumnDto> columns,
        List<String> pkColumns,
        List<RemoteRocksJdbcIndexDto> indexes,
        List<String> names
) {
    public RemoteRocksJdbcTableDto {
        Objects.requireNonNull(schemaName, "schemaName");
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(cfName, "cfName");
        Objects.requireNonNull(columns, "columns");
        Objects.requireNonNull(pkColumns, "pkColumns");
        Objects.requireNonNull(indexes, "indexes");
        Objects.requireNonNull(names, "names");
    }
}
