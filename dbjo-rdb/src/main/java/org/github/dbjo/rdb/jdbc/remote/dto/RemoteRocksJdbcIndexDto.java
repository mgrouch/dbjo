package org.github.dbjo.rdb.jdbc.remote.dto;

import java.util.List;
import java.util.Objects;

public record RemoteRocksJdbcIndexDto(
        String indexName,
        boolean unique,
        List<String> columnNames
) {
    public RemoteRocksJdbcIndexDto {
        Objects.requireNonNull(indexName, "indexName");
        Objects.requireNonNull(columnNames, "columnNames");
    }
}
