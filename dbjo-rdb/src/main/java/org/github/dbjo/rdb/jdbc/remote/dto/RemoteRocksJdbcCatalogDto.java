package org.github.dbjo.rdb.jdbc.remote.dto;

import java.util.List;
import java.util.Objects;

public record RemoteRocksJdbcCatalogDto(List<RemoteRocksJdbcTableDto> tables) {
    public RemoteRocksJdbcCatalogDto {
        Objects.requireNonNull(tables, "tables");
    }
}
