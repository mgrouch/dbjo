package org.github.dbjo.rdb.jdbc.remote.dto;

import java.util.Objects;

public record RemoteRocksJdbcQueryRequest(String sql, int maxRows) {
    public RemoteRocksJdbcQueryRequest {
        Objects.requireNonNull(sql, "sql");
    }
}
