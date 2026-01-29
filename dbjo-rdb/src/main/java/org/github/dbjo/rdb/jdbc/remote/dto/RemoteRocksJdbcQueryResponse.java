package org.github.dbjo.rdb.jdbc.remote.dto;

import java.util.Objects;

public record RemoteRocksJdbcQueryResponse(String rowsetXml) {
    public RemoteRocksJdbcQueryResponse {
        Objects.requireNonNull(rowsetXml, "rowsetXml");
    }
}
