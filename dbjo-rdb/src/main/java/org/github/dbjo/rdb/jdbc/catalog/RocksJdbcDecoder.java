package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.SQLException;

@FunctionalInterface
public interface RocksJdbcDecoder {
    Object decode(byte[] valueBytes) throws SQLException;
}
