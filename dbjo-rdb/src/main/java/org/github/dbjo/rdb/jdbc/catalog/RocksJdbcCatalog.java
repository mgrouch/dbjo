package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.SQLException;
import java.util.List;

public interface RocksJdbcCatalog {
    List<RocksJdbcTable> tables();
    RocksJdbcTable table(String name);

    default RocksJdbcTable requireTable(String name) throws SQLException {
        RocksJdbcTable t = table(name);
        if (t == null) throw new SQLException("Unknown table: " + name);
        return t;
    }
}
