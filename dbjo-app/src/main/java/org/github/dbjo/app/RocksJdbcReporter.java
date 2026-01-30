package org.github.dbjo.app;

import org.github.dbjo.meta.jdbc.JdbcUtil;
import org.github.dbjo.rdb.jdbc.RocksJdbcEngine;

import java.sql.ResultSet;
import java.sql.SQLException;

public record RocksJdbcReporter(RocksJdbcEngine engine) {

    public void reportTables() throws SQLException {
        try (ResultSet resultSet = engine.query("select * from tables", 0)) {
            JdbcUtil.printResultSet(System.out, resultSet);
        }
    }
}
