package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public interface RocksJdbcCatalog {

    /** For SHOW/LIST TABLES. */
    List<String> listTables() throws SQLException;

    /** Used by the planner (columns + indexes). */
    RocksJdbcTableMeta tableMeta(String tableName) throws SQLException;

    /**
     * Execute a planned query. This is where index path + projection + where + limit are applied.
     */
    ResultSet execute(RocksJdbcPlan plan) throws SQLException;
}
