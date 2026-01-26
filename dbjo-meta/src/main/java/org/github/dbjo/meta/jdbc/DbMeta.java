package org.github.dbjo.meta.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;

public interface DbMeta<T> {
    String schema();
    String table();
    String fqn();

    String insertSql();
    String updateByIdSql();
    String selectAllSql();

    Object[] insertParams(T e);
    SQLType[] insertParamTypes();

    Object[] updateByIdParams(T e);
    SQLType[] updateByIdParamTypes();

    // Upsert
    String upsertByIdSql(DbDialect dialect);
    Object[] upsertByIdParams(T e);
    SQLType[] upsertByIdParamTypes();

    // Optional temp-table batch upsert support
    default boolean supportsUpsertTemp(DbDialect dialect) { return false; }

    default String createUpsertTempTableSql(DbDialect dialect, String suffix) {
        throw new UnsupportedOperationException("Temp upsert not supported for " + dialect);
    }
    default String dropUpsertTempTableSql(DbDialect dialect, String suffix) {
        throw new UnsupportedOperationException("Temp upsert not supported for " + dialect);
    }
    default String insertUpsertTempSql(DbDialect dialect, String suffix) {
        throw new UnsupportedOperationException("Temp upsert not supported for " + dialect);
    }
    default String mergeUpsertFromTempSql(DbDialect dialect, String suffix) {
        throw new UnsupportedOperationException("Temp upsert not supported for " + dialect);
    }

    T fromRow(ResultSet rs) throws SQLException;
}
