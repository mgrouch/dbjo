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

    // --- Upsert (required for your request) ---
    String upsertByIdSql(DbDialect dialect);
    Object[] upsertByIdParams(T e);
    SQLType[] upsertByIdParamTypes();

    // --- Optional temp-table batch upsert support ---
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

    // ----------------------------------------------------------------------
    // Criteria SQL support (property-name based)
    // ----------------------------------------------------------------------

    /** camelCase -> snake_case by default (globalRegion -> global_region). */
    default String columnOf(String propertyName) {
        if (propertyName == null) return null;
        String s = propertyName.trim();
        if (s.isEmpty()) return s;
        if (s.indexOf('_') >= 0) return s; // already snake_case

        StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isUpperCase(ch)) {
                if (i > 0) {
                    char prev = s.charAt(i - 1);
                    if (Character.isLowerCase(prev) || Character.isDigit(prev)) out.append('_');
                }
                out.append(Character.toLowerCase(ch));
            } else {
                out.append(ch);
            }
        }
        return out.toString();
    }

    /** SQL-ready column identifier (override if you need quoting/case preservation). */
    default String columnSql(String propertyName) {
        return columnOf(propertyName);
    }
}
