// File: src/main/java/org/github/dbjo/meta/jdbc/DbMeta.java
package org.github.dbjo.meta.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Locale;

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

    T fromRow(ResultSet rs) throws SQLException;

    // ------------------------------------------------------------------
    // Upsert (single row)
    // ------------------------------------------------------------------

    /**
     * Dialect-specific "upsert by primary key" SQL.
     * The generated SQL uses the entity's PK columns for matching.
     */
    default String upsertByIdSql(DbDialect dialect) {
        throw new UnsupportedOperationException("upsertByIdSql not implemented for " + fqn());
    }

    /** Parameters for upsertByIdSql(..) in the exact placeholder order. */
    default Object[] upsertByIdParams(T e) {
        throw new UnsupportedOperationException("upsertByIdParams not implemented for " + fqn());
    }

    /** SQL types for upsertByIdParams(..) in the exact placeholder order. */
    default SQLType[] upsertByIdParamTypes() {
        throw new UnsupportedOperationException("upsertByIdParamTypes not implemented for " + fqn());
    }

    // ------------------------------------------------------------------
    // Batch upsert helpers (temp table + merge)
    // ------------------------------------------------------------------

    /**
     * Create temp table used for batch upsert. Name includes caller-provided suffix.
     * (Suffix is sanitized by generated implementations to avoid SQL injection.)
     */
    default String createUpsertTempTableSql(DbDialect dialect, String suffix) {
        throw new UnsupportedOperationException("createUpsertTempTableSql not implemented for " + fqn());
    }

    /** Optional (but recommended) cleanup. */
    default String dropUpsertTempTableSql(DbDialect dialect, String suffix) {
        throw new UnsupportedOperationException("dropUpsertTempTableSql not implemented for " + fqn());
    }

    /**
     * Insert one row into the upsert temp table. Execute repeatedly for batch load.
     * Params/types match upsertTempParams/Types (defaults to upsertById* ordering).
     */
    default String insertUpsertTempSql(DbDialect dialect, String suffix) {
        throw new UnsupportedOperationException("insertUpsertTempSql not implemented for " + fqn());
    }

    /** Defaults to the same param ordering as upsertById. */
    default Object[] upsertTempParams(T e) { return upsertByIdParams(e); }

    /** Defaults to the same type ordering as upsertById. */
    default SQLType[] upsertTempParamTypes() { return upsertByIdParamTypes(); }

    /**
     * Merge temp rows into the real table.
     * On MSSQL/Sybase this is the preferred fast path for multi-row upsert.
     */
    default String mergeUpsertFromTempSql(DbDialect dialect, String suffix) {
        throw new UnsupportedOperationException("mergeUpsertFromTempSql not implemented for " + fqn());
    }
}
