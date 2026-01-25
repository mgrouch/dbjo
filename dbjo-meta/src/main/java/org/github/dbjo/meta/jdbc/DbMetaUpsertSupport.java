// File: src/main/java/org/github/dbjo/meta/jdbc/DbMetaUpsertSupport.java
package org.github.dbjo.meta.jdbc;

import java.util.Locale;

public abstract class DbMetaUpsertSupport<T> implements DbMeta<T> {

    // ---- table-specific hooks (generated classes implement these) ----

    protected abstract String upsertByIdSqlMssql();
    protected abstract String upsertByIdSqlSybase();
    protected abstract String upsertByIdSqlOracle();
    protected abstract String upsertByIdSqlHsql();

    protected abstract String upsertTempColDefs();            // "ID BIGINT NOT NULL, EMAIL VARCHAR(255) NOT NULL, ..."
    protected abstract String upsertTempInsertColumns();      // "ID, EMAIL, NAME, CREATED_AT"
    protected abstract int upsertTempParamCount();            // number of '?' in temp insert row

    protected abstract String mergeFromTempTplMssql();        // contains "{TEMP}"
    protected abstract String mergeFromTempTplSybase();
    protected abstract String mergeFromTempTplOracle();
    protected abstract String mergeFromTempTplHsql();

    // ---- DbMeta upsert API ----

    @Override
    public final String upsertByIdSql(DbDialect dialect) {
        if (dialect == null) throw new IllegalArgumentException("dialect is null");
        return switch (dialect) {
            case MSSQL  -> upsertByIdSqlMssql();
            case SYBASE -> upsertByIdSqlSybase();
            case ORACLE -> upsertByIdSqlOracle();
            case HSQL   -> upsertByIdSqlHsql();
        };
    }

    @Override
    public final String createUpsertTempTableSql(DbDialect dialect, String suffix) {
        if (dialect == null) throw new IllegalArgumentException("dialect is null");
        String tn = upsertTempName(dialect, suffix);
        String defs = upsertTempColDefs();

        return switch (dialect) {
            case MSSQL, SYBASE ->
                    "CREATE TABLE " + tn + " (" + defs + ")";
            case ORACLE, HSQL ->
                    "CREATE GLOBAL TEMPORARY TABLE " + tn + " (" + defs + ") ON COMMIT DELETE ROWS";
        };
    }

    @Override
    public final String dropUpsertTempTableSql(DbDialect dialect, String suffix) {
        if (dialect == null) throw new IllegalArgumentException("dialect is null");
        String tn = upsertTempName(dialect, suffix);
        return "DROP TABLE " + tn;
    }

    @Override
    public final String insertUpsertTempSql(DbDialect dialect, String suffix) {
        if (dialect == null) throw new IllegalArgumentException("dialect is null");
        String tn = upsertTempName(dialect, suffix);

        // single-row insert template; runtime batch builder uses PreparedStatement batching
        return "INSERT INTO " + tn + " (" + upsertTempInsertColumns() + ") VALUES (" + qmarks(upsertTempParamCount()) + ")";
    }

    @Override
    public final String mergeUpsertFromTempSql(DbDialect dialect, String suffix) {
        if (dialect == null) throw new IllegalArgumentException("dialect is null");
        String tn = upsertTempName(dialect, suffix);
        String tpl = switch (dialect) {
            case MSSQL  -> mergeFromTempTplMssql();
            case SYBASE -> mergeFromTempTplSybase();
            case ORACLE -> mergeFromTempTplOracle();
            case HSQL   -> mergeFromTempTplHsql();
        };
        return tpl.replace("{TEMP}", tn);
    }

    // ---- shared helpers (NO LONGER GENERATED PER TABLE) ----

    protected final String upsertTempName(DbDialect dialect, String suffix) {
        String sfx = safeSuffix(suffix);
        String base = table() + "_UPSERT_" + sfx;
        return switch (dialect) {
            case MSSQL, SYBASE -> "#" + base;
            case ORACLE, HSQL  -> base;
        };
    }

    protected static String safeSuffix(String suffix) {
        String s = (suffix == null) ? "" : suffix.trim();
        if (s.isEmpty()) return "X";

        StringBuilder b = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if ((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_') b.append(ch);
            else b.append('_');
        }
        String out = b.toString();
        return out.length() > 32 ? out.substring(0, 32) : out;
    }

    private static String qmarks(int n) {
        if (n <= 0) return "";
        StringBuilder sb = new StringBuilder(n * 3);
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append(", ");
            sb.append("?");
        }
        return sb.toString();
    }
}
