package org.github.dbjo.meta.jdbc;

public abstract class DbMetaUpsertSupport<T> implements DbMeta<T> {

    // ---- per-dialect upsert sql (generated) ----
    protected abstract String upsertByIdSqlMssql();
    protected abstract String upsertByIdSqlSybase();
    protected abstract String upsertByIdSqlOracle(); // regular Oracle MERGE (no temp)
    protected abstract String upsertByIdSqlHsql();

    // ---- temp-table batch plumbing (generated only if used) ----
    protected abstract String upsertTempColDefs();
    protected abstract String upsertTempInsertColumns();
    protected abstract int upsertTempParamCount();

    protected abstract String mergeFromTempTplMssql();  // contains "{TEMP}"
    protected abstract String mergeFromTempTplSybase(); // contains "{TEMP}"

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
    public final boolean supportsUpsertTemp(DbDialect dialect) {
        return dialect == DbDialect.MSSQL || dialect == DbDialect.SYBASE;
    }

    @Override
    public final String createUpsertTempTableSql(DbDialect dialect, String suffix) {
        if (!supportsUpsertTemp(dialect)) return DbMeta.super.createUpsertTempTableSql(dialect, suffix);
        String tn = upsertTempName(dialect, suffix);
        return "CREATE TABLE " + tn + " (" + upsertTempColDefs() + ")";
    }

    @Override
    public final String dropUpsertTempTableSql(DbDialect dialect, String suffix) {
        if (!supportsUpsertTemp(dialect)) return DbMeta.super.dropUpsertTempTableSql(dialect, suffix);
        String tn = upsertTempName(dialect, suffix);
        return "DROP TABLE " + tn;
    }

    @Override
    public final String insertUpsertTempSql(DbDialect dialect, String suffix) {
        if (!supportsUpsertTemp(dialect)) return DbMeta.super.insertUpsertTempSql(dialect, suffix);
        String tn = upsertTempName(dialect, suffix);
        return "INSERT INTO " + tn + " (" + upsertTempInsertColumns() + ") VALUES (" + qmarks(upsertTempParamCount()) + ")";
    }

    @Override
    public final String mergeUpsertFromTempSql(DbDialect dialect, String suffix) {
        if (!supportsUpsertTemp(dialect)) return DbMeta.super.mergeUpsertFromTempSql(dialect, suffix);
        String tn = upsertTempName(dialect, suffix);
        String tpl = (dialect == DbDialect.MSSQL) ? mergeFromTempTplMssql() : mergeFromTempTplSybase();
        return tpl.replace("{TEMP}", tn);
    }

    // ---- shared helpers (not generated per table) ----

    protected final String upsertTempName(DbDialect dialect, String suffix) {
        String sfx = safeSuffix(suffix);
        // Local temp tables for MSSQL/Sybase
        return "#" + table() + "_UPSERT_" + sfx;
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
