package org.github.dbjo.meta.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Locale;

public enum DbDialect {
    MSSQL(128, "#"),
    SYBASE(30, "#"),
    ORACLE(30, null),
    HSQL(128, null);

    private final int identMaxLen;
    private final String tempPrefix;

    DbDialect(int identMaxLen, String tempPrefix) {
        this.identMaxLen = identMaxLen;
        this.tempPrefix = tempPrefix;
    }

    public int identMaxLen() { return identMaxLen; }

    /** True for the “temp table load + single MERGE” strategy. */
    public boolean prefersTempMergeBatch() {
        return this == MSSQL || this == SYBASE;
    }

    public boolean supportsTempTables() { return tempPrefix != null; }

    public String tempPrefix() { return tempPrefix; }

    public static DbDialect from(DatabaseMetaData md) throws SQLException {
        return fromProductName(md.getDatabaseProductName());
    }

    public static DbDialect fromProductName(String productName) {
        if (productName == null) throw new IllegalArgumentException("productName is null");
        String s = productName.toLowerCase(Locale.ROOT);

        if (s.contains("microsoft") || s.contains("sql server")) return MSSQL;
        if (s.contains("sybase")) return SYBASE;
        if (s.contains("oracle")) return ORACLE;
        if (s.contains("hsql")) return HSQL;

        throw new IllegalArgumentException("Unknown DB dialect for productName=" + productName);
    }
}
