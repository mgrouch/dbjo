package org.github.dbjo.meta.jdbc;

import java.util.Locale;

public enum DbDialect {
    MSSQL,
    SYBASE,
    ORACLE,
    HSQL;

    /** Best-effort mapping from DatabaseMetaData.getDatabaseProductName(). */
    public static DbDialect fromProductName(String productName) {
        if (productName == null) throw new IllegalArgumentException("productName is null");
        String n = productName.toLowerCase(Locale.ROOT);

        if (n.contains("microsoft") || n.contains("sql server")) return MSSQL;

        // Covers Adaptive Server Enterprise (ASE), SQL Anywhere, etc.
        if (n.contains("sybase") || n.contains("adaptive server") || n.contains("ase") || n.contains("sql anywhere"))
            return SYBASE;

        if (n.contains("oracle")) return ORACLE;

        if (n.contains("hsql")) return HSQL;

        throw new IllegalArgumentException("Unsupported DB product: " + productName);
    }
}
