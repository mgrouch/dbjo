package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Runtime interface for Rocks JDBC catalog.
 *
 * Generator emits:
 *  - tables()
 *  - table(String)
 *  - requireTable(String)
 *
 * Executor expects:
 *  - tableMeta(String)
 *
 * Query execution is optional:
 *  - runQuery(sql,maxRows)
 *  - execute(plan)
 */
public interface RocksJdbcCatalog {

    // ----------------------------------------------------------------------
    // Metadata API (matches GeneratedRocksJdbcCatalog)
    // ----------------------------------------------------------------------

    List<RocksJdbcTable> tables();

    /** Case-insensitive table lookup by alias/name. */
    default RocksJdbcTable table(String name) {
        if (name == null) return null;
        String k = name.trim().toLowerCase(Locale.ROOT);
        if (k.isEmpty()) return null;

        for (RocksJdbcTable t : tables()) {
            for (String n : t.names()) {
                if (n == null) continue;
                if (n.trim().toLowerCase(Locale.ROOT).equals(k)) return t;
            }
            if (t.tableName() != null && t.tableName().trim().toLowerCase(Locale.ROOT).equals(k)) return t;
        }
        return null;
    }

    default RocksJdbcTable requireTable(String name) throws SQLException {
        RocksJdbcTable t = table(name);
        if (t != null) return t;
        throw new SQLException("Unknown table: " + name);
    }

    /** Convenience for "SHOW TABLES". */
    default List<String> listTables() throws SQLException {
        LinkedHashSet<String> uniq = new LinkedHashSet<>();
        for (RocksJdbcTable t : tables()) {
            if (t.tableName() != null && !t.tableName().isBlank()) uniq.add(t.tableName());
        }
        return new ArrayList<>(uniq);
    }

    /**
     * Adapter used by RocksJdbcExecutor (and planner/executor layer).
     * Builds RocksJdbcTableMeta(tableName, columns, indexes) from RocksJdbcTable.
     */
    default RocksJdbcTableMeta tableMeta(String tableName) throws SQLException {
        RocksJdbcTable t = requireTable(tableName);

        // Columns
        RocksJdbcColumn[] colsArr = t.columns();
        ArrayList<String> cols = new ArrayList<>(colsArr.length);
        for (RocksJdbcColumn c : colsArr) cols.add(c.name());

        // Index metas:
        //  1) Add PRIMARY key meta if pkColumns() present
        //  2) Add secondary indexes
        String[] pk = t.pkColumns();
        ArrayList<RocksJdbcTableMeta.IndexMeta> idx = new ArrayList<>();

        boolean hasPrimary = pk != null && pk.length > 0;
        if (hasPrimary) {
            idx.add(new RocksJdbcTableMeta.IndexMeta(
                    "PRIMARY",
                    listOf(pk),
                    true,
                    true
            ));
        }

        RocksJdbcIndex[] indexes = t.indexes();
        for (RocksJdbcIndex ix : indexes) {
            if (ix == null) continue;

            String ixName = ix.indexName();
            boolean unique = ix.unique();
            String[] ixCols = ix.columnNames();

            boolean primary = hasPrimary && sameColsIgnoreCase(ixCols, pk);

            // Avoid duplicating PRIMARY if it matches pk
            if (primary && hasPrimary) continue;

            idx.add(new RocksJdbcTableMeta.IndexMeta(
                    (ixName == null || ixName.isBlank()) ? "IDX" : ixName,
                    listOf(ixCols),
                    unique,
                    primary
            ));
        }

        return new RocksJdbcTableMeta(t.tableName(), cols, idx);
    }

    // ----------------------------------------------------------------------
    // Optional execution hooks (Statements / Driver)
    // ----------------------------------------------------------------------

    default ResultSet runQuery(String sql, int maxRows) throws SQLException {
        throw new SQLException("Catalog does not support queries: " + getClass().getName());
    }

    default ResultSet query(String sql, int maxRows) throws SQLException { return runQuery(sql, maxRows); }
    default ResultSet query(String sql) throws SQLException { return runQuery(sql, 0); }

    default ResultSet execute(RocksJdbcPlan plan) throws SQLException {
        Objects.requireNonNull(plan, "plan");
        throw new SQLException("Catalog does not support plans: " + getClass().getName());
    }

    // ----------------------------------------------------------------------
    // Small helpers (typed, no reflection)
    // ----------------------------------------------------------------------

    private static List<String> listOf(String[] arr) {
        if (arr == null || arr.length == 0) return List.of();
        ArrayList<String> out = new ArrayList<>(arr.length);
        for (String s : arr) if (s != null) out.add(s);
        return out;
    }

    private static boolean sameColsIgnoreCase(String[] a, String[] b) {
        if (a == null || b == null) return false;
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            String x = a[i], y = b[i];
            if (x == null || y == null) return false;
            if (!x.equalsIgnoreCase(y)) return false;
        }
        return true;
    }
}
