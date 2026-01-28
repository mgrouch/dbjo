package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Runtime interface for Rocks JDBC catalog.
 *
 * Primary metadata API (what the generator emits):
 *  - tables()
 *  - table(name)
 *  - requireTable(name)
 *
 * Everything else is OPTIONAL/defaulted for compatibility with query execution layers.
 */
public interface RocksJdbcCatalog {

    // Metadata API (matches GeneratedRocksJdbcCatalog)

    /** All known tables (rich metadata). */
    List<RocksJdbcTable> tables();

    /**
     * Lookup table by name/alias (case-insensitive).
     * Generator overrides this with a fast map lookup.
     */
    default RocksJdbcTable table(String name) {
        if (name == null) return null;
        String k = name.trim().toLowerCase(Locale.ROOT);
        if (k.isEmpty()) return null;

        for (RocksJdbcTable t : tables()) {
            for (String n : t.names()) {
                if (n != null && n.trim().toLowerCase(Locale.ROOT).equals(k)) return t;
            }
        }
        return null;
    }

    /** Like table(name) but throws SQLException if missing. */
    default RocksJdbcTable requireTable(String name) throws SQLException {
        RocksJdbcTable t = table(name);
        if (t != null) return t;
        throw new SQLException("Unknown table: " + name);
    }

    // Compatibility helpers (optional)

    /** Old-style listTables helper (derived from tables()). */
    default List<String> listTables() {
        HashSet<String> uniq = new HashSet<>();
        for (RocksJdbcTable t : tables()) {
            if (t.tableName() != null) uniq.add(t.tableName());
        }
        return new ArrayList<>(uniq);
    }

    /**
     * Optional: execution entry for Statements.
     * Implement in your executor catalog; default throws.
     */
    default ResultSet runQuery(String sql, int maxRows) throws SQLException {
        throw new SQLException("Catalog does not support queries: " + getClass().getName());
    }

    /** Optional alias names some code may call. */
    default ResultSet query(String sql, int maxRows) throws SQLException { return runQuery(sql, maxRows); }
    default ResultSet query(String sql) throws SQLException { return runQuery(sql, 0); }

    /**
     * Optional planner/executor entry.
     * If you add planner/executor layer, override this in your executor catalog.
     */
    default ResultSet execute(RocksJdbcPlan plan) throws SQLException {
        Objects.requireNonNull(plan, "plan");
        throw new SQLException("Catalog does not support plans: " + getClass().getName());
    }
}
