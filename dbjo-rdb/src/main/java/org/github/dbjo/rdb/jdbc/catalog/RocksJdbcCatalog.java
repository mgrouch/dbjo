package org.github.dbjo.rdb.jdbc.catalog;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compatibility-first catalog API.
 *
 * Generated catalogs have historically varied between:
 *  - runQuery(sql,maxRows) vs executeSql(sql,maxRows) vs query(sql)
 *  - tableMeta(table) vs tableMeta(schema,table)
 *  - listTables() vs listTables(schema)
 *
 * This interface provides DEFAULT overloads/adapters so older generated catalogs keep compiling.
 */
public interface RocksJdbcCatalog {

    // -------------------------------------------------------------------------
    // Metadata (schema-aware overloads)
    // -------------------------------------------------------------------------

    /**
     * List tables. Default delegates to schema-aware form.
     * Generated code may override either listTables() or listTables(schema).
     */
    default List<String> listTables() throws SQLException {
        return listTables(null);
    }

    /**
     * List tables for a schema (schema may be null).
     * Default throws; generated catalogs can override this.
     */
    default List<String> listTables(String schema) throws SQLException {
        throw new SQLException("listTables(schema) not implemented by " + getClass().getName());
    }

    /**
     * Table metadata. Default delegates to schema-aware form.
     * Generated code may override either tableMeta(table) or tableMeta(schema,table).
     */
    default RocksJdbcTableMeta tableMeta(String tableName) throws SQLException {
        return tableMeta(null, tableName);
    }

    /**
     * Table metadata for a schema (schema may be null).
     * Default throws; generated catalogs can override this.
     */
    default RocksJdbcTableMeta tableMeta(String schema, String tableName) throws SQLException {
        throw new SQLException("tableMeta(schema,table) not implemented by " + getClass().getName());
    }

    // -------------------------------------------------------------------------
    // SQL execution entrypoints (multiple aliases)
    // -------------------------------------------------------------------------

    /**
     * Primary runtime entrypoint used by RocksJdbcConnection/Statement.
     * Default delegates to executeSql(sql,maxRows). Generated code can override either.
     */
    default ResultSet runQuery(String sql, int maxRows) throws SQLException {
        return executeSql(sql, maxRows);
    }

    /**
     * Alternate name used by some generated code.
     * Default delegates to runQuery(sql,maxRows).
     */
    default ResultSet executeSql(String sql, int maxRows) throws SQLException {
        // If neither is overridden, this will recurse. Detect and fail cleanly.
        if (!overrides(getClass(), "runQuery", String.class, int.class)
                && !overrides(getClass(), "executeSql", String.class, int.class)) {
            throw new SQLException("Catalog must override runQuery(sql,maxRows) or executeSql(sql,maxRows): "
                    + getClass().getName());
        }
        // If executeSql() was not overridden but runQuery() was, call runQuery().
        if (!overrides(getClass(), "executeSql", String.class, int.class)) {
            return runQuery(sql, maxRows);
        }
        // If we are here, executeSql *is* overridden; but this default shouldn't be called.
        throw new SQLException("executeSql default should not be reached for " + getClass().getName());
    }

    /** Convenience alias: query(sql,maxRows). */
    default ResultSet query(String sql, int maxRows) throws SQLException {
        return runQuery(sql, maxRows);
    }

    /** Convenience alias: query(sql). */
    default ResultSet query(String sql) throws SQLException {
        return runQuery(sql, 0);
    }

    /** Convenience alias: execute(sql). */
    default ResultSet execute(String sql) throws SQLException {
        return runQuery(sql, 0);
    }

    /** Convenience alias: execute(sql,maxRows). */
    default ResultSet execute(String sql, int maxRows) throws SQLException {
        return runQuery(sql, maxRows);
    }

    // -------------------------------------------------------------------------
    // Planner/executor entry (new style)
    // -------------------------------------------------------------------------

    /**
     * Execute a pre-built plan.
     *
     * Default adapter: render plan to SQL and call runQuery().
     * Planner-aware catalogs (e.g. AbstractRocksJdbcCatalog) can override execute(plan).
     */
    default ResultSet execute(RocksJdbcPlan plan) throws SQLException {
        String sql = renderPlanToSql(plan);
        int mr = maxRowsFromPlan(plan);
        return runQuery(sql, mr);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static boolean overrides(Class<?> cls, String name, Class<?>... params) {
        try {
            return cls.getMethod(name, params).getDeclaringClass() != RocksJdbcCatalog.class;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static int maxRowsFromPlan(RocksJdbcPlan plan) {
        if (plan instanceof RocksJdbcPlan.ListTables lt) {
            Integer lim = lt.limit();
            return lim == null ? 0 : lim;
        }
        if (plan instanceof RocksJdbcPlan.Select s) {
            Integer lim = s.limit();
            return lim == null ? 0 : lim;
        }
        if (plan instanceof RocksJdbcPlan.Count c) {
            Integer lim = c.limit();
            return lim == null ? 0 : lim;
        }
        return 0;
    }

    private static String renderPlanToSql(RocksJdbcPlan plan) throws SQLException {
        if (plan instanceof RocksJdbcPlan.ListTables lt) {
            if (lt.limit() == null) return "SHOW TABLES";
            return "SHOW TABLES LIMIT " + lt.limit();
        }

        if (plan instanceof RocksJdbcPlan.Count c) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT COUNT(*) FROM ").append(c.table());
            if (c.whereSql() != null && !c.whereSql().isBlank()) sb.append(" WHERE ").append(c.whereSql());
            if (c.limit() != null) sb.append(" LIMIT ").append(c.limit());
            return sb.toString();
        }

        if (plan instanceof RocksJdbcPlan.Select s) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ");
            if (s.projection() == null || s.projection().isEmpty()) sb.append("*");
            else sb.append(String.join(", ", s.projection()));
            sb.append(" FROM ").append(s.table());
            if (s.whereSql() != null && !s.whereSql().isBlank()) sb.append(" WHERE ").append(s.whereSql());
            if (s.limit() != null) sb.append(" LIMIT ").append(s.limit());
            return sb.toString();
        }

        throw new SQLException("Unknown plan type: " + plan.getClass().getName());
    }

    // (Optional) If you later want: parse SQL -> plan, keep using your existing planner.
    // I’m not forcing it here to avoid constructor-order mismatches.
}
