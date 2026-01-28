package org.github.dbjo.rdb.jdbc.catalog;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Backwards-compatible catalog API.
 *
 * - Old/generated catalogs typically implement: listTables/tableMeta/runQuery.
 * - New planner/executor catalogs implement: execute(RocksJdbcPlan) (e.g. AbstractRocksJdbcCatalog).
 *
 * This interface provides DEFAULT adapters so either style compiles and runs.
 */
public interface RocksJdbcCatalog {

    // ---- metadata ----

    List<String> listTables() throws SQLException;

    RocksJdbcTableMeta tableMeta(String tableName) throws SQLException;

    // ---- primary query entry (old style) ----

    /**
     * Execute SQL query with optional maxRows.
     * This is what RocksJdbcConnection/Statement can call.
     */
    default ResultSet runQuery(String sql, int maxRows) throws SQLException {
        // Default implementation for "new style" catalogs that override execute(plan).
        // If execute is not overridden, we must fail (otherwise we'd recurse).
        if (!overridesExecute()) {
            throw new SQLException("Catalog must override runQuery(sql,maxRows) or execute(plan): " + getClass().getName());
        }
        RocksJdbcPlan plan = planFromSql(sql, maxRows);
        return execute(plan);
    }

    /** Alias (some code calls query instead of runQuery). */
    default ResultSet query(String sql, int maxRows) throws SQLException {
        return runQuery(sql, maxRows);
    }

    /** Alias (no maxRows). */
    default ResultSet query(String sql) throws SQLException {
        return runQuery(sql, 0);
    }

    // ---- planner/executor entry (new style) ----

    /**
     * Execute a pre-built plan.
     *
     * Default implementation for "old style" catalogs that override runQuery(sql,maxRows).
     * If runQuery is not overridden, we must fail (otherwise we'd recurse).
     */
    default ResultSet execute(RocksJdbcPlan plan) throws SQLException {
        if (!overridesRunQuery()) {
            throw new SQLException("Catalog must override execute(plan) or runQuery(sql,maxRows): " + getClass().getName());
        }
        String sql = renderPlanToSql(plan);
        int mr = maxRowsFromPlan(plan);
        return runQuery(sql, mr);
    }

    // ---- helpers ----

    private boolean overridesExecute() {
        try {
            return getClass().getMethod("execute", RocksJdbcPlan.class).getDeclaringClass() != RocksJdbcCatalog.class;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private boolean overridesRunQuery() {
        try {
            return getClass().getMethod("runQuery", String.class, int.class).getDeclaringClass() != RocksJdbcCatalog.class;
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

    private static RocksJdbcPlan planFromSql(String sql, int maxRows) throws SQLException {
        RocksJdbcSql.Parsed p = RocksJdbcSql.parse(sql);

        Integer lim = p.limit();
        if (lim == null && maxRows > 0) lim = maxRows;
        if (lim != null && lim < 0) lim = null;

        RocksJdbcPlan.AccessPath full = newRecord(RocksJdbcPlan.FullScan.class, new HashMap<>());

        return switch (p.kind()) {
            case LIST_TABLES -> {
                HashMap<String, Object> m = new HashMap<>();
                m.put("limit", lim);
                yield newRecord(RocksJdbcPlan.ListTables.class, m);
            }

            case COUNT -> {
                HashMap<String, Object> m = new HashMap<>();
                m.put("table", p.tableName());
                m.put("whereSql", p.whereSql());
                m.put("limit", lim);
                m.put("accessPath", full);
                yield newRecord(RocksJdbcPlan.Count.class, m);
            }

            case SELECT -> {
                HashMap<String, Object> m = new HashMap<>();
                m.put("table", p.tableName());
                m.put("projection", p.projection());
                m.put("whereSql", p.whereSql());
                m.put("limit", lim);
                m.put("accessPath", full);
                yield newRecord(RocksJdbcPlan.Select.class, m);
            }
        };
    }

    /**
     * Instantiate a record by matching values to record component NAMES,
     * so we don't care about canonical constructor parameter order.
     */
    private static <T> T newRecord(Class<T> rc, Map<String, Object> values) throws SQLException {
        try {
            if (!rc.isRecord()) throw new SQLException("Not a record: " + rc.getName());

            RecordComponent[] comps = rc.getRecordComponents();
            Class<?>[] ptypes = new Class<?>[comps.length];
            Object[] args = new Object[comps.length];

            for (int i = 0; i < comps.length; i++) {
                RecordComponent c = comps[i];
                ptypes[i] = c.getType();

                Object v = values.get(c.getName()); // may be null
                args[i] = coerceForParam(ptypes[i], v);
            }

            @SuppressWarnings("unchecked")
            Constructor<T> ctor = (Constructor<T>) rc.getDeclaredConstructor(ptypes);
            ctor.setAccessible(true);
            return ctor.newInstance(args);
        } catch (SQLException e) {
            throw e;
        } catch (ReflectiveOperationException e) {
            throw new SQLException("Failed to construct record " + rc.getName() + " with values " + values.keySet(), e);
        }
    }

    private static Object coerceForParam(Class<?> pt, Object v) {
        if (v == null) {
            if (pt.isPrimitive()) {
                if (pt == boolean.class) return false;
                if (pt == byte.class) return (byte) 0;
                if (pt == short.class) return (short) 0;
                if (pt == int.class) return 0;
                if (pt == long.class) return 0L;
                if (pt == float.class) return 0f;
                if (pt == double.class) return 0d;
                if (pt == char.class) return (char) 0;
            }
            return null;
        }

        // handle common primitive wrappers
        if (pt == int.class || pt == Integer.class) {
            if (v instanceof Number n) return n.intValue();
        }
        if (pt == long.class || pt == Long.class) {
            if (v instanceof Number n) return n.longValue();
        }
        if (pt == boolean.class || pt == Boolean.class) {
            if (v instanceof Boolean b) return b;
        }

        return v;
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
}
