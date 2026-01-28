package org.github.dbjo.rdb.jdbc.catalog;

import org.github.dbjo.criteria.Condition;
import org.github.dbjo.criteria.PropertyTerm;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Default planner/executor runtime:
 *  - executes a RocksJdbcPlan via scanTable/scanIndex* hooks
 *  - applies WHERE (AST evaluation), projection, and limit
 *  - (optional) validates WHERE via Criteria compiler (RocksJdbcWhereCompiler)
 *
 * Extend this and implement scanTable() and tableMeta().
 * Override scanIndex*() to actually use secondary indexes.
 *
 * NOTE: This class is intentionally portable: it does NOT use CachedRowSet or com.sun.rowset.
 */
public abstract class AbstractRocksJdbcCatalog implements RocksJdbcCatalog {

    /** Required: full table scan returning rows as column->value maps. */
    protected abstract Iterable<Map<String, Object>> scanTable(String tableName) throws SQLException;

    /** Optional: index equality scan. Default falls back to full scan. */
    protected Iterable<Map<String, Object>> scanIndexEq(String tableName, RocksJdbcPlan.IndexEq ap) throws SQLException {
        return scanTable(tableName);
    }

    /** Optional: index range scan. Default falls back to full scan. */
    protected Iterable<Map<String, Object>> scanIndexRange(String tableName, RocksJdbcPlan.IndexRange ap) throws SQLException {
        return scanTable(tableName);
    }

    /** Optional: index IN scan. Default falls back to full scan. */
    protected Iterable<Map<String, Object>> scanIndexIn(String tableName, RocksJdbcPlan.IndexIn ap) throws SQLException {
        return scanTable(tableName);
    }

    /**
     * Optional: provide terms-by-column map so we can compile WHERE into a Criteria Condition
     * (validation + keeps the Criteria layer used).
     *
     * Return null if you don’t have criteria terms for this table yet.
     */
    @SuppressWarnings("unused")
    protected <B extends java.io.Serializable> Map<String, PropertyTerm<B, ? extends java.io.Serializable>>
    termsByColumnLower(String tableName) throws SQLException {
        return null;
    }

    @Override
    public final ResultSet execute(RocksJdbcPlan plan) throws SQLException {
        Objects.requireNonNull(plan, "plan");

        if (plan instanceof RocksJdbcPlan.ListTables lt) {
            return executeListTables(lt);
        }
        if (plan instanceof RocksJdbcPlan.Select sel) {
            return executeSelect(sel);
        }
        if (plan instanceof RocksJdbcPlan.Count cnt) {
            return executeCount(cnt);
        }
        throw new SQLException("Unknown plan: " + plan.getClass().getName());
    }

    private ResultSet executeListTables(RocksJdbcPlan.ListTables lt) throws SQLException {
        List<String> tables = new ArrayList<>(listTables());
        Integer lim = lt.limit();
        if (lim != null && lim >= 0 && tables.size() > lim) tables = tables.subList(0, lim);

        List<String> cols = List.of("TABLE_NAME");
        List<Map<String, Object>> rows = new ArrayList<>(tables.size());
        for (String t : tables) rows.add(Map.of("TABLE_NAME", t));

        return RocksJdbcResultSets.fromRows(cols, rows);
    }

    private ResultSet executeSelect(RocksJdbcPlan.Select sel) throws SQLException {
        String table = sel.table();
        RocksJdbcTableMeta meta = tableMeta(table);

        // Parse WHERE AST (used for planning + runtime filtering)
        RocksJdbcWhere.Expr whereAst = RocksJdbcWhere.parse(sel.whereSql());

        // Optional: compile WHERE using Criteria API for validation / continuity
        tryCompileCriteriaWhere(table, sel.whereSql());

        // Determine projection columns
        List<String> projCols = normalizeProjection(sel.projection(), meta);

        Iterable<Map<String, Object>> rowsIt = rowsForAccessPath(table, sel.accessPath());

        List<Map<String, Object>> out = new ArrayList<>();
        int outCount = 0;
        Integer lim = sel.limit();

        for (Map<String, Object> row : rowsIt) {
            if (whereAst != null && !eval(whereAst, row)) continue;

            LinkedHashMap<String, Object> projRow = new LinkedHashMap<>();
            for (String c : projCols) projRow.put(c, getCol(row, c));
            out.add(projRow);

            outCount++;
            if (lim != null && lim >= 0 && outCount >= lim) break;
        }

        return RocksJdbcResultSets.fromRows(projCols, out);
    }

    private ResultSet executeCount(RocksJdbcPlan.Count cnt) throws SQLException {
        String table = cnt.table();
        RocksJdbcWhere.Expr whereAst = RocksJdbcWhere.parse(cnt.whereSql());

        tryCompileCriteriaWhere(table, cnt.whereSql());

        Iterable<Map<String, Object>> rowsIt = rowsForAccessPath(table, cnt.accessPath());

        long n = 0;
        Integer lim = cnt.limit();
        for (Map<String, Object> row : rowsIt) {
            if (whereAst != null && !eval(whereAst, row)) continue;
            n++;
            if (lim != null && lim >= 0 && n >= lim) break;
        }

        return RocksJdbcResultSets.fromRows(
                List.of("COUNT"),
                List.of(Map.of("COUNT", n))
        );
    }

    private Iterable<Map<String, Object>> rowsForAccessPath(String table, RocksJdbcPlan.AccessPath ap) throws SQLException {
        if (ap == null || ap instanceof RocksJdbcPlan.FullScan) return scanTable(table);
        if (ap instanceof RocksJdbcPlan.IndexEq x) return scanIndexEq(table, x);
        if (ap instanceof RocksJdbcPlan.IndexRange x) return scanIndexRange(table, x);
        if (ap instanceof RocksJdbcPlan.IndexIn x) return scanIndexIn(table, x);
        return scanTable(table);
    }

    private void tryCompileCriteriaWhere(String table, String whereSql) throws SQLException {
        if (whereSql == null || whereSql.isBlank()) return;

        Map<?, ?> terms = termsByColumnLowerUnchecked(table);
        if (terms == null || terms.isEmpty()) return;

        // Compile for validation + to keep Criteria layer active (even if execution uses AST evaluation here)
        @SuppressWarnings({"rawtypes", "unchecked"})
        Map<String, PropertyTerm<java.io.Serializable, ? extends java.io.Serializable>> casted = (Map) terms;

        @SuppressWarnings({"rawtypes", "unchecked"})
        Condition<java.io.Serializable> ignored =
                (Condition) RocksJdbcWhereCompiler.compile(whereSql, (Map) casted);
    }

    @SuppressWarnings({"rawtypes"})
    private Map<?, ?> termsByColumnLowerUnchecked(String table) throws SQLException {
        return (Map) termsByColumnLower(table);
    }

    private static List<String> normalizeProjection(List<String> proj, RocksJdbcTableMeta meta) throws SQLException {
        if (proj == null || proj.isEmpty()) return new ArrayList<>(meta.columns());

        ArrayList<String> out = new ArrayList<>(proj.size());
        for (String item : proj) {
            String col = normalizeIdent(item);
            if (col.isEmpty()) throw new SQLException("Bad projection item: " + item);
            out.add(col);
        }
        return out;
    }

    private static String normalizeIdent(String ident) {
        String raw = (ident == null) ? "" : ident.trim();
        int dot = raw.lastIndexOf('.');
        if (dot >= 0) raw = raw.substring(dot + 1).trim();
        if (raw.length() >= 2) {
            char a = raw.charAt(0), b = raw.charAt(raw.length() - 1);
            if ((a == '"' && b == '"') || (a == '`' && b == '`')) raw = raw.substring(1, raw.length() - 1);
        }
        return raw;
    }

    // ---------------- WHERE evaluation (AST) ----------------

    private static boolean eval(RocksJdbcWhere.Expr e, Map<String, Object> row) throws SQLException {
        if (e instanceof RocksJdbcWhere.And a) return eval(a.left(), row) && eval(a.right(), row);
        if (e instanceof RocksJdbcWhere.Or o)  return eval(o.left(), row) || eval(o.right(), row);
        if (e instanceof RocksJdbcWhere.Not n) return !eval(n.inner(), row);

        if (e instanceof RocksJdbcWhere.IsNull p) {
            Object v = getCol(row, p.col());
            boolean isNull = (v == null);
            return p.not() != isNull;
        }
        if (e instanceof RocksJdbcWhere.Between p) {
            Object v = getCol(row, p.col());
            if (v == null || p.a() == null || p.b() == null) return false;
            return cmp(v, p.a()) >= 0 && cmp(v, p.b()) <= 0;
        }
        if (e instanceof RocksJdbcWhere.In p) {
            Object v = getCol(row, p.col());
            if (v == null) return false;
            for (Object x : p.values()) {
                if (Objects.equals(normalizeCompare(v), normalizeCompare(x))) return true;
            }
            return false;
        }
        if (e instanceof RocksJdbcWhere.Cmp p) {
            Object v = getCol(row, p.col());
            Object lit = p.value();

            return switch (p.op()) {
                case EQ -> Objects.equals(normalizeCompare(v), normalizeCompare(lit));
                case NE -> !Objects.equals(normalizeCompare(v), normalizeCompare(lit));
                case LT -> v != null && lit != null && cmp(v, lit) < 0;
                case LE -> v != null && lit != null && cmp(v, lit) <= 0;
                case GT -> v != null && lit != null && cmp(v, lit) > 0;
                case GE -> v != null && lit != null && cmp(v, lit) >= 0;
            };
        }

        throw new SQLException("Unsupported WHERE node: " + e.getClass().getName());
    }

    /** Case-insensitive column lookup (also tolerates "table.col"). */
    private static Object getCol(Map<String, Object> row, String colIdent) {
        if (row == null) return null;
        String col = normalizeKey(colIdent);

        if (row.containsKey(colIdent)) return row.get(colIdent);
        if (row.containsKey(col)) return row.get(col);

        for (Map.Entry<String, Object> e : row.entrySet()) {
            if (e.getKey() != null && e.getKey().equalsIgnoreCase(col)) return e.getValue();
        }
        return null;
    }

    private static String normalizeKey(String ident) {
        String raw = (ident == null) ? "" : ident.trim();
        int dot = raw.lastIndexOf('.');
        if (dot >= 0) raw = raw.substring(dot + 1).trim();
        if (raw.length() >= 2) {
            char a = raw.charAt(0), b = raw.charAt(raw.length() - 1);
            if ((a == '"' && b == '"') || (a == '`' && b == '`')) raw = raw.substring(1, raw.length() - 1);
        }
        return raw.toLowerCase(Locale.ROOT);
    }

    private static Object normalizeCompare(Object x) {
        if (x == null) return null;
        if (x instanceof LocalDateTime ldt) return java.sql.Timestamp.valueOf(ldt);
        if (x instanceof LocalDate ld) return java.sql.Date.valueOf(ld);
        return x;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int cmp(Object a, Object b) throws SQLException {
        a = normalizeCompare(a);
        b = normalizeCompare(b);
        if (a == null || b == null) throw new SQLException("Cannot compare NULL");

        if (a instanceof Number && b instanceof Number) {
            java.math.BigDecimal da = new java.math.BigDecimal(a.toString());
            java.math.BigDecimal db = new java.math.BigDecimal(b.toString());
            return da.compareTo(db);
        }

        if (a instanceof Comparable && a.getClass().isInstance(b)) {
            return ((Comparable) a).compareTo(b);
        }

        if (a instanceof java.util.Date && b instanceof java.util.Date) {
            return Long.compare(((java.util.Date) a).getTime(), ((java.util.Date) b).getTime());
        }

        return a.toString().compareTo(b.toString());
    }
}
