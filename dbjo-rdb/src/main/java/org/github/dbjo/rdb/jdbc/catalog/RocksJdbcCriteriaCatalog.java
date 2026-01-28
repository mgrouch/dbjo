package org.github.dbjo.rdb.jdbc.catalog;

import org.github.dbjo.criteria.Condition;
import org.github.dbjo.criteria.PropertyTerm;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Concrete planner/executor catalog:
 *   plan (index/full scan) -> execute -> WHERE filter -> projection -> limit
 *
 * Storage-agnostic: you provide per-table access adapters (scanAll / scanIndex*).
 */
public final class RocksJdbcCriteriaCatalog implements RocksJdbcCatalog {

    public interface TableAccess<B extends Serializable> {
        RocksJdbcTableMeta meta();

        /** Optional: used only to compile WHERE into Criteria Condition for validation/continuity. */
        default Map<String, PropertyTerm<B, ? extends Serializable>> termsByColumnLower() { return null; }

        /** Required: full scan. Row is a column->value map. */
        Iterable<Map<String, Object>> scanAll() throws SQLException;

        /** Optional: index equality scan. Default falls back to scanAll(). */
        default Iterable<Map<String, Object>> scanIndexEq(RocksJdbcPlan.IndexEq ap) throws SQLException { return scanAll(); }

        /** Optional: index range scan. Default falls back to scanAll(). */
        default Iterable<Map<String, Object>> scanIndexRange(RocksJdbcPlan.IndexRange ap) throws SQLException { return scanAll(); }

        /** Optional: index IN scan. Default falls back to scanAll(). */
        default Iterable<Map<String, Object>> scanIndexIn(RocksJdbcPlan.IndexIn ap) throws SQLException { return scanAll(); }
    }

    public static final class Builder {
        private final Map<String, TableAccess<?>> tables = new LinkedHashMap<>();

        public <B extends Serializable> Builder addTable(String tableName, TableAccess<B> access) {
            Objects.requireNonNull(tableName, "tableName");
            Objects.requireNonNull(access, "access");
            tables.put(normTable(tableName), access);
            return this;
        }

        public RocksJdbcCriteriaCatalog build() {
            return new RocksJdbcCriteriaCatalog(tables);
        }
    }

    public static Builder builder() { return new Builder(); }

    private final Map<String, TableAccess<?>> tablesByLower;

    public RocksJdbcCriteriaCatalog(Map<String, TableAccess<?>> tablesByLower) {
        Objects.requireNonNull(tablesByLower, "tablesByLower");
        this.tablesByLower = new LinkedHashMap<>();
        for (var e : tablesByLower.entrySet()) {
            this.tablesByLower.put(normTable(e.getKey()), Objects.requireNonNull(e.getValue(), "table access"));
        }
    }

    @Override
    public List<String> listTables() {
        ArrayList<String> out = new ArrayList<>();
        for (TableAccess<?> ta : tablesByLower.values()) out.add(ta.meta().tableName());
        return out;
    }

    @Override
    public RocksJdbcTableMeta tableMeta(String tableName) throws SQLException {
        return table(tableName).meta();
    }

    @Override
    public ResultSet execute(RocksJdbcPlan plan) throws SQLException {
        Objects.requireNonNull(plan, "plan");

        if (plan instanceof RocksJdbcPlan.ListTables lt) {
            return execListTables(lt);
        }
        if (plan instanceof RocksJdbcPlan.Select sel) {
            return execSelect(sel);
        }
        if (plan instanceof RocksJdbcPlan.Count cnt) {
            return execCount(cnt);
        }
        throw new SQLException("Unknown plan: " + plan.getClass().getName());
    }

    // ---------------- execution ----------------

    private ResultSet execListTables(RocksJdbcPlan.ListTables lt) throws SQLException {
        List<String> tables = new ArrayList<>(listTables());
        Integer lim = lt.limit();
        if (lim != null && lim >= 0 && tables.size() > lim) tables = tables.subList(0, lim);

        List<String> cols = List.of("TABLE_NAME");
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String t : tables) rows.add(Map.of("TABLE_NAME", t));

        return RocksJdbcResultSets.fromRows(cols, rows);
    }

    private ResultSet execCount(RocksJdbcPlan.Count cnt) throws SQLException {
        TableAccess<?> ta = table(cnt.table());

        RocksJdbcWhere.Expr whereAst = RocksJdbcWhere.parse(cnt.whereSql());
        tryCompileCriteria(cnt.table(), cnt.whereSql(), ta);

        Iterable<Map<String, Object>> rowsIt = rowsForAccessPath(ta, cnt.accessPath());

        long n = 0;
        Integer lim = cnt.limit();
        for (Map<String, Object> row : rowsIt) {
            if (whereAst != null && !eval(whereAst, row)) continue;
            n++;
            if (lim != null && lim >= 0 && n >= lim) break;
        }

        return RocksJdbcResultSets.fromRows(List.of("COUNT"), List.of(Map.of("COUNT", n)));
    }

    private ResultSet execSelect(RocksJdbcPlan.Select sel) throws SQLException {
        TableAccess<?> ta = table(sel.table());
        RocksJdbcTableMeta meta = ta.meta();

        RocksJdbcWhere.Expr whereAst = RocksJdbcWhere.parse(sel.whereSql());
        tryCompileCriteria(sel.table(), sel.whereSql(), ta);

        List<String> projCols = normalizeProjection(sel.projection(), meta);
        Iterable<Map<String, Object>> rowsIt = rowsForAccessPath(ta, sel.accessPath());

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

    private static Iterable<Map<String, Object>> rowsForAccessPath(TableAccess<?> ta, RocksJdbcPlan.AccessPath ap) throws SQLException {
        if (ap == null || ap instanceof RocksJdbcPlan.FullScan) return ta.scanAll();
        if (ap instanceof RocksJdbcPlan.IndexEq x) return ta.scanIndexEq(x);
        if (ap instanceof RocksJdbcPlan.IndexRange x) return ta.scanIndexRange(x);
        if (ap instanceof RocksJdbcPlan.IndexIn x) return ta.scanIndexIn(x);
        return ta.scanAll();
    }

    // ---------------- criteria validation hook ----------------

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void tryCompileCriteria(String table, String whereSql, TableAccess<?> ta) throws SQLException {
        if (whereSql == null || whereSql.isBlank()) return;
        Map terms = ta.termsByColumnLower();
        if (terms == null || terms.isEmpty()) return;

        Condition ignored = RocksJdbcWhereCompiler.compile(
                whereSql,
                (Map<String, PropertyTerm<Serializable, ? extends Serializable>>) terms
        );
    }

    // ---------------- helpers ----------------

    private TableAccess<?> table(String tableName) throws SQLException {
        if (tableName == null || tableName.isBlank()) throw new SQLException("Missing table name");
        TableAccess<?> ta = tablesByLower.get(normTable(tableName));
        if (ta == null) throw new SQLException("Unknown table: " + tableName);
        return ta;
    }

    private static String normTable(String name) {
        return name.trim().toLowerCase(Locale.ROOT);
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
            return p.not() ? !isNull : isNull;
        }

        if (e instanceof RocksJdbcWhere.Between p) {
            Object v = getCol(row, p.col());
            if (v == null || p.a() == null || p.b() == null) return false;
            return cmp(v, p.a()) >= 0 && cmp(v, p.b()) <= 0;
        }

        if (e instanceof RocksJdbcWhere.In p) {
            Object v = getCol(row, p.col());
            if (v == null) return false;
            Object nv = normalizeCompare(v);
            for (Object x : p.values()) {
                if (Objects.equals(nv, normalizeCompare(x))) return true;
            }
            return false;
        }

        if (e instanceof RocksJdbcWhere.Cmp p) {
            Object v = getCol(row, p.col());
            Object lit = p.value();
            Object nv = normalizeCompare(v);
            Object nl = normalizeCompare(lit);

            return switch (p.op()) {
                case EQ -> Objects.equals(nv, nl);
                case NE -> !Objects.equals(nv, nl);
                case LT -> v != null && lit != null && cmp(v, lit) < 0;
                case LE -> v != null && lit != null && cmp(v, lit) <= 0;
                case GT -> v != null && lit != null && cmp(v, lit) > 0;
                case GE -> v != null && lit != null && cmp(v, lit) >= 0;
            };
        }

        throw new SQLException("Unsupported WHERE node: " + e.getClass().getName());
    }

    /** Case-insensitive + tolerates "table.col". */
    private static Object getCol(Map<String, Object> row, String colIdent) {
        if (row == null) return null;
        String want = normalizeKey(colIdent);

        if (row.containsKey(colIdent)) return row.get(colIdent);
        if (row.containsKey(want)) return row.get(want);

        for (Map.Entry<String, Object> e : row.entrySet()) {
            String k = e.getKey();
            if (k != null && normalizeKey(k).equals(want)) return e.getValue();
        }
        return null;
    }

    private static String normalizeKey(String ident) {
        if (ident == null) return "";
        String raw = ident.trim();
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
