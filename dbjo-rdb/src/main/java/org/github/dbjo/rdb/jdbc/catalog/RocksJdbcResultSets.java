package org.github.dbjo.rdb.jdbc.catalog;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;

/**
 * Portable ResultSet factory (no com.sun.*, no rowset deps).
 *
 * Produces a lightweight ResultSet via dynamic proxy.
 * Supports the subset used by this JDBC layer:
 *   next, getObject(int/String), getString, getInt, getLong, close, isClosed, wasNull,
 *   findColumn, getMetaData, getStatement (null), getType, getConcurrency, getHoldability.
 *
 * Everything else throws SQLFeatureNotSupportedException.
 */
public final class RocksJdbcResultSets {
    private RocksJdbcResultSets() {}

    public static ResultSet fromRows(List<String> columns, List<Map<String, Object>> rows) {
        Objects.requireNonNull(columns, "columns");
        Objects.requireNonNull(rows, "rows");

        List<String> cols = List.copyOf(columns);
        Map<String, Integer> colIndex = buildIndex(cols);

        // store rows as Object[] aligned to columns for fast access
        List<Object[]> data = new ArrayList<>(rows.size());
        for (Map<String, Object> r : rows) {
            Object[] arr = new Object[cols.size()];
            for (int i = 0; i < cols.size(); i++) {
                String c = cols.get(i);
                arr[i] = getCol(r, c);
            }
            data.add(arr);
        }

        ResultSetMetaData md = new SimpleMeta(cols);

        InvocationHandler h = new Handler(cols, colIndex, data, md);

        return (ResultSet) Proxy.newProxyInstance(
                RocksJdbcResultSets.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                h
        );
    }

    private static Map<String, Integer> buildIndex(List<String> cols) {
        HashMap<String, Integer> m = new HashMap<>();
        for (int i = 0; i < cols.size(); i++) {
            String k = normalize(cols.get(i));
            // first wins
            m.putIfAbsent(k, i + 1);
        }
        return m;
    }

    /** Case-insensitive column lookup (also tolerates "table.col"). */
    private static Object getCol(Map<String, Object> row, String colIdent) {
        if (row == null) return null;
        if (row.containsKey(colIdent)) return row.get(colIdent);

        String want = normalize(colIdent);
        // direct lookup by normalized name
        if (row.containsKey(want)) return row.get(want);

        for (Map.Entry<String, Object> e : row.entrySet()) {
            String k = e.getKey();
            if (k != null && normalize(k).equals(want)) return e.getValue();
        }
        return null;
    }

    private static String normalize(String ident) {
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

    private static final class Handler implements InvocationHandler {
        private final List<String> cols;
        private final Map<String, Integer> colIndex;
        private final List<Object[]> rows;
        private final ResultSetMetaData md;

        private int idx = -1;
        private boolean closed = false;
        private Object last = null;

        Handler(List<String> cols, Map<String, Integer> colIndex, List<Object[]> rows, ResultSetMetaData md) {
            this.cols = cols;
            this.colIndex = colIndex;
            this.rows = rows;
            this.md = md;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();

            // Object methods
            if (name.equals("toString") && (args == null || args.length == 0)) return "ResultSet[" + cols + "]";
            if (name.equals("hashCode") && (args == null || args.length == 0)) return System.identityHashCode(proxy);
            if (name.equals("equals") && args != null && args.length == 1) return proxy == args[0];

            checkOpenIfNeeded(name);

            switch (name) {
                case "next" -> {
                    last = null;
                    idx++;
                    return idx < rows.size();
                }
                case "close" -> {
                    closed = true;
                    return null;
                }
                case "isClosed" -> {
                    return closed;
                }
                case "wasNull" -> {
                    return last == null;
                }
                case "getMetaData" -> {
                    return md;
                }
                case "findColumn" -> {
                    String label = (String) args[0];
                    Integer i = colIndex.get(normalize(label));
                    if (i == null) throw new SQLException("Unknown column: " + label);
                    return i;
                }
                case "getObject" -> {
                    if (args == null || args.length != 1) throw ns();
                    Object v;
                    if (args[0] instanceof Integer ci) {
                        v = getObjectByIndex(ci);
                    } else if (args[0] instanceof String cs) {
                        int ci = (int) invoke(proxy, ResultSet.class.getMethod("findColumn", String.class), new Object[]{cs});
                        v = getObjectByIndex(ci);
                    } else {
                        throw ns();
                    }
                    last = v;
                    return v;
                }
                case "getString" -> {
                    Object v = invoke(proxy, ResultSet.class.getMethod("getObject", args[0] instanceof Integer ? int.class : String.class), args);
                    return v == null ? null : String.valueOf(v);
                }
                case "getInt" -> {
                    Object v = invoke(proxy, ResultSet.class.getMethod("getObject", args[0] instanceof Integer ? int.class : String.class), args);
                    if (v == null) { last = null; return 0; }
                    Number n = asNumber(v);
                    last = n;
                    return n.intValue();
                }
                case "getLong" -> {
                    Object v = invoke(proxy, ResultSet.class.getMethod("getObject", args[0] instanceof Integer ? int.class : String.class), args);
                    if (v == null) { last = null; return 0L; }
                    Number n = asNumber(v);
                    last = n;
                    return n.longValue();
                }
                case "getStatement" -> { // not meaningful for our ResultSet
                    return (Statement) null;
                }
                case "getType" -> {
                    return ResultSet.TYPE_FORWARD_ONLY;
                }
                case "getConcurrency" -> {
                    return ResultSet.CONCUR_READ_ONLY;
                }
                case "getHoldability" -> {
                    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
                }
                case "unwrap" -> {
                    Class<?> iface = (Class<?>) args[0];
                    if (iface.isInstance(proxy)) return iface.cast(proxy);
                    throw new SQLException("Not a wrapper for " + iface.getName());
                }
                case "isWrapperFor" -> {
                    Class<?> iface = (Class<?>) args[0];
                    return iface.isInstance(proxy);
                }
                default -> throw ns();
            }
        }

        private Object getObjectByIndex(int columnIndex) throws SQLException {
            if (columnIndex < 1 || columnIndex > cols.size()) throw new SQLException("Bad column index: " + columnIndex);
            if (idx < 0 || idx >= rows.size()) throw new SQLException("Cursor not positioned on a row");
            return rows.get(idx)[columnIndex - 1];
        }

        private void checkOpenIfNeeded(String method) throws SQLException {
            // allow isClosed/close even when closed
            if (closed && !(method.equals("isClosed") || method.equals("close"))) {
                throw new SQLException("ResultSet closed");
            }
        }

        private static Number asNumber(Object v) throws SQLException {
            if (v instanceof Number n) return n;
            try { return new java.math.BigDecimal(String.valueOf(v)); }
            catch (Exception e) { throw new SQLException("Not a number: " + v); }
        }

        private static SQLFeatureNotSupportedException ns() {
            return new SQLFeatureNotSupportedException("Not supported");
        }
    }

    private static final class SimpleMeta implements ResultSetMetaData {
        private final List<String> cols;

        SimpleMeta(List<String> cols) { this.cols = cols; }

        @Override public int getColumnCount() { return cols.size(); }
        @Override public String getColumnLabel(int column) { return cols.get(column - 1); }
        @Override public String getColumnName(int column) { return cols.get(column - 1); }
        @Override public int getColumnType(int column) { return Types.JAVA_OBJECT; }
        @Override public String getColumnTypeName(int column) { return "JAVA_OBJECT"; }
        @Override public String getColumnClassName(int column) { return Object.class.getName(); }

        @Override public int isNullable(int column) { return columnNullableUnknown; }
        @Override public boolean isAutoIncrement(int column) { return false; }
        @Override public boolean isCaseSensitive(int column) { return true; }
        @Override public boolean isSearchable(int column) { return true; }
        @Override public boolean isCurrency(int column) { return false; }
        @Override public boolean isSigned(int column) { return true; }
        @Override public int getColumnDisplaySize(int column) { return 32; }
        @Override public String getSchemaName(int column) { return ""; }
        @Override public int getPrecision(int column) { return 0; }
        @Override public int getScale(int column) { return 0; }
        @Override public String getTableName(int column) { return ""; }
        @Override public String getCatalogName(int column) { return ""; }

        @Override public boolean isReadOnly(int column) { return true; }
        @Override public boolean isWritable(int column) { return false; }
        @Override public boolean isDefinitelyWritable(int column) { return false; }

        @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLFeatureNotSupportedException("Not supported"); }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }
}
