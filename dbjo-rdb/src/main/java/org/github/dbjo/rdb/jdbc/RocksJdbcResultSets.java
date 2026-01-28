package org.github.dbjo.rdb.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;

final class RocksJdbcResultSets {
    private RocksJdbcResultSets() {}

    static ResultSet of(String[] colNames, int[] colTypes, List<Object[]> rows) throws SQLException {
        Objects.requireNonNull(colNames, "colNames");
        Objects.requireNonNull(colTypes, "colTypes");
        Objects.requireNonNull(rows, "rows");
        if (colNames.length != colTypes.length) {
            throw new SQLException("colNames and colTypes must have same length");
        }
        for (Object[] r : rows) {
            if (r == null || r.length != colNames.length) {
                throw new SQLException("Row length mismatch (expected " + colNames.length + ")");
            }
        }

        State st = new State(colNames, colTypes, rows);
        return (ResultSet) Proxy.newProxyInstance(
                RocksJdbcResultSets.class.getClassLoader(),
                new Class<?>[]{ ResultSet.class },
                new RsHandler(st)
        );
    }

    private static final class State {
        final String[] colNames;
        final int[] colTypes;
        final List<Object[]> rows;
        final Map<String, Integer> colIndexByLowerName; // 1-based

        int cursor = -1; // beforeFirst
        boolean closed = false;
        boolean lastWasNull = false;

        final ResultSetMetaData meta;

        State(String[] colNames, int[] colTypes, List<Object[]> rows) {
            this.colNames = colNames.clone();
            this.colTypes = colTypes.clone();
            this.rows = rows;

            Map<String, Integer> m = new HashMap<>(colNames.length * 2);
            for (int i = 0; i < colNames.length; i++) {
                String k = (colNames[i] == null ? "" : colNames[i]).trim().toLowerCase(Locale.ROOT);
                if (!k.isEmpty()) m.putIfAbsent(k, i + 1);
            }
            this.colIndexByLowerName = m;

            this.meta = (ResultSetMetaData) Proxy.newProxyInstance(
                    RocksJdbcResultSets.class.getClassLoader(),
                    new Class<?>[]{ ResultSetMetaData.class },
                    new MdHandler(this)
            );
        }

        void ensureOpen() throws SQLException {
            if (closed) throw new SQLException("ResultSet is closed");
        }

        boolean next() throws SQLException {
            ensureOpen();
            int n = cursor + 1;
            if (n >= rows.size()) {
                cursor = rows.size();
                return false;
            }
            cursor = n;
            lastWasNull = false;
            return true;
        }

        void beforeFirst() throws SQLException {
            ensureOpen();
            cursor = -1;
            lastWasNull = false;
        }

        Object get(int col) throws SQLException {
            ensureOpen();
            if (cursor < 0 || cursor >= rows.size()) throw new SQLException("Cursor not on a row");
            if (col < 1 || col > colNames.length) throw new SQLException("Bad column index: " + col);
            Object v = rows.get(cursor)[col - 1];
            lastWasNull = (v == null);
            return v;
        }

        int findColumn(String label) throws SQLException {
            ensureOpen();
            if (label == null) throw new SQLException("Column label is null");
            String k = label.trim().toLowerCase(Locale.ROOT);
            Integer idx = colIndexByLowerName.get(k);
            if (idx == null) throw new SQLException("Unknown column: " + label);
            return idx;
        }
    }

    private record RsHandler(State st) implements InvocationHandler {

        @Override
            public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
                String name = method.getName();

                switch (name) {
                    case "next" -> {
                        return st.next();
                    }
                    case "beforeFirst" -> {
                        st.beforeFirst();
                        return null;
                    }
                    case "close" -> {
                        st.closed = true;
                        return null;
                    }
                    case "isClosed" -> {
                        return st.closed;
                    }
                    case "wasNull" -> {
                        return st.lastWasNull;
                    }

                    case "getMetaData" -> {
                        return st.meta;
                    }
                    case "findColumn" -> {
                        return st.findColumn((String) args[0]);
                    }

                    case "isBeforeFirst" -> {
                        return st.cursor < 0;
                    }
                    case "isAfterLast" -> {
                        return st.cursor >= st.rows.size();
                    }
                    case "getRow" -> {
                        return (st.cursor < 0 ? 0 : Math.min(st.cursor + 1, st.rows.size()));
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

                    case "getStatement", "clearWarnings", "getWarnings" -> {
                        return null;
                    }

                    case "getObject" -> {
                        if (args == null || args.length == 0) throw unsup(name);
                        if (args[0] instanceof Integer i) return st.get(i);
                        if (args[0] instanceof String s) return st.get(st.findColumn(s));
                        throw unsup(name);
                    }

                    case "getString" -> {
                        Object v = getByArg(args);
                        return (v == null) ? null : String.valueOf(v);
                    }

                    case "getInt" -> {
                        return coerceInt(getByArg(args));
                    }
                    case "getLong" -> {
                        return coerceLong(getByArg(args));
                    }
                    case "getBoolean" -> {
                        return coerceBoolean(getByArg(args));
                    }
                    case "getDouble" -> {
                        return coerceDouble(getByArg(args));
                    }

                    case "unwrap" -> {
                        Class<?> iface = (Class<?>) args[0];
                        if (iface.isInstance(proxy)) return proxy;
                        throw new SQLException("Not a wrapper for " + iface.getName());
                    }
                    case "isWrapperFor" -> {
                        Class<?> iface = (Class<?>) args[0];
                        return iface.isInstance(proxy);
                    }
                }

                // Safe defaults for probes
                Class<?> rt = method.getReturnType();
                if (rt == boolean.class) return false;
                if (rt == int.class) return 0;
                if (rt == long.class) return 0L;
                if (rt == void.class) return null;

                throw unsup("ResultSet." + name);
            }

            private Object getByArg(Object[] args) throws SQLException {
                if (args == null || args.length == 0) throw new SQLException("Missing argument");
                if (args[0] instanceof Integer i) return st.get(i);
                if (args[0] instanceof String s) return st.get(st.findColumn(s));
                throw new SQLException("Bad column selector: " + args[0]);
            }

            private static SQLFeatureNotSupportedException unsup(String what) {
                return new SQLFeatureNotSupportedException(what + " not supported");
            }
        }

    private record MdHandler(State st) implements InvocationHandler {

        @Override
            public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
                String name = method.getName();

                return switch (name) {
                    case "getColumnCount" -> st.colNames.length;

                    case "getColumnName", "getColumnLabel" -> st.colNames[idx(args) - 1];

                    case "getColumnType" -> st.colTypes[idx(args) - 1];

                    case "getColumnTypeName" -> {
                        int t = st.colTypes[idx(args) - 1];
                        try {
                            yield JDBCType.valueOf(t).getName();
                        } catch (Throwable ignore) {
                            yield "UNKNOWN";
                        }
                    }

                    case "isNullable" -> ResultSetMetaData.columnNullable;

                    case "isAutoIncrement", "isCurrency", "isWritable", "isDefinitelyWritable" -> false;
                    case "isCaseSensitive", "isSearchable", "isSigned", "isReadOnly" -> true;
                    case "getSchemaName", "getTableName", "getCatalogName" -> "";
                    case "getPrecision", "getScale", "getColumnDisplaySize" -> 0;
                    case "getColumnClassName" -> "java.lang.Object";

                    case "unwrap" -> {
                        Class<?> iface = (Class<?>) args[0];
                        if (iface.isInstance(proxy)) yield proxy;
                        throw new SQLException("Not a wrapper for " + iface.getName());
                    }
                    case "isWrapperFor" -> {
                        Class<?> iface = (Class<?>) args[0];
                        yield iface.isInstance(proxy);
                    }

                    default -> throw new SQLFeatureNotSupportedException("ResultSetMetaData." + name + " not supported");
                };
            }

            private static int idx(Object[] args) throws SQLException {
                if (args == null || args.length == 0 || !(args[0] instanceof Integer i)) {
                    throw new SQLException("Missing column index");
                }
                return i;
            }
        }

    private static int coerceInt(Object v) throws SQLException {
        if (v == null) return 0;
        if (v instanceof Number n) return n.intValue();
        if (v instanceof Boolean b) return b ? 1 : 0;
        try { return Integer.parseInt(String.valueOf(v)); }
        catch (Exception e) { throw new SQLException("Cannot coerce to int: " + v, e); }
    }

    private static long coerceLong(Object v) throws SQLException {
        if (v == null) return 0L;
        if (v instanceof Number n) return n.longValue();
        if (v instanceof Boolean b) return b ? 1L : 0L;
        try { return Long.parseLong(String.valueOf(v)); }
        catch (Exception e) { throw new SQLException("Cannot coerce to long: " + v, e); }
    }

    private static double coerceDouble(Object v) throws SQLException {
        if (v == null) return 0.0;
        if (v instanceof Number n) return n.doubleValue();
        if (v instanceof Boolean b) return b ? 1.0 : 0.0;
        try { return Double.parseDouble(String.valueOf(v)); }
        catch (Exception e) { throw new SQLException("Cannot coerce to double: " + v, e); }
    }

    private static boolean coerceBoolean(Object v) {
        if (v == null) return false;
        if (v instanceof Boolean b) return b;
        if (v instanceof Number n) return n.longValue() != 0L;
        String s = String.valueOf(v).trim().toLowerCase(Locale.ROOT);
        return s.equals("true") || s.equals("1") || s.equals("y") || s.equals("yes");
    }
}
