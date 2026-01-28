package org.github.dbjo.rdb.jdbc;

import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcSql;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcStatement;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcTable;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcWhere;
import org.rocksdb.*;

import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

public final class RocksJdbcConnection implements Connection {
    private final String url;
    private final String dbPath;

    private final RocksDB db;
    private final DBOptions dbOptions;
    private final ColumnFamilyOptions cfOptions;
    private final List<ColumnFamilyHandle> handles;

    private final Map<String, ColumnFamilyHandle> cfByName;

    private final RocksJdbcCatalog catalog;
    private final Map<RocksJdbcTable, MethodGetterCache> getterCache = new HashMap<>();

    private volatile boolean closed = false;

    private final DatabaseMetaData metaDataProxy;

    static RocksJdbcConnection open(String url, String dbPath, RocksJdbcCatalog catalog) throws SQLException {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(dbPath, "dbPath");
        Objects.requireNonNull(catalog, "catalog");

        List<byte[]> cfs;
        try (Options opts = new Options()) {
            cfs = RocksDB.listColumnFamilies(opts, dbPath);
        } catch (RocksDBException e) {
            throw new SQLException("Failed to list RocksDB column families at: " + dbPath, e);
        }

        DBOptions dbo = new DBOptions();
        dbo.setCreateIfMissing(false);
        dbo.setErrorIfExists(false);

        ColumnFamilyOptions cfo = new ColumnFamilyOptions();

        List<ColumnFamilyDescriptor> desc = new ArrayList<>();
        if (cfs.isEmpty()) {
            desc.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfo));
        } else {
            for (byte[] name : cfs) desc.add(new ColumnFamilyDescriptor(name, cfo));
        }

        List<ColumnFamilyHandle> handles = new ArrayList<>(desc.size());
        try {
            RocksDB db = RocksDB.openReadOnly(dbo, dbPath, desc, handles);
            return new RocksJdbcConnection(url, dbPath, db, dbo, cfo, handles, catalog);
        } catch (RocksDBException e) {
            for (ColumnFamilyHandle h : handles) {
                try { if (h != null) h.close(); } catch (Throwable ignored) {}
            }
            try { cfo.close(); } catch (Throwable ignored) {}
            try { dbo.close(); } catch (Throwable ignored) {}
            throw new SQLException("Failed to open RocksDB read-only at: " + dbPath, e);
        }
    }

    private RocksJdbcConnection(
            String url,
            String dbPath,
            RocksDB db,
            DBOptions dbOptions,
            ColumnFamilyOptions cfOptions,
            List<ColumnFamilyHandle> handles,
            RocksJdbcCatalog catalog
    ) {
        this.url = url;
        this.dbPath = dbPath;
        this.db = db;
        this.dbOptions = dbOptions;
        this.cfOptions = cfOptions;
        this.handles = handles;
        this.catalog = catalog;

        this.cfByName = buildCfMap(handles);

        this.metaDataProxy = (DatabaseMetaData) Proxy.newProxyInstance(
                RocksJdbcConnection.class.getClassLoader(),
                new Class<?>[]{ DatabaseMetaData.class },
                (proxy, method, args) -> handleMetaCall(method.getName(), args)
        );
    }

    RocksDB db() { return db; }

    ColumnFamilyHandle cfHandle(String cfName) throws SQLException {
        if (cfName == null) throw new SQLException("cfName is null");
        String k = cfName.trim().toLowerCase(Locale.ROOT);
        if (k.isEmpty()) throw new SQLException("cfName is empty");
        ColumnFamilyHandle h = cfByName.get(k);
        if (h == null) throw new SQLException("Unknown/missing column family: " + cfName);
        return h;
    }

    public ResultSet runQuery(String sql, int statementMaxRows) throws SQLException {
        ensureOpen();

        RocksJdbcSql.Parsed p = RocksJdbcSql.parse(sql);
        int effMaxRows = applyLimit(statementMaxRows, p.limit());

        return switch (p.kind()) {
            case LIST_TABLES -> queryTables(effMaxRows);
            case SELECT -> querySelect(p.tableName(), p.whereSql(), effMaxRows, p.projection());
            case COUNT -> queryCount(p.tableName(), p.whereSql());
        };
    }

    private static int applyLimit(int stmtMaxRows, int sqlLimit) {
        if (sqlLimit > 0) {
            if (stmtMaxRows <= 0) return sqlLimit;
            return Math.min(stmtMaxRows, sqlLimit);
        }
        return stmtMaxRows;
    }

    private ResultSet queryTables(int maxRows) throws SQLException {
        String[] cols = { "table_name", "cf_name", "column_count" };
        int[] types = { Types.VARCHAR, Types.VARCHAR, Types.INTEGER };

        List<Object[]> rows = new ArrayList<>();
        int n = 0;
        for (RocksJdbcTable t : catalog.tables()) {
            rows.add(new Object[]{ t.tableName(), t.cfName(), t.columnNames().length });
            n++;
            if (maxRows > 0 && n >= maxRows) break;
        }
        return RocksJdbcResultSets.of(cols, types, rows);
    }

    private ResultSet queryCount(String tableName, String whereSql) throws SQLException {
        RocksJdbcTable t = catalog.requireTable(tableName);
        ColumnFamilyHandle cf = cfHandle(t.cfName());

        String[] allColNames = t.columnNames();
        RocksJdbcWhere.Predicate pred = RocksJdbcWhere.compile(whereSql, allColNames);

        MethodGetterCache getters = getterCache.computeIfAbsent(t, MethodGetterCache::new);

        long count = 0;
        RocksIterator it = null;
        try {
            it = db.newIterator(cf);
            it.seekToFirst();
            while (it.isValid()) {
                Object rowObj = t.decoder().decode(it.value());
                if (pred.test(idx -> getters.get(rowObj, idx))) count++;
                it.next();
            }
        } finally {
            if (it != null) it.close();
        }

        String[] cols = { "count" };
        int[] types = { Types.BIGINT };
        return RocksJdbcResultSets.of(cols, types, java.util.Collections.singletonList(new Object[]{ count }));
    }

    private ResultSet querySelect(String tableName, String whereSql, int maxRows, RocksJdbcSql.SelectedCol[] projection) throws SQLException {
        RocksJdbcTable t = catalog.requireTable(tableName);
        ColumnFamilyHandle cf = cfHandle(t.cfName());

        String[] allColNames = t.columnNames();
        int[] allColTypes = t.columnSqlTypes();

        // WHERE predicate always uses full-table column indexes
        RocksJdbcWhere.Predicate pred = RocksJdbcWhere.compile(whereSql, allColNames);

        MethodGetterCache getters = getterCache.computeIfAbsent(t, MethodGetterCache::new);

        // Resolve projection to selected column indexes (or all)
        final int[] selIdx;
        final String[] outNames;
        final int[] outTypes;

        if (projection == null) {
            selIdx = null; // means "all"
            outNames = allColNames;
            outTypes = allColTypes;
        } else {
            selIdx = new int[projection.length];
            outNames = new String[projection.length];
            outTypes = new int[projection.length];

            for (int i = 0; i < projection.length; i++) {
                String source = projection[i].sourceName();
                int idx = resolveColumnIndex(allColNames, source);
                selIdx[i] = idx;

                outNames[i] = projection[i].label() != null && !projection[i].label().isBlank()
                        ? projection[i].label()
                        : allColNames[idx];

                outTypes[i] = allColTypes[idx];
            }
        }

        List<Object[]> rows = new ArrayList<>();
        int emitted = 0;

        RocksIterator it = null;
        try {
            it = db.newIterator(cf);
            it.seekToFirst();

            while (it.isValid()) {
                Object rowObj = t.decoder().decode(it.value());

                if (pred.test(idx -> getters.get(rowObj, idx))) {
                    Object[] out;
                    if (selIdx == null) {
                        out = new Object[allColNames.length];
                        getters.fill(rowObj, out);
                    } else {
                        out = new Object[selIdx.length];
                        for (int k = 0; k < selIdx.length; k++) {
                            out[k] = getters.get(rowObj, selIdx[k]);
                        }
                    }
                    rows.add(out);

                    emitted++;
                    if (maxRows > 0 && emitted >= maxRows) break;
                }

                it.next();
            }
        } finally {
            if (it != null) it.close();
        }

        return RocksJdbcResultSets.of(outNames, outTypes, rows);
    }

    private static int resolveColumnIndex(String[] allColNames, String col) throws SQLException {
        if (col == null) throw new SQLException("Bad projection column: null");
        String want = col.trim();
        if (want.isEmpty()) throw new SQLException("Bad projection column: empty");

        for (int i = 0; i < allColNames.length; i++) {
            if (allColNames[i] != null && allColNames[i].equalsIgnoreCase(want)) return i;
        }
        throw new SQLException("Unknown column in projection: " + col);
    }

    private void ensureOpen() throws SQLException {
        if (closed) throw new SQLException("Connection is closed");
    }

    // --- DatabaseMetaData proxy handler ---
    private Object handleMetaCall(String name, Object[] args) throws SQLException {
        return switch (name) {
            case "getURL" -> url;
            case "getUserName" -> "";
            case "getDatabaseProductName" -> "RocksDB";
            case "getDatabaseProductVersion" -> "read-only";
            case "getDriverName" -> "dbjo-rocks-jdbc";
            case "getDriverVersion" -> "0.1";
            case "getDriverMajorVersion" -> 0;
            case "getDriverMinorVersion" -> 1;
            case "isReadOnly" -> true;
            case "supportsTransactions" -> false;
            case "supportsBatchUpdates" -> false;

            case "getTables" -> metaGetTables();
            case "getColumns" -> metaGetColumns(args);

            case "allProceduresAreCallable" -> false;
            case "allTablesAreSelectable" -> true;

            default -> {
                Class<?> rt = findReturnType(DatabaseMetaData.class, name, args);
                if (rt == boolean.class) yield false;
                if (rt == int.class) yield 0;
                if (rt == String.class) yield "";
                throw new SQLFeatureNotSupportedException("DatabaseMetaData." + name + " not supported");
            }
        };
    }

    private static Class<?> findReturnType(Class<?> iface, String methodName, Object[] args) {
        for (var m : iface.getMethods()) {
            if (!m.getName().equals(methodName)) continue;
            if (args == null && m.getParameterCount() == 0) return m.getReturnType();
            if (args != null && m.getParameterCount() == args.length) return m.getReturnType();
        }
        return Object.class;
    }

    private ResultSet metaGetTables() throws SQLException {
        String[] cols = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS" };
        int[] types = { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR };

        List<Object[]> rows = new ArrayList<>();
        for (RocksJdbcTable t : catalog.tables()) {
            rows.add(new Object[]{ null, null, t.tableName(), "TABLE", null });
        }
        return RocksJdbcResultSets.of(cols, types, rows);
    }

    private ResultSet metaGetColumns(Object[] args) throws SQLException {
        String tablePattern = (args != null && args.length >= 3) ? (String) args[2] : null;

        String[] cols = {
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                "COLUMN_SIZE", "DECIMAL_DIGITS", "NULLABLE",
                "ORDINAL_POSITION"
        };
        int[] types = {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                Types.INTEGER, Types.INTEGER, Types.INTEGER,
                Types.INTEGER
        };

        List<Object[]> rows = new ArrayList<>();

        for (RocksJdbcTable t : catalog.tables()) {
            if (tablePattern != null && !tablePattern.isBlank()) {
                boolean match = t.tableName().equalsIgnoreCase(tablePattern);
                List<String> aliases = t.names();
                if (!match && aliases != null) {
                    match = aliases.stream().anyMatch(n -> n != null && n.equalsIgnoreCase(tablePattern));
                }
                if (!match) continue;
            }

            String[] cn = t.columnNames();
            int[] ct = t.columnSqlTypes();

            for (int i = 0; i < cn.length; i++) {
                rows.add(new Object[]{
                        null, null, t.tableName(),
                        cn[i], ct[i], null,
                        null, null, ResultSetMetaData.columnNullable,
                        i + 1
                });
            }
        }

        return RocksJdbcResultSets.of(cols, types, rows);
    }

    private static Map<String, ColumnFamilyHandle> buildCfMap(List<ColumnFamilyHandle> handles) {
        Map<String, ColumnFamilyHandle> m = new HashMap<>();
        for (ColumnFamilyHandle h : handles) {
            if (h == null) continue;
            try {
                String name = new String(h.getName(), java.nio.charset.StandardCharsets.UTF_8);
                String k = name.trim().toLowerCase(Locale.ROOT);
                if (!k.isEmpty()) m.putIfAbsent(k, h);
            } catch (Throwable ignored) {}
        }
        if (!handles.isEmpty() && handles.get(0) != null) {
            m.putIfAbsent("default", handles.get(0));
        }
        return m;
    }

    // --- Connection interface (minimal) ---

    @Override
    public Statement createStatement() throws SQLException {
        ensureOpen();
        return new RocksJdbcStatement(this);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        ensureOpen();
        return metaDataProxy;
    }

    @Override
    public void close() throws SQLException {
        if (closed) return;
        closed = true;

        for (ColumnFamilyHandle h : handles) {
            try { if (h != null) h.close(); } catch (Throwable ignored) {}
        }

        try { db.close(); } catch (Throwable ignored) {}
        try { cfOptions.close(); } catch (Throwable ignored) {}
        try { dbOptions.close(); } catch (Throwable ignored) {}
    }

    @Override
    public boolean isClosed() { return closed; }

    @Override
    public boolean isReadOnly() { return true; }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (!readOnly) throw new SQLException("RocksJdbcConnection is read-only");
    }

    @Override
    public void commit() throws SQLException { throw new SQLFeatureNotSupportedException("Transactions not supported"); }

    @Override
    public void rollback() throws SQLException { throw new SQLFeatureNotSupportedException("Transactions not supported"); }

    @Override
    public boolean getAutoCommit() { return true; }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (!autoCommit) throw new SQLException("Only autoCommit=true supported");
    }

    @Override
    public String nativeSQL(String sql) { return sql; }

    private static SQLFeatureNotSupportedException unsup() {
        return new SQLFeatureNotSupportedException("Not supported in rudimentary Rocks JDBC");
    }

    @Override public PreparedStatement prepareStatement(String sql) throws SQLException { throw unsup(); }
    @Override public CallableStatement prepareCall(String sql) throws SQLException { throw unsup(); }
    @Override public String getCatalog() { return null; }
    @Override public void setCatalog(String catalog) throws SQLException { throw unsup(); }
    @Override public int getTransactionIsolation() { return Connection.TRANSACTION_NONE; }
    @Override public void setTransactionIsolation(int level) throws SQLException { throw unsup(); }
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException { return createStatement(); }
    @Override public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { return createStatement(); }
    @Override public Map<String, Class<?>> getTypeMap() { return new HashMap<>(); }
    @Override public void setTypeMap(Map<String, Class<?>> map) throws SQLException { throw unsup(); }
    @Override public void setHoldability(int holdability) throws SQLException { throw unsup(); }
    @Override public int getHoldability() { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override public Savepoint setSavepoint() throws SQLException { throw unsup(); }
    @Override public Savepoint setSavepoint(String name) throws SQLException { throw unsup(); }
    @Override public void rollback(Savepoint savepoint) throws SQLException { throw unsup(); }
    @Override public void releaseSavepoint(Savepoint savepoint) throws SQLException { throw unsup(); }
    @Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException { throw unsup(); }
    @Override public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException { throw unsup(); }
    @Override public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException { throw unsup(); }
    @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException { throw unsup(); }
    @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { throw unsup(); }
    @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException { throw unsup(); }
    @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { throw unsup(); }
    @Override public Clob createClob() throws SQLException { throw unsup(); }
    @Override public Blob createBlob() throws SQLException { throw unsup(); }
    @Override public NClob createNClob() throws SQLException { throw unsup(); }
    @Override public SQLXML createSQLXML() throws SQLException { throw unsup(); }
    @Override public boolean isValid(int timeout) { return !closed; }
    @Override public void setClientInfo(String name, String value) {}
    @Override public void setClientInfo(Properties properties) {}
    @Override public String getClientInfo(String name) { return null; }
    @Override public Properties getClientInfo() { return new Properties(); }
    @Override public Array createArrayOf(String typeName, Object[] elements) throws SQLException { throw unsup(); }
    @Override public Struct createStruct(String typeName, Object[] attributes) throws SQLException { throw unsup(); }
    @Override public void setSchema(String schema) throws SQLException { throw unsup(); }
    @Override public String getSchema() { return null; }
    @Override public void abort(Executor executor) throws SQLException { close(); }
    @Override public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException { throw unsup(); }
    @Override public int getNetworkTimeout() { return 0; }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("unwrap not supported"); }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }

    // --- tiny helper: cache getters per table ---
    private static final class MethodGetterCache {
        private final java.lang.reflect.Method[] methods;

        MethodGetterCache(RocksJdbcTable t) {
            try {
                String[] getterNames = t.getterNames();
                methods = new java.lang.reflect.Method[getterNames.length];
                Class<?> c = t.rowClass();
                for (int i = 0; i < getterNames.length; i++) {
                    methods[i] = c.getMethod(getterNames[i]);
                }
            } catch (Throwable e) {
                throw new RuntimeException("Failed to resolve getters for " + t, e);
            }
        }

        Object get(Object rowObj, int colIndex) throws SQLException {
            try {
                return methods[colIndex].invoke(rowObj);
            } catch (Throwable e) {
                throw new SQLException("Failed to read getter at index " + colIndex, e);
            }
        }

        void fill(Object rowObj, Object[] out) throws SQLException {
            try {
                for (int i = 0; i < methods.length; i++) out[i] = methods[i].invoke(rowObj);
            } catch (Throwable e) {
                throw new SQLException("Failed to read row getters", e);
            }
        }
    }
}
