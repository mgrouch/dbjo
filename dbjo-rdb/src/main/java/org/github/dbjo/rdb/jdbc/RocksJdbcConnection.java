package org.github.dbjo.rdb.jdbc;

import org.github.dbjo.criteria.Condition;
import org.github.dbjo.criteria.Conditions;
import org.github.dbjo.criteria.Query;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.rdb.DaoRegistry;
import org.github.dbjo.rdb.IndexedRocksDao;
import org.github.dbjo.rdb.RocksSessions;
import org.github.dbjo.rdb.criteria.CriteriaSupport;
import org.github.dbjo.rdb.jdbc.catalog.*;
import org.rocksdb.*;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.RecordComponent;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

/**
 * Read-only RocksDB JDBC Connection.
 *
 * Supports:
 *  - SHOW TABLES / LIST TABLES (driver-specific)
 *  - SELECT <projection> FROM <table> [WHERE ...] [LIMIT n]
 *  - SELECT COUNT(*) FROM <table> [WHERE ...]
 *
 * Fast path (index-aware):
 *  - If catalog implements RocksJdbcCriteriaCatalog and this connection has sessions+registry,
 *    we compile WHERE into dbjo criteria and call generated IndexedRocksDao.select(criteriaQuery).
 *
 * Fallback:
 *  - Primary CF iterator scan + decode + (optional) CriteriaSupport.test(...)
 */
public final class RocksJdbcConnection implements Connection {
    private final String url;
    private final String dbPath;

    private final RocksDB db;
    private final DBOptions dbOptions;
    private final ColumnFamilyOptions cfOptions;
    private final List<ColumnFamilyHandle> handles;

    private final Map<String, ColumnFamilyHandle> cfByName;

    private final RocksJdbcCatalog catalog;

    // Optional: enables index-aware DAO fast-path
    private final RocksSessions sessions;   // nullable
    private final DaoRegistry registry;     // nullable
    private final Map<RocksJdbcTable, Object> daoCache = new HashMap<>();

    // getters cache per table (column order getters)
    private final Map<RocksJdbcTable, MethodGetterCache> getterCache = new HashMap<>();

    private volatile boolean closed = false;

    private final DatabaseMetaData metaDataProxy;

    public static RocksJdbcConnection open(String url, String dbPath, RocksJdbcCatalog catalog) throws SQLException {
        return open(url, dbPath, catalog, null, null);
    }

    /** Open connection with optional dbjo sessions+registry, enabling index-aware DAO fast-path for WHERE. */
    public static RocksJdbcConnection open(
            String url,
            String dbPath,
            RocksJdbcCatalog catalog,
            RocksSessions sessions,
            DaoRegistry registry
    ) throws SQLException {
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
            for (byte[] name : cfs) {
                desc.add(new ColumnFamilyDescriptor(name, cfo));
            }
        }

        List<ColumnFamilyHandle> handles = new ArrayList<>(desc.size());
        try {
            RocksDB db = RocksDB.openReadOnly(dbo, dbPath, desc, handles);
            return new RocksJdbcConnection(url, dbPath, db, dbo, cfo, handles, catalog, sessions, registry);
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
            RocksJdbcCatalog catalog,
            RocksSessions sessions,
            DaoRegistry registry
    ) {
        this.url = url;
        this.dbPath = dbPath;
        this.db = db;
        this.dbOptions = dbOptions;
        this.cfOptions = cfOptions;
        this.handles = handles;
        this.catalog = catalog;
        this.sessions = sessions;
        this.registry = registry;

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

    /** Entry point for Statements. */
    public ResultSet runQuery(String sql, int maxRowsFromStatement) throws SQLException {
        ensureOpen();

        RocksJdbcSql.Parsed p = RocksJdbcSql.parse(sql);

        Integer parsedLimit = parsedLimit(p);
        int effectiveSelectLimit = mergeLimits(parsedLimit, maxRowsFromStatement);
        String whereSql = parsedWhereSql(p);

        return switch (p.kind()) {
            case LIST_TABLES -> queryTables(effectiveSelectLimit);
            case COUNT -> queryCount(p.tableName(), whereSql);
            case SELECT -> querySelect(
                    p.tableName(),
                    parsedProjection(p),     // may be null or empty => "*"
                    whereSql,                // may be null/blank
                    effectiveSelectLimit
            );
        };
    }

    private static int mergeLimits(Integer parsedLimit, int stmtMaxRows) {
        int a = (parsedLimit == null) ? Integer.MAX_VALUE : Math.max(0, parsedLimit);
        int b = (stmtMaxRows <= 0) ? Integer.MAX_VALUE : stmtMaxRows;
        long m = Math.min((long) a, (long) b);
        return (m > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) m;
    }

    private ResultSet queryTables(int limit) throws SQLException {
        String[] cols = { "table_name", "cf_name", "column_count" };
        int[] types = { Types.VARCHAR, Types.VARCHAR, Types.INTEGER };

        List<Object[]> rows = new ArrayList<>();
        int emitted = 0;
        for (RocksJdbcTable t : catalog.tables()) {
            if (limit == 0) break;
            rows.add(new Object[]{ t.tableName(), t.cfName(), t.columns().length });
            emitted++;
            if (emitted >= limit) break;
        }
        return RocksJdbcResultSets.of(cols, types, rows);
    }

    private ResultSet queryCount(String tableName, String whereSql) throws SQLException {
        RocksJdbcTable t = catalog.requireTable(tableName);

        long count;
        if (whereSql != null && !whereSql.isBlank()) {
            count = countWithFilter(t, whereSql);
        } else {
            ColumnFamilyHandle cf = cfHandle(t.cfName());
            long c = 0;
            try (RocksIterator it = db.newIterator(cf)) {
                it.seekToFirst();
                while (it.isValid()) {
                    c++;
                    it.next();
                }
            }
            count = c;
        }

        String[] cols = { "count" };
        int[] types = { Types.BIGINT };
        return RocksJdbcResultSets.of(cols, types, Collections.singletonList(new Object[]{ count }));
    }

    private long countWithFilter(RocksJdbcTable t, String whereSql) throws SQLException {
        // 1) DAO fast path (index-aware), if available
        CriteriaPlan<?> plan = buildCriteriaPlanIfPossible(t, whereSql, Integer.MAX_VALUE);
        if (plan != null && plan.dao != null) {
            @SuppressWarnings("unchecked")
            List<Object> rows = (List<Object>) plan.dao.select((Query<?>) plan.criteriaQuery);
            return rows.size();
        }

        // 2) fallback scan+decode+CriteriaSupport.test
        if (plan == null) {
            plan = buildCriteriaPlanFallback(t, whereSql, Integer.MAX_VALUE);
        }
        if (plan == null) throw new SQLException("Failed to compile WHERE for COUNT(*)");

        ColumnFamilyHandle cf = cfHandle(t.cfName());
        long count = 0;

        try (RocksIterator it = db.newIterator(cf)) {
            it.seekToFirst();
            while (it.isValid()) {
                Object bean = t.decoder().decode(it.value());
                if (CriteriaSupport.test(plan.criteriaQuery, (Serializable) bean)) {
                    count++;
                }
                it.next();
            }
        }
        return count;
    }

    private ResultSet querySelect(
            String tableName,
            List<String> projection,
            String whereSql,
            int limit
    ) throws SQLException {
        RocksJdbcTable t = catalog.requireTable(tableName);

        Projection proj = resolveProjection(t, projection);

        // LIMIT 0 => empty immediately (and avoid Query.limit(0) which is illegal in your criteria Query builder)
        if (limit == 0) {
            return RocksJdbcResultSets.of(proj.outNames, proj.outTypes, List.of());
        }

        CriteriaPlan<?> plan = null;
        if (whereSql != null && !whereSql.isBlank()) {
            plan = buildCriteriaPlanIfPossible(t, whereSql, limit);
            if (plan == null) plan = buildCriteriaPlanFallback(t, whereSql, limit);
            if (plan == null) throw new SQLException("Failed to compile WHERE clause");
        }

        List<Object[]> rows = new ArrayList<>();

        if (plan != null && plan.dao != null) {
            emitFromDao(t, proj, plan, rows, limit);
        } else {
            emitFromPrimaryScan(t, proj, plan, rows, limit);
        }

        return RocksJdbcResultSets.of(proj.outNames, proj.outTypes, rows);
    }

    private void emitFromDao(RocksJdbcTable t, Projection proj, CriteriaPlan<?> plan, List<Object[]> out, int limit) throws SQLException {
        @SuppressWarnings("unchecked")
        List<Object> beans = (List<Object>) ((IndexedRocksDao<?, ?>) plan.dao).select((Query<?>) plan.criteriaQuery);

        MethodGetterCache getters = getterCache.computeIfAbsent(t, MethodGetterCache::new);

        int emitted = 0;
        for (Object bean : beans) {
            Object[] full = new Object[t.getterNames().length];
            getters.fill(bean, full);

            out.add(projectRow(full, proj));
            emitted++;
            if (emitted >= limit) break;
        }
    }

    private void emitFromPrimaryScan(RocksJdbcTable t, Projection proj, CriteriaPlan<?> plan, List<Object[]> out, int limit) throws SQLException {
        ColumnFamilyHandle cf = cfHandle(t.cfName());
        MethodGetterCache getters = getterCache.computeIfAbsent(t, MethodGetterCache::new);

        int emitted = 0;

        try (RocksIterator it = db.newIterator(cf)) {
            it.seekToFirst();
            while (it.isValid()) {
                Object bean = t.decoder().decode(it.value());

                if (plan == null || CriteriaSupport.test(plan.criteriaQuery, (Serializable) bean)) {
                    Object[] full = new Object[t.getterNames().length];
                    getters.fill(bean, full);

                    out.add(projectRow(full, proj));
                    emitted++;
                    if (emitted >= limit) break;
                }
                it.next();
            }
        }
    }

    private static Object[] projectRow(Object[] fullRowInTableOrder, Projection proj) {
        if (proj.identity) return fullRowInTableOrder;

        Object[] out = new Object[proj.indices.length];
        for (int i = 0; i < proj.indices.length; i++) {
            out[i] = fullRowInTableOrder[proj.indices[i]];
        }
        return out;
    }

    private Projection resolveProjection(RocksJdbcTable t, List<String> projection) throws SQLException {
        RocksJdbcColumn[] cols = t.columns();

        // If null/empty => "*"
        if (projection == null || projection.isEmpty()) {
            String[] outNames = new String[cols.length];
            int[] outTypes = new int[cols.length];
            for (int i = 0; i < cols.length; i++) {
                outNames[i] = colName(cols[i]);
                outTypes[i] = colType(cols[i]);
            }
            return new Projection(true, range0(cols.length), outNames, outTypes);
        }

        // Build lookup: lower-name -> index in table columns
        Map<String, Integer> byLower = new HashMap<>();
        for (int i = 0; i < cols.length; i++) {
            byLower.put(colName(cols[i]).toLowerCase(Locale.ROOT), i);
        }

        int[] idx = new int[projection.size()];
        String[] outNames = new String[projection.size()];
        int[] outTypes = new int[projection.size()];

        for (int i = 0; i < projection.size(); i++) {
            String raw = projection.get(i);
            if (raw == null || raw.isBlank()) throw new SQLException("Empty projection column");
            String key = stripQuotes(raw).toLowerCase(Locale.ROOT);

            Integer ci = byLower.get(key);
            if (ci == null) {
                throw new SQLException("Unknown column in projection: " + raw + " (table=" + t.tableName() + ")");
            }
            idx[i] = ci;
            outNames[i] = colName(cols[ci]);
            outTypes[i] = colType(cols[ci]);
        }

        boolean identity = idx.length == cols.length;
        if (identity) {
            for (int i = 0; i < idx.length; i++) {
                if (idx[i] != i) { identity = false; break; }
            }
        }

        return new Projection(identity, idx, outNames, outTypes);
    }

    private static String stripQuotes(String s) {
        String t = s.trim();
        if (t.length() >= 2) {
            if ((t.startsWith("\"") && t.endsWith("\"")) || (t.startsWith("`") && t.endsWith("`"))) {
                return t.substring(1, t.length() - 1);
            }
        }
        return t;
    }

    private static int[] range0(int n) {
        int[] r = new int[n];
        for (int i = 0; i < n; i++) r[i] = i;
        return r;
    }

    // --- WHERE -> Criteria compilation + optional DAO instantiation ---

    private CriteriaPlan<?> buildCriteriaPlanIfPossible(RocksJdbcTable t, String whereSql, int limit) throws SQLException {
        if (!(catalog instanceof RocksJdbcCriteriaCatalog cc)) return null;
        RocksJdbcCriteriaBinding<?> b = cc.bindingFor(t.tableName());
        if (b == null) return null;

        @SuppressWarnings("unchecked")
        CriteriaPlan<?> plan = compileWhereUsingTerms((RocksJdbcCriteriaBinding<? extends Serializable>) b, whereSql, limit);
        if (plan == null) return null;

        // Instantiate generated DAO if possible (index-aware)
        if (sessions != null && registry != null && b.daoClass() != null) {
            Object dao = daoCache.computeIfAbsent(t, __ -> {
                try {
                    return b.daoClass().getConstructor(RocksSessions.class, DaoRegistry.class)
                            .newInstance(sessions, registry);
                } catch (Throwable ex) {
                    return null;
                }
            });
            plan.dao = (dao instanceof IndexedRocksDao<?, ?>) ? (IndexedRocksDao<?, ?>) dao : null;
        }

        return plan;
    }

    private CriteriaPlan<?> buildCriteriaPlanFallback(RocksJdbcTable t, String whereSql, int limit) throws SQLException {
        if (catalog instanceof RocksJdbcCriteriaCatalog cc) {
            RocksJdbcCriteriaBinding<?> b = cc.bindingFor(t.tableName());
            if (b != null) {
                @SuppressWarnings("unchecked")
                CriteriaPlan<?> plan = compileWhereUsingTerms((RocksJdbcCriteriaBinding<? extends Serializable>) b, whereSql, limit);
                if (plan != null) return plan;
            }
        }
        throw new SQLException("WHERE is not supported without generated criteria bindings for table: " + t.tableName());
    }

    private static <B extends Serializable> CriteriaPlan<B> compileWhereUsingTerms(
            RocksJdbcCriteriaBinding<B> binding,
            String whereSql,
            int limit
    ) throws SQLException {
        EntityMeta<B> meta = binding.meta();
        if (meta == null) return null;

        Condition<B> cond = RocksJdbcCriteriaCompiler.compile(whereSql, binding.termsByColumnLower());
        if (cond == null) cond = Conditions.trueCondition();

        Query.Builder<B> qb = Query.from(meta).where(cond);
        if (limit > 0 && limit < Integer.MAX_VALUE) {
            qb.limit(limit);
        }
        return new CriteriaPlan<>(qb.build());
    }

    private static final class CriteriaPlan<B extends Serializable> {
        final Query<B> criteriaQuery;
        IndexedRocksDao<?, ?> dao; // optional
        CriteriaPlan(Query<B> q) { this.criteriaQuery = q; }
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
        String colPattern   = (args != null && args.length >= 4) ? (String) args[3] : null;

        String[] cols = {
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                "COLUMN_SIZE", "DECIMAL_DIGITS", "NULLABLE",
                "ORDINAL_POSITION", "IS_AUTOINCREMENT"
        };
        int[] types = {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                Types.INTEGER, Types.INTEGER, Types.INTEGER,
                Types.INTEGER, Types.VARCHAR
        };

        List<Object[]> rows = new ArrayList<>();

        for (RocksJdbcTable t : catalog.tables()) {
            if (tablePattern != null && !tablePattern.isBlank()) {
                boolean match = t.tableName().equalsIgnoreCase(tablePattern);
                if (!match) {
                    // names() in your codebase is a List<String>
                    match = t.names().stream().anyMatch(n -> n.equalsIgnoreCase(tablePattern));
                }
                if (!match) continue;
            }

            for (RocksJdbcColumn c : t.columns()) {
                String cn = colName(c);
                if (colPattern != null && !colPattern.isBlank()) {
                    if (!cn.equalsIgnoreCase(colPattern)) continue;
                }

                rows.add(new Object[]{
                        null, null, t.tableName(),
                        cn,
                        colType(c),
                        colTypeName(c),
                        colSize(c),
                        colScale(c),
                        colNullable(c),
                        colOrdinal(c),
                        colAutoInc(c)
                });
            }
        }

        return RocksJdbcResultSets.of(cols, types, rows);
    }

    private void ensureOpen() throws SQLException {
        if (closed) throw new SQLException("Connection is closed");
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

        void fill(Object rowObj, Object[] out) throws SQLException {
            try {
                for (int i = 0; i < methods.length; i++) {
                    out[i] = methods[i].invoke(rowObj);
                }
            } catch (Throwable e) {
                throw new SQLException("Failed to read row getters", e);
            }
        }
    }

    private record Projection(boolean identity, int[] indices, String[] outNames, int[] outTypes) {}

    // ---------------------------------------------------------------------
    // Parsed / Column reflection (stabilizes against record component renames)
    // ---------------------------------------------------------------------

    private static String parsedWhereSql(RocksJdbcSql.Parsed p) {
        // your Parsed likely has whereSql()/whereClause()/where()
        return asString(readAny(p, "whereSql", "whereClause", "where", "predicate", "wherePart"));
    }

    @SuppressWarnings("unchecked")
    private static List<String> parsedProjection(RocksJdbcSql.Parsed p) {
        Object v = readAny(p, "projection", "columns", "selectColumns");
        if (v == null) return null;
        if (v instanceof List<?> l) return (List<String>) l;
        return null;
    }

    private static Integer parsedLimit(RocksJdbcSql.Parsed p) {
        Object v = readAny(p, "limit", "limitRows", "rowLimit");
        if (v instanceof Integer i) return i;
        if (v instanceof Number n) return n.intValue();
        return null;
    }

    private static String colName(RocksJdbcColumn c) {
        return or(asString(readAny(c, "columnName", "name", "colName", "column")), "");
    }

    private static int colType(RocksJdbcColumn c) {
        Integer v = asInt(readAny(c, "dataType", "sqlType", "jdbcType", "type"));
        return (v == null) ? Types.VARCHAR : v;
    }

    private static String colTypeName(RocksJdbcColumn c) {
        return asString(readAny(c, "typeName", "sqlTypeName", "jdbcTypeName"));
    }

    private static int colSize(RocksJdbcColumn c) {
        Integer v = asInt(readAny(c, "columnSize", "size", "precision"));
        return (v == null) ? 0 : v;
    }

    private static int colScale(RocksJdbcColumn c) {
        Integer v = asInt(readAny(c, "decimalDigits", "scale"));
        return (v == null) ? 0 : v;
    }

    private static int colNullable(RocksJdbcColumn c) {
        Integer v = asInt(readAny(c, "nullable", "nullability"));
        return (v == null) ? ResultSetMetaData.columnNullableUnknown : v;
    }

    private static int colOrdinal(RocksJdbcColumn c) {
        Integer v = asInt(readAny(c, "ordinalPosition", "ordinal", "position"));
        return (v == null) ? 0 : v;
    }

    private static String colAutoInc(RocksJdbcColumn c) {
        String v = asString(readAny(c, "isAutoincrement", "autoIncrement", "isAutoIncrement"));
        return (v == null) ? "" : v;
    }

    private static Object readAny(Object obj, String... names) {
        if (obj == null || names == null) return null;

        Class<?> cls = obj.getClass();

        // 1) exact public no-arg methods by name
        for (String n : names) {
            try {
                Method m = cls.getMethod(n);
                return m.invoke(obj);
            } catch (Throwable ignored) {}
        }

        // 2) record component accessors (case-insensitive)
        try {
            if (cls.isRecord()) {
                RecordComponent[] rcs = cls.getRecordComponents();
                for (String n : names) {
                    for (RecordComponent rc : rcs) {
                        if (rc.getName().equalsIgnoreCase(n)) {
                            Method acc = rc.getAccessor();
                            return acc.invoke(obj);
                        }
                    }
                }
            }
        } catch (Throwable ignored) {}

        return null;
    }

    private static String asString(Object o) {
        return (o == null) ? null : String.valueOf(o);
    }

    private static Integer asInt(Object o) {
        if (o == null) return null;
        if (o instanceof Integer i) return i;
        if (o instanceof Number n) return n.intValue();
        return null;
    }

    private static String or(String s, String dflt) {
        return (s == null) ? dflt : s;
    }
}
