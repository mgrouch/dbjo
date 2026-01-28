package org.github.dbjo.rdb.jdbc;

import org.github.dbjo.rdb.jdbc.catalog.*;
import org.rocksdb.*;

import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.regex.Pattern;

public final class RocksJdbcConnection implements Connection {
    private static final String SCHEMA = "PUBLIC";
    private static final String DRIVER_NAME = "dbjo-rocks-jdbc";
    private static final String DRIVER_VERSION = "0.2";

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

    public static RocksJdbcConnection open(String url, String dbPath, RocksJdbcCatalog catalog) throws SQLException {
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
        if (cfs == null || cfs.isEmpty()) {
            desc.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfo));
        } else {
            for (byte[] name : cfs) {
                desc.add(new ColumnFamilyDescriptor(name, cfo));
            }
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

    ColumnFamilyHandle cfHandle(String cfName) throws SQLException {
        if (cfName == null) throw new SQLException("cfName is null");
        String k = cfName.trim().toLowerCase(Locale.ROOT);
        if (k.isEmpty()) throw new SQLException("cfName is empty");
        ColumnFamilyHandle h = cfByName.get(k);
        if (h == null) throw new SQLException("Unknown/missing column family: " + cfName);
        return h;
    }

    public ResultSet runQuery(String sql, int stmtMaxRows) throws SQLException {
        ensureOpen();

        RocksJdbcSql.Parsed p = RocksJdbcSql.parse(sql);

        int limit = 0;
        if (stmtMaxRows > 0) limit = stmtMaxRows;
        if (p.limit() != null && p.limit() > 0) limit = (limit == 0) ? p.limit() : Math.min(limit, p.limit());

        return switch (p.kind()) {
            case LIST_TABLES -> queryTables();
            case SELECT_ALL -> querySelectAll(p.tableName(), limit);
            case COUNT -> queryCount(p.tableName());
        };
    }

    private ResultSet queryTables() throws SQLException {
        String[] cols = { "table_name", "cf_name", "column_count" };
        int[] types = { Types.VARCHAR, Types.VARCHAR, Types.INTEGER };

        List<Object[]> rows = new ArrayList<>();
        for (RocksJdbcTable t : catalog.tables()) {
            rows.add(new Object[]{ t.tableName(), t.cfName(), t.columns().length });
        }
        return RocksJdbcResultSets.of(cols, types, rows);
    }

    private ResultSet queryCount(String tableName) throws SQLException {
        RocksJdbcTable t = catalog.requireTable(tableName);
        ColumnFamilyHandle cf = cfHandle(t.cfName());

        long count = 0;
        RocksIterator it = null;
        try {
            it = db.newIterator(cf);
            it.seekToFirst();
            while (it.isValid()) {
                count++;
                it.next();
            }
        } finally {
            if (it != null) it.close();
        }

        String[] cols = { "count" };
        int[] types = { Types.BIGINT };
        return RocksJdbcResultSets.of(cols, types, java.util.Collections.singletonList(new Object[]{ count }));
    }

    private ResultSet querySelectAll(String tableName, int limit) throws SQLException {
        RocksJdbcTable t = catalog.requireTable(tableName);
        ColumnFamilyHandle cf = cfHandle(t.cfName());

        String[] colNames = t.columnNames();
        int[] colTypes = t.columnSqlTypes();

        MethodGetterCache getters = getterCache.computeIfAbsent(t, MethodGetterCache::new);

        List<Object[]> rows = new ArrayList<>();
        int emitted = 0;

        RocksIterator it = null;
        try {
            it = db.newIterator(cf);
            it.seekToFirst();

            while (it.isValid()) {
                byte[] value = it.value();
                Object rowObj = t.decoder().decode(value);

                Object[] out = new Object[colNames.length];
                getters.fill(rowObj, out);
                rows.add(out);

                emitted++;
                if (limit > 0 && emitted >= limit) break;

                it.next();
            }
        } finally {
            if (it != null) it.close();
        }

        return RocksJdbcResultSets.of(colNames, colTypes, rows);
    }

    private void ensureOpen() throws SQLException {
        if (closed) throw new SQLException("Connection is closed");
    }

    // --- DatabaseMetaData proxy handler ---
    private Object handleMetaCall(String name, Object[] args) throws SQLException {
        return switch (name) {
            // identity
            case "getConnection" -> this;
            case "getURL" -> url;
            case "getUserName" -> "";
            case "getDatabaseProductName" -> "RocksDB";
            case "getDatabaseProductVersion" -> "read-only";
            case "getDriverName" -> DRIVER_NAME;
            case "getDriverVersion" -> DRIVER_VERSION;
            case "getDriverMajorVersion" -> 0;
            case "getDriverMinorVersion" -> 2;
            case "getJDBCMajorVersion" -> 4;
            case "getJDBCMinorVersion" -> 2;

            // quoting / naming
            case "getIdentifierQuoteString" -> "\"";
            case "getSearchStringEscape" -> "\\";
            case "getCatalogTerm" -> "catalog";
            case "getSchemaTerm" -> "schema";
            case "getCatalogSeparator" -> ".";
            case "supportsSchemasInTableDefinitions" -> true;
            case "supportsCatalogsInTableDefinitions" -> false;

            case "storesLowerCaseIdentifiers" -> true;
            case "storesUpperCaseIdentifiers" -> false;
            case "storesMixedCaseIdentifiers" -> false;

            case "isReadOnly" -> true;

            // result set capabilities
            case "supportsResultSetType" -> {
                int t = (args != null && args.length > 0 && args[0] instanceof Integer) ? (Integer) args[0] : ResultSet.TYPE_FORWARD_ONLY;
                yield t == ResultSet.TYPE_FORWARD_ONLY;
            }
            case "supportsResultSetConcurrency" -> {
                int type = (args != null && args.length > 0 && args[0] instanceof Integer) ? (Integer) args[0] : ResultSet.TYPE_FORWARD_ONLY;
                int conc = (args != null && args.length > 1 && args[1] instanceof Integer) ? (Integer) args[1] : ResultSet.CONCUR_READ_ONLY;
                yield type == ResultSet.TYPE_FORWARD_ONLY && conc == ResultSet.CONCUR_READ_ONLY;
            }

            // transactions
            case "supportsTransactions" -> false;
            case "getDefaultTransactionIsolation" -> Connection.TRANSACTION_NONE;

            // schema exploration
            case "getCatalogs" -> metaGetCatalogs();
            case "getSchemas" -> metaGetSchemas();
            case "getTableTypes" -> metaGetTableTypes();
            case "getTables" -> metaGetTables(args);
            case "getColumns" -> metaGetColumns(args);
            case "getPrimaryKeys" -> metaGetPrimaryKeys(args);
            case "getIndexInfo" -> metaGetIndexInfo(args);
            case "getTypeInfo" -> metaGetTypeInfo();

            // harmless probes
            case "allProceduresAreCallable" -> false;
            case "allTablesAreSelectable" -> true;
            case "supportsBatchUpdates" -> false;

            // defaults
            default -> {
                Class<?> rt = findReturnType(DatabaseMetaData.class, name, args);
                if (rt == boolean.class) yield false;
                if (rt == int.class) yield 0;
                if (rt == String.class) yield "";
                if (rt == ResultSet.class) yield emptyResultSet();
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

    private ResultSet emptyResultSet() throws SQLException {
        return RocksJdbcResultSets.of(new String[]{"_"}, new int[]{Types.VARCHAR}, List.of());
    }

    private ResultSet metaGetCatalogs() throws SQLException {
        // spec: single column TABLE_CAT
        String[] cols = { "TABLE_CAT" };
        int[] types = { Types.VARCHAR };
        return RocksJdbcResultSets.of(cols, types, List.of());
    }

    private ResultSet metaGetSchemas() throws SQLException {
        // spec: TABLE_SCHEM, TABLE_CATALOG
        String[] cols = { "TABLE_SCHEM", "TABLE_CATALOG" };
        int[] types = { Types.VARCHAR, Types.VARCHAR };

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{ SCHEMA, null });

        return RocksJdbcResultSets.of(cols, types, rows);
    }

    private ResultSet metaGetTableTypes() throws SQLException {
        String[] cols = { "TABLE_TYPE" };
        int[] types = { Types.VARCHAR };
        return RocksJdbcResultSets.of(cols, types, java.util.Collections.singletonList(new Object[]{}));
    }

    private ResultSet metaGetTables(Object[] args) throws SQLException {
        // getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
        String schemaPattern = (args != null && args.length >= 2) ? (String) args[1] : null;
        String tablePattern  = (args != null && args.length >= 3) ? (String) args[2] : null;
        String[] types       = (args != null && args.length >= 4) ? (String[]) args[3] : null;

        if (!schemaMatches(schemaPattern)) {
            return RocksJdbcResultSets.of(
                    new String[]{"TABLE_CAT","TABLE_SCHEM","TABLE_NAME","TABLE_TYPE","REMARKS","TYPE_CAT","TYPE_SCHEM","TYPE_NAME","SELF_REFERENCING_COL_NAME","REF_GENERATION"},
                    new int[]{Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR},
                    List.of()
            );
        }

        if (types != null && types.length > 0) {
            boolean ok = false;
            for (String t : types) if ("TABLE".equalsIgnoreCase(t)) ok = true;
            if (!ok) {
                return RocksJdbcResultSets.of(
                        new String[]{"TABLE_CAT","TABLE_SCHEM","TABLE_NAME","TABLE_TYPE","REMARKS","TYPE_CAT","TYPE_SCHEM","TYPE_NAME","SELF_REFERENCING_COL_NAME","REF_GENERATION"},
                        new int[]{Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR},
                        List.of()
                );
            }
        }

        String[] cols = {
                "TABLE_CAT","TABLE_SCHEM","TABLE_NAME","TABLE_TYPE","REMARKS",
                "TYPE_CAT","TYPE_SCHEM","TYPE_NAME","SELF_REFERENCING_COL_NAME","REF_GENERATION"
        };
        int[] typesOut = {
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR
        };

        List<Object[]> rows = new ArrayList<>();
        for (RocksJdbcTable t : catalog.tables()) {
            if (!likeMatchesAny(t, tablePattern)) continue;

            rows.add(new Object[]{
                    null, SCHEMA, t.tableName(), "TABLE", null,
                    null, null, null, null, null
            });
        }
        return RocksJdbcResultSets.of(cols, typesOut, rows);
    }

    private ResultSet metaGetColumns(Object[] args) throws SQLException {
        // getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
        String schemaPattern = (args != null && args.length >= 2) ? (String) args[1] : null;
        String tablePattern  = (args != null && args.length >= 3) ? (String) args[2] : null;
        String colPattern    = (args != null && args.length >= 4) ? (String) args[3] : null;

        String[] cols = {
                "TABLE_CAT","TABLE_SCHEM","TABLE_NAME",
                "COLUMN_NAME","DATA_TYPE","TYPE_NAME",
                "COLUMN_SIZE","BUFFER_LENGTH","DECIMAL_DIGITS",
                "NUM_PREC_RADIX","NULLABLE","REMARKS",
                "COLUMN_DEF","SQL_DATA_TYPE","SQL_DATETIME_SUB",
                "CHAR_OCTET_LENGTH","ORDINAL_POSITION","IS_NULLABLE",
                "SCOPE_CATALOG","SCOPE_SCHEMA","SCOPE_TABLE",
                "SOURCE_DATA_TYPE","IS_AUTOINCREMENT","IS_GENERATEDCOLUMN"
        };
        int[] typesOut = {
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,
                Types.VARCHAR,Types.INTEGER,Types.VARCHAR,
                Types.INTEGER,Types.INTEGER,Types.INTEGER,
                Types.INTEGER,Types.INTEGER,Types.VARCHAR,
                Types.VARCHAR,Types.INTEGER,Types.INTEGER,
                Types.INTEGER,Types.INTEGER,Types.VARCHAR,
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,
                Types.SMALLINT,Types.VARCHAR,Types.VARCHAR
        };

        if (!schemaMatches(schemaPattern)) {
            return RocksJdbcResultSets.of(cols, typesOut, List.of());
        }

        List<Object[]> rows = new ArrayList<>();

        for (RocksJdbcTable t : catalog.tables()) {
            if (!likeMatchesAny(t, tablePattern)) continue;

            for (RocksJdbcColumn c : t.columns()) {
                if (colPattern != null && !like(c.name(), colPattern)) continue;

                String isNullable = (c.nullable() == DatabaseMetaData.columnNoNulls) ? "NO" : "YES";

                rows.add(new Object[]{
                        null, SCHEMA, t.tableName(),
                        c.name(), c.sqlType(), c.typeName(),
                        c.size(), null, c.scale(),
                        10, c.nullable(), null,
                        c.defaultValue(), null, null,
                        null, c.pos(), isNullable,
                        null, null, null,
                        null, c.isAutoIncrement(), "NO"
                });
            }
        }

        return RocksJdbcResultSets.of(cols, typesOut, rows);
    }

    private ResultSet metaGetPrimaryKeys(Object[] args) throws SQLException {
        // getPrimaryKeys(String catalog, String schema, String table)
        String schema = (args != null && args.length >= 2) ? (String) args[1] : null;
        String table  = (args != null && args.length >= 3) ? (String) args[2] : null;

        String[] cols = { "TABLE_CAT","TABLE_SCHEM","TABLE_NAME","COLUMN_NAME","KEY_SEQ","PK_NAME" };
        int[] typesOut = { Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.SMALLINT,Types.VARCHAR };

        if (!schemaMatches(schema)) {
            return RocksJdbcResultSets.of(cols, typesOut, List.of());
        }

        RocksJdbcTable t = (table == null) ? null : catalog.table(table);
        if (t == null) return RocksJdbcResultSets.of(cols, typesOut, List.of());

        List<Object[]> rows = new ArrayList<>();
        String pkName = "PK_" + t.tableName();
        String[] pkCols = t.pkColumns();

        for (int i = 0; i < pkCols.length; i++) {
            rows.add(new Object[]{ null, SCHEMA, t.tableName(), pkCols[i], (short)(i + 1), pkName });
        }

        return RocksJdbcResultSets.of(cols, typesOut, rows);
    }

    private ResultSet metaGetIndexInfo(Object[] args) throws SQLException {
        // getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
        String schema = (args != null && args.length >= 2) ? (String) args[1] : null;
        String table  = (args != null && args.length >= 3) ? (String) args[2] : null;
        boolean uniqueOnly = (args != null && args.length >= 4 && args[3] instanceof Boolean) ? (Boolean) args[3] : false;

        String[] cols = {
                "TABLE_CAT","TABLE_SCHEM","TABLE_NAME",
                "NON_UNIQUE","INDEX_QUALIFIER","INDEX_NAME",
                "TYPE","ORDINAL_POSITION","COLUMN_NAME",
                "ASC_OR_DESC","CARDINALITY","PAGES","FILTER_CONDITION"
        };
        int[] typesOut = {
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,
                Types.BOOLEAN,Types.VARCHAR,Types.VARCHAR,
                Types.SMALLINT,Types.SMALLINT,Types.VARCHAR,
                Types.VARCHAR,Types.BIGINT,Types.BIGINT,Types.VARCHAR
        };

        if (!schemaMatches(schema)) {
            return RocksJdbcResultSets.of(cols, typesOut, List.of());
        }

        RocksJdbcTable t = (table == null) ? null : catalog.table(table);
        if (t == null) return RocksJdbcResultSets.of(cols, typesOut, List.of());

        List<Object[]> rows = new ArrayList<>();
        for (RocksJdbcIndex ix : t.indexes()) {
            if (uniqueOnly && !ix.unique()) continue;

            boolean nonUnique = !ix.unique();
            String[] cn = ix.columnNames();
            for (int i = 0; i < cn.length; i++) {
                rows.add(new Object[]{
                        null, SCHEMA, t.tableName(),
                        nonUnique, null, ix.indexName(),
                        DatabaseMetaData.tableIndexOther,
                        (short)(i + 1),
                        cn[i],
                        null, null, null, null
                });
            }
        }

        return RocksJdbcResultSets.of(cols, typesOut, rows);
    }

    private ResultSet metaGetTypeInfo() throws SQLException {
        // A small, practical set for IDEs
        String[] cols = {
                "TYPE_NAME","DATA_TYPE","PRECISION","LITERAL_PREFIX","LITERAL_SUFFIX",
                "CREATE_PARAMS","NULLABLE","CASE_SENSITIVE","SEARCHABLE","UNSIGNED_ATTRIBUTE",
                "FIXED_PREC_SCALE","AUTO_INCREMENT","LOCAL_TYPE_NAME","MINIMUM_SCALE","MAXIMUM_SCALE",
                "SQL_DATA_TYPE","SQL_DATETIME_SUB","NUM_PREC_RADIX"
        };
        int[] typesOut = {
                Types.VARCHAR,Types.INTEGER,Types.INTEGER,Types.VARCHAR,Types.VARCHAR,
                Types.VARCHAR,Types.SMALLINT,Types.BOOLEAN,Types.SMALLINT,Types.BOOLEAN,
                Types.BOOLEAN,Types.BOOLEAN,Types.VARCHAR,Types.SMALLINT,Types.SMALLINT,
                Types.INTEGER,Types.INTEGER,Types.INTEGER
        };

        List<Object[]> rows = new ArrayList<>();
        addType(rows, "VARCHAR", Types.VARCHAR, 0, true);
        addType(rows, "INTEGER", Types.INTEGER, 0, false);
        addType(rows, "BIGINT", Types.BIGINT, 0, false);
        addType(rows, "DOUBLE", Types.DOUBLE, 0, false);
        addType(rows, "FLOAT", Types.FLOAT, 0, false);
        addType(rows, "BOOLEAN", Types.BOOLEAN, 0, false);
        addType(rows, "TIMESTAMP", Types.TIMESTAMP, 0, false);
        addType(rows, "VARBINARY", Types.VARBINARY, 0, false);

        return RocksJdbcResultSets.of(cols, typesOut, rows);
    }

    private static void addType(List<Object[]> rows, String name, int jdbcType, int prec, boolean caseSensitive) {
        rows.add(new Object[]{
                name, jdbcType, prec, null, null,
                null, DatabaseMetaData.typeNullable, caseSensitive, DatabaseMetaData.typeSearchable, false,
                false, false, null, (short)0, (short)0,
                null, null, 10
        });
    }

    private static boolean schemaMatches(String schemaPatternOrSchema) {
        if (schemaPatternOrSchema == null || schemaPatternOrSchema.isBlank()) return true;
        return like(SCHEMA, schemaPatternOrSchema);
    }

    private static boolean likeMatchesAny(RocksJdbcTable t, String pattern) {
        if (pattern == null || pattern.isBlank()) return true;

        // match main name
        if (like(t.tableName(), pattern)) return true;

        // match aliases
        for (String n : t.names()) {
            if (n != null && like(n, pattern)) return true;
        }
        return false;
    }

    // JDBC LIKE pattern: % and _, escape with backslash
    private static boolean like(String value, String pattern) {
        if (pattern == null) return true;
        if (value == null) return false;

        String p = pattern.trim();
        if (p.isEmpty() || "%".equals(p)) return true;

        Pattern re = likeToRegex(p, '\\');
        return re.matcher(value).matches();
    }

    private static Pattern likeToRegex(String like, char escape) {
        StringBuilder sb = new StringBuilder(like.length() * 2);
        sb.append("^");

        boolean esc = false;
        for (int i = 0; i < like.length(); i++) {
            char c = like.charAt(i);
            if (esc) {
                sb.append(Pattern.quote(String.valueOf(c)));
                esc = false;
                continue;
            }
            if (c == escape) {
                esc = true;
                continue;
            }
            if (c == '%') sb.append(".*");
            else if (c == '_') sb.append(".");
            else sb.append(Pattern.quote(String.valueOf(c)));
        }

        sb.append("$");
        return Pattern.compile(sb.toString(), Pattern.CASE_INSENSITIVE);
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

    @Override public boolean isClosed() { return closed; }
    @Override public boolean isReadOnly() { return true; }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (!readOnly) throw new SQLException("RocksJdbcConnection is read-only");
    }

    @Override public boolean getAutoCommit() { return true; }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (!autoCommit) throw new SQLException("Only autoCommit=true supported");
    }

    @Override public void commit() throws SQLException { throw new SQLFeatureNotSupportedException("Transactions not supported"); }
    @Override public void rollback() throws SQLException { throw new SQLFeatureNotSupportedException("Transactions not supported"); }
    @Override public String nativeSQL(String sql) { return sql; }

    // IntelliJ sometimes calls schema APIs; keep them harmless:
    @Override public String getSchema() { return SCHEMA; }
    @Override public void setSchema(String schema) { /* ignore */ }

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
    @Override public void abort(Executor executor) throws SQLException { close(); }
    @Override public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException { throw unsup(); }
    @Override public int getNetworkTimeout() { return 0; }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("unwrap not supported"); }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }

    // --- cache getters per table ---
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
}
