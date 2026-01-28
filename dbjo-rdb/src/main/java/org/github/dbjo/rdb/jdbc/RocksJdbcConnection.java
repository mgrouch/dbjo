package org.github.dbjo.rdb.jdbc;

import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcExecutor;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcStatement;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.ShardingKey;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;

public final class RocksJdbcConnection implements Connection {

    private final String url;
    private final String user;
    private final Properties info;
    private final RocksJdbcCatalog rocksCatalog;

    private boolean closed = false;
    private boolean autoCommit = true;
    private boolean readOnly = false;

    private String catalog;
    private String schema;

    private int transactionIsolation = Connection.TRANSACTION_READ_COMMITTED;
    private int networkTimeoutMs = 0;

    /** Factory expected by RocksJdbcDriver. */
    public static RocksJdbcConnection open(String url, String user, RocksJdbcCatalog catalog) throws SQLException {
        return new RocksJdbcConnection(url, user, new Properties(), catalog);
    }

    public RocksJdbcConnection(String url, String user, Properties info, RocksJdbcCatalog rocksCatalog) {
        this.url = Objects.requireNonNull(url, "url");
        this.user = (user == null) ? "" : user;
        this.info = (info == null) ? new Properties() : info;
        this.rocksCatalog = Objects.requireNonNull(rocksCatalog, "rocksCatalog");
    }

    public String url() { return url; }
    public String user() { return user; }
    public Properties info() { return info; }
    public RocksJdbcCatalog rocksCatalog() { return rocksCatalog; }

    private void checkOpen() throws SQLException {
        if (closed) throw new SQLException("Connection is closed");
    }

    private static SQLFeatureNotSupportedException notSupported() {
        return new SQLFeatureNotSupportedException("Not supported");
    }

    /**
     * Method expected by RocksJdbcStatement.
     * This is now the single runtime entry point that wires:
     * SQL parse -> WHERE parse/compile -> plan -> execute (index/full scan) -> projection/limit.
     */
    public ResultSet runQuery(String sql, int maxRows) throws SQLException {
        checkOpen();
        return RocksJdbcExecutor.execute(this, sql, maxRows);
    }

    // Connection

    @Override
    public Statement createStatement() throws SQLException {
        checkOpen();
        return new RocksJdbcStatement(this);
    }

    @Override public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException { return createStatement(); }
    @Override public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { return createStatement(); }

    @Override public PreparedStatement prepareStatement(String sql) throws SQLException { throw notSupported(); }
    @Override public CallableStatement prepareCall(String sql) throws SQLException { throw notSupported(); }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkOpen();
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkOpen();
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkOpen();
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkOpen();
        if (autoCommit) throw new SQLException("commit() not allowed when autoCommit=true");
        // no-op for now
    }

    @Override
    public void rollback() throws SQLException {
        checkOpen();
        if (autoCommit) throw new SQLException("rollback() not allowed when autoCommit=true");
        // no-op for now
    }

    @Override public void close() throws SQLException { closed = true; }
    @Override public boolean isClosed() throws SQLException { return closed; }

    @Override public DatabaseMetaData getMetaData() throws SQLException { throw notSupported(); }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkOpen();
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkOpen();
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        checkOpen();
        return catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkOpen();
        this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkOpen();
        return transactionIsolation;
    }

    @Override public SQLWarning getWarnings() throws SQLException { checkOpen(); return null; }
    @Override public void clearWarnings() throws SQLException { checkOpen(); }

    @Override public Map<String, Class<?>> getTypeMap() throws SQLException { throw notSupported(); }
    @Override public void setTypeMap(Map<String, Class<?>> map) throws SQLException { throw notSupported(); }

    @Override public void setHoldability(int holdability) throws SQLException { throw notSupported(); }
    @Override public int getHoldability() throws SQLException { throw notSupported(); }

    @Override public Savepoint setSavepoint() throws SQLException { throw notSupported(); }
    @Override public Savepoint setSavepoint(String name) throws SQLException { throw notSupported(); }
    @Override public void rollback(Savepoint savepoint) throws SQLException { throw notSupported(); }
    @Override public void releaseSavepoint(Savepoint savepoint) throws SQLException { throw notSupported(); }

    @Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException { throw notSupported(); }
    @Override public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException { throw notSupported(); }
    @Override public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException { throw notSupported(); }
    @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException { throw notSupported(); }
    @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { throw notSupported(); }

    @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException { throw notSupported(); }
    @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { throw notSupported(); }

    @Override public Clob createClob() throws SQLException { throw notSupported(); }
    @Override public Blob createBlob() throws SQLException { throw notSupported(); }
    @Override public NClob createNClob() throws SQLException { throw notSupported(); }
    @Override public SQLXML createSQLXML() throws SQLException { throw notSupported(); }

    @Override public boolean isValid(int timeout) throws SQLException { return !closed; }

    @Override public void setClientInfo(String name, String value) throws SQLClientInfoException { }
    @Override public void setClientInfo(Properties properties) throws SQLClientInfoException { }
    @Override public String getClientInfo(String name) throws SQLException { return null; }
    @Override public Properties getClientInfo() throws SQLException { return new Properties(); }

    @Override public Array createArrayOf(String typeName, Object[] elements) throws SQLException { throw notSupported(); }
    @Override public Struct createStruct(String typeName, Object[] attributes) throws SQLException { throw notSupported(); }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkOpen();
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        checkOpen();
        return schema;
    }

    @Override public void abort(Executor executor) throws SQLException { close(); }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkOpen();
        this.networkTimeoutMs = milliseconds;
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkOpen();
        return networkTimeoutMs;
    }

    // Java 9+ request boundaries
    @Override public void beginRequest() throws SQLException { checkOpen(); }
    @Override public void endRequest() throws SQLException { checkOpen(); }

    // Java 9+ sharding (unsupported)
    @Override public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey, int timeout) throws SQLException { throw notSupported(); }
    @Override public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException { throw notSupported(); }
    @Override public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException { throw notSupported(); }
    @Override public void setShardingKey(ShardingKey shardingKey) throws SQLException { throw notSupported(); }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("Not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
