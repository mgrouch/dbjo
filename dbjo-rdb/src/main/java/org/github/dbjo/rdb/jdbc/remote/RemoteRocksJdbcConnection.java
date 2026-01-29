package org.github.dbjo.rdb.jdbc.remote;

import org.github.dbjo.rdb.jdbc.RocksJdbcDatabaseMetaData;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;

import javax.sql.rowset.CachedRowSet;
import java.sql.*;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;

public final class RemoteRocksJdbcConnection implements Connection {
    private final String url;
    private final Properties info;
    private final RemoteRocksJdbcClient client;
    private final RocksJdbcCatalog catalog;
    private boolean closed;

    RemoteRocksJdbcConnection(String url, Properties info, RemoteRocksJdbcClient client, RocksJdbcCatalog catalog) {
        this.url = Objects.requireNonNull(url, "url");
        this.info = (info == null) ? new Properties() : info;
        this.client = Objects.requireNonNull(client, "client");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    CachedRowSet runQuery(String sql, int maxRows) throws SQLException {
        checkOpen();
        return client.query(sql, maxRows);
    }

    RocksJdbcCatalog catalog() {
        return catalog;
    }

    private void checkOpen() throws SQLException {
        if (closed) throw new SQLException("Connection is closed");
    }

    @Override public Statement createStatement() throws SQLException { checkOpen(); return new RemoteRocksJdbcStatement(this); }
    @Override public DatabaseMetaData getMetaData() throws SQLException { checkOpen(); return new RocksJdbcDatabaseMetaData(this, catalog); }
    @Override public void close() { closed = true; }
    @Override public boolean isClosed() { return closed; }

    @Override public void setAutoCommit(boolean autoCommit) {}
    @Override public boolean getAutoCommit() { return true; }
    @Override public void commit() {}
    @Override public void rollback() {}
    @Override public PreparedStatement prepareStatement(String sql) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public CallableStatement prepareCall(String sql) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public String nativeSQL(String sql) { return sql; }
    @Override public void setReadOnly(boolean readOnly) {}
    @Override public boolean isReadOnly() { return true; }
    @Override public void setCatalog(String catalog) {}
    @Override public String getCatalog() { return null; }
    @Override public void setTransactionIsolation(int level) {}
    @Override public int getTransactionIsolation() { return Connection.TRANSACTION_NONE; }
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}

    @Override public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException { return createStatement(); }
    @Override public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { return createStatement(); }
    @Override public Map<String, Class<?>> getTypeMap() { return Map.of(); }
    @Override public void setTypeMap(Map<String, Class<?>> map) {}
    @Override public void setHoldability(int holdability) {}
    @Override public int getHoldability() { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override public Savepoint setSavepoint() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Savepoint setSavepoint(String name) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void rollback(Savepoint savepoint) {}
    @Override public void releaseSavepoint(Savepoint savepoint) {}
    @Override public Clob createClob() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Blob createBlob() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public NClob createNClob() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public SQLXML createSQLXML() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public boolean isValid(int timeout) { return !closed; }
    @Override public void setClientInfo(String name, String value) {}
    @Override public void setClientInfo(Properties properties) {}
    @Override public String getClientInfo(String name) { return null; }
    @Override public Properties getClientInfo() { return info; }
    @Override public Array createArrayOf(String typeName, Object[] elements) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Struct createStruct(String typeName, Object[] attributes) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setSchema(String schema) {}
    @Override public String getSchema() { return "PUBLIC"; }
    @Override public void abort(Executor executor) { close(); }
    @Override public void setNetworkTimeout(Executor executor, int milliseconds) {}
    @Override public int getNetworkTimeout() { return 0; }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("unwrap");
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }

    @Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { throw new SQLFeatureNotSupportedException(); }
}
