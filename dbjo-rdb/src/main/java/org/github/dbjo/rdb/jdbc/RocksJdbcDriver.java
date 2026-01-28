package org.github.dbjo.rdb.jdbc;

import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;
import org.rocksdb.RocksDB;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public final class RocksJdbcDriver implements Driver {
    static {
        try { RocksDB.loadLibrary(); } catch (Throwable ignored) {}
        try { DriverManager.registerDriver(new RocksJdbcDriver()); } catch (Throwable ignored) {}
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;

        RocksJdbcUtil.ParsedUrl p = RocksJdbcUtil.parseUrl(url, info);
        RocksJdbcCatalog catalog = RocksJdbcUtil.loadCatalog(p.catalogClassName());

        return RocksJdbcConnection.open(url, p.dbPath(), catalog);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(RocksJdbcUtil.URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        DriverPropertyInfo cat = new DriverPropertyInfo("catalog", (info == null) ? null : info.getProperty("catalog"));
        cat.required = true;
        cat.description = "FQN of generated RocksJdbcCatalog (e.g. ...GeneratedRocksJdbcCatalog).";
        return new DriverPropertyInfo[]{ cat };
    }

    @Override public int getMajorVersion() { return 0; }
    @Override public int getMinorVersion() { return 1; }
    @Override public boolean jdbcCompliant() { return false; }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
